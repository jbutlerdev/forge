/**
 * H2.4: the compaction threshold, moved from forge-api into the harness.
 *
 * The legacy heuristic (forge-api `api/mod.rs`, long-context resume
 * prelude) estimated tokens as `total message chars / 4` and sent pi a
 * `compact` RPC above ~300k estimated tokens. This module keeps the same
 * shape, per conversation:
 *
 * - The threshold lives in a per-conversation **config document** — the
 *   generic `forge.document` family under the name `config`:
 *   `{ "compaction": { "maxContextChars": 300000, "divisor": 4 } }`
 *   (both fields optional; the defaults reproduce today's heuristic).
 *   It is edited through the ordinary document surface
 *   (`documentPut` / `PUT /sessions/:id/documents/config`).
 * - After every committed `pi.assistant` entry (a turn boundary, or the
 *   end of an answer segment) `maybeEnqueueCompaction` measures the
 *   conversation's model context (chars of `ContextView.messages`, the
 *   same source the summarizer reads). Above the threshold it enqueues
 *   the built-in pi-durable `CompactionTask` as a **conversation-owned
 *   background task** (`reason: "threshold"`) — exactly the shape
 *   pi-durable's own `createCompaction` uses for non-manual compaction.
 *   The task runs without interrupting an in-flight turn; its summary is
 *   placed at once when idle, otherwise at the next turn boundary
 *   (pi-durable admission semantics — nothing here blocks a turn).
 * - A compaction already in flight (live status in `pi.live`) means the
 *   check bails: overlapping summaries only race over who cuts further
 *   back (the nearer cut settles `stale`), and the second one just
 *   costs summarization spend.
 *
 * Nothing is deleted by compaction — the older entries stay in
 * `durable_entries`, which is what `GET /sessions/:id/history?q=`
 * (forge-api) searches.
 */
import type { Context } from "@earendil-works/chord";
import type {
	AgentState,
	CompactionHooks,
	CompactionInput,
	ContextView,
	Harness,
	HookApi,
	ModelRef,
} from "@earendil-works/pi-durable";
import { AgentDoc, CompactionTask, LiveDoc, type ConversationId } from "@earendil-works/pi-durable";
import type { AssistantMessage, Message, Models, TextContent, ThinkingContent, ToolCall } from "@earendil-works/pi-ai";
import { ForgeDocument, ForgeMeta, META_KEY, CONFIG_KEY } from "./docs.js";

/** Defaults: today's forge-api heuristic (chars / 4, compact above ~300k). */
export const DEFAULT_MAX_CONTEXT_CHARS = 300_000;
export const DEFAULT_DIVISOR = 4;

/** One per-conversation compaction threshold. */
export interface CompactionThreshold {
	readonly maxContextChars: number;
	readonly divisor: number;
}

const DEFAULT_THRESHOLD: CompactionThreshold = {
	maxContextChars: DEFAULT_MAX_CONTEXT_CHARS,
	divisor: DEFAULT_DIVISOR,
};

function asFinitePositive(value: unknown): number | undefined {
	return typeof value === "number" && Number.isFinite(value) && value > 0 ? value : undefined;
}

/** Read the conversation's compaction threshold from its config document
 * (absent document or fields ⇒ the defaults). */
export async function readCompactionThreshold(
	harness: Harness,
	conversationId: number,
	context: Context,
): Promise<CompactionThreshold> {
	const value = (await harness.snapshot(ForgeDocument, conversationId as ConversationId, CONFIG_KEY, context))?.value;
	if (value === null || typeof value !== "object" || Array.isArray(value)) return DEFAULT_THRESHOLD;
	const compaction = (value as { compaction?: unknown }).compaction;
	if (compaction === null || typeof compaction !== "object" || Array.isArray(compaction)) return DEFAULT_THRESHOLD;
	const c = compaction as { maxContextChars?: unknown; divisor?: unknown };
	return {
		maxContextChars: asFinitePositive(c.maxContextChars) ?? DEFAULT_MAX_CONTEXT_CHARS,
		divisor: asFinitePositive(c.divisor) ?? DEFAULT_DIVISOR,
	};
}

/** Char count of one model message: text, thinking, and tool-call
 * arguments — the same content the summarizer would serialize. */
function messageChars(message: Message): number {
	let chars = 0;
	const content: unknown = "content" in message ? message.content : undefined;
	const textOf = (value: unknown): number => {
		if (typeof value === "string") return value.length;
		if (Array.isArray(value)) {
			let n = 0;
			for (const block of value) {
				if (block !== null && typeof block === "object" && (block as { text?: unknown }).text !== undefined) {
					n += String((block as { text: unknown }).text).length;
				}
			}
			return n;
		}
		return 0;
	};
	if (typeof content === "string") {
		chars += content.length;
	} else if (Array.isArray(content)) {
		for (const block of content as Array<TextContent | ThinkingContent | ToolCall | { image?: unknown }>) {
			if (block === null || typeof block !== "object") continue;
			const typed = block as { type?: string; text?: string; thinking?: string; arguments?: unknown };
			if (typed.type === "text" && typeof typed.text === "string") chars += typed.text.length;
			else if (typed.type === "thinking" && typeof typed.thinking === "string") chars += typed.thinking.length;
			else if (typed.type === "toolCall") chars += JSON.stringify(typed.arguments ?? {}).length;
		}
	}
	return chars;
}

/** Total char count of the conversation's current model context — the
 * legacy `SUM(LENGTH(content))` analog over the active segment. */
export function contextChars(view: ContextView): number {
	return view.messages.reduce((chars, message) => chars + messageChars(message), 0);
}

/** Char count of the text blocks of a `pi.compaction` entry's summary
 * message (the entry's `model` is a single user message holding the
 * prefixed summary). */
export function compactionSummaryChars(model: readonly Message[] | undefined): number {
	let chars = 0;
	for (const message of model ?? []) {
		chars += messageChars(message);
	}
	return chars;
}

/**
 * Enqueue the background compaction when the context exceeds the
 * conversation's configured threshold. No-op otherwise (and when a
 * compaction is already in flight). Returns the new task id, or `null`.
 */
export async function maybeEnqueueCompaction(args: {
	readonly harness: Harness;
	readonly conversationId: number;
	readonly context: Context;
	/** Skip the chars/4 estimate gate — used when the provider itself
	 *  rejected a request for being too long (the estimate is a
	 *  heuristic; the 400 is ground truth, see the generation-failure
	 *  guard in main.ts). The in-flight dedup still applies. */
	readonly force?: boolean;
	readonly log?: (record: { level: "info"; msg: string; conversationId: number; taskId: number; estimatedTokens: number }) => void;
}): Promise<number | null> {
	const { harness, conversationId, context, log } = args;

	// A compaction already in flight covers the threshold; overlapping
	// ones only race over the cut point.
	const inFlight = (await harness.snapshot(LiveDoc, conversationId as ConversationId, context))?.compactions;
	if (inFlight !== undefined && inFlight.length > 0) return null;

	const conversation = await harness.conversation(conversationId as ConversationId, context);
	if (conversation === undefined) return null;
	const view = await conversation.context(context);
	const threshold = await readCompactionThreshold(harness, conversationId, context);
	const estimatedTokens = Math.floor(contextChars(view) / threshold.divisor);
	if (!args.force && estimatedTokens <= threshold.maxContextChars) return null;

	const taskId = await harness.commit(
		(tx) =>
			tx.createTask(
				CompactionTask,
				{ reason: "threshold" } satisfies CompactionInput,
				{ ownership: { kind: "conversation" }, conversationId: conversationId as ConversationId, background: true },
			),
		context,
	);
	log?.({
		level: "info",
		msg: "threshold compaction enqueued",
		conversationId,
		taskId: taskId as number,
		estimatedTokens,
	});
	return taskId as number;
}

/* ------------------------------------------------------------------ */
/* H2.7: bounded, fail-safe compaction (the `beforeCompact` hook)      */
/* ------------------------------------------------------------------ */

/**
 * Production incident (2026-10): automatic threshold compaction was
 * PERMANENTLY BROKEN for very large conversations and failed SILENTLY
 * (durable_tasks, conversation 5080, `pi.compaction` tasks 7887/7896/
 * 7898): the built-in summarizer request — the serialized transcript —
 * itself exceeded the model context (`Summarization failed: 400: …
 * maximum context length is 262144 tokens …`), every turn re-tripped
 * the threshold, compaction failed again, and nothing surfaced it.
 *
 * The fix lives HERE, not in the vendored `CompactionTask` (read-only):
 * the harness registers a `beforeCompact` hook (see
 * `createBeforeCompactHook` and the hooks list in `forge-ext.ts`) that
 * owns summarization:
 *
 *  1. The transcript is serialized per-message and TRIMMED FROM THE
 *     OLDEST SIDE until it fits a budget of
 *     `(model.contextWindow − output reserve − safety margin) × 4`
 *     chars (the conservative chars/4 heuristic the whole compaction
 *     surface uses). The trimming is recorded (count in the result and
 *     a note folded into the placed summary).
 *  2. If the summarizer STILL fails (provider error, no usable text,
 *     throw), the conversation is compacted by TRUNCATION instead: a
 *     head+tail window of the raw transcript with an explicit
 *     `[oldest N entries truncated — summary unavailable]` marker, so
 *     the conversation always becomes compactable and never re-trips
 *     the threshold forever.
 *  3. The outcome — `compacted`, `compacted-truncated`, or
 *     `summary-failed-truncated` — is logged at WARN level, and the
 *     fallback path additionally writes a `⚠ `-prefixed assistant chat
 *     row into the forge `messages` table (via the harness's forge-api
 *     HTTP client, `POST /messages/assistant`) so humans/phones get
 *     notified.
 */

/** Conservative chars-per-token heuristic (matches the legacy forge-api
 * `SUM(LENGTH(content))/4` estimator). */
export const CHARS_PER_TOKEN = 4;
/** Tokens reserved for the summarizer's own output. */
export const SUMMARIZER_OUTPUT_TOKENS = 8_192;
/** Fraction of the context window kept as the prompt/output safety margin. */
export const SAFETY_FRACTION = 0.25;
/** Floor on the safety margin even for generously-sized windows. */
export const MIN_SAFETY_TOKENS = 4_096;
/** Never trim the transcript below this estimated token floor. */
export const MIN_TRANSCRIPT_TOKENS = 512;
/** Budget fallback when the model's `contextWindow` is unknown. */
export const FALLBACK_MODEL_CONTEXT_TOKENS = 128_000;
/** Summarizer attempts per compaction before the truncation fallback. */
export const SUMMARIZER_ATTEMPTS = 2;
/** Longest tool result text the serialized transcript keeps. */
const TOOL_RESULT_MAX_CHARS = 2_000;

/**
 * The process model catalog for summarization. The installed
 * pi-durable `HookApi` carries no `models`, so `startHarness` registers
 * the same catalog it handed `Harness.open` here (the hook prefers an
 * injected `options.models`, e.g. in tests).
 */
let processSummarizerModels: SummarizerModels | undefined;

export function setSummarizerModels(models: SummarizerModels): void {
	processSummarizerModels = models;
}

const SUMMARIZATION_SYSTEM_PROMPT = `You are a context summarization assistant. Your task is to read a conversation between a user and an AI assistant, then produce a structured summary following the exact format specified.

Do NOT continue the conversation. Do NOT respond to any questions in the conversation. ONLY output the structured summary.`;

const SUMMARIZATION_PROMPT = `The messages above are a conversation to summarize. Create a structured context checkpoint summary that another LLM will use to continue the work. If the conversation starts with an earlier summary, preserve its information and fold the newer messages into it.

Use this EXACT format:

## Goal
## Constraints & Preferences
## Progress
### Done
### In Progress
### Blocked
## Key Decisions
## Next Steps
## Critical Context

Keep each section concise. Preserve exact file paths, function names, and error messages.`;

/** The compaction outcome exposed by the guard (surfaced in the harness
 * log at warn level on EVERY compaction). */
export type CompactionOutcome = "compacted" | "compacted-truncated" | "summary-failed-truncated";

/** What the guard needs from the model catalog (structurally a subset of
 * pi-ai `Models`; the test mocks it). */
export type SummarizerModels = Pick<Models, "getModel" | "completeSimple">;

export interface CompactionGuardDeps {
	readonly models: SummarizerModels;
	readonly log?: (record: Record<string, unknown>) => void;
	/** Writes the ⚠ chat row (fallback path ONLY). Injected so tests can
	 * assert the notification without any network. */
	readonly notify?: (text: string) => Promise<void>;
	readonly now?: () => number;
}

export interface BoundedSummarizeResult {
	readonly summary: string;
	readonly outcome: CompactionOutcome;
	/** Transcript blocks dropped from the OLDEST side before the
	 * summarizer call (0 when the transcript already fit). */
	readonly trimmedCount: number;
	/** Entries replaced by the truncation marker on the fallback path
	 * (0 when the summarizer produced a summary). */
	readonly truncatedCount: number;
	readonly error?: string;
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function contentBlocksText(content: string | readonly { type: string; text?: string }[]): string {
	if (typeof content === "string") return content;
	return content
		.flatMap((block) => (block.type === "text" && block.text !== undefined ? [block.text] : []))
		.join("\n");
}

function truncateText(text: string, maxChars: number): string {
	if (text.length <= maxChars) return text;
	return `${text.slice(0, maxChars)}\n\n[... ${text.length - maxChars} more characters truncated]`;
}

/** One model message → one transcript block (`[User]: …`,
 * `[Assistant]: …`, `[Tool result]: …`, …); empty string when the
 * message serializes to nothing (skipped by callers). Mirrors the
 * built-in pi-durable serialization shape. */
export function transcriptPart(message: Message): string {
	const role = (message as { role?: string }).role;
	if (role === "user") {
		const text = contentBlocksText((message as unknown as { content?: string | readonly { type: string; text?: string }[] }).content ?? "");
		return text.length > 0 ? `[User]: ${text}` : "";
	}
	if (role === "assistant") {
		const content = (message as unknown as { content?: readonly Record<string, unknown>[] }).content ?? [];
		const thinking = content.flatMap((c) => (c.type === "thinking" && typeof c.thinking === "string" ? [c.thinking] : []));
		const text = content.flatMap((c) => (c.type === "text" && typeof c.text === "string" ? [c.text] : []));
		const calls = content.flatMap((c) =>
			c.type === "toolCall"
				? [
						`${String(c.name ?? "?")}(${Object.entries((c.arguments as Record<string, unknown>) ?? {})
							.map(([key, value]) => `${key}=${JSON.stringify(value)}`)
							.join(", ")})`,
					]
				: [],
		);
		const blocks: string[] = [];
		if (thinking.length > 0) blocks.push(`[Assistant thinking]: ${thinking.join("\n")}`);
		if (text.length > 0) blocks.push(`[Assistant]: ${text.join("\n")}`);
		if (calls.length > 0) blocks.push(`[Assistant tool calls]: ${calls.join("; ")}`);
		return blocks.join("\n");
	}
	if (role === "toolResult") {
		const text = contentBlocksText((message as unknown as { content?: string | readonly { type: string; text?: string }[] }).content ?? "");
		return text.length > 0 ? `[Tool result]: ${truncateText(text, TOOL_RESULT_MAX_CHARS)}` : "";
	}
	return "";
}

/** The transcript as per-message blocks, oldest first, empties dropped. */
export function transcriptParts(messages: readonly Message[]): string[] {
	const blocks: string[] = [];
	for (const message of messages) {
		const part = transcriptPart(message);
		if (part.length > 0) blocks.push(part);
	}
	return blocks;
}

/** Summarizer input budget in chars: the model context minus the output
 * reserve and a safety margin for the prompt itself, converted with the
 * conservative chars/4 heuristic. */
export function summarizerBudgetChars(model: { contextWindow: number; maxTokens: number } | undefined): number {
	const context =
		model !== undefined && model.contextWindow > 0 ? model.contextWindow : FALLBACK_MODEL_CONTEXT_TOKENS;
	const outputReserve =
		model !== undefined && model.maxTokens > 0
			? Math.min(model.maxTokens, SUMMARIZER_OUTPUT_TOKENS)
			: SUMMARIZER_OUTPUT_TOKENS;
	const safetyMargin = Math.max(MIN_SAFETY_TOKENS, Math.floor(context * SAFETY_FRACTION));
	const budgetTokens = Math.max(MIN_TRANSCRIPT_TOKENS, context - outputReserve - safetyMargin);
	return budgetTokens * CHARS_PER_TOKEN;
}

/** Drop WHOLE blocks from the OLDEST side until the joined transcript
 * fits `budgetChars` (the newest block is always kept). */
export function trimTranscriptOldest(
	parts: readonly string[],
	budgetChars: number,
): { text: string; trimmedCount: number } {
	let start = 0;
	let total = parts.reduce((sum, part) => sum + part.length + 2, 0);
	while (start < parts.length - 1 && total > budgetChars) {
		total -= parts[start]!.length + 2;
		start++;
	}
	return { text: parts.slice(start).join("\n\n"), trimmedCount: start };
}

/**
 * Truncation-style compaction: keep a head window (up to 3 blocks,
 * ≤25% of the budget) and as many of the newest blocks as fit, with an
 * explicit `[oldest N entries truncated — summary unavailable]` marker
 * between them. Guarantees a compactable summary whenever the
 * summarizer itself cannot run.
 */
export function truncationSummary(
	parts: readonly string[],
	budgetChars: number,
): { text: string; truncatedCount: number } {
	if (parts.length === 0) return { text: "(no content)", truncatedCount: 0 };
	const budget = Math.max(1_024, budgetChars);
	const headLimit = Math.floor(budget * 0.25);
	const head: string[] = [];
	let headChars = 0;
	let next = 0;
	while (next < parts.length && head.length < 3 && headChars + parts[next]!.length + 2 <= headLimit) {
		head.push(parts[next]!);
		headChars += parts[next]!.length + 2;
		next++;
	}
	const markerFor = (n: number) => `[oldest ${n} entries truncated — summary unavailable]`;
	const tail: string[] = [];
	let used = headChars + markerFor(1).length + 2;
	let last = parts.length - 1;
	while (last >= next && used + parts[last]!.length + 2 <= budget) {
		tail.unshift(parts[last]!);
		used += parts[last]!.length + 2;
		last--;
	}
	const truncatedCount = last - next + 1;
	if (truncatedCount <= 0) {
		// Vacuous-guard (EA incident 2026-10-23): a window that drops
		// nothing is not a compaction — the fallback returned the FULL
		// transcript as the "summary", restoring everything the cut was
		// meant to replace, and every later turn kept 400-ing. Force the
		// summary to at most 60% of the budget: keep the newest blocks
		// only, with an explicit marker for the dropped head.
		const cap = Math.max(1_024, Math.floor(budget * 0.6));
		const forced: string[] = [];
		let usedF = markerFor(1).length + 2;
		let lastF = parts.length - 1;
		while (lastF >= 0 && usedF + parts[lastF]!.length + 2 <= cap) {
			forced.unshift(parts[lastF]!);
			usedF += parts[lastF]!.length + 2;
			lastF--;
		}
		if (forced.length === 0) forced.push(parts[parts.length - 1]!);
		const droppedF = parts.length - forced.length;
		if (droppedF <= 0) return { text: parts.join("\n\n"), truncatedCount: 0 };
		return { text: [markerFor(droppedF), ...forced].join("\n\n"), truncatedCount: droppedF };
	}
	// Always keep at least the newest block, even over budget.
	if (tail.length === 0) tail.push(parts[parts.length - 1]!);
	return { text: [...head, markerFor(truncatedCount), ...tail].join("\n\n"), truncatedCount };
}

/** The summary of a clean `stop` with text and no tool call (same rule
 * as the built-in task); anything else is not a summary. */
function summaryTextOf(message: AssistantMessage): string | undefined {
	if (message.content.some((content) => content.type === "toolCall")) return undefined;
	// "length" is accepted too: a reasoning model can exhaust the output
	// budget AFTER emitting usable text — partial beats nothing, and
	// rejecting it produced the vacuous-compaction incident below.
	if (message.stopReason !== "stop" && message.stopReason !== "length") return undefined;
	const text = message.content
		.flatMap((content) => (content.type === "text" ? [content.text] : []))
		.join("\n")
		.trim();
	if (text.length > 0) return text;
	// Reasoning-model fallback (EA incident 2026-10-23): the summarizer
	// spent the ENTIRE output budget thinking and emitted no text —
	// stopReason length with an empty text part. The thinking stream is
	// still a faithful compression of the transcript, and vastly better
	// material than the truncation fallback.
	const thinking = message.content
		.flatMap((content) => (content.type === "thinking" ? [content.thinking] : []))
		.join("\n")
		.trim();
	return thinking.length > 0 ? `[summarizer reasoning]\n${thinking}` : undefined;
}

/**
 * The bounded, fail-safe summarization the `beforeCompact` hook runs:
 * trim oldest-first into budget → call the summarizer (bounded retries)
 * → on failure fall back to truncation and notify via the ⚠ row. NEVER
 * throws and NEVER leaves the conversation uncompacting.
 */
export async function runBoundedSummarization(args: {
	readonly deps: CompactionGuardDeps;
	readonly conversationId: number;
	readonly messages: readonly Message[];
	readonly modelRef?: ModelRef;
	readonly instructions?: string;
	readonly signal?: AbortSignal;
}): Promise<BoundedSummarizeResult> {
	const { deps, conversationId, messages, modelRef, instructions, signal } = args;
	const log = deps.log ?? (() => {});
	const now = deps.now ?? (() => Date.now());
	const model =
		modelRef === undefined
			? undefined
			: (deps.models.getModel(modelRef.provider, modelRef.modelId) as
					| { contextWindow: number; maxTokens: number }
					| undefined);
	const budgetChars = summarizerBudgetChars(model);
	const parts = transcriptParts(messages);
	const { text: transcript, trimmedCount } = trimTranscriptOldest(parts, budgetChars);
	let lastError: string | undefined;

	if (model !== undefined && parts.length > 0) {
		const focus = instructions === undefined ? "" : `\n\nAdditional focus: ${instructions}`;
		const requestMessages: Message[] = [
			{ role: "system", content: SUMMARIZATION_SYSTEM_PROMPT, timestamp: now() },
			{
				role: "user",
				content: [
					{
						type: "text",
						text: `<conversation>\n${transcript}\n</conversation>\n\n${SUMMARIZATION_PROMPT}${focus}`,
					},
				],
				timestamp: now(),
			},
		];
		const maxTokens = Math.max(
			256,
			Math.min(model.maxTokens > 0 ? model.maxTokens : SUMMARIZER_OUTPUT_TOKENS, SUMMARIZER_OUTPUT_TOKENS),
		);
		for (let attempt = 1; attempt <= SUMMARIZER_ATTEMPTS; attempt++) {
			try {
				const response = await deps.models.completeSimple(
					model as never,
					{ messages: requestMessages } as never,
					{ maxTokens, cacheRetention: "none", ...(signal !== undefined ? { signal } : {}) } as never,
				);
				const text = summaryTextOf(response);
				if (text !== undefined) {
					const outcome: CompactionOutcome = trimmedCount > 0 ? "compacted-truncated" : "compacted";
					const summary =
						trimmedCount > 0
							? `${text}\n\n[summarizer input trimmed: the oldest ${trimmedCount} transcript blocks were omitted to fit the model context]`
							: text;
					log({
						level: "warn",
						msg: "compaction outcome",
						conversationId,
						outcome,
						trimmedCount,
						truncatedCount: 0,
						attempts: attempt,
					});
					return { summary, outcome, trimmedCount, truncatedCount: 0 };
				}
				lastError = response.errorMessage ?? `summarizer ${response.stopReason} without a usable summary`;
			} catch (error) {
				lastError = error instanceof Error ? error.message : String(error);
			}
		}
	} else {
		lastError =
			modelRef === undefined
				? "no model is configured for the conversation"
				: `model ${modelRef.provider}/${modelRef.modelId} is not available`;
	}

	// Fallback: truncation-style compaction. The conversation ALWAYS
	// becomes compactable — never leave it re-tripping the threshold.
	const fallback = truncationSummary(parts, budgetChars);
	const outcome: CompactionOutcome = "summary-failed-truncated";
	const summary = `${fallback.text}${lastError === undefined ? "" : `\n\n[summarizer error: ${lastError}]`}`;
	log({
		level: "warn",
		msg: "compaction outcome",
		conversationId,
		outcome,
		trimmedCount,
		truncatedCount: fallback.truncatedCount,
		error: lastError,
	});
	if (deps.notify !== undefined) {
		try {
			await deps.notify(
				`⚠ Compaction could not summarize this conversation` +
					`${lastError === undefined ? "" : ` (${lastError})`}; the oldest ${fallback.truncatedCount} ` +
					`entries were truncated without a summary to keep the conversation running (conversation ${conversationId}).`,
			);
		} catch (error) {
			log({
				level: "warn",
				msg: "compaction fallback notification failed",
				conversationId,
				error: error instanceof Error ? error.message : String(error),
			});
		}
	}
	return { summary, outcome, trimmedCount, truncatedCount: fallback.truncatedCount, error: lastError };
}

export interface BeforeCompactHookOptions {
	/** forge API base for the ⚠ assistant-row write (`POST
	 * /messages/assistant`, the harness's chat-projection client). */
	readonly apiUrl?: string;
	readonly apiKey?: string;
	readonly log?: (record: Record<string, unknown>) => void;
	/** Test seam: defaults to the asking task's `api.models`. */
	readonly models?: SummarizerModels;
	/** Test seam: defaults to the ⚠ assistant-row POST into the forge
	 * `messages` table (fallback path only). */
	readonly notify?: (text: string) => Promise<void>;
	readonly fetchImpl?: typeof fetch;
	readonly now?: () => number;
}

/**
 * The harness's `beforeCompact` hook for the built-in `CompactionTask`
 * (registered in `forge-ext.ts`). Returning `{ summary }` makes the
 * task place THIS summary — the vendored (unbounded, silently-failing)
 * summarize phase never runs.
 */
export function createBeforeCompactHook(options: BeforeCompactHookOptions = {}): CompactionHooks["beforeCompact"] {
	const log = options.log ?? ((record) => console.error(JSON.stringify(record)));
	return async (compaction, api: HookApi, context: Context) => {
		const models = options.models ?? processSummarizerModels;
		if (models === undefined) {
			log({
				level: "warn",
				msg: "compaction guard has no model catalog; deferring to the built-in summarizer",
				conversationId: api.conversationId,
			});
			return undefined;
		}
		const notify =
			options.notify ??
			(async (text: string): Promise<void> => {
				const apiUrl = options.apiUrl ?? "";
				if (apiUrl === "") return;
				const meta = (await api.snapshot(ForgeMeta, api.conversationId, META_KEY, context))?.value;
				const sessionId = isJsonObject(meta) ? meta.forgeSessionId : undefined;
				if (typeof sessionId !== "string" || sessionId === "") return;
				const response = await (options.fetchImpl ?? fetch)(`${apiUrl}/messages/assistant`, {
					method: "POST",
					headers: {
						"Content-Type": "application/json",
						...(options.apiKey !== undefined && options.apiKey !== "" ? { Authorization: `Bearer ${options.apiKey}` } : {}),
					},
					body: JSON.stringify({ session_id: sessionId, content: text }),
				});
				if (!response.ok) throw new Error(`POST /messages/assistant: HTTP ${response.status}`);
			});
		let agent: AgentState | undefined;
		try {
			agent = await api.snapshot(AgentDoc, api.conversationId, context);
		} catch {
			agent = undefined;
		}
		try {
			const result = await runBoundedSummarization({
				deps: { models, log, notify, ...(options.now !== undefined ? { now: options.now } : {}) },
				conversationId: api.conversationId as number,
				messages: compaction.messages,
				...(agent?.model !== undefined ? { modelRef: agent.model } : {}),
				...(compaction.instructions !== undefined ? { instructions: compaction.instructions } : {}),
			});
			return { summary: result.summary };
		} catch (error) {
			// Should be unreachable (runBoundedSummarization never throws);
			// falling through to the built-in phase keeps the OLD behaviour
			// rather than failing the task outright.
			log({
				level: "warn",
				msg: "compaction guard failed; deferring to the built-in summarizer",
				conversationId: api.conversationId,
				error: error instanceof Error ? error.message : String(error),
			});
			return undefined;
		}
	};
}
