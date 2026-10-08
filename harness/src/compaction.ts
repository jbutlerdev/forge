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
import type { CompactionInput, ContextView, Harness } from "@earendil-works/pi-durable";
import { CompactionTask, LiveDoc, type ConversationId } from "@earendil-works/pi-durable";
import type { Message, TextContent, ThinkingContent, ToolCall } from "@earendil-works/pi-ai";
import { ForgeDocument, CONFIG_KEY } from "./docs.js";

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
	if (estimatedTokens <= threshold.maxContextChars) return null;

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
