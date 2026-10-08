/**
 * The forge pi-durable extension: the ONE tool path for agent turns.
 *
 * Registers the same four tools as the standalone `extensions/forge-tools`
 * pi extension (same input schemas, same defaults, same request shapes):
 *
 *   - bash  → POST {apiUrl}/tools/execute/stream  (SSE, real-time output)
 *   - read/write/edit → POST {apiUrl}/tools/execute  (single JSON response)
 *
 * Plus the malleable layer (H2.5): a `before_tool` hook enforcing the
 * agent's `tools_allowlist` (H1.1 column; the call is blocked with a
 * reason the model sees in the transcript before it ever reaches
 * /tools/execute — tenancy there is unchanged), and prompt sections
 * `document_plan` / `document_handoff` / `memory_beliefs` (H4 slot).
 *
 * Every call carries `session_id` (the conversation's `forgeSessionId`, read
 * from its `forge.meta` document), `tool_call_id` (pi-durable's `api.callId`),
 * and `Authorization: Bearer $FORGE_API_KEY`. Tenancy and tool allowlists
 * stay enforced by forge-api's /tools/execute — this extension only relays.
 *
 * Each conversation gets its own extension instance named
 * `forge-ext-<uuid>` so `createConversation` can honor the per-conversation
 * `replaySafeTools` list: an interrupted tool call is rerun on recovery only
 * when its tool is registered `replay: "safe"`, otherwise the model is told
 * the call was interrupted (pi-durable semantics).
 */
import {
	defineExtension,
	defineTool,
	hook,
	section,
	ToolTask,
	type Extension,
	type PromptInput,
	type Registry,
	type ToolExecutionApi,
	type ToolExecutionResult,
} from "@earendil-works/pi-durable";
import { Type, type TSchema } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/chord";
import { createSpawnSubagentTool, SubagentAnchor, type SubagentSpawnedEvent } from "./subagent.js";
import { ForgeDocument, ForgeMeta, META_KEY } from "./docs.js";

/** The standard forge tool names (the model's own tool surface). */
export const FORGE_TOOL_NAMES = ["bash", "read", "write", "edit"] as const;

/**
 * Bash default timeout: 1 hour. This must match `BASH_DEFAULT_TIMEOUT_MS` in
 * `crates/forge-api/src/tool_executor.rs` and in `extensions/forge-tools`.
 * The LLM may pass any value up to and beyond this — it's a default, not a
 * cap. A shorter default would race the harness's `TOOL_READ_TIMEOUT_SECS`
 * and the streaming-bash / sandbox outer grace window: a long
 * `cargo test --release` or `git clone` would be killed at the first
 * read-timeout boundary the harness hit.
 */
export const BASH_DEFAULT_TIMEOUT_MS = 3_600_000;

const BashInputSchema = Type.Object({
	command: Type.String({ description: "The shell command to execute" }),
	timeout_ms: Type.Optional(
		Type.Integer({ description: "Timeout in milliseconds", default: BASH_DEFAULT_TIMEOUT_MS }),
	),
});

const ReadInputSchema = Type.Object({
	path: Type.String({ description: "Path to the file to read" }),
	offset: Type.Optional(Type.Integer({ description: "Line to start reading from (1-indexed)", default: 1 })),
	limit: Type.Optional(Type.Integer({ description: "Maximum lines to read", default: 100 })),
});

const WriteInputSchema = Type.Object({
	path: Type.String({ description: "Path to the file to write" }),
	content: Type.String({ description: "Content to write to the file" }),
});

const EditInputSchema = Type.Object({
	path: Type.String({ description: "Path to the file to edit" }),
	old_text: Type.String({ description: "Exact text to find and replace" }),
	new_text: Type.String({ description: "Replacement text" }),
});

type ForgeToolResult = ToolExecutionResult;

interface ForgeToolOptions {
	readonly apiUrl: string;
	readonly apiKey: string;
	/** When a tool name is in this list, an interrupted execution reruns on
	 * recovery; otherwise pi-durable tells the model the call was
	 * interrupted instead. */
	readonly replaySafeTools?: readonly string[];
	/** Default true: stream bash output over SSE, as forge-tools does. */
	readonly useStreaming?: boolean;
	/** Extension instance name; unique per conversation (see module header). */
	readonly name?: string;
	/** Which standard tools this instance offers (default: all four).
	 * `spawn_subagent` is governed separately by `subagent`. */
	readonly tools?: readonly string[];
	/** Default true: offer the `spawn_subagent` tool (H2.2). Subagent
	 * children get `subagent: false` unless re-granted. */
	readonly subagent?: boolean;
	/** Herd H2.5: the agent's `tools_allowlist` (H1.1). When non-empty,
	 * the `before_tool` hook BLOCKS any tool call whose name is not in
	 * the list (the model sees the reason in the transcript; the call
	 * never reaches /tools/execute). Empty/absent = no allowlist = every
	 * offered tool runs (non-breaking for pre-H2.5 conversations). */
	readonly toolsAllowlist?: readonly string[];
	/** Process registry (required when `subagent` is offered: the child's
	 * extension instance is installed there at spawn time). */
	readonly registry?: Registry;
	/** Fired when this extension's spawn_subagent creates a child
	 * conversation (the harness event push, H2.2 exposure). */
	readonly onSubagent?: (event: SubagentSpawnedEvent) => void;
}

/** Split an accumulated SSE wire-format buffer into complete event blocks
 * plus the in-progress tail. (Copied from extensions/forge-tools.) */
function splitSSEEvents(buffer: string): { events: string[]; tail: string } {
	const parts = buffer.split("\n\n");
	const tail = parts.pop() || "";
	return { events: parts, tail };
}

/** Parse one SSE event block into its `event:` name and first `data:` line.
 * (Copied from extensions/forge-tools.) */
function parseSSEEventBlock(raw: string): { eventName?: string; data: string } | null {
	if (!raw) return null;
	let eventName: string | undefined;
	let dataLine: string | undefined;
	for (const line of raw.split("\n")) {
		if (line.startsWith(":")) continue;
		if (line.startsWith("event:") && eventName === undefined) {
			eventName = line.slice(6).trim();
		} else if (line.startsWith("data:") && dataLine === undefined) {
			dataLine = line.slice(5).trim();
		}
	}
	if (dataLine === undefined) return null;
	return { eventName, data: dataLine };
}

function isJson(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textResult(text: string, isError = false): ForgeToolResult {
	return { content: [{ type: "text", text }], ...(isError ? { isError: true } : {}) };
}

/** Read the conversation's forge session id from its meta document. */
async function forgeSessionId(api: ToolExecutionApi, context: Context): Promise<string | undefined> {
	const meta = (await api.snapshot(ForgeMeta, api.conversationId, META_KEY, context))?.value;
	return isJson(meta) && typeof meta.forgeSessionId === "string" ? meta.forgeSessionId : undefined;
}

function headers(options: ForgeToolOptions, extra: Record<string, string>): Record<string, string> {
	return {
		...extra,
		...(options.apiKey ? { Authorization: `Bearer ${options.apiKey}` } : {}),
	};
}

/** Execute a tool via the forge SSE streaming endpoint (bash). Accumulates
 * stdout/stderr instead of writing to process streams — the harness has no
 * terminal to show them to; the final text becomes the tool result. */
async function executeStreaming(
	toolName: string,
	toolInput: Record<string, unknown>,
	toolCallId: string,
	sessionId: string,
	options: ForgeToolOptions,
): Promise<ForgeToolResult> {
	try {
		const response = await fetch(`${options.apiUrl}/tools/execute/stream`, {
			method: "POST",
			headers: headers(options, { "Content-Type": "application/json", Accept: "text/event-stream" }),
			body: JSON.stringify({
				session_id: sessionId,
				tool: toolName,
				input: toolInput,
				tool_call_id: toolCallId,
			}),
		});

		if (!response.ok) {
			// The server rejected the request before executing it (bad session
			// id, missing fields, session not found). Do NOT fall back to
			// /tools/execute: the error is real and re-POSTing could run the
			// command a second time if the first attempt actually started.
			const errorText = await response.text();
			console.error(
				JSON.stringify({ level: "warn", msg: "forge SSE API error", status: response.status, body: errorText }),
			);
			return textResult(`Error: ${response.status} ${errorText}`, true);
		}

		const reader = response.body?.getReader();
		if (!reader) return textResult("Error: No response body", true);

		const decoder = new TextDecoder();
		let buffer = "";
		let output = "";
		let errorOutput = "";
		let success = true;
		let durationMs = 0;
		// The forge side always ends a completed call with a `tool_end` event
		// followed by `done`. If the stream ends without `tool_end` (network
		// died, server crashed mid-call), the command did NOT report
		// completion — surface that to the model instead of a clean success.
		let sawToolEnd = false;

		while (true) {
			const { done, value } = await reader.read();
			if (done) break;
			buffer += decoder.decode(value, { stream: true });
			const split = splitSSEEvents(buffer);
			buffer = split.tail;
			for (const raw of split.events) {
				const parsed = parseSSEEventBlock(raw);
				if (parsed === null) continue;
				let payload: unknown;
				try {
					payload = JSON.parse(parsed.data);
				} catch {
					continue; // malformed JSON — skip, never crash
				}
				if (!isJson(payload)) continue;
				switch (parsed.eventName) {
					case "stdout":
						if (typeof payload.chunk === "string") output += payload.chunk;
						break;
					case "stderr":
						if (typeof payload.chunk === "string") errorOutput += payload.chunk;
						break;
					case "tool_end":
						sawToolEnd = true;
						success = payload.success === true;
						durationMs = typeof payload.duration_ms === "number" ? payload.duration_ms : 0;
						break;
					case "error":
						errorOutput += `Error: ${typeof payload.error === "string" ? payload.error : String(payload.error)}\n`;
						break;
					case "tool_start":
					case "done":
						break;
					default:
						// Protocol drift between the Rust API and this extension.
						console.warn(
							JSON.stringify({ level: "warn", msg: "unknown SSE event", event: parsed.eventName ?? null }),
						);
				}
			}
		}
		reader.releaseLock();

		if (!sawToolEnd) {
			success = false;
			errorOutput +=
				`\n[forge-ext] Stream ended without a tool_end event — the command did not report completion. ` +
				`It was NOT re-executed; verify what ran on the server side.`;
		}
		console.error(
			JSON.stringify({ level: "info", msg: "forge tool completed", tool: toolName, duration_ms: durationMs, success }),
		);
		if (success) return textResult(output || "Command completed successfully");
		return textResult(errorOutput || "Command failed", true);
	} catch (error) {
		// Network error MID-stream: the command may have run (or even
		// finished). Do not re-POST — surface and let the model decide.
		const message = error instanceof Error ? error.message : String(error);
		console.error(JSON.stringify({ level: "warn", msg: "SSE streaming error", error: message }));
		return textResult(
			`Streaming error: ${message}. The command may have run on the server; it was NOT re-executed. ` +
				`Check the session logs before re-running.`,
			true,
		);
	}
}

/** Execute a tool via the forge plain-JSON endpoint (read/write/edit). */
async function executePlain(
	toolName: string,
	toolInput: Record<string, unknown>,
	toolCallId: string,
	sessionId: string,
	options: ForgeToolOptions,
): Promise<ForgeToolResult> {
	try {
		const response = await fetch(`${options.apiUrl}/tools/execute`, {
			method: "POST",
			headers: headers(options, { "Content-Type": "application/json" }),
			body: JSON.stringify({
				session_id: sessionId,
				tool: toolName,
				input: toolInput,
				tool_call_id: toolCallId,
			}),
		});
		if (!response.ok) {
			const errorText = await response.text();
			console.error(
				JSON.stringify({ level: "warn", msg: "forge API error", status: response.status, body: errorText }),
			);
			return textResult(`Error: ${response.status} ${errorText}`, true);
		}
		const result = (await response.json()) as { success: boolean; output: string | null; error: string | null };
		if (result.success) return textResult(result.output || "");
		return textResult(result.error || "Unknown error", true);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		console.error(JSON.stringify({ level: "warn", msg: "network error", error: message }));
		return textResult(`Network error: ${message}`, true);
	}
}

/** Build one forge tool registration. `replay` follows the per-conversation
 * allowlist; everything else is shared by every instance. */
function forgeTool(
	name: "bash" | "read" | "write" | "edit",
	description: string,
	parameters: TSchema,
	options: ForgeToolOptions,
): ReturnType<typeof defineTool> {
	const useStreaming = options.useStreaming !== false;
	const replaySafe = (options.replaySafeTools ?? []).includes(name);
	return defineTool({
		name,
		description,
		parameters,
		...(replaySafe ? { replay: "safe" as const } : {}),
		async execute(args: Record<string, unknown>, api: ToolExecutionApi, context: Context) {
			const sessionId = await forgeSessionId(api, context);
			if (sessionId === undefined) {
				// A conversation created outside the harness IPC never got a
				// meta document. Fail loud to the model instead of POSTing
				// with an empty session id.
				return textResult(
					`Error: this conversation has no forge session id (missing ${ForgeMeta.definition.kind} document). ` +
						`Conversation ${api.conversationId} was not created through the harness.`,
					true,
				);
			}
			const input = { ...args };
			if (name === "bash" && (input as { timeout_ms?: unknown }).timeout_ms === undefined) {
				(input as { timeout_ms?: unknown }).timeout_ms = BASH_DEFAULT_TIMEOUT_MS;
			}
			if (useStreaming && name === "bash") {
				return executeStreaming(name, input, api.callId, sessionId, options);
			}
			return executePlain(name, input, api.callId, sessionId, options);
		},
	});
}

/** Render one conversation document (by family name) as prompt text:
 * strings verbatim, everything else pretty-printed JSON. Absent document
 * ⇒ the section renders nothing (pi-durable omits it). */
async function renderDocument(name: string, input: PromptInput, context: Context): Promise<string | undefined> {
	const value = (await input.read.snapshot(ForgeDocument, input.conversationId, name, context))?.value;
	if (value === undefined || value === null) return undefined;
	return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

/**
 * One extension instance for one conversation. The instance name is part of
 * the conversation's stored agent config, so a harness that reopens later
 * must re-install it (see `reinstallConversationExtensions` in main.ts).
 *
 * `tools` narrows the standard tool surface (H2.2 subagent children get
 * their subset from the spawn args); `subagent: false` is how subagent
 * children avoid spawning subagents of their own by default.
 */
export function createForgeExtension(options: ForgeToolOptions & { name?: string }): Extension {
	const name = options.name ?? "forge-ext";
	const allowed = options.tools ?? [...FORGE_TOOL_NAMES];
	const baseTools = [
		forgeTool(
			"bash",
			"Execute a shell command and return stdout/stderr. Output is streamed in real-time for long-running commands.",
			BashInputSchema,
			options,
		),
		forgeTool("read", "Read file contents", ReadInputSchema, options),
		forgeTool("write", "Write content to a file (creates or overwrites)", WriteInputSchema, options),
		forgeTool("edit", "Apply a targeted text replacement to a file", EditInputSchema, options),
	].filter((tool) => allowed.includes(tool.name));
	const subagentTool =
		options.subagent === false || options.registry === undefined
			? undefined
			: (() => {
					// The anchor task definition is installed at most ONCE per
					// registry: many forge extension instances share it, and
					// registering it twice throws (registry.ts).
					if (options.registry!.snapshot().task(SubagentAnchor.definition.name) !== undefined) return undefined;
					return createSpawnSubagentTool({
						apiUrl: options.apiUrl,
						apiKey: options.apiKey,
						registry: options.registry!,
						replaySafeTools: options.replaySafeTools,
						onSubagent: options.onSubagent,
					});
				})();
	return defineExtension({
		name,
		// The detached-subagent anchor task definition ships with every
		// extension that offers spawn_subagent, so an anchor task still
		// resolves after a restart (task definitions live in the registry,
		// not the database).
		tasks: subagentTool === undefined ? undefined : [SubagentAnchor],
		tools: subagentTool === undefined ? baseTools : [...baseTools, subagentTool],
		// Herd H2.5: prompt sections, rebuilt per request (pi-durable keeps
		// only the delta to the previous request). `document_<name>` renders
		// the conversation's document of that name (section keys are
		// `[a-z][a-z0-9_-]*` — no colons) — the first users are
		// `plan` (the agent's plan/todo) and `handoff` (notes that a reset
		// carries forward); `memory_beliefs` is the H4 slot (stub for now).
		sections: [
			section("document_plan", (input, context) => renderDocument("plan", input, context)),
			section("document_handoff", (input, context) => renderDocument("handoff", input, context)),
			// H4 plugs in here: this section will fetch the agent's active
			// beliefs from forge's memory API (over the same HTTP bridge the
			// tools use) and render them. Until then it renders nothing.
			section("memory_beliefs", () => undefined),
		],
		// Herd H2.5: the agent's `tools_allowlist` (H1.1 column, carried in
		// this instance's closed-over options from the `forge.meta` document)
		// enforced as a pi-durable `before_tool` hook: a blocked call never
		// executes and the model sees the reason in the transcript. Tenancy
		// stays enforced at /tools/execute (unchanged).
		hooks: [
			hook(ToolTask, {
				beforeTool: (call) => {
					const allowlist = options.toolsAllowlist;
					if (allowlist !== undefined && allowlist.length > 0 && !allowlist.includes(call.name)) {
						return {
							block:
								`Tool '${call.name}' is not in this agent's tool allowlist ` +
								`(allowed: ${allowlist.join(", ")}). ` +
								`The call was NOT executed; use an allowed tool instead.`,
						};
					}
					// H3.5 POLICY HOOK SLOT: the mule policy evaluation plugs in
					// here (PLAN-HERD §H3.5 — the hook evaluates mule policy
					// rules; an `ask` disposition checkpoints the task on a
					// ranch approval card). H2.5 ships NO mule calls: this
					// returns `undefined` (no decision) on purpose.
					return undefined;
				},
			}),
		],
	});
}
