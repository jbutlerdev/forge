/**
 * The `spawn_subagent` tool (Herd H2.2): the pi-durable subagent
 * primitive, exposed to forge models.
 *
 * The tool is the foreground/detach pattern of pi-durable's own
 * subagent examples (`vendor/pi-durable/packages/durable/test/examples/
 * 22-subagent-foreground.ts` + `23-subagent-background.ts`):
 *
 *  - A **foreground** subagent is a child durable conversation owned by
 *    the calling tool task (`ownership: { kind: "task", taskId }`). The
 *    tool submits one input there and `wait()`s (checkpointed — a crash
 *    mid-child recovers: the tool is `replay: "safe"`, the spawn is
 *    deduped through the `forge.subagent` document, and the submission
 *    through pi-durable's `submissionByRequest` on
 *    `requestId: "subagent:<taskId>"`). Aborting the parent aborts the
 *    child through the ownership tree (pi-durable native).
 *  - A **detached** subagent is owned by a `background: true` anchor
 *    task of the parent conversation instead: the tool submits and
 *    returns the child's id WITHOUT waiting, so the child survives the
 *    parent's turn completing AND the parent's abort/idle waits
 *    (background tasks are the abort boundary in pi-durable).
 *
 * The child starts as a copy of the parent's agent (model, cwd) and
 * gets its OWN forge tool extension instance: the model and/or tool
 * subset come from the tool args, and `spawn_subagent` is not offered
 * to children by default. The child's `forge.meta` document carries a
 * pre-minted forge session id; the `subagent_spawned` event (fired
 * through `options.onSubagent`) hands it to forge-api, which mints the
 * matching session row under the parent (H2.2 exposure).
 */
import { randomUUID } from "node:crypto";
import type { JsonValue } from "@earendil-works/chord";
import { AssistantEntry, configure, defineDocFamily, defineTask, defineTool, type JsonObject, type Registry } from "@earendil-works/pi-durable";
import { Type, type TSchema } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/chord";
import type { ToolExecutionApi, ToolExecutionResult } from "@earendil-works/pi-durable";
import { createForgeExtension } from "./forge-ext.js";
import { ForgeMeta, META_KEY } from "./docs.js";

/**
 * The standard tool names, duplicated locally (NOT imported from
 * forge-ext): forge-ext imports THIS module's tool factory, so a
 * module-level import of its `FORGE_TOOL_NAMES` would read it before
 * forge-ext has finished evaluating (ESM circular init).
 */
const STANDARD_TOOLS = ["bash", "read", "write", "edit"];

/** One forge tool execution result. */
type Result = ToolExecutionResult;

function textResult(text: string, isError = false): Result {
	return { content: [{ type: "text", text }], ...(isError ? { isError: true } : {}) };
}

/**
 * Spawn bookkeeping: one document family member per calling tool task,
 * so a replayed (crash-recovered) tool call finds the child it already
 * created instead of creating a second one.
 */
export interface SubagentSpawnValue {
	readonly childConversationId: number;
	readonly detached: boolean;
	readonly childForgeSessionId: string;
}

/** The document value (pi-durable document values are JsonObject). */
export interface SubagentSpawnDoc extends JsonObject {
	childConversationId: number;
	detached: boolean;
	childForgeSessionId: string;
}

export const SubagentSpawn = defineDocFamily<SubagentSpawnDoc, JsonValue>({
	kind: "forge.subagent",
	version: 1,
	scope: "conversation",
	history: "latest",
	fork: "initial",
	family: true,
	initial: (seed) => seed as SubagentSpawnDoc,
});

/**
 * The detached-subagent anchor: a background task of the parent
 * conversation that finishes at once. A background task is pi-durable's
 * abort/idle boundary: work it owns (the child conversation's turns)
 * survives the parent's abort and idle waits, and the parent's busy
 * state never includes it.
 */
export const SubagentAnchor = defineTask<null, { phase: "done" }, null>({
	name: "forge.subagent-anchor",
	version: 1,
	initial: () => ({ phase: "done" }),
	phases: {
		done: (_anchor, runtime, taskContext) =>
			runtime.commit(() => ({ status: "terminal", outcome: { status: "completed", result: null } }), taskContext),
	},
	abort: (_anchor, runtime, taskContext) =>
		runtime.commit(() => ({ status: "terminal", outcome: { status: "aborted" } }), taskContext),
});

export interface SubagentSpawnedEvent {
	readonly parentConversationId: number;
	readonly childConversationId: number;
	readonly childForgeSessionId: string;
	/** The `task` argument of the spawn tool call (the child's prompt).
	 * forge-api uses it for the child session's title. */
	readonly task: string;
	readonly detached: boolean;
}

export interface SpawnSubagentOptions {
	readonly apiUrl: string;
	readonly apiKey: string;
	/** Process registry: the child's own extension instance is installed
	 * here at spawn time (registry snapshots resolve by name at commit). */
	readonly registry: Registry;
	/** Tool names safe to replay (inherited by the child extension). */
	readonly replaySafeTools?: readonly string[];
	/** Fired (fire-and-forget) after the spawn commit: forge-api maps the
	 * child conversation to a new forge session row. */
	readonly onSubagent?: (event: SubagentSpawnedEvent) => void;
}

const SubagentParameters: TSchema = Type.Object({
	task: Type.String({ description: "The self-contained task to delegate to the subagent" }),
	model: Type.Optional(
		Type.Object({
			provider: Type.String({ description: "Model provider (defaults to the parent's model)" }),
			modelId: Type.String({ description: "Model id within the provider" }),
		}),
	),
	tools: Type.Optional(
		Type.Array(
			Type.String({
				description: `Tool subset for the child; one or more of: ${STANDARD_TOOLS.join(", ")}`,
			}),
		),
	),
	detach: Type.Optional(
		Type.Boolean({
			description:
				"Detached subagents keep working after your turn completes and survive your abort; you get the child's id instead of its answer.",
		}),
	),
});

/** Read one assistant entry's text out of a committed entry (the child's
 * final answer). */
async function answerText(api: ToolExecutionApi, entryId: number, context: Context): Promise<string> {
	const entry = await api.commit((tx) => tx.entry(AssistantEntry, entryId as never), context);
	const message = entry?.model?.[0] as { content?: readonly { type?: string; text?: string }[] } | undefined;
	if (entry === undefined || message === undefined) return "";
	return message.content
		?.filter((block) => block.type === "text" && typeof block.text === "string")
		.map((block) => block.text)
		.join("") ?? "";
}

/** The tool body (extracted so the `execute` wrapper can log debug stacks). */
async function executeSpawnSubagent(
args: Record<string, unknown>,
api: ToolExecutionApi,
callContext: Context,
options: SpawnSubagentOptions,
): Promise<Result> {
	const task = typeof args.task === "string" ? args.task : "";
		if (task.length === 0) return textResult("Error: spawn_subagent requires a non-empty `task`.", true);
		const detach = args.detach === true;
		const model =
			args.model !== undefined && typeof args.model === "object" && args.model !== null
				? { provider: String((args.model as { provider?: unknown }).provider ?? ""), modelId: String((args.model as { modelId?: unknown }).modelId ?? "") }
				: undefined;
		if (model !== undefined && (model.provider.length === 0 || model.modelId.length === 0)) {
			return textResult("Error: `model` must set both `provider` and `modelId`.", true);
		}
		const toolSubset: string[] | undefined =
			Array.isArray(args.tools) && args.tools.length > 0
				? args.tools.filter((t): t is string => typeof t === "string" && (STANDARD_TOOLS as readonly string[]).includes(t))
				: undefined;
		if (args.tools !== undefined && (toolSubset === undefined || toolSubset.length === 0)) {
			return textResult(`Error: tools must name at least one of: ${STANDARD_TOOLS.join(", ")}.`, true);
		}

		// --- 1. the child (deduped through the spawn document) ---
		const key = String(api.taskId);
		const prior = (await api.snapshot(SubagentSpawn, api.conversationId, key, callContext)) as
			| SubagentSpawnValue
			| undefined;
		let childId: number;
		let childForgeSessionId: string;
		let detached = detach;
		if (prior !== undefined) {
			// Replay of a crash-interrupted call: reuse what the first
			// attempt committed.
			childId = prior.childConversationId;
			childForgeSessionId = prior.childForgeSessionId;
			detached = prior.detached;
		} else {
			const childForgeSessionId_ = randomUUID();
			const childExtensionName = `forge-ext-${randomUUID()}`;
			// The child's own extension instance: the requested tool
			// subset (default: every standard tool) and NO
			// spawn_subagent of its own.
			const childExtension = createForgeExtension({
				name: childExtensionName,
				apiUrl: options.apiUrl,
				apiKey: options.apiKey,
				replaySafeTools: options.replaySafeTools,
				registry: options.registry,
				tools: toolSubset,
				subagent: false,
			});
			options.registry.install(childExtension);
			// The parent extension instance (from the parent's meta
			// document) is what the child agent copy carries; swap it
			// for the child's own.
			const parentMeta = (await api.snapshot(ForgeMeta, api.conversationId, META_KEY, callContext))?.value as
				| { extensionName?: unknown }
				| undefined;
			const parentExtensionName =
				typeof parentMeta?.extensionName === "string" ? parentMeta.extensionName : "forge-ext";
			childId = await api.commit(async (tx) => {
				// Detach: the child's owner is a background anchor task
				// of the PARENT conversation — pi-durable's boundary
				// that keeps the child out of the parent's abort and
				// idle waits (and out of the parent's busy state).
				// Foreground: the child is owned by this tool task, so
				// aborting the parent aborts the child.
				const anchor = detach
					? await tx.createTask(SubagentAnchor, null, {
							ownership: { kind: "conversation" },
							background: true,
						} as never)
					: undefined;
				const created = await tx.createConversation({
					ownership:
						anchor !== undefined
							? ({ kind: "task", taskId: anchor } as never)
							: ({ kind: "task", taskId: api.taskId } as never),
				});
				await configure(tx, created.id, {
					...(model !== undefined ? { model } : {}),
					extensions: {
						add: [childExtension],
						remove: [({ name: parentExtensionName } as never)],
					},
					instructions: "You are a subagent of a forge agent. Answer the delegated task directly and concisely.",
				});
				const meta = await tx.doc(ForgeMeta, created.id, META_KEY, {
					forgeSessionId: childForgeSessionId_,
					extensionName: childExtensionName,
					replaySafeTools: [...(options.replaySafeTools ?? [])],
					...(toolSubset !== undefined ? { tools: toolSubset } : {}),
					subagent: false,
				});
				meta.value = {
					forgeSessionId: childForgeSessionId_,
					extensionName: childExtensionName,
					replaySafeTools: [...(options.replaySafeTools ?? [])],
					...(toolSubset !== undefined ? { tools: toolSubset } : {}),
					subagent: false,
				};
				const spawn = await tx.doc(SubagentSpawn, api.conversationId, key, {
					childConversationId: 0,
					detached,
					childForgeSessionId: childForgeSessionId_,
				});
				spawn.value = {
					childConversationId: created.id,
					detached,
					childForgeSessionId: childForgeSessionId_,
				};
				return created.id;
			}, callContext);
			childForgeSessionId = childForgeSessionId_;
		}

		// --- 2. forge-api exposure: map the child to a session row ---
		options.onSubagent?.({
			parentConversationId: api.conversationId,
			childConversationId: childId,
			childForgeSessionId,
			task,
			detached,
		});
		// UIs watching the parent can attach to the child through the
		// call's details (pi-durable subagent pattern).
		await api.details({ conversationId: childId, detached }, callContext);

		// --- 3. the task, exactly once (requestId dedup) ---
		const child = (await api.conversation(childId as never, callContext))!;
		const submission = await child.submit({ type: "input", content: task, requestId: `subagent:${api.taskId}` } as never, callContext);
		if (detached) {
			return textResult(
				`Detached subagent started (conversation ${childId}, forge session ${childForgeSessionId}). ` +
					`It keeps working after your turn completes; its answer lands in its own transcript.`,
			);
		}
		const settled = await submission.wait(callContext);
		if (settled.status !== "done" || settled.type !== "input") {
			return textResult(
				`Error: subagent did not complete (${settled.status}${settled.type !== "input" ? "" : `: ${settled.reason ?? "unknown"}`})`,
				true,
			);
		}
		const text = await answerText(api, settled.answer as number, callContext);
		return {
			content: [{ type: "text", text: text.length > 0 ? text : "(the subagent finished without text)" }],
			details: { conversationId: childId, detached: false },
		};
};

export function createSpawnSubagentTool(options: SpawnSubagentOptions) {
	return defineTool({
		name: "spawn_subagent",
		description:
			"Delegate a self-contained task to a subagent: it runs in its own conversation with its own transcript. " +
			"Returns the subagent's answer; with detach=true it returns the child's id and the subagent keeps " +
			"working in the background after your turn completes.",
		parameters: SubagentParameters,
		// A crash-recovered rerun finds the same child (forge.subagent
		// document) and the same submission (requestId dedup), so rerunning
		// it cannot double-spawn or double-send.
		replay: "safe",
		execute: async (args: Record<string, unknown>, api: ToolExecutionApi, callContext: Context) =>
			executeSpawnSubagent(args, api, callContext, options),
	});
}
