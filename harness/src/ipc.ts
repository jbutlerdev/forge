/**
 * The harness IPC surface: a unix-socket JSON-RPC server for forge-api plus
 * the event push channel.
 *
 * RPC wire format — one JSON object per line on the harness socket:
 *   request:  {"id": <string|number>, "method": <string>, "params": <object>}
 *   response: {"id": <same>, "result": <any>}  |  {"id": <same>, "error": {"code", "message"}}
 *
 * Events wire format — one JSON object per line, harness → forge-api, on the
 * events socket (see events.ts for the vocabulary). One client at a time; a
 * second connection displaces the first. No replay on reconnect.
 *
 * The handler map is exported so it can be exercised in-process (tests)
 * without a listening socket; the socket server is a thin line-buffered
 * dispatcher over it.
 */
import { createServer, type Server, type Socket } from "node:net";
import type { Context } from "@earendil-works/chord";
import type {
	Conversation,
	ConversationId,
	Cursor,
	Harness,
	Registry,
	SubmissionDraft,
	TaskId,
} from "@earendil-works/pi-durable";
import { createForgeExtension } from "./forge-ext.js";
import { ForgeDocument, ForgeMeta, META_KEY } from "./docs.js";
import { contextChars, compactionSummaryChars } from "./compaction.js";
import { LiveDoc } from "@earendil-works/pi-durable";
import type { EventBus, HarnessEvent } from "./events.js";
import type { TimerRegistry } from "./timers.js";

/** Typed RPC error. `code` is part of the wire contract. */
export class RpcError extends Error {
	readonly code: string;
	constructor(code: string, message: string) {
		super(message);
		this.code = code;
	}
}

export type RpcResult =
	| { readonly version: string; readonly activeTasks: number; readonly conversations: number; readonly timers: number }
	| { readonly conversationId: number }
	| { readonly submissionId: number }
	| { readonly imported: number }
	| { readonly aborted: number }
	| { readonly timerId: string }
	| { readonly cleared: boolean }
	| null
	| Record<string, unknown>;

export type Handler = (params: Record<string, unknown>) => Promise<RpcResult>;
export type HandlerMap = Record<string, Handler>;

export interface HandlerDeps {
	readonly harness: Harness;
	readonly registry: Registry;
	readonly timers: TimerRegistry | undefined;
	readonly events: EventBus;
	readonly version: string;
	readonly apiUrl: string;
	readonly apiKey: string;
	readonly context: Context;
}

function asNumber(value: unknown, field: string): number {
	if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
		throw new RpcError("invalid_params", `${field} must be a non-negative integer`);
	}
	return value;
}

function asString(value: unknown, field: string, allowEmpty = true): string {
	if (typeof value !== "string" || (!allowEmpty && value.length === 0)) {
		throw new RpcError("invalid_params", `${field} must be a ${allowEmpty ? "" : "non-empty "}string`);
	}
	return value;
}

function asObject(value: unknown, field: string): Record<string, unknown> {
	if (typeof value !== "object" || value === null || Array.isArray(value)) {
		throw new RpcError("invalid_params", `${field} must be an object`);
	}
	return value as Record<string, unknown>;
}

async function requireConversation(deps: HandlerDeps, conversationId: number): Promise<Conversation> {
	const conversation = await deps.harness.conversation(conversationId as ConversationId, deps.context);
	if (conversation === undefined) {
		throw new RpcError("unknown_conversation", `conversation ${conversationId} does not exist`);
	}
	return conversation;
}

/** Count all conversations by walking the scan pages. */
async function countConversations(deps: HandlerDeps): Promise<number> {
	return deps.harness.commit(async (tx) => {
		let count = 0;
		let cursor: unknown = undefined;
		for (;;) {
			const page = await tx.scanConversations({}, 256, cursor as never);
			count += page.items.length;
			if (page.next === undefined) break;
			cursor = page.next;
		}
		return count;
	}, deps.context);
}

export function makeHandlers(deps: HandlerDeps): HandlerMap {
	const { harness, registry, timers, events, version, apiUrl, apiKey } = deps;
	const context = deps.context;

	return {
		/** `{ version, activeTasks, conversations }` — health + bookkeeping. */
		async status() {
			const inspection = await harness.inspect(context);
			const conversations = await countConversations(deps);
			return {
				version,
				activeTasks: inspection.tasks.length,
				conversations,
				timers: timers === undefined ? 0 : await timers.size(),
			} satisfies RpcResult;
		},

		/**
		 * Create a conversation owned by the harness, carrying its forge
		 * session id in the `forge.meta` document and its own forge tool
		 * extension (honoring `replaySafeTools`, the agent's
		 * `toolsAllowlist` (H2.5, enforced by the extension's
		 * `before_tool` hook) and, from H3.5, the `policyAgentId` — the
		 * mule policy engine's agent id for the hook's evaluate calls —
		 * all persisted in the meta document for boot re-install).
		 */
		async createConversation(params) {
			const forgeSessionId = asString(params.forgeSessionId ?? null, "forgeSessionId", false);
			const agent = asObject(params.agent ?? null, "agent");
			const provider = asString(agent.provider ?? null, "agent.provider", false);
			const modelId = asString(agent.modelId ?? null, "agent.modelId", false);
			const systemPrompt = typeof agent.systemPrompt === "string" ? agent.systemPrompt : undefined;
			const extraInstructions = typeof params.extraInstructions === "string" ? params.extraInstructions : undefined;
			const replaySafeTools =
				Array.isArray(params.replaySafeTools) && params.replaySafeTools.every((t) => typeof t === "string")
					? (params.replaySafeTools as string[])
					: [];
			const toolsAllowlist =
				Array.isArray(params.toolsAllowlist) && params.toolsAllowlist.every((t) => typeof t === "string")
					? (params.toolsAllowlist as string[])
					: [];
			// Herd H3.5: the mule policy engine's agent id (the FORGE
			// agent id by the v1 convention); `undefined` when the
			// session has no agent.
			const policyAgentId = typeof params.policyAgentId === "string" && params.policyAgentId.length > 0
				? params.policyAgentId
				: undefined;

			const extensionName = `forge-ext-${Math.random().toString(36).slice(2)}${Date.now().toString(36)}`;
			const extension = createForgeExtension({
				name: extensionName,
				apiUrl,
				apiKey,
				replaySafeTools,
				registry,
				toolsAllowlist,
				...((policyAgentId !== undefined) ? { policyAgentId } : {}),
				harness,
				onSubagent: (event) => {
					events.emit({
						type: "subagent_spawned",
						parentConversationId: event.parentConversationId as ConversationId,
						childConversationId: event.childConversationId as ConversationId,
						childForgeSessionId: event.childForgeSessionId,
						task: event.task,
						detached: event.detached,
					});
				},
			});
			registry.install(extension);

			const instructions = [systemPrompt, extraInstructions].filter((s) => s !== undefined && s.length > 0);
			const conversation = await harness.createConversation(
				{
					ownership: { kind: "ownerless" },
					agent: {
						model: { provider, modelId },
						extensions: [extension],
						...(instructions.length > 0 ? { instructions: instructions.join("\n\n") } : {}),
					},
					init: async (tx, conversationId) => {
						const meta = await tx.doc(ForgeMeta, conversationId, META_KEY, {
							forgeSessionId,
							extensionName,
							replaySafeTools,
							subagent: true,
							toolsAllowlist,
							...((policyAgentId !== undefined) ? { policyAgentId } : {}),
						});
						meta.value = {
							forgeSessionId,
							extensionName,
							replaySafeTools,
							subagent: true,
							toolsAllowlist,
							...((policyAgentId !== undefined) ? { policyAgentId } : {}),
						};
					},
				},
				context,
			);
			return { conversationId: conversation.id };
		},

		/**
		 * Durably admit one submission (a turn, or a passive entry write).
		 * `requestId` gives exactly-once: resubmitting the same request
		 * returns the original submission id (pi-durable
		 * `submissionByRequest`).
		 */
		async submit(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const requestId = asString(params.requestId ?? null, "requestId", false);
			const draft = asObject(params.entryDraft ?? null, "entryDraft");
			const submissionDraft: SubmissionDraft =
				draft.type === "input"
					? { type: "input", requestId, content: draft.content as never }
					: draft.type === "write"
						? { type: "write", requestId, entry: draft.entry as never }
						: (() => {
								throw new RpcError("invalid_params", 'entryDraft.type must be "input" or "write"');
							})();
			const conversation = await requireConversation(deps, conversationId);
			const submission = await conversation.submit(submissionDraft, context);
			return { submissionId: submission.id };
		},

		/**
		 * Herd H2.6 (cutover): bulk-import a batch of entry drafts into an
		 * existing conversation in ONE commit (the lazy-migration import —
		 * forge-api replays a legacy session's `messages` transcript as
		 * `pi.user` / `pi.assistant` / `pi.tool-result` entries before its
		 * first turn). One commit keeps the import atomic and cheap at any
		 * transcript size; the ids are assigned by the session line.
		 */
		async importEntries(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const raw = params.entries;
			if (!Array.isArray(raw)) {
				throw new RpcError("invalid_params", "entries must be an array of entry drafts");
			}
			const MAX_BATCH = 50_000;
			if (raw.length > MAX_BATCH) {
				throw new RpcError("invalid_params", `entries batch exceeds ${MAX_BATCH} drafts`);
			}
			const entries = raw.map((e, i) => {
				const o = asObject(e, `entries[${i}]`);
				if (typeof o.kind !== "string" || o.kind.length === 0) {
					throw new RpcError("invalid_params", `entries[${i}].kind must be a non-empty string`);
				}
				return o as never;
			});
			await requireConversation(deps, conversationId);
			if (entries.length > 0) {
				await harness.commit(
					async (tx) => {
						for (const entry of entries) {
							await tx.appendEntry(conversationId as ConversationId, entry as never);
						}
					},
					context,
				);
			}
			return { imported: entries.length };
		},

		/** Steer the live turn of the task's conversation with new input. */
		async steer(params) {
			const taskId = asNumber(params.taskId ?? null, "taskId");
			const text = asString(params.text ?? null, "text", false);
			const record = await harness.getTask(taskId as TaskId, context);
			if (record === undefined) throw new RpcError("unknown_task", `task ${taskId} does not exist`);
			const conversation = await requireConversation(deps, record.conversationId as number);
			await conversation.submit({ type: "input", content: text, whenBusy: "steer" }, context);
			return null;
		},

		/**
		 * Abort a task. `tree` (default true) aborts the task and every task
		 * it owns; false aborts just the task. pi-durable also cascades the
		 * mark below a cancelled owner on its own — the explicit walk
		 * additionally reaches background descendants, which the ordinary
		 * cascade skips.
		 */
		async abort(params) {
			const taskId = asNumber(params.taskId ?? null, "taskId");
			const tree = params.tree === undefined ? true : params.tree === true;
			const record = await harness.getTask(taskId as TaskId, context);
			if (record === undefined) throw new RpcError("unknown_task", `task ${taskId} does not exist`);
			const targets = new Set<TaskId>([taskId as TaskId]);
			if (tree) {
				// One commit walks the whole task table for the ownership tree.
				const byParent = await harness.commit(
					async (tx) => {
						const map = new Map<number, TaskId[]>();
						let cursor: unknown = undefined;
						for (;;) {
							const page = await tx.scanTasks({}, 256, cursor as never);
							for (const task of page.items) {
								if (task.owner === undefined) continue;
								const kids = map.get(task.owner) ?? [];
								kids.push(task.id);
								map.set(task.owner, kids);
							}
							if (page.next === undefined) break;
							cursor = page.next;
						}
						return map;
					},
					context,
				);
				const frontier: number[] = [taskId];
				while (frontier.length > 0) {
					const parent = frontier.pop()!;
					for (const child of byParent.get(parent) ?? []) {
						if (!targets.has(child)) {
							targets.add(child);
							frontier.push(child as number);
						}
					}
				}
			}
			for (const id of targets) await harness.abortTask(id, context);
			return { aborted: targets.size };
		},

		/** Read a named conversation document; null when absent. */
		async documentGet(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const name = asString(params.name ?? null, "name", false);
			await requireConversation(deps, conversationId);
			const value = (await harness.snapshot(ForgeDocument, conversationId as ConversationId, name, context))
				?.value;
			return (value ?? null) as RpcResult;
		},

		/** Create or replace a named conversation document. */
		async documentPut(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const name = asString(params.name ?? null, "name", false);
			await requireConversation(deps, conversationId);
			await harness.commit(
				async (tx) => {
					const doc = await tx.doc(
						ForgeDocument,
						conversationId as ConversationId,
						name,
						(params.value ?? null) as never,
					);
					doc.value = (params.value ?? null) as never;
				},
				context,
			);
			return null;
		},

		/**
		 * Set a timer on a conversation. Exactly one of `at` (absolute epoch
		 * ms) or `cron` (5-field expression) plus the `prompt` that becomes
		 * the fired turn.
		 */
		async timerSet(params) {
			if (timers === undefined) throw new RpcError("timers_disabled", "timers are disabled in this harness (no timer store)");
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const prompt = asString(params.prompt ?? null, "prompt", false);
			const at = params.at === undefined ? undefined : asNumber(params.at, "at");
			const cron = params.cron === undefined ? undefined : asString(params.cron, "cron", false);
			await requireConversation(deps, conversationId);
			const timerId = await timers.set(conversationId, { at, cron, prompt });
			return { timerId };
		},

		/** Clear a timer. Result reports whether it existed. */
		async timerClear(params) {
			if (timers === undefined) throw new RpcError("timers_disabled", "timers are disabled in this harness (no timer store)");
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const timerId = asString(params.timerId ?? null, "timerId", false);
			return { cleared: await timers.clear(conversationId, timerId) };
		},

		/**
		 * List a conversation's live timers (H2.3): the Postgres-backed
		 * rows behind `timerSet` (one-shots only while un-fired; cron
		 * timers for their whole lifetime). `conversationId` is optional
		 * (the harness-wide listing).
		 */
		async timerList(params) {
			if (timers === undefined) throw new RpcError("timers_disabled", "timers are disabled in this harness (no timer store)");
			const conversationId = params.conversationId === undefined ? undefined : asNumber(params.conversationId, "conversationId");
			return { timers: await timers.list(conversationId) };
		},

		/**
		 * H2.4: force a manual compaction of the conversation now. Creates
		 * the built-in pi-durable CompactionTask (reason `manual`,
		 * conversation-owned): the summary is made in the background, the
		 * conversation keeps working, and the summary lands at once when
		 * idle or at the next turn boundary. Returns the task id; follow
		 * it through `task_state` events / `compactionStatus`.
		 */
		async compact(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const instructions = typeof params.instructions === "string" && params.instructions.length > 0 ? (params.instructions as string) : undefined;
			const conversation = await requireConversation(deps, conversationId);
			const taskId = await conversation.compact(instructions, context);
			return { taskId: taskId as number };
		},

		/**
		 * H2.4: start a new context from a handoff note (pi-durable
		 * `reset()`): the model no longer sees older entries, but they
		 * stay in storage (the `history?q=` read path searches them). The
		 * reset is admitted as a write submission: placed at once when
		 * idle, otherwise at the next turn boundary.
		 */
		async reset(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const handoffNote =
				params.handoffNote === undefined || params.handoffNote === null
					? undefined
					: asString(params.handoffNote, "handoffNote");
			const conversation = await requireConversation(deps, conversationId);
			await conversation.reset(handoffNote, context);
			return null;
		},

		/**
		 * H2.4: compaction + active-window report for a conversation:
		 * the live compaction tasks (from `pi.live`), the newest
		 * `pi.compaction` entry (any segment), and the ACTIVE context
		 * size (post-compaction/reset window — the pre-compaction entries
		 * are excluded by the head marker).
		 */
		async compactionStatus(params) {
			const conversationId = asNumber(params.conversationId ?? null, "conversationId");
			const conversation = await requireConversation(deps, conversationId);
			const live = await harness.snapshot(LiveDoc, conversationId as ConversationId, context);
			const compactions = (live?.compactions ?? []).map((c) => ({
				taskId: c.taskId,
				reason: c.reason,
				blocking: c.blocking,
				attempt: c.attempt,
			}));
			// Newest `pi.compaction` entry (walk newest-first pages until
			// one is found; conversations compact at most a few times in
			// practice, and most have none at all).
			let lastCompaction: { entryId: number; reason: string; summaryChars: number } | null = null;
			{
				let cursor: Cursor | undefined = undefined;
				outer: for (;;) {
					const page = await conversation.entries({}, 32, cursor, context);
					for (const entry of page.items) {
						if (entry.kind === "pi.compaction") {
							const reason = (entry.data as { reason?: unknown } | undefined)?.reason;
							lastCompaction = {
								entryId: entry.id,
								reason: typeof reason === "string" ? reason : "unknown",
								summaryChars: compactionSummaryChars(entry.model),
							};
							break outer;
						}
					}
					if (page.next === undefined) break;
					cursor = page.next;
				}
			}
			const view = await conversation.context(context);
			return {
				compactions,
				lastCompaction,
				activeContextChars: contextChars(view),
				activeEntryCount: view.entries.length - (view.head === undefined ? 0 : 1),
			};
		},
	};
}

/* ------------------------------------------------------------------ */
/* RPC socket server                                                   */
/* ------------------------------------------------------------------ */

export interface RpcServer {
	readonly socketPath: string;
	/** Resolves when the socket is listening; rejects on bind failure. */
	readonly ready: Promise<void>;
	readonly close: () => Promise<void>;
}

/**
 * Unix-socket JSON-RPC server: one JSON object per line. Request
 * `{"id","method","params"}` → `{"id","result"}` or
 * `{"id","error":{"code","message"}}`.
 */
export function startRpcServer(socketPath: string, handlers: HandlerMap): RpcServer {
	let server: Server;
	const ready = new Promise<void>((resolve, reject) => {
		server = createServer((socket) => {
			let buffer = "";
			socket.setEncoding("utf8");
			socket.on("data", (chunk) => {
				buffer += chunk;
				let newline = buffer.indexOf("\n");
				while (newline !== -1) {
					const line = buffer.slice(0, newline);
					buffer = buffer.slice(newline + 1);
					newline = buffer.indexOf("\n");
					if (line.length === 0) continue;
					void dispatchLine(socket, line, handlers);
				}
			});
			socket.on("error", () => {}); // client vanished mid-line
		});
		server.on("error", (error) => reject(error));
		server.listen(socketPath, () => resolve());
	});
	return {
		socketPath,
		ready,
		close: async () => {
			await ready.catch(() => {});
			await new Promise<void>((resolve) => server.close(() => resolve()));
		},
	};
}

async function dispatchLine(socket: Socket, line: string, handlers: HandlerMap): Promise<void> {
	let request: { id?: unknown; method?: unknown; params?: unknown };
	try {
		request = JSON.parse(line);
	} catch {
		socket.write(
			`${JSON.stringify({ id: null, error: { code: "bad_request", message: "line is not a JSON object" } })}\n`,
		);
		return;
	}
	const id = request.id ?? null;
	const method = typeof request.method === "string" ? request.method : undefined;
	const handler = method === undefined ? undefined : handlers[method];
	if (method === undefined || handler === undefined) {
		socket.write(
			`${JSON.stringify({ id, error: { code: "unknown_method", message: `no such method: ${String(method)}` } })}\n`,
		);
		return;
	}
	try {
		const result = await handler(request.params === undefined ? {} : asObject(request.params, "params"));
		socket.write(`${JSON.stringify({ id, result })}\n`);
	} catch (error) {
		const code = error instanceof RpcError ? error.code : "internal";
		const message = error instanceof Error ? error.message : String(error);
		socket.write(`${JSON.stringify({ id, error: { code, message } })}\n`);
	}
}

/**
 * Send one request to an RPC socket (used by tests and ops tooling).
 * Resolves with `result`, rejects with the RPC error (code on the Error).
 */
export function rpcRequest(
	socket: Socket,
	id: string | number,
	method: string,
	params: Record<string, unknown> = {},
): Promise<unknown> {
	const line = `${JSON.stringify({ id, method, params })}\n`;
	return new Promise((resolve, reject) => {
		const cleanup = () => socket.off("data", onData);
		const onData = (chunk: Buffer) => {
			const text = chunk.toString("utf8");
			const idx = text.indexOf("\n");
			if (idx === -1) return;
			cleanup();
			try {
				const reply = JSON.parse(text.slice(0, idx));
				if (reply.id !== id) return; // mismatched/late reply
				if (reply.error !== undefined) {
					const error = new Error(reply.error.message ?? "rpc error") as Error & { code: string };
					error.code = reply.error.code;
					reject(error);
				} else {
					resolve(reply.result);
				}
			} catch (parseError) {
				reject(parseError);
			}
		};
		socket.on("data", onData);
		socket.write(line);
	});
}

/* ------------------------------------------------------------------ */
/* Events push channel                                                 */
/* ------------------------------------------------------------------ */

export interface EventsServer {
	readonly socketPath: string;
	/** Resolves when the socket is listening; rejects on bind failure. */
	readonly ready: Promise<void>;
	readonly close: () => Promise<void>;
	/** Test/ops hook: whether a client is connected. */
	readonly connected: () => boolean;
}

/**
 * Unix-socket event channel: the harness LISTENS; forge-api connects.
 * One client at a time — a second connection displaces the first (logged,
 * not an error). Events are fire-and-forget JSON lines; there is NO replay
 * on reconnect. forge-api resynchronizes through its API instead (the
 * messages/transcript tables are the source of truth; H2.1).
 */
export function startEventsServer(
	socketPath: string,
	events: EventBus,
	log: (message: string) => void = console.error,
): EventsServer {
	let client: Socket | undefined;
	let server: Server;
	const ready = new Promise<void>((resolve, reject) => {
		server = createServer((socket) => {
			if (client !== undefined && client !== socket) {
				log("harness events: second client connected; displacing the first");
				client.destroy();
			}
			client = socket;
			socket.write(`${JSON.stringify({ type: "hello", version: "0.1.0" })}\n`);
			socket.on("error", () => {});
			socket.on("close", () => {
				if (client === socket) client = undefined;
			});
		});
		server.on("error", (error) => reject(error));
		server.listen(socketPath, () => resolve());
	});
	const push = (event: HarnessEvent) => {
		const socket = client;
		if (socket === undefined || socket.destroyed) return;
		try {
			socket.write(`${JSON.stringify(event)}\n`);
		} catch {
			// The socket dies on its own; close() will clear it.
		}
	};
	const unsubscribe = events.subscribe(push);
	return {
		socketPath,
		ready,
		connected: () => client !== undefined && !client.destroyed,
		close: async () => {
			await ready.catch(() => {});
			unsubscribe();
			if (client !== undefined) client.destroy();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		},
	};
}
