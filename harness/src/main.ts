/**
 * The forge harness process.
 *
 * Boot sequence:
 *   1. Read the environment (see README / `readConfig`).
 *   2. Open `PgStorage` (durable-pg) over the forge Postgres database; pending
 *      migrations apply on open.
 *   3. Build the pi-ai model catalog (built-in providers; credentials from
 *      the standard env vars — OPENAI_API_KEY, ANTHROPIC_API_KEY, …; or the
 *      faux provider when FORGE_HARNESS_FAUX=1).
 *   4. `Harness.open(...)` — pi-durable's open path reconciles unfinished
 *      work from a previous process (running → pending, checkpoints and memos
 *      intact) before the scheduler is allowed to run.
 *   5. Re-install per-conversation forge extensions for conversations that
 *      already exist (conversations store extension NAMES, not code; the
 *      flags come from each conversation's `forge.meta` document).
 *   6. `harness.resume()` — self-supervision: every unfinished task (turns,
 *      tool calls, queued submissions) is scheduled again.
 *   7. Start the RPC socket and the event channel.
 *
 * SIGTERM/SIGINT → close the servers, `harness.close(context)` (which closes
 * storage and its pool), exit 0.
 */
import { existsSync, mkdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Context } from "@earendil-works/chord";
import { createModels, fauxAssistantMessage, fauxProvider, fauxToolCall, type Models } from "@earendil-works/pi-ai";
import {
	AgentDoc,
	Harness,
	createRegistry,
	type ConversationId,
	type HarnessSettings,
	type Storage,
} from "@earendil-works/pi-durable";
import { Pool } from "pg";
import { PgStorage } from "@forge/durable-pg";
import { createForgeExtension } from "./forge-ext.js";
import { ForgeMeta, META_KEY } from "./docs.js";
import { maybeEnqueueCompaction } from "./compaction.js";
import { ensureHistoryIndex } from "./history-index.js";
import { EventBus, watchCommits } from "./events.js";
import { TimerStore } from "./timer-store.js";
import {
	makeHandlers,
	startEventsServer,
	startRpcServer,
	type EventsServer,
	type RpcServer,
} from "./ipc.js";
import { TimerRegistry } from "./timers.js";

/** Harness version reported by the `status` RPC method and the events hello. */
export const HARNESS_VERSION = "0.1.0";

/** One process context for every harness call (no caller cancellation). */
const context = { get: () => undefined } as unknown as Context;

type Log = (entry: Record<string, unknown>) => void;

function defaultLog(entry: Record<string, unknown>): void {
	console.error(JSON.stringify(entry));
}

export interface HarnessConfig {
	readonly databaseUrl: string;
	readonly apiUrl: string;
	readonly apiKey: string;
	/** Optional schema the durable_* tables live in (pins search_path). */
	readonly schema?: string;
	readonly rpcSocket: string;
	readonly eventsSocket: string;
}

/** Resolve the harness configuration from an environment map. */
export function readConfig(
	env: Record<string, string | undefined> = process.env,
): HarnessConfig {
	const databaseUrl = env.FORGE_DATABASE_URL;
	const apiKey = env.FORGE_API_KEY;
	if (databaseUrl === undefined || databaseUrl.length === 0) {
		throw new Error("FORGE_DATABASE_URL is required (e.g. postgres://postgres@localhost/forge)");
	}
	if (apiKey === undefined || apiKey.length === 0) {
		throw new Error("FORGE_API_KEY is required (a real forge API key for /tools/execute)");
	}
	const home = env.HOME ?? homedir();
	return {
		databaseUrl,
		apiUrl: env.FORGE_API_URL ?? "http://127.0.0.1:8080",
		apiKey,
		schema: env.FORGE_HARNESS_SCHEMA || undefined,
		rpcSocket: env.FORGE_HARNESS_SOCKET ?? join(home, ".local/state/forge/harness.sock"),
		eventsSocket: env.FORGE_HARNESS_EVENTS_SOCKET ?? join(home, ".local/state/forge/harness-events.sock"),
	};
}

/**
 * Build the model catalog. pi-ai's built-in providers (openai, anthropic,
 * google, …) resolve credentials from the standard environment variables
 * themselves — the harness does not plumb per-machine model credentials
 * yet; that forge-side plumbing is a later task. FORGE_HARNESS_FAUX=1 adds
 * the faux provider for tests and dry runs.
 *
 * FORGE_HARNESS_FAUX_TOKENS_PER_SEC (only honored with FORGE_HARNESS_FAUX=1)
 * paces the faux provider's token stream (pi-ai `tokensPerSecond`). The
 * H2.6 dual kill -9 test uses a low rate (e.g. 5) so a turn takes long
 * enough to be killed mid-stream and recovered on the second process.
 *
 * FORGE_HARNESS_FAUX_RESPONSES (JSON array, only honored when
 * FORGE_HARNESS_FAUX=1) pre-queues faux assistant answers, one per
 * generation call, so a scripted turn has something to say. Each item is
 * either a string (a plain text answer) or an object
 * `{ "toolCall": { "name", "input" } }` (an answer whose stop reason is
 * `toolUse` and which calls the named tool — H2.2: the forge-api
 * integration tests script a `spawn_subagent` call this way). The faux
 * provider's default queue is EMPTY — an unqueued call answers with an
 * error ("No more faux responses queued") and the turn fails — so a
 * test that drives a turn must queue one response per generation call
 * it expects.
 */
export function buildModels(env: Record<string, string | undefined> = process.env): Models {
	const models = createModels();
	if (env.FORGE_HARNESS_FAUX === "1") {
		const tps = env.FORGE_HARNESS_FAUX_TOKENS_PER_SEC
			? Number.parseInt(env.FORGE_HARNESS_FAUX_TOKENS_PER_SEC, 10)
			: undefined;
		if (tps !== undefined && (!Number.isFinite(tps) || tps <= 0)) {
			throw new Error("FORGE_HARNESS_FAUX_TOKENS_PER_SEC must be a positive number");
		}
		const faux = fauxProvider(tps !== undefined ? { tokensPerSecond: tps } : undefined);
		if (env.FORGE_HARNESS_FAUX_RESPONSES !== undefined && env.FORGE_HARNESS_FAUX_RESPONSES.length > 0) {
			const parsed: unknown = JSON.parse(env.FORGE_HARNESS_FAUX_RESPONSES);
			if (!Array.isArray(parsed)) {
				throw new Error("FORGE_HARNESS_FAUX_RESPONSES must be a JSON array");
			}
			for (const item of parsed) {
				if (typeof item === "string") {
					faux.appendResponses([fauxAssistantMessage(item)]);
				} else if (item !== null && typeof item === "object" && "toolCall" in item) {
					const call = (item as { toolCall: unknown }).toolCall;
					if (call === null || typeof call !== "object" || !("name" in call) || typeof call.name !== "string") {
						throw new Error('FORGE_HARNESS_FAUX_RESPONSES toolCall item must be { "toolCall": { "name", "input" } }');
					}
					const input = "input" in call && call.input !== undefined ? call.input : {};
					faux.appendResponses([fauxAssistantMessage([fauxToolCall(call.name, input as Parameters<typeof fauxToolCall>[1])], { stopReason: "toolUse" })]);
				} else {
					throw new Error("FORGE_HARNESS_FAUX_RESPONSES items must be strings or { toolCall } objects");
				}
			}
		}
		models.setProvider(faux.provider);
	}
	return models;
}

export interface StartOptions {
	/** Pre-opened storage (tests). Otherwise `databaseUrl` is required. */
	readonly storage?: Storage;
	readonly databaseUrl?: string;
	/** Which schema the durable_* tables (and `harness_timers`) live in
	 * (default `public`). */
	readonly schema?: string;
	/** Injected model catalog (tests). Otherwise built from the environment. */
	readonly models?: Models;
	readonly apiUrl: string;
	readonly apiKey: string;
	/** Listen on the RPC socket (omit for in-process use). */
	readonly rpcSocket?: string;
	/** Listen on the events socket (omit for in-process use). */
	readonly eventsSocket?: string;
	/**
	 * Pool for the harness-owned `harness_timers` table (H2.3). Omit to
	 * DISABLE timers (in-process consumers that never use them); the
	 * process entry always supplies one.
	 */
	readonly timerPool?: Pool;
	/** Timer clock multiplier (tests): pair with a `now()` that runs
	 * `timerScale` times faster than the wall clock. */
	readonly timerScale?: number;
	/** pi-durable run policy shared by every conversation (H2.4: the
	 * `compaction.keepRecentTokens` floor below which the built-in
	 * `selectCut` finds nothing to summarize is set here; the
	 * per-conversation TRIGGER threshold lives in the conversation's
	 * `config` document). Omit for the built-in defaults. */
	readonly settings?: HarnessSettings;
	readonly now?: () => number;
	readonly log?: Log;
}

export interface HarnessHandle {
	readonly context: Context;
	readonly storage: Storage;
	readonly harness: Harness;
	readonly registry: ReturnType<typeof createRegistry>;
	readonly timers: TimerRegistry | undefined;
	readonly events: EventBus;
	readonly handlers: ReturnType<typeof makeHandlers>;
	readonly rpc?: RpcServer;
	readonly eventsServer?: EventsServer;
	/** Close the servers, the harness (and with it, owned storage/pool), timers. */
	stop(): Promise<void>;
}

function isJsonObject(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Re-install the forge tool extensions of every existing conversation so
 * their stored agent configs resolve against the registry again after a
 * process (re)start. A fresh machine with an empty database installs
 * nothing.
 */
async function reinstallConversationExtensions(args: {
	harness: Harness;
	registry: ReturnType<typeof createRegistry>;
	apiUrl: string;
	apiKey: string;
	onSubagent?: (event: import("./subagent.js").SubagentSpawnedEvent) => void;
}): Promise<number> {
	const { harness, registry } = args;
	const ids = await harness.commit(async (tx) => {
		const found: ConversationId[] = [];
		let cursor: unknown = undefined;
		for (;;) {
			const page = await tx.scanConversations({}, 256, cursor as never);
			found.push(...page.items.map((record) => record.id));
			if (page.next === undefined) break;
			cursor = page.next;
		}
		return found;
	}, context);
	let reinstalled = 0;
	for (const id of ids) {
		const agent = await harness.snapshot(AgentDoc, id, context);
		if (agent === undefined) continue;
		const stored = agent.extensions;
		const names: readonly unknown[] = Array.isArray(stored) ? stored : (stored?.add ?? []);
		for (const name of names) {
			if (typeof name !== "string" || !name.startsWith("forge-ext-")) continue;
			if (registry.snapshot().extension(name) !== undefined) continue;
			const meta = (await harness.snapshot(ForgeMeta, id, META_KEY, context))?.value;
			const replaySafeTools =
				isJsonObject(meta) && Array.isArray(meta.replaySafeTools)
					? meta.replaySafeTools.filter((t): t is string => typeof t === "string")
					: [];
			const storedTools =
				isJsonObject(meta) && Array.isArray(meta.tools)
					? meta.tools.filter((t): t is string => typeof t === "string")
					: undefined;
			// `subagent` defaults to true: it is absent from meta written
			// before H2.2 (every such conversation offers the tool).
			const subagent = isJsonObject(meta) ? meta.subagent !== false : true;
			// H2.5: the agent's tool allowlist (absent on pre-H2.5 meta
			// documents ⇒ no allowlist, same as today).
			const toolsAllowlist =
				isJsonObject(meta) && Array.isArray(meta.toolsAllowlist)
					? meta.toolsAllowlist.filter((t): t is string => typeof t === "string")
					: [];
			registry.install(
				createForgeExtension({
					name,
					apiUrl: args.apiUrl,
					apiKey: args.apiKey,
					replaySafeTools,
					registry: args.registry,
					onSubagent: args.onSubagent,
					...(storedTools !== undefined && storedTools.length > 0 ? { tools: storedTools } : {}),
					subagent,
					toolsAllowlist,
				}),
			);
			reinstalled++;
		}
	}
	return reinstalled;
}

/** Run one boot sequence; see the module header for the ordered steps. */
export async function startHarness(options: StartOptions): Promise<HarnessHandle> {
	const log = options.log ?? defaultLog;
	const now = options.now ?? (() => Date.now());

	// 1 — storage
	// A missing schema would not fail `SET search_path` — Postgres silently
	// falls back to `public` and every table would land there. Create it.
	if (options.schema !== undefined && options.storage === undefined) {
		const { Pool } = await import("pg");
		if (!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(options.schema)) {
			throw new Error(`invalid schema name: ${options.schema}`);
		}
		const admin = new Pool({ connectionString: options.databaseUrl ?? process.env.FORGE_DATABASE_URL });
		try {
			await admin.query(`CREATE SCHEMA IF NOT EXISTS ${options.schema}`);
		} finally {
			await admin.end();
		}
	}
	const storage =
		options.storage ??
		(await PgStorage.open(
			options.databaseUrl === undefined
				? (() => {
						throw new Error("startHarness: provide storage or databaseUrl");
					})()
				: {
						connectionString: options.databaseUrl,
						...(options.schema !== undefined ? { schema: options.schema } : {}),
					},
		));

	// 2 — models
	const models = options.models ?? buildModels();

	// 3 — registry (forge extensions are installed per conversation)
	const registry = createRegistry();

	// 4 — the harness; open reconciles unfinished work from a dead process
	const harness = await Harness.open(
		storage,
		{
			models,
			registry,
			now,
			...(options.settings !== undefined ? { settings: options.settings } : {}),
			onReport: (error) =>
				log({
					level: "error",
					msg: "pi-durable report",
					error: error instanceof Error ? error.message : String(error),
				}),
		},
		context,
	);

	// 5 — events: commit publications → event vocabulary
	const events = new EventBus();
	const unsubscribeCommits = watchCommits(harness, events);

	// 5.5 — H2.4: the compaction threshold lives in the harness. After
	// every committed `pi.assistant` entry (a turn boundary, or the end
	// of an answer segment) the conversation's configured threshold
	// (its `config` document, defaults = today's forge-api heuristic)
	// is checked; above it the built-in compaction task is enqueued as a
	// conversation-owned BACKGROUND task — it never interrupts an
	// in-flight turn, and its summary lands at the next boundary.
	const unsubscribeCompactionWatch = harness.subscribeCommits((publication) => {
		for (const change of publication.changes) {
			if (change.type === "entry" && change.value.kind === "pi.assistant") {
				void maybeEnqueueCompaction({
					harness,
					conversationId: change.value.conversationId as number,
					context,
					log: (record) => log({ ...record }),
				}).catch((error) =>
						log({
							level: "warn",
							msg: "threshold compaction check failed",
							conversationId: change.value.conversationId,
							error: error instanceof Error ? error.message : String(error),
						}),
					);
				}
			}
		});

	// 6 — timers (H2.3): Postgres-backed, re-armed on boot, overdue ones
	// fire exactly once through the atomic claim (timer-store.ts).
	let timers: TimerRegistry | undefined;
	if (options.timerPool !== undefined) {
		const timerStore = new TimerStore(options.timerPool, options.schema ?? "public");
		await timerStore.ensureSchema();
		timers = new TimerRegistry(timerStore, {
			submit: async (conversationId, content, requestId) => {
				const conversation = await harness.conversation(conversationId as ConversationId, context);
				if (conversation === undefined) throw new Error(`conversation ${conversationId} is gone`);
				// The request id makes a re-submission after any restart dedupe
				// to the original (pi-durable submissionByRequest).
				await conversation.submit({ type: "input", content, requestId } as never, context);
			},
			onFire: (fire) => {
				events.emit({
					type: "timer_fired",
					timerId: fire.timerId,
					conversationId: fire.conversationId as ConversationId,
					prompt: fire.prompt,
				});
			},
			now,
			...(options.timerScale !== undefined ? { scale: options.timerScale } : {}),
		});
		// Re-arm every live timer; overdue ones fire now (exactly once).
		const reloaded = await timers.reload();
		log({ level: "info", msg: "timers reloaded", liveTimers: reloaded });
	}

	// 6.5 — H2.4: the trigram/GIN companion index behind forge-api's
	// `GET /sessions/:id/history?q=` (guarded DDL; portable ILIKE runs
	// without it). Uses the timer pool (one small harness-owned pool
	// for harness-owned DDL); skipped when timers are disabled.
	if (options.timerPool !== undefined) {
		const index = await ensureHistoryIndex(options.timerPool, options.schema ?? "public", (message) =>
			log({ level: "info", msg: message }),
		);
		log({ level: "info", msg: "history index ensured", trigram: index.trigram });
	}

	// 7 — re-install conversation extensions, then self-supervise
	const reinstalled = await reinstallConversationExtensions({
		harness,
		registry,
		apiUrl: options.apiUrl,
		apiKey: options.apiKey,
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
	const inspection = await harness.inspect(context);
	harness.resume();
	log({
		level: "info",
		msg: "harness booted",
		activeTasks: inspection.tasks.length,
		queuedSubmissions: inspection.submissions.length,
		reinstalledExtensions: reinstalled,
	});

	// 8 — handler map
	const handlers = makeHandlers({
		harness,
		registry,
		timers,
		events,
		version: HARNESS_VERSION,
		apiUrl: options.apiUrl,
		apiKey: options.apiKey,
		context,
	});

	// 9 — sockets
	let rpc: RpcServer | undefined;
	let eventsServer: EventsServer | undefined;
	if (options.rpcSocket !== undefined) {
		const path = options.rpcSocket;
		if (existsSync(path)) rmSync(path);
		mkdirSync(dirname(path), { recursive: true });
		rpc = startRpcServer(path, handlers);
		await rpc.ready.catch((error) => {
			throw new Error(`RPC socket ${path}: ${error instanceof Error ? error.message : String(error)}`);
		});
	}
	if (options.eventsSocket !== undefined) {
		const path = options.eventsSocket;
		if (existsSync(path)) rmSync(path);
		mkdirSync(dirname(path), { recursive: true });
		eventsServer = startEventsServer(path, events, (message) => log({ level: "info", msg: message }));
		await eventsServer.ready.catch((error) => {
			throw new Error(`events socket ${path}: ${error instanceof Error ? error.message : String(error)}`);
		});
	}

	return {
		context,
		storage,
		harness,
		registry,
		timers,
		events,
		handlers,
		rpc,
		eventsServer,
		stop: async () => {
			log({ level: "info", msg: "harness shutting down" });
			unsubscribeCommits();
			unsubscribeCompactionWatch();
			timers?.dispose();
			await eventsServer?.close().catch(() => {});
			await rpc?.close().catch(() => {});
			await harness.close(context).catch((error) =>
				log({
					level: "warn",
					msg: "harness close failed",
					error: error instanceof Error ? error.message : String(error),
				}),
			);
		},
	};
}

/* ------------------------------------------------------------------ */
/* process entry                                                       */
/* ------------------------------------------------------------------ */

async function main(): Promise<void> {
	const config = readConfig();
	// A small dedicated pool for the harness-owned timer table (H2.3);
	// PgStorage keeps its own pool for the durable_* tables.
	const timerPool = new Pool({ connectionString: config.databaseUrl, max: 2 });
	const handle = await startHarness({
		databaseUrl: config.databaseUrl,
		schema: config.schema,
		apiUrl: config.apiUrl,
		apiKey: config.apiKey,
		timerPool,
		rpcSocket: config.rpcSocket,
		eventsSocket: config.eventsSocket,
	});
	defaultLog({
		level: "info",
		msg: "listening",
		version: HARNESS_VERSION,
		rpc: handle.rpc?.socketPath,
		events: handle.eventsServer?.socketPath,
	});

	let stopping = false;
	const stop = async (signal: string): Promise<void> => {
		if (stopping) return;
		stopping = true;
		defaultLog({ level: "info", msg: `received ${signal}, stopping` });
		await handle.stop().catch(() => {});
		await timerPool.end().catch(() => {});
		process.exit(0);
	};
	process.on("SIGTERM", () => void stop("SIGTERM"));
	process.on("SIGINT", () => void stop("SIGINT"));
}

const isMain = (() => {
	try {
		return import.meta.url === pathToFileURL(process.argv[1] ?? "").href;
	} catch {
		return false;
	}
})();

if (isMain) {
	main().catch((error) => {
		console.error(
			JSON.stringify({
				level: "fatal",
				msg: "harness failed to start",
				error: error instanceof Error ? error.stack ?? error.message : String(error),
			}),
		);
		process.exit(1);
	});
}
