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
import { existsSync, mkdirSync, readFileSync, rmSync } from "node:fs";

import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import type { Context } from "@earendil-works/chord";
import { createModels, createProvider, fauxAssistantMessage, fauxProvider, fauxToolCall, type Models, type MutableModels } from "@earendil-works/pi-ai";
import { getApiProvider, registerBuiltInApiProviders } from "@earendil-works/pi-ai/compat";
import { builtinProviders } from "@earendil-works/pi-ai/providers/all";
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
import { agentPausedForConversation } from "./agent-pause.js";
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
	/** Herd H6.5c: mule wake engine (create_agent tool's schedule
	 * surface). Absent = the tool provisions the agent row only and
	 * says the wake was skipped. */
	readonly muleUrl?: string;
	readonly muleApiKey?: string;
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
		muleUrl: env.MULE_URL || undefined,
		muleApiKey: env.MULE_API_KEY || undefined,
	};
}

/**
 * Build the model catalog. pi-ai's built-in providers (openai, anthropic,
 * google, …) resolve credentials from the standard environment variables
 * themselves; non-builtin providers (e.g. the lab LLM proxy's "proxy"
 * provider) come from the machine's pi `models.json` — see
 * loadMachineModels / applyModelsJson below (FORGE_HARNESS_MODELS
 * overrides the default /root/.pi/agent/models.json path). The test
 * harnesses use FORGE_HARNESS_FAUX=1 instead, which adds the faux
 * provider for tests and dry runs.
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
	// Built-in providers (openai, anthropic, …). createModels() starts EMPTY —
	// pi-durable receives runtime.models prebuilt, so the harness is the only
	// registration site; without this, even a stock openai profile would
	// fail with no_model. Machine models.json and faux layer on top and may
	// supersede a builtin id.
	for (const p of builtinProviders()) models.setProvider(p);
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
	loadMachineModels(models, env);
	return models;
}

/**
 * Machine-level model catalog. pi's `models.json` (the shape pi itself
 * reads from `~/.pi/agent/models.json`) declares custom
 * OpenAI/Anthropic-compatible endpoints and their model catalogs; without
 * registering them here, a profile that references a non-builtin provider
 * (e.g. `proxy/<model>` for the lab LLM proxy) fails at turn start with
 * `no_model`. The harness runs as the root /opt/forge service, where the
 * pi-config unit drops the file; `FORGE_HARNESS_MODELS` overrides the path
 * (tests, non-standard layouts). A missing file is normal (builtin-
 * providers-only machines) and silently ignored.
 */
export const MACHINE_MODELS_DEFAULT_PATH = "/root/.pi/agent/models.json";

export function applyModelsJson(models: MutableModels, json: unknown): void {
	if (json === null || typeof json !== "object" || !("providers" in json)) return;
	const providers = (json as { providers?: unknown }).providers;
	if (providers === null || typeof providers !== "object") return;
	registerBuiltInApiProviders();
	for (const [id, raw] of Object.entries(providers as Record<string, unknown>)) {
		if (raw === null || typeof raw !== "object") continue;
		const cfg = raw as {
			name?: string;
			baseUrl?: string;
			api?: string;
			apiKey?: string;
			compat?: Record<string, unknown>;
			models?: unknown;
		};
		const apiId = cfg.api ?? "openai-completions";
		const impl = getApiProvider(apiId as "openai-completions");
		if (!impl) {
			// models.json only configures SUPPORTED endpoints; an unknown
			// wire API needs a pi provider extension, which the harness does
			// not load.
			console.warn(`[harness] models.json: skipping provider "${id}": no built-in implementation for api "${apiId}"`);
			continue;
		}
		if (!Array.isArray(cfg.models) || cfg.models.length === 0) {
			console.warn(`[harness] models.json: provider "${id}" declares no models; skipping`);
			continue;
		}
		// createProvider does NOT rewrite the entries (it dispatches on
		// model.api and pi-ai streams on model.provider), so the
		// provider-scoped fields the models.json entries omit must be
		// filled here: api (provider-level default), provider, baseUrl.
		// Provider-level compat applies to every model unless the model
		// carries its own.
		const modelList = (cfg.models as Record<string, unknown>[]).map((m) => ({
			...m,
			api: (typeof m.api === "string" && m.api) || apiId,
			provider: id,
			baseUrl: (typeof m.baseUrl === "string" && m.baseUrl) || cfg.baseUrl || "",
			compat: (m.compat as Record<string, unknown> | undefined) ?? cfg.compat,
			type: "chat",
		}));
		try {
			models.setProvider(
				createProvider({
					id,
					name: cfg.name ?? id,
					baseUrl: cfg.baseUrl,
					auth: {
						apiKey: {
							name: `${id} api key`,
							resolve: async () => ({
								auth: { apiKey: cfg.apiKey ?? "not-needed" },
								source: cfg.apiKey ? `models.json (${id})` : "models.json (keyless)",
							}),
						},
					},
					models: modelList as unknown as Parameters<typeof createProvider>[0]["models"],
					api: { [apiId]: { stream: impl.stream, streamSimple: impl.streamSimple } },
				}),
			);
		} catch (err) {
			console.warn(`[harness] models.json: provider "${id}" failed to register: ${String(err)}`);
		}
	}
}

function loadMachineModels(models: MutableModels, env: Record<string, string | undefined>): void {
	const path = env.FORGE_HARNESS_MODELS ?? MACHINE_MODELS_DEFAULT_PATH;
	if (!existsSync(path)) return;
	try {
		applyModelsJson(models, JSON.parse(readFileSync(path, "utf-8")));
	} catch (err) {
		console.warn(`[harness] failed to load machine models at ${path}: ${String(err)}`);
	}
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
	/** Mule wake engine for the create_agent tool (H6.5c). */
	readonly muleUrl?: string;
	readonly muleApiKey?: string;
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
	muleUrl?: string;
	muleApiKey?: string;
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
			// Herd H5.1: research conversations rebuild their
			// read-only-by-construction registry (the pinned `tools`
			// subset `["read"]` + the research tools, no subagent).
			const research = isJsonObject(meta) ? meta.research === true : false;
			// H3.5: the mule policy engine's agent id (absent on pre-H3.5
			// meta documents ⇒ the hook falls back to session:<id>).
			const policyAgentId = isJsonObject(meta) && typeof meta.policyAgentId === "string" ? meta.policyAgentId : undefined;
			registry.install(
				createForgeExtension({
					name,
					apiUrl: args.apiUrl,
					apiKey: args.apiKey,
					...(args.muleUrl ? { muleUrl: args.muleUrl } : {}),
					...(args.muleApiKey ? { muleApiKey: args.muleApiKey } : {}),
					replaySafeTools,
					registry: args.registry,
					onSubagent: args.onSubagent,
					...(research
						? { tools: storedTools !== undefined && storedTools.length > 0 ? storedTools : ["read"], research: true }
						: {}),
					...(!research && storedTools !== undefined && storedTools.length > 0 ? { tools: storedTools } : {}),
					subagent,
					toolsAllowlist,
					...((policyAgentId !== undefined) ? { policyAgentId } : {}),
					harness,
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
			// Herd H5.3 kill switch: at the fire seam, refuse the prompt
			// when the owning agent is paused (`agents.paused`, migration
			// 026) — the skipped tick is logged by the registry and the
			// claimed row does not re-fire.
			agentPaused: (conversationId) => agentPausedForConversation(options.timerPool, conversationId),
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
		muleUrl: options.muleUrl,
		muleApiKey: options.muleApiKey,
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
		muleUrl: options.muleUrl,
		muleApiKey: options.muleApiKey,
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
		muleUrl: config.muleUrl,
		muleApiKey: config.muleApiKey,
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
