/**
 * Herd H2.3 acceptance: durable timers.
 *
 *  (a) a real harness CHILD PROCESS has a ~2s `at` timer set, is
 *      SIGKILLed during it (process.kill("SIGKILL")), and a fresh child
 *      process is started on the same schema → the prompt fires
 *      EXACTLY ONCE (one `timer fired:` entry, one
 *      `timer-fired:<id>:<firedAt>` submission);
 *  (b) an overdue timer (row with a past `at`) present at boot fires
 *      exactly once during the boot reload — and NOT again when the
 *      harness restarts a second time;
 *  (c) a recurring cron timer re-arms after each fire and survives a
 *      restart: with a 60x-fake clock the every-5-fake-minutes timer
 *      twice in one process, then the reloaded process fires it a third
 *      time from the persisted re-armed row.
 *
 * The store is `harness_timers` in the harness schema
 * (`src/timer-store.ts`); the exactly-once claim is the atomic
 * `UPDATE … WHERE fired_at IS NULL AND deleted_at IS NULL RETURNING`.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { spawn } from "node:child_process";
import { access, mkdir } from "node:fs/promises";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect } from "node:net";
import { createModels, fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/pi-durable";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { rpcRequest } from "../src/ipc.js";
import { startHarness } from "../src/main.js";
import { TimerStore } from "../src/timer-store.js";
import { freshSchemaName, freshStorage, openExistingStorage, PG_URL } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	const pool = new Pool({ connectionString: PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

async function waitFor<T>(what: string, deadlineMs: number, probe: () => Promise<T | undefined> | T | undefined): Promise<T> {
	const deadline = Date.now() + deadlineMs;
	for (;;) {
		const value = await probe();
		if (value !== undefined) return value;
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
}

/** Count `timer fired: <prompt>` user entries of one conversation. */
async function firedEntryCount(pool: Pool, schema: string, conversationId: number, prompt: string): Promise<number> {
	const result = await pool.query(
		`SELECT COUNT(*) AS n FROM "${schema}".durable_entries
		 WHERE conversation_id = $1 AND record LIKE $2`,
		[conversationId, `%timer fired: ${prompt}%`],
	);
	return Number(result.rows[0].n);
}

async function timerSubmissions(pool: Pool, schema: string, timerId: string): Promise<string[]> {
	const result = await pool.query(
		`SELECT request_id FROM "${schema}".durable_submissions WHERE request_id LIKE $1 ORDER BY request_id`,
		[`%timer-fired:${timerId}:%`],
	);
	return result.rows.map((r) => r.request_id as string);
}

async function createRpcConversation(rpcSock: string, sessionId: string): Promise<number> {
	const socket = connect(rpcSock);
	await new Promise<void>((resolve, reject) => {
		socket.once("connect", resolve);
		socket.once("error", reject);
	});
	const result = (await rpcRequest(socket, 1, "createConversation", {
		forgeSessionId: sessionId,
		agent: { provider: "faux", modelId: "faux-1" },
	})) as { conversationId: number };
	socket.destroy();
	return result.conversationId;
}

async function createTimerViaRpc(rpcSock: string, conversationId: number, atMs: number, prompt: string): Promise<string> {
	const socket = connect(rpcSock);
	await new Promise<void>((resolve, reject) => {
		socket.once("connect", resolve);
		socket.once("error", reject);
	});
	const result = (await rpcRequest(socket, 1, "timerSet", {
		conversationId,
		at: atMs,
		prompt,
	})) as { timerId: string };
	socket.destroy();
	return result.timerId;
}

describe("durable timers (H2.3)", () => {
	it("(a) a SIGKILLed harness fires its pending timer exactly once after restart", async () => {
		const schema = freshSchemaName();
		track(schema);
		const dir = await mkdtemp(join(tmpdir(), "forge-harness-kill-"));
		const socketDir = join(dir, "socks");
		await mkdir(socketDir, { recursive: true });
		const rpcSock = join(socketDir, "h1.sock");
		const eventsSock = join(socketDir, "h1-events.sock");
		const rpcSock2 = join(socketDir, "h2.sock");
		const eventsSock2 = join(socketDir, "h2-events.sock");
		const pool = new Pool({ connectionString: PG_URL, max: 4 });
		await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);

		const spawnChild = (rpc: string, events: string, responses: string): NodeJS.ChildProcess => {
			const child = spawn(process.execPath, ["--import", "tsx", "src/main.ts"], {
				cwd: process.cwd(),
				env: {
					...process.env,
					FORGE_DATABASE_URL: PG_URL,
					FORGE_API_URL: "http://127.0.0.1:9",
					FORGE_API_KEY: "test-key",
					FORGE_HARNESS_SOCKET: rpc,
					FORGE_HARNESS_EVENTS_SOCKET: events,
					FORGE_HARNESS_SCHEMA: schema,
					FORGE_HARNESS_FAUX: "1",
					FORGE_HARNESS_FAUX_RESPONSES: responses,
				},
				stdio: ["ignore", "pipe", "pipe"],
			});
			child.stderr?.on("data", (d: Buffer) => process.env.VITEST_DEBUG && console.error(`[harness] ${d}`));
			return child;
		};

		let child = spawnChild(rpcSock, eventsSock, '["Timer answer after kill."]');
		try {
			// Wait for the RPC socket to appear (tsx boot + migrations +
			// Harness.open + timer reload).
			await waitFor("harness h1 rpc socket", 90_000, async () => access(rpcSock).then(() => true).catch(() => undefined));

			// Create the conversation, set a ~2s timer.
			const conversationId = await createRpcConversation(rpcSock, "sess-ta");
			const timerId = await createTimerViaRpc(rpcSock, conversationId, Date.now() + 2_000, "ping");
			expect(timerId).toMatch(/^timer_/);

			// Kill -9 the harness while the timer is still pending.
			await new Promise((resolve) => setTimeout(resolve, 500));
			child.kill("SIGKILL");
			await new Promise<void>((resolve) => child.once("exit", () => resolve()));

			// Restart on the same schema: the boot reload re-arms the
			// overdue row and the atomic claim fires it exactly once.
			child = spawnChild(rpcSock2, eventsSock2, '["Timer answer after kill."]');
			await waitFor("harness h2 rpc socket", 90_000, async () => access(rpcSock2).then(() => true).catch(() => undefined));

			await waitFor("timer fired once after restart", 30_000, async () =>
				(await firedEntryCount(pool, schema, conversationId, "ping")) >= 1 ? true : undefined,
			);
			expect(await firedEntryCount(pool, schema, conversationId, "ping")).toBe(1);
			expect(await timerSubmissions(pool, schema, timerId)).toHaveLength(1);

			// The row is claimed (fired_at set); it is no longer "live".
			const row = await pool.query<{ fired_at: Date | null; cron: string | null }>(
				`SELECT fired_at, cron FROM "${schema}".harness_timers WHERE timer_id = $1`,
				[timerId],
			);
			expect(row.rows[0].fired_at).not.toBeNull();
			expect(row.rows[0].cron).toBeNull();

			// Let any (buggy) double-fire window pass: still exactly one.
			await new Promise((resolve) => setTimeout(resolve, 3_000));
			expect(await firedEntryCount(pool, schema, conversationId, "ping")).toBe(1);
			expect(await timerSubmissions(pool, schema, timerId)).toHaveLength(1);
		} finally {
			try {
				child.kill("SIGKILL");
			} catch {
				// already dead
			}
			await rm(dir, { recursive: true, force: true }).catch(() => {});
			await pool.end().catch(() => {});
		}
	}, 180_000);

	it("(b) an overdue timer present at boot fires exactly once on reload", async () => {
		const schema = freshSchemaName();
		track(schema);
		const timerPool = new Pool({ connectionString: PG_URL, max: 4 });
		await timerPool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
		const faux = fauxProvider();
		const models = createModels();
		models.setProvider(faux.provider);
		const store = new TimerStore(timerPool, schema);
		await store.ensureSchema();
		let conversationId = 0;
		const timerId = `timer_overdue_${Date.now().toString(36)}`;
		// Multi-boot: each boot opens a pool over the SAME schema without
		// dropping it (the schema outlives intermediate harness stops).
		const boot = async () => {
			const { storage, close } = await openExistingStorage(schema);
			const handle = await startHarness({
				storage,
				models,
				apiUrl: "http://127.0.0.1:9",
				apiKey: "test-key",
				schema,
				timerPool,
				log: () => {},
			});
			return { handle, close };
		};
		try {
			// Seed an OVERDUE row directly (as a kill would leave it):
			// armed in the dead process's memory, un-fired in Postgres.
			const seed = await boot();
			({ conversationId } = (await seed.handle.handlers.createConversation({
				forgeSessionId: "sess-tb",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number });
			await seed.handle.stop();
			await seed.close();
			await timerPool.query(
				`INSERT INTO "${schema}".harness_timers (timer_id, conversation_id, at, prompt, created_at)
				 VALUES ($1, $2, NOW() - INTERVAL '30 seconds', 'overdue ping', NOW())`,
				[timerId, conversationId],
			);

			// BOOT 1: reload fires the overdue timer exactly once. The fire
			// happens DURING startHarness (before any subscription can
			// exist), so assert on the durable state, not the event stream.
			const booted = await boot();
			try {
				await waitFor("overdue timer fired on boot", 15_000, async () =>
					(await firedEntryCount(timerPool, schema, conversationId, "overdue ping")) >= 1 ? true : undefined,
				);
				await new Promise((resolve) => setTimeout(resolve, 1_000));
				expect(await firedEntryCount(timerPool, schema, conversationId, "overdue ping")).toBe(1);
				expect(await timerSubmissions(timerPool, schema, timerId)).toHaveLength(1);
				await booted.handle.stop();
			} finally {
				await booted.close();
			}

			// BOOT 2 on the same schema: the claim is spent — no refire.
			const reopened = await boot();
			try {
				const fired2: string[] = [];
				const unsubscribe2 = reopened.handle.events.subscribe((event) => {
					if (event.type === "timer_fired") fired2.push(event.timerId);
				});
				await new Promise((resolve) => setTimeout(resolve, 2_000));
				expect(fired2).toHaveLength(0);
				expect(await firedEntryCount(timerPool, schema, conversationId, "overdue ping")).toBe(1);
				expect(await timerSubmissions(timerPool, schema, timerId)).toHaveLength(1);
				unsubscribe2();
				await reopened.handle.stop();
			} finally {
				await reopened.close();
			}
		} finally {
			await timerPool.end().catch(() => {});
		}
	}, 120_000);

	it("(c) a cron timer re-arms after each fire and survives a restart", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { storage, drop } = await freshStorage(schema);
		const timerPool = new Pool({ connectionString: PG_URL, max: 4 });
		const faux = fauxProvider();
		const models = createModels();
		models.setProvider(faux.provider);
		// 60x-fake clock: 1 real second = 1 fake minute, so an
		// every-5-fake-minutes timer fires every ~5s (scale divides the
		// armed delays back into wall-clock ms).
		const fakeStart = Date.now();
		const fakeNow = () => fakeStart + (Date.now() - fakeStart) * 60;
		try {
			const handle = await startHarness({
				storage,
				models,
				apiUrl: "http://127.0.0.1:9",
				apiKey: "test-key",
				schema,
				timerPool,
				now: fakeNow,
				timerScale: 60,
				log: () => {},
			});
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-tc",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };
			faux.setResponses([
				fauxAssistantMessage("tick one"),
				fauxAssistantMessage("tick two"),
				fauxAssistantMessage("tick three"),
				fauxAssistantMessage("tick four"),
			]);
			const { timerId } = (await handle.handlers.timerSet({
				conversationId,
				cron: "*/5 * * * *",
				prompt: "cron ping",
			})) as { timerId: string };

			// Two in-process fires (re-arm after each).
			await waitFor("cron fired twice in process", 40_000, async () =>
				(await firedEntryCount(timerPool, schema, conversationId, "cron ping")) >= 2 ? true : undefined,
			);
			expect(await timerSubmissions(timerPool, schema, timerId)).toHaveLength(2);
			// A live cron row: fired_at set (last fire) but still re-armed.
			const row = await timerPool.query<{ cron: string | null; at: Date | null }>(
				`SELECT cron, at FROM "${schema}".harness_timers WHERE timer_id = $1 AND deleted_at IS NULL`,
				[timerId],
			);
			expect(row.rows).toHaveLength(1);
			expect(row.rows[0].cron).toBe("*/5 * * * *");
			expect(row.rows[0].at).not.toBeNull();
			await handle.stop();

			// Restart on the same schema: the reloaded row fires again.
			// (A fresh storage: PgStorage closes its pool on close.)
			const reopened = await freshStorage(schema);
			try {
				const handle2 = await startHarness({
					storage: reopened.storage,
					models,
					apiUrl: "http://127.0.0.1:9",
					apiKey: "test-key",
					schema,
					timerPool,
					now: fakeNow,
					timerScale: 60,
					log: () => {},
				});
				await waitFor("cron fired after restart", 40_000, async () =>
					(await firedEntryCount(timerPool, schema, conversationId, "cron ping")) >= 3 ? true : undefined,
				);
				expect(await timerSubmissions(timerPool, schema, timerId)).toHaveLength(3);
				await handle2.stop();
			} finally {
				await reopened.drop();
			}
		} finally {
			await drop();
			await timerPool.end().catch(() => {});
		}
	}, 120_000);
});
