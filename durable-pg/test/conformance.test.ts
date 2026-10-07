/**
 * Conformance + recovery tests for the PgStorage backend.
 *
 * Conformance: the vendor's own suite, registered from the installed
 * @earendil-works/pi-durable/testing, over a fresh Postgres schema per case.
 *
 * Kill-9 tests:
 *  1. Process-level SIGKILL mid-commit: reopen and verify the surviving log
 *     is a prefix of whole batches (atomicity) and allocators are consistent.
 *  2. Process-level SIGKILL after harness open: reopen the vendor Harness
 *     over PgStorage and verify it resumes (root readable, usable).
 *
 * Environment: DURABLE_PG_URL (default postgres://postgres:forge@127.0.0.1:5432/postgres).
 * Each test uses a uniquely-named schema, dropped afterwards.
 */
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Context } from "@earendil-works/chord";
import { createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness } from "@earendil-works/pi-durable";
import { registerStorageConformance } from "@earendil-works/pi-durable/testing";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { PgStorage } from "../src/storage-pg.js";

const CONNECTION_STRING =
	process.env.DURABLE_PG_URL ?? "postgres://postgres:forge@127.0.0.1:5432/postgres";

const context = { get: () => undefined } as unknown as Context;

const PACKAGE_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

let schemaCounter = 0;
/** Unique fresh schema name; callers create/drop it. */
const freshSchemaName = (): string =>
	`durable_test_${Date.now().toString(36)}_${(schemaCounter++).toString(36)}_${Math.floor(Math.random() * 1e9).toString(36)}`;

/** A fresh schema + PgStorage over it. Returned closer drops the schema. */
async function freshStorage(): Promise<{ storage: PgStorage; schema: string; done: () => Promise<void> }> {
	const schema = freshSchemaName();
	const pool = new Pool({ connectionString: CONNECTION_STRING, max: 4 });
	await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
	const storage = await PgStorage.open({ pool, schema });
	return {
		storage,
		schema,
		done: async () => {
			await storage.close(context).catch(() => {});
			await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {});
			await pool.end().catch(() => {});
		},
	};
}

const cleanupPools: Pool[] = [];
afterAll(async () => {
	for (const pool of cleanupPools) await pool.end().catch(() => {});
	// Drop any leftover test schemas from crashed runs.
	const pool = new Pool({ connectionString: CONNECTION_STRING });
	const leftovers = await pool.query<{ n: string }>(
		`SELECT nspname AS n FROM pg_namespace WHERE nspname LIKE 'durable_test_%'`,
	);
	for (const { n } of leftovers.rows) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`);
	await pool.end();
});

const adminPool = () => {
	const pool = new Pool({ connectionString: CONNECTION_STRING, max: 2 });
	cleanupPools.push(pool);
	return pool;
};

describe("PgStorage conformance", () => {
	registerStorageConformance({ describe, expect, it }, "Postgres", async (use) => {
		const { storage, done } = await freshStorage();
		try {
			await use(storage);
		} finally {
			await done();
		}
	});
});

/** Spawn a child that runs `body`; resolves when the child exits. Returns the child + captured output. */
function spawnChild(scriptPath: string, args: string[] = []): {
	child: ReturnType<typeof spawn>;
	stdout: () => string;
	stderr: () => string;
	exited: () => Promise<void>;
} {
	const child = spawn(process.execPath, ["--experimental-strip-types", ...args, scriptPath], {
		cwd: PACKAGE_ROOT,
		stdio: ["ignore", "pipe", "pipe"],
	});
	let out = "";
	let err = "";
	child.stdout!.on("data", (chunk) => (out += String(chunk)));
	child.stderr!.on("data", (chunk) => (err += String(chunk)));
	return { child, stdout: () => out, stderr: () => err, exited: () => new Promise((r) => child.on("exit", () => r())) };
}

describe("PgStorage kill-9 recovery", () => {
	it("survives SIGKILL mid-commit: the surviving log is whole batches (atomicity)", async () => {
		const schema = freshSchemaName();
		const pool = new Pool({ connectionString: CONNECTION_STRING, max: 2 });
		cleanupPools.push(pool);
		await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);

		// Child: commit a root conversation, then 400 batches of 5 entries; hang after each batch.
		const dir = await mkdtemp(join(tmpdir(), "durable-pg-kill9-"));
		const scriptPath = join(dir, "child.mjs");
		await writeFile(
			scriptPath,
			`
			import pg from ${JSON.stringify(join(PACKAGE_ROOT, "node_modules", "pg", "lib", "index.js"))};
			import { PgStorage } from ${JSON.stringify(join(PACKAGE_ROOT, "src", "storage-pg.ts"))};
			const { Pool } = pg;
			const pool = new Pool({ connectionString: ${JSON.stringify(CONNECTION_STRING)}, max: 2 });
			const storage = await PgStorage.open({ pool, schema: ${JSON.stringify(schema)} });
			const context = { get: () => undefined };
			await storage.commit([{ type: "conversation", value: { id: 1 } }], context);
			for (let batch = 0; batch < 400; batch++) {
				const writes = [];
				for (let i = 0; i < 5; i++) {
					writes.push({ type: "entry", value: { id: 2 + batch * 5 + i, conversationId: 1, data: { batch, i } } });
				}
				await storage.commit(writes, context);
				console.log("BATCH " + batch);
			}
			console.log("DONE");
			process.exit(0);
			`,
		);
		const run = spawnChild(scriptPath);
		// SIGKILL once ~50 batches are committed (entries > 1 + 50*5).
		let killed = false;
		for (let i = 0; i < 600 && !killed; i++) {
			const { rows } = await pool
				.query<{ count: string }>(`SELECT count(*) AS count FROM ${schema}.durable_entries`)
				.catch(() => ({ rows: [{ count: "0" }] }));
			if (Number(rows[0]?.count ?? 0) > 1 + 50 * 5) {
				run.child.kill("SIGKILL");
				killed = true;
				break;
			}
			await new Promise((r) => setTimeout(r, 10));
		}
		await run.exited();
		expect(killed).toBe(true);
		expect(run.stdout()).not.toContain("DONE");

		// Reopen (fresh pool): the committed prefix must be intact and consistent.
		const check = new Pool({
			connectionString: CONNECTION_STRING,
			max: 2,
			options: "-c search_path=" + schema + ",public",
		});
		const entries = await check.query<{ count: string }>(`SELECT count(*) AS count FROM durable_entries`);
		const count = Number(entries.rows[0]!.count);

		const idsDump = await check.query<{ ids: string[] }>(
			`SELECT array_agg(id ORDER BY id) AS ids FROM durable_entries`,
		);
		// Entries are 5-per-batch, ids 2..count+1 (the root conversation lives in durable_conversations).
		const conversations = await check.query<{ count: string }>(`SELECT count(*) AS count FROM durable_conversations`);
		expect(Number(conversations.rows[0]!.count)).toBe(1); // the root survived
		expect(count).toBeGreaterThanOrEqual(1);
		expect(count % 5).toBe(0); // whole batches only — no partial commit survived
		// no gaps in the surviving ID prefix (2..count+1)
		const gap = await check.query<{ missing: string }>(
			`SELECT count(*) AS missing FROM generate_series(2, ${count + 1}) AS g(id)
			WHERE NOT EXISTS (SELECT 1 FROM durable_entries WHERE id = g.id)`,
		);
		expect(Number(gap.rows[0]!.missing)).toBe(0);
		// allocators consistent with survivors: next_id = max id + 1, next_seq = 1 (root) + batches + 1
		const metadata = await check.query<{ next_id: string; next_seq: string }>(`SELECT next_id, next_seq FROM durable_metadata`);
		expect(Number(metadata.rows[0]!.next_id)).toBe(count + 2);
		expect(Number(metadata.rows[0]!.next_seq)).toBe(1 + count / 5 + 1);
		await check.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
		await check.end();
		await rm(dir, { recursive: true, force: true }).catch(() => {});
	}, 60_000);

	it("survives SIGKILL after harness open: Harness reopens over PgStorage and resumes", async () => {
		const schema = freshSchemaName();
		const dir = await mkdtemp(join(tmpdir(), "durable-pg-kill9-harness-"));
		const scriptPath = join(dir, "harness.mjs");
		await writeFile(
			scriptPath,
			`
			import pg from ${JSON.stringify(join(PACKAGE_ROOT, "node_modules", "pg", "lib", "index.js"))};
			import { PgStorage } from ${JSON.stringify(join(PACKAGE_ROOT, "src", "storage-pg.ts"))};
			import { createModels } from ${JSON.stringify(join(PACKAGE_ROOT, "node_modules", "@earendil-works", "pi-ai", "dist", "index.js"))};
			import { createRegistry, Harness } from ${JSON.stringify(join(PACKAGE_ROOT, "node_modules", "@earendil-works", "pi-durable", "dist", "index.js"))};
			const { Pool } = pg;
			const pool = new Pool({ connectionString: ${JSON.stringify(CONNECTION_STRING)}, max: 2 });
			const storage = await PgStorage.open({ pool, schema: ${JSON.stringify(schema)} });
			const context = { get: () => undefined };
			const harness = await Harness.open(storage, { models: createModels(), registry: createRegistry() }, context);
			console.log("HARNESS_READY");
			await new Promise(() => {}); // hang until killed
			`,
		);
		const run = spawnChild(scriptPath);
		let ready = false;
		for (let i = 0; i < 100 && !ready; i++) {
			ready = run.stdout().includes("HARNESS_READY");
			if (!ready) await new Promise((r) => setTimeout(r, 100));
		}
		run.child.kill("SIGKILL");
		await run.exited();
		expect(ready).toBe(true);

		// Reopen the vendor Harness over the same schema.
		const pool = new Pool({
			connectionString: CONNECTION_STRING,
			max: 2,
			options: "-c search_path=" + schema + ",public",
		});
		const storage = await PgStorage.open({ pool, schema });
		const reopened = await Harness.open(storage, { models: createModels(), registry: createRegistry() }, context);
		const root = await reopened.root(context);
		expect(root).toBeDefined();
		await storage.close(context);
		await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
		await pool.end();
		await rm(dir, { recursive: true, force: true }).catch(() => {});
	}, 60_000);
});
