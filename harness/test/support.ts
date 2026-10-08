/**
 * Shared test support: scratch Postgres schemas (one per test, dropped
 * afterwards, mirroring durable-pg's conformance setup) and in-process
 * harness boot with the faux provider.
 */
import { createModels, fauxProvider, type FauxProviderHandle, type Models } from "@earendil-works/pi-ai";
import type { HarnessSettings } from "@earendil-works/pi-durable";
import { PgStorage } from "@forge/durable-pg";
import { Pool } from "pg";
import { startHarness, type HarnessHandle } from "../src/main.js";

export const PG_URL = process.env.HARNESS_TEST_PG ?? "postgres://postgres:forge@127.0.0.1:5432/postgres";

let counter = 0;
/** A uniquely-named fresh schema; callers create/drop it. */
export function freshSchemaName(): string {
	return `harness_test_${Date.now().toString(36)}_${(counter++).toString(36)}_${Math.floor(Math.random() * 1e9).toString(36)}`;
}

/** Create the schema; returns a storage over it + a drop closure. */
export async function freshStorage(schema = freshSchemaName()): Promise<{
	schema: string;
	storage: PgStorage;
	drop: () => Promise<void>;
}> {
	const pool = new Pool({ connectionString: PG_URL, max: 4 });
	await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
	const storage = await PgStorage.open({ pool, schema });
	return {
		schema,
		storage,
		drop: async () => {
			await storage.close({ get: () => undefined } as never).catch(() => {});
			await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`).catch(() => {});
			await pool.end().catch(() => {});
		},
	};
}

/** Faux provider wired into a Models catalog, like the vendor chat tests. */
export function fauxSetup(): { faux: FauxProviderHandle; models: Models } {
	const faux = fauxProvider();
	const models = createModels();
	models.setProvider(faux.provider);
	return { faux, models };
}

/**
 * Open a PgStorage over a schema WITHOUT owning its lifecycle: `close()`
 * closes the pool only. For multi-boot tests where the schema must
 * outlive intermediate harness stops (H2.2 recovery, H2.3 restarts);
 * the schema itself is dropped by the test's `track()` cleanup.
 */
export async function openExistingStorage(schema: string): Promise<{ storage: PgStorage; close: () => Promise<void> }> {
	const pool = new Pool({ connectionString: PG_URL, max: 4 });
	await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
	const storage = await PgStorage.open({ pool, schema });
	return {
		storage,
		close: async () => {
			await storage.close({ get: () => undefined } as never).catch(() => {});
			await pool.end().catch(() => {});
		},
	};
}

/** Boot the harness in-process over a fresh schema with the faux provider.
 * A dedicated timer pool over the same database backs `harness_timers`
 * (H2.3); `timerPool` lives on the handle so tests can drop it.
 * `settings` (H2.4) overrides the pi-durable run policy — notably the
 * compaction `keepRecentTokens` floor, which a small synthetic context
 * must exceed for the built-in `selectCut` to find a cut. */
export async function startTestHarness(
	schema: string,
	settings?: HarnessSettings,
): Promise<{ handle: HarnessHandle; faux: FauxProviderHandle; storage: PgStorage; timerPool: Pool; done: () => Promise<void> }> {
	const { faux, models } = fauxSetup();
	const { storage, drop } = await freshStorage(schema);
	const timerPool = new Pool({ connectionString: PG_URL, max: 4 });
	const handle = await startHarness({
		storage,
		models,
		apiUrl: "http://127.0.0.1:9", // never hit in these tests
		apiKey: "test-key",
		schema,
		timerPool,
		...(settings !== undefined ? { settings } : {}),
		log: () => {},
	});
	return {
		handle,
		faux,
		storage,
		timerPool,
		done: async () => {
			await drop();
			await timerPool.end().catch(() => {});
		},
	};
}
