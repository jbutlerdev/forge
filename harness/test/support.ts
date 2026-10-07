/**
 * Shared test support: scratch Postgres schemas (one per test, dropped
 * afterwards, mirroring durable-pg's conformance setup) and in-process
 * harness boot with the faux provider.
 */
import { createModels, fauxProvider, type FauxProviderHandle, type Models } from "@earendil-works/pi-ai";
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

/** Boot the harness in-process over a fresh schema with the faux provider. */
export async function startTestHarness(
	schema: string,
): Promise<{ handle: HarnessHandle; faux: FauxProviderHandle; storage: PgStorage; done: () => Promise<void> }> {
	const { faux, models } = fauxSetup();
	const { storage, drop } = await freshStorage(schema);
	const handle = await startHarness({
		storage,
		models,
		apiUrl: "http://127.0.0.1:9", // never hit in these tests
		apiKey: "test-key",
		log: () => {},
	});
	return { handle, faux, storage, done: drop };
}
