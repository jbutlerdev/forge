/**
 * H2.4: the trigram/GIN companion index behind `GET
 * /sessions/:id/history?q=` (forge-api searches
 * `{schema}.durable_entries.record` with `ILIKE '%…%'`).
 *
 * Harness-owned DDL, mirroring how `harness_timers` is created
 * (`timer-store.ts` `ensureSchema`): one idempotent call at boot, in the
 * harness's own schema. The index is a COMPANION — the search query is
 * portable ILIKE and runs with or without it; the index only accelerates
 * it. `pg_trgm` must be installable in the database (the scratch/fleet
 * PG have it in `pg_available_extensions`); on databases where creating
 * the extension fails (read-only roles, no-superuser hosts) the whole
 * step is skipped with a log line and the ILIKE path degrades to a
 * sequential scan — nothing else changes.
 */
import type { Pool } from "pg";

export interface HistoryIndexResult {
	/** Whether the GIN trigram index now exists on the schema. */
	readonly trigram: boolean;
}

/**
 * Idempotently install `pg_trgm` + the GIN trigram index on
 * `durable_entries.record`. Never throws: a host that cannot create the
 * extension simply has no trigram index.
 */
export async function ensureHistoryIndex(
	pool: Pool,
	schema: string,
	log: (message: string) => void = (message) => console.error(`history-index: ${message}`),
): Promise<HistoryIndexResult> {
	try {
		await pool.query(`CREATE EXTENSION IF NOT EXISTS pg_trgm`);
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		log(`pg_trgm unavailable, skipping the trigram index: ${message}`);
		return { trigram: false };
	}
	try {
		await pool.query(
			`CREATE INDEX IF NOT EXISTS durable_entries_record_trgm ON "${schema}".durable_entries USING gin (record gin_trgm_ops)`,
		);
		return { trigram: true };
	} catch (error) {
		const message = error instanceof Error ? error.message : String(error);
		log(`trigram index creation failed: ${message}`);
		return { trigram: false };
	}
}
