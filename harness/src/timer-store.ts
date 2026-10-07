/**
 * The Postgres store behind the harness timers (H2.3).
 *
 * One `harness_timers` table in the harness's own schema (the same
 * schema the `durable_*` tables live in — `FORGE_HARNESS_SCHEMA`),
 * created by `ensureSchema` on boot with a plain `CREATE TABLE IF NOT
 * EXISTS` (independent of the durable-pg migration stream; the harness
 * owns this table).
 *
 * Live timers are rows with `deleted_at IS NULL` that are either one-
 * shot and un-fired (`fired_at IS NULL`) or recurring (`cron` set —
 * `fired_at` then records the LAST fire and the row re-arms on `at`).
 *
 * The exactly-once claim is the `claim` UPDATE: `fired_at IS NULL` is
 * part of the guard, and Postgres' row lock makes concurrent claimants
 * (two harness processes racing on boot, or an in-memory timer firing
 * in the same instant as a boot claim) serialize — exactly one UPDATE
 * returns the row. `clear`/`delete` mark `deleted_at`, which the claim
 * also excludes.
 */
import type { Pool } from "pg";

export interface TimerRow {
	readonly timerId: string;
	readonly conversationId: number;
	/** Next scheduled fire, epoch ms (always set: `at` timers keep their
	 * time; cron timers carry their next matching wall-clock time). */
	readonly at: number | undefined;
	/** 5-field cron expression (UTC); set for recurring timers. */
	readonly cron: string | undefined;
	readonly prompt: string;
	readonly createdAt: number;
	/** Set once fired (a one-shot timer stays set; a cron timer records
	 * the last fire). */
	readonly firedAt: number | undefined;
}

const ROW_COLUMNS = "timer_id, conversation_id, at, cron, prompt, created_at, fired_at";

function toRow(row: {
	timer_id: string;
	conversation_id: number;
	at: Date | string | null;
	cron: string | null;
	prompt: string;
	created_at: Date | string;
	fired_at: Date | string | null;
}): TimerRow {
	const ms = (value: Date | string | null): number | undefined =>
		value === null || value === undefined ? undefined : Date.parse(value as string);
	return {
		timerId: row.timer_id,
		conversationId: row.conversation_id,
		at: ms(row.at),
		cron: row.cron ?? undefined,
		prompt: row.prompt,
		createdAt: ms(row.created_at)!,
		firedAt: ms(row.fired_at),
	};
}

export class TimerStore {
	readonly #pool: Pool;
	readonly #table: string;

	constructor(pool: Pool, schema: string) {
		if (!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(schema)) {
			throw new Error(`invalid timer schema name: ${schema}`);
		}
		this.#pool = pool;
		this.#table = `"${schema}".harness_timers`;
	}

	/** Idempotent DDL (runs on every boot). */
	async ensureSchema(): Promise<void> {
		await this.#pool.query(
			`CREATE TABLE IF NOT EXISTS ${this.#table} (
				timer_id      TEXT PRIMARY KEY,
				conversation_id BIGINT NOT NULL,
				at            TIMESTAMPTZ,
				cron          TEXT,
				prompt        TEXT NOT NULL,
				created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				fired_at      TIMESTAMPTZ,
				deleted_at    TIMESTAMPTZ
			)`,
		);
	}

	/** Persist a new timer. */
	async create(conversationId: number, timerId: string, at: number, cron: string | undefined, prompt: string): Promise<void> {
		await this.#pool.query(
			`INSERT INTO ${this.#table} (timer_id, conversation_id, at, cron, prompt, created_at)
			 VALUES ($1, $2, to_timestamp($3 / 1000.0), $4, $5, NOW())`,
			[timerId, conversationId, at, cron, prompt],
		);
	}

	/** All live timers (optionally scoped to one conversation). */
	async list(conversationId?: number): Promise<TimerRow[]> {
		const result =
			conversationId === undefined
				? await this.#pool.query(
						`SELECT ${ROW_COLUMNS} FROM ${this.#table}
						 WHERE deleted_at IS NULL AND (cron IS NOT NULL OR fired_at IS NULL)
						 ORDER BY timer_id`,
					)
				: await this.#pool.query(
						`SELECT ${ROW_COLUMNS} FROM ${this.#table}
						 WHERE deleted_at IS NULL AND (cron IS NOT NULL OR fired_at IS NULL) AND conversation_id = $1
						 ORDER BY timer_id`,
						[conversationId],
					);
		return (result.rows as Array<Parameters<typeof toRow>[0]>).map(toRow);
	}

	/**
	 * Atomically claim one fire of a timer: returns the claimed row only to
	 * the connection that successfully set `fired_at`.
	 *
	 * Exactly-once semantics: the UPDATE matches a row only when it is not
	 * deleted and its last fire is either absent (first fire) or — for a
	 * cron row whose `at` has been re-armed to a NEW fire time — strictly
	 * before that re-armed `at`. A one-shot that already fired keeps its
	 * original `at`, so `fired_at < at` can never hold and it can never be
	 * claimed again. Two racing claims (two harness processes on the same
	 * schema, or the in-memory arming racing a reload of the same row) both
	 * run the UPDATE; Postgres row locking lets exactly one observe a match,
	 * and the loser gets no row back. `nextAtMs` (cron only) re-arms the
	 * schedule in the SAME atomic write, so the next fire time survives a
	 * crash between the claim and the fire.
	 */
	async claim(timerId: string, nextAtMs: number | null): Promise<TimerRow | null> {
		const result = await this.#pool.query(
			`UPDATE ${this.#table}
			 SET fired_at = NOW(),
				 at = CASE WHEN cron IS NOT NULL THEN to_timestamp($2 / 1000.0) ELSE at END
			 WHERE timer_id = $1 AND deleted_at IS NULL
				 AND (fired_at IS NULL OR (cron IS NOT NULL AND fired_at < at))
			 RETURNING ${ROW_COLUMNS}`,
			[timerId, nextAtMs],
		);
		const row = result.rows[0] as Parameters<typeof toRow>[0] | undefined;
		return row === undefined ? null : toRow(row);
	}

	/** Mark deleted. `true` when a live timer was found. */
	async clear(conversationId: number, timerId: string): Promise<boolean> {
		const result = await this.#pool.query(
			`UPDATE ${this.#table} SET deleted_at = NOW()
			 WHERE timer_id = $1 AND conversation_id = $2 AND deleted_at IS NULL AND (cron IS NOT NULL OR fired_at IS NULL)`,
			[timerId, conversationId],
		);
		return (result.rowCount ?? 0) > 0;
	}

	/** Live-timer count (the `status` bookkeeping number). */
	async count(): Promise<number> {
		const result = await this.#pool.query(`SELECT COUNT(*) AS n FROM ${this.#table} WHERE deleted_at IS NULL AND (cron IS NOT NULL OR fired_at IS NULL)`);
		return Number(result.rows[0]?.n ?? 0);
	}
}
