/**
 * Herd H5.3 kill switch — the timer-fire side.
 *
 * The api-side `sessions`/`agents` tables live in the default
 * (`public`) schema: forge-api has no schema knob (its migrations run
 * on the connection's default search path), while the harness's own
 * `durable_*`/`harness_timers` tables may live elsewhere
 * (`FORGE_HARNESS_SCHEMA`). The timer pool is a plain `pg` Pool with
 * the default search path, so the unqualified names below resolve to
 * the api schema.
 *
 * The check is one join: the owning agent of the session stamped on
 * this durable conversation (`sessions.durable_conversation_id →
 * sessions.agent_id → agents.paused`, migrations 016/017/026).
 *
 * Fail-open: a query error (api tables absent in a bare harness test
 * DB, transient blip) means "not paused" — the kill switch must never
 * wedge the timer fire path; the failure is logged.
 */
import type { Pool } from "pg";

export async function agentPausedForConversation(
	pool: Pool | undefined,
	conversationId: number,
): Promise<boolean> {
	if (pool === undefined) return false; // no timer infra ⇒ no pause state to check
	try {
		const result = await pool.query(
			`SELECT a.paused
			   FROM sessions s
			   JOIN agents a ON a.id = s.agent_id
			 WHERE s.durable_conversation_id = $1`,
			[conversationId],
		);
		return result.rows[0]?.paused === true;
	} catch (error) {
		console.error(
			JSON.stringify({
				level: "warn",
				msg: "agent pause check failed; treating conversation as unpaused",
				conversationId,
				error: error instanceof Error ? error.message : String(error),
			}),
		);
		return false;
	}
}
