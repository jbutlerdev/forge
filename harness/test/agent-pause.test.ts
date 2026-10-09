/**
 * Herd H5.3 kill switch — the timer-fire side (the harness is the
 * ADMISSION SEAM for timer-fired prompts: `timers.ts` `fire()` claims
 * the row and submits the prompt in-process; there is no Rust-side
 * admission to test).
 *
 *  (a) `agentPausedForConversation` predicate against real PG rows
 *      (paused / unpaused / unstamped / absent api tables);
 *  (b) a timer on a PAUSED agent's conversation produces NO
 *      conversation write and NO submission — the row stays claimed
 *      (fired_at set, so the one-shot tick is lost, by design);
 *      after the agent resumes, the NEXT timer fires normally.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres). The api-side
 * `public.agents`/`public.sessions` tables are created minimally here
 * (the harness test DBs carry no forge-api migrations) and dropped in
 * afterAll — no other harness test touches them.
 */
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { agentPausedForConversation } from "../src/agent-pause.js";
import { freshSchemaName, PG_URL, startTestHarness } from "./support.js";

const pool = new Pool({ connectionString: PG_URL, max: 4 });
const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.query(`DROP TABLE IF EXISTS public.agents, public.sessions`).catch(() => {});
	await pool.end().catch(() => {});
});

async function seedAgent(api: Pool, agentId: string, paused: boolean): Promise<void> {
	await api.query(`CREATE TABLE IF NOT EXISTS public.agents (
		id UUID PRIMARY KEY,
		owner_id UUID,
		name TEXT,
		paused BOOLEAN NOT NULL DEFAULT FALSE
	)`);
	await api.query(`CREATE TABLE IF NOT EXISTS public.sessions (
		id UUID PRIMARY KEY,
		profile_id UUID,
		agent_id UUID,
		durable_conversation_id BIGINT
	)`);
	await api.query(`INSERT INTO public.agents (id, owner_id, name, paused)
		VALUES ($1, $2, 'pause-test-agent', $3)
		ON CONFLICT (id) DO UPDATE SET paused = EXCLUDED.paused`, [agentId, "00000000-0000-0000-0000-000000000001", paused]);
}

async function stampSession(api: Pool, conversationId: number, agentId: string): Promise<void> {
	const sessionId = `${agentId.slice(0, 8)}-0000-0000-0000-000000000001`;
	await api.query(
		`INSERT INTO public.sessions (id, profile_id, agent_id, durable_conversation_id)
		 VALUES ($1, '00000000-0000-0000-0000-000000000002', $3, $2)
		 ON CONFLICT (id) DO UPDATE SET durable_conversation_id = EXCLUDED.durable_conversation_id`,
		[sessionId, conversationId, agentId],
	);
}

async function firedEntryCount(schema: string, conversationId: number, prompt: string): Promise<number> {
	const result = await pool.query(
		`SELECT COUNT(*) AS n FROM "${schema}".durable_entries
		 WHERE conversation_id = $1 AND record LIKE $2`,
		[conversationId, `%timer fired: ${prompt}%`],
	);
	return Number(result.rows[0].n);
}

describe("agent pause kill switch — timer fire seam (H5.3)", () => {
	it("(a) agentPausedForConversation resolves the owning agent's flag", async () => {
		const AGENT_PAUSED = "aaaaaaaa-0000-0000-0000-00000000000a";
		const AGENT_OPEN = "bbbbbbbb-0000-0000-0000-00000000000b";
		await seedAgent(pool, AGENT_PAUSED, true);
		await seedAgent(pool, AGENT_OPEN, false);
		await stampSession(pool, 41, AGENT_PAUSED);
		await stampSession(pool, 42, AGENT_OPEN);

		expect(await agentPausedForConversation(pool, 41)).toBe(true);
		expect(await agentPausedForConversation(pool, 42)).toBe(false);
		// No session stamped on this conversation → unpaused.
		expect(await agentPausedForConversation(pool, 99)).toBe(false);

		// Resume → flips to false.
		await pool.query(`UPDATE public.agents SET paused = FALSE WHERE id = $1`, [AGENT_PAUSED]);
		expect(await agentPausedForConversation(pool, 41)).toBe(false);
	});

	it("(b) a paused agent's timer fire produces no conversation write; the next fire after resume lands", async () => {
		const schema = freshSchemaName();
		track(schema);
		const AGENT = "cccccccc-0000-0000-0000-00000000000c";
		await seedAgent(pool, AGENT, true);

		const { handle, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "f5e1-0000-0000-0000-00000000000d",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };
			await stampSession(pool, conversationId, AGENT);

			// Paused: the tick is claimed but the prompt is NOT admitted.
			const timerId = await handle.timers!.set(conversationId, {
				at: Date.now() + 1500,
				prompt: "pause-probe-1",
			});
			const deadline = Date.now() + 15_000;
			for (;;) {
				const claimed = await pool.query(
					`SELECT fired_at FROM "${schema}".harness_timers WHERE timer_id = $1`,
					[timerId],
				);
				if (claimed.rows[0]?.fired_at !== null && (await firedEntryCount(schema, conversationId, "pause-probe-1")) === 0) {
					break;
				}
				if (Date.now() > deadline) throw new Error("timed out waiting for the paused tick to be claimed");
				await new Promise((resolve) => setTimeout(resolve, 250));
			}
			expect(await firedEntryCount(schema, conversationId, "pause-probe-1"), "paused fire must not admit the prompt").toBe(0);
			const submissions = await pool.query(
				`SELECT COUNT(*) AS n FROM "${schema}".durable_submissions WHERE request_id LIKE $1`,
				[`%timer-fired:${timerId}:%`],
			);
			expect(Number(submissions.rows[0].n), "paused fire must not submit").toBe(0);

			// Resume: the NEXT timer on the same conversation fires normally.
			await pool.query(`UPDATE public.agents SET paused = FALSE WHERE id = $1`, [AGENT]);
			await handle.timers!.set(conversationId, {
				at: Date.now() + 1500,
				prompt: "pause-probe-2",
			});
			const deadline2 = Date.now() + 20_000;
			for (;;) {
				const n = await firedEntryCount(schema, conversationId, "pause-probe-2");
				if (n > 0) break;
				if (Date.now() > deadline2) throw new Error("timed out waiting for the resumed fire");
				await new Promise((resolve) => setTimeout(resolve, 250));
			}
		} finally {
			await done().catch(() => {});
		}
	}, 120_000);
});
