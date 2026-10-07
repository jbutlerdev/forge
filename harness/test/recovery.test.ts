/**
 * Kill/reopen recovery, mirroring pi-durable's harness-tasks-recovery
 * semantics, over Postgres (durable-pg):
 *
 *  1. Start a turn whose faux response never answers (the generation task
 *     is `running`, its progress checkpoints committed).
 *  2. Simulate a crash: the first harness is ABANDONED without close (its
 *     storage is dropped — the in-process equivalent of the process dying
 *     mid-turn; a true process kill over this storage is covered by
 *     durable-pg's own SIGKILL suite).
 *  3. Open a FRESH harness on the same schema. Open reconciles the running
 *     task back to pending; `resume()` re-dispatches it.
 *  4. The recovered turn settles with the answer queued on the new process's
 *     faux provider.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { fauxAssistantMessage, fauxProvider, createModels, type FauxResponseStep, type SimpleStreamOptions } from "@earendil-works/pi-ai";
import { createRegistry, Harness, type Context } from "@earendil-works/pi-durable";
import { PgStorage } from "@forge/durable-pg";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, freshStorage, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	const { Pool } = await import("pg");
	const { PG_URL } = await import("./support.js");
	const pool = new Pool({ connectionString: PG_URL });
	const leftovers = await pool.query<{ n: string }>(
		`SELECT nspname AS n FROM pg_namespace WHERE nspname LIKE 'harness_test_%'`,
	).catch(() => ({ rows: [] }));
	for (const { n } of leftovers.rows ?? []) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

/** A faux step that is reached but never answers until aborted. */
function slowStep(reached: { current: Promise<void> | null }): FauxResponseStep {
	return (_input, options) =>
		new Promise<never>((_resolve, reject) => {
			const signal = (options as SimpleStreamOptions | undefined)?.signal;
			reached.current = Promise.resolve();
			if (signal) signal.addEventListener("abort", () => reject(signal.reason), { once: true });
		});
}

describe("kill/reopen", () => {
	it("recovers a turn that was mid-generation when the harness died", async () => {
		const schema = freshSchemaName();
		track(schema);
		const reached = { current: null as Promise<void> | null };

		// --- first process ---
		const first = await startTestHarness(schema);
		const { handle: harness1, faux: faux1 } = first;
		const { conversationId } = (await harness1.handlers.createConversation({
			forgeSessionId: "sess-e",
			agent: { provider: "faux", modelId: "faux-1" },
		})) as { conversationId: number };

		faux1.setResponses([slowStep(reached)]);
		const { submissionId } = (await harness1.handlers.submit({
			conversationId,
			requestId: "req-e-1",
			entryDraft: { type: "input", content: "recovery probe" },
		})) as { submissionId: number };

		// The generation task must be live and have sent its request: the
		// task is `running` and the faux step has been reached.
		const deadline = Date.now() + 20_000;
		while (reached.current === null || (await harness1.harness.inspect(context)).tasks.filter((t) => t.record.state.status === "running").length === 0) {
			if (Date.now() > deadline) throw new Error("turn never reached the provider");
			await new Promise((resolve) => setTimeout(resolve, 50));
		}
		await reached.current!;

		// Crash simulation: abandon the harness WITHOUT close and drop its
		// storage reference. The generation request stays in flight forever
		// (its faux step rejects only on abort, and nobody aborts), so the
		// dead process commits nothing further — exactly like a SIGKILL'd
		// process whose held commit never lands.
		const deadHarness = harness1.harness;
		const deadStorage: PgStorage = first.storage;
		void deadHarness;
		void deadStorage;

		// --- second process ---
		const second = await freshStorage(schema);
		const { faux: faux2, models: models2 } = ((): { faux: ReturnType<typeof fauxProvider>; models: ReturnType<typeof createModels> } => {
			const faux = fauxProvider();
			const models = createModels();
			models.setProvider(faux.provider);
			return { faux, models };
		})();
		try {
			const registry = createRegistry();
			const harness2 = await Harness.open(second.storage, { models: models2, registry, onReport: () => {} }, context);
			try {
				// Open reconciled the running generation back to pending with
				// its checkpoint intact; nothing ran yet.
				const runningAfterOpen = (await harness2.inspect(context)).tasks.filter(
					(t) => t.record.state.status === "running",
				);
				expect(runningAfterOpen).toHaveLength(0);

				// Self-supervision: resume re-dispatches the interrupted turn.
				harness2.resume();
				faux2.setResponses([fauxAssistantMessage("recovered")]);

				const sub2 = await harness2.submission(submissionId, context);
				expect(sub2).toBeDefined();
				const settled = await sub2!.wait(context);
				expect(settled.status).toBe("done");

				// The recovered answer is in the transcript.
				const conversation = (await harness2.conversation(conversationId, context))!;
				const page = await conversation.entries({}, 100, undefined, context);
				const serialized = JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? entry));
				expect(serialized).toContain("recovered");
			} finally {
				await harness2.close(context);
			}
		} finally {
			await second.drop();
		}
	}, 60_000);
});
