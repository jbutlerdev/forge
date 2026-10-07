/**
 * Herd H2.2 acceptance: subagents over the harness.
 *
 *  (a) a foreground `spawn_subagent` returns the child's answer and the
 *      child conversation is owned by the calling tool task;
 *  (b) aborting the parent's task mid-subagent kills the child task
 *      (pi-durable's ownership-tree abort, exposed through
 *      `handlers.abort` with the default `tree: true`);
 *  (c) a detached subagent survives its parent turn completing: it is
 *      owned by a `background` anchor task and keeps running after the
 *      parent's submission has settled;
 *  (d) kill the harness mid-subagent (abandon without close), restart
 *      on the same schema → the conversation is consistent and the
 *      turn settles (the tool's `replay: "safe"` rerun finds the same
 *      child and the same submission; the interrupted child generation
 *      is re-dispatched and answers).
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { fauxAssistantMessage, fauxProvider, fauxToolCall, type FauxResponseStep, type SimpleStreamOptions, createModels } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/pi-durable";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { startHarness } from "../src/main.js";
import { freshSchemaName, freshStorage, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	const pool = new Pool({ connectionString: (await import("./support.js")).PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
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

/** A faux step that answers only after the gate opens. */
function gatedStep(gate: { open: () => void }): FauxResponseStep {
	let resolve: (() => void) | undefined;
	const opened = new Promise<void>((r) => {
		resolve = r;
	});
	gate.open = () => resolve!();
	return async () => {
		await opened;
		return fauxAssistantMessage("background done");
	};
}

async function waitFor<T>(what: string, deadlineMs: number, probe: () => Promise<T | undefined> | T | undefined): Promise<T> {
	const deadline = Date.now() + deadlineMs;
	for (;;) {
		const value = await probe();
		if (value !== undefined) return value;
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
}

describe("subagents (H2.2)", () => {
	it("(a) foreground spawn_subagent returns the child's answer; the child is task-owned", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done, timerPool } = await startTestHarness(schema);
		const spawned: { childConversationId: number; childForgeSessionId: string; detached: boolean }[] = [];
		const unsubscribe = handle.events.subscribe((event) => {
			if (event.type === "subagent_spawned") {
				spawned.push({
					childConversationId: event.childConversationId,
					childForgeSessionId: event.childForgeSessionId,
					detached: event.detached,
				});
			}
		});
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-sa",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			// Parent: tool call → child answers → parent reports.
			faux.setResponses([
				fauxAssistantMessage([fauxToolCall("spawn_subagent", { task: "Name three prime numbers." })], { stopReason: "toolUse" }),
				fauxAssistantMessage("2, 3, and 5."),
				fauxAssistantMessage("The subagent says: 2, 3, and 5."),
			]);

			const { submissionId } = (await handle.handlers.submit({
				conversationId,
				requestId: "req-sa-1",
				entryDraft: { type: "input", content: "Delegate to a subagent." },
			})) as { submissionId: number };
			const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
			expect(settled.status).toBe("done");

			// The spawn event mapped the child to a pre-minted forge session id.
			expect(spawned).toHaveLength(1);
			expect(spawned[0].detached).toBe(false);
			expect(spawned[0].childForgeSessionId).toMatch(/^[0-9a-f-]{36}$/);
			const childId = spawned[0].childConversationId;
			expect(childId).not.toBe(conversationId);

			// The child conversation is owned by the tool task (durable-pg
			// indexes the owner-task edge; a conversation-owned task would
			// have owner_task_id NULL).
			const ownerRow = await timerPool.query<{ owner_task_id: number | null }>(
				`SELECT owner_task_id FROM "${schema}".durable_conversations WHERE id = $1`,
				[childId],
			);
			expect(ownerRow.rows[0]?.owner_task_id).not.toBeNull();

			// The child answered; the parent saw the tool result.
			const childEntries = JSON.stringify(
				(await (await handle.harness.conversation(childId as never, context))!.entries({}, 100, undefined, context)).items.map(
					(entry) => entry.model ?? entry.data ?? entry,
				),
			);
			expect(childEntries).toContain("Name three prime numbers.");
			expect(childEntries).toContain("2, 3, and 5.");
			const parentEntries = JSON.stringify(
				(await (await handle.harness.conversation(conversationId as never, context))!.entries({}, 100, undefined, context)).items.map(
					(entry) => entry.model ?? entry.data ?? entry,
				),
			);
			expect(parentEntries).toContain("2, 3, and 5.");
			expect(parentEntries).toContain("The subagent says: 2, 3, and 5.");
			await handle.stop();
		} finally {
			unsubscribe();
			await done().catch(() => {});
		}
	}, 60_000);

	it("(b) aborting the parent mid-subagent aborts the child task", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done, timerPool } = await startTestHarness(schema);
		const reached = { current: null as Promise<void> | null };
		const taskEvents: Array<{ conversationId: number; status: string }> = [];
		const unsubscribe = handle.events.subscribe((event) => {
			if (event.type === "task_state") taskEvents.push({ conversationId: event.conversationId, status: event.status });
			if (event.type === "subagent_spawned") spawnedChild[0] = event.childConversationId;
		});
		const spawnedChild = [0] as number[];
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-sb",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			faux.setResponses([
				fauxAssistantMessage([fauxToolCall("spawn_subagent", { task: "A long task." })], { stopReason: "toolUse" }),
				slowStep(reached),
			]);
			const { submissionId } = (await handle.handlers.submit({
				conversationId,
				requestId: "req-sb-1",
				entryDraft: { type: "input", content: "Delegate a long task." },
			})) as { submissionId: number };

			// Wait until the subagent's generation is live and blocked.
			await waitFor("child generation reached", 30_000, async () => (reached.current !== null ? true : undefined));
			await new Promise((resolve) => setTimeout(resolve, 100));
			expect(spawnedChild[0]).not.toBe(0);
			const childId = spawnedChild[0];

			// The parent's turn task id: the parent conversation's own
			// conversation-owned task (the tool task owns the child and is
			// itself owned BY the turn task).
			const parentTaskId = await taskTaskId(handle, conversationId);

			// Default tree abort (what `POST /sessions/:id/interrupt` sends
			// with no `?tree=`): the whole ownership tree, child included.
			const { aborted } = (await handle.handlers.abort({ taskId: parentTaskId, tree: true })) as { aborted: number };
			expect(aborted).toBeGreaterThanOrEqual(1);

			// Both the parent's turn and the child's generation end aborted.
			await waitFor("child task aborted", 30_000, () =>
				taskEvents.some((e) => e.conversationId === childId && e.status === "aborted") ? true : undefined,
			);
			await waitFor("parent task aborted", 30_000, () =>
				taskEvents.some((e) => e.conversationId === conversationId && e.status === "aborted") ? true : undefined,
			);
			// Durable state agrees: every task of the child conversation is
			// terminal.
			const childTasks = await timerPool.query<{ status: string }>(
				`SELECT status FROM "${schema}".durable_tasks WHERE conversation_id = $1`,
				[childId],
			);
			expect(childTasks.rows.length).toBeGreaterThan(0);
			for (const row of childTasks.rows) expect(row.status).toBe("terminal");
			await handle.stop();
		} finally {
			unsubscribe();
			await done().catch(() => {});
		}
	}, 60_000);

	it("(c) a detached subagent survives its parent turn completing", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done, timerPool } = await startTestHarness(schema);
		const gate: { open: () => void } = { open: () => {} };
		const spawned: Array<{ childConversationId: number }> = [];
		const taskEvents: Array<{ conversationId: number; status: string }> = [];
		const unsubscribe = handle.events.subscribe((event) => {
			if (event.type === "subagent_spawned") {
				spawned.push({ childConversationId: event.childConversationId });
				expect(event.detached).toBe(true);
			}
			if (event.type === "task_state") taskEvents.push({ conversationId: event.conversationId, status: event.status });
		});
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-sc",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			// Parent: spawn (detached) → final answer. The child generation
			// is gated so it provably outlives the parent's settlement.
			faux.setResponses([
				fauxAssistantMessage([fauxToolCall("spawn_subagent", { task: "Background work.", detach: true })], { stopReason: "toolUse" }),
				gatedStep(gate),
				fauxAssistantMessage("parent final"),
			]);
			const { submissionId } = (await handle.handlers.submit({
				conversationId,
				requestId: "req-sc-1",
				entryDraft: { type: "input", content: "Run background work." },
			})) as { submissionId: number };
			const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
			expect(settled.status).toBe("done");
			const childId = spawned[0]?.childConversationId;
			expect(childId).toBeDefined();

			// The detached child is owned by a BACKGROUND task (the anchor),
			// not by any task of the parent's turn: it is outside the
			// parent's abort/idle scope.
			const ownerRow = await timerPool.query<{ owner_task_id: number | null }>(
				`SELECT owner_task_id FROM "${schema}".durable_conversations WHERE id = $1`,
				[childId],
			);
			const anchor = ownerRow.rows[0]?.owner_task_id;
			expect(anchor).not.toBeNull();
			const anchorTask = await timerPool.query<{ background: boolean }>(
				`SELECT background FROM "${schema}".durable_tasks WHERE id = $1`,
				[anchor],
			);
			expect(anchorTask.rows[0]?.background).toBe(true);

			// While the parent has settled, the child's generation is still
			// live (gated) — it survived the parent's turn completing.
			const liveTasks = (await handle.harness.inspect(context)).tasks.filter((t) => t.record.conversationId === childId);
			expect(
				liveTasks.some((t) => ["pending", "running", "waiting", "completing"].includes(t.record.state.status)),
			).toBe(true);

			// Release the gate: the child answers and its task settles done.
			gate.open();
			await waitFor("child task done", 30_000, () =>
				taskEvents.some((e) => e.conversationId === childId && e.status === "done") ? true : undefined,
			);
			const childEntries = JSON.stringify(
				(await (await handle.harness.conversation(childId as never, context))!.entries({}, 100, undefined, context)).items.map(
					(entry) => entry.model ?? entry.data ?? entry,
				),
			);
			expect(childEntries).toContain("background done");
			await handle.stop();
		} finally {
			unsubscribe();
			await done().catch(() => {});
		}
	}, 60_000);

	it("(d) killed mid-subagent, the restart settles the turn with a consistent conversation", async () => {
		const schema = freshSchemaName();
		track(schema);
		const reached = { current: null as Promise<void> | null };

		// --- first process ---
		const first = await startTestHarness(schema);
		const { handle: harness1, faux: faux1 } = first;
		const { conversationId } = (await harness1.handlers.createConversation({
			forgeSessionId: "sess-sd",
			agent: { provider: "faux", modelId: "faux-1" },
		})) as { conversationId: number };
		let submissionId = 0;
		{
			faux1.setResponses([
				fauxAssistantMessage([fauxToolCall("spawn_subagent", { task: "Primes again." })], { stopReason: "toolUse" }),
				slowStep(reached),
			]);
			const result = (await harness1.handlers.submit({
				conversationId,
				requestId: "req-sd-1",
				entryDraft: { type: "input", content: "Recovery probe." },
			})) as { submissionId: number };
			submissionId = result.submissionId;
		}
		// The subagent's generation must be live (and blocked) when we die.
		await waitFor("child generation reached", 30_000, async () => (reached.current !== null ? true : undefined));

		// Crash simulation: abandon the harness WITHOUT close, exactly like
		// the recovery test — the in-flight generation commits nothing.
		const deadHarness = harness1.harness;
		const deadStorage = first.storage;
		void deadHarness;
		void deadStorage;

		// --- second process ---
		const second = await freshStorage(schema);
		const faux2 = fauxProvider();
		const models2 = createModels();
		models2.setProvider(faux2.provider);
		const timerPool2 = new Pool({ connectionString: (await import("./support.js")).PG_URL, max: 4 });
		let handle2;
		try {
			handle2 = await startHarness({
				storage: second.storage,
				models: models2,
				apiUrl: "http://127.0.0.1:9",
				apiKey: "test-key",
				schema,
				timerPool: timerPool2,
				log: () => {},
			});
			// startHarness already re-installed the forge extensions
			// (parent + child) and ran resume(): the tool task's replay-safe
			// rerun finds the child through the spawn document, re-uses the
			// submission (requestId dedup), and the interrupted child
			// generation is re-dispatched.
			faux2.setResponses([fauxAssistantMessage("recovered primes"), fauxAssistantMessage("done: recovered")]);
			const sub = await handle2.harness.submission(submissionId, context);
			expect(sub).toBeDefined();
			const settled = await sub!.wait(context);
			expect(settled.status).toBe("done");

			// One child conversation exists (the rerun did not re-spawn).
			const children = await timerPool2.query<{ id: number }>(
				`SELECT id FROM "${schema}".durable_conversations WHERE owner_task_id IS NOT NULL`,
			);
			expect(children.rows).toHaveLength(1);
			const childId = children.rows[0].id;
			const childEntries = JSON.stringify(
				(await (await handle2.harness.conversation(childId as never, context))!.entries({}, 100, undefined, context)).items.map(
					(entry) => entry.model ?? entry.data ?? entry,
				),
			);
			expect(childEntries).toContain("recovered primes");
			const parentEntries = JSON.stringify(
				(await (await handle2.harness.conversation(conversationId as never, context))!.entries({}, 100, undefined, context)).items.map(
					(entry) => entry.model ?? entry.data ?? entry,
				),
			);
			expect(parentEntries).toContain("done: recovered");
		} finally {
			await handle2?.stop().catch(() => {});
			await timerPool2.end().catch(() => {});
			await second.drop();
		}
	}, 90_000);
});

/** The parent conversation's turn task id (its conversation-owned task).
 *The tool task the subagent runs under is owned BY the turn task, so it
 *has an owner edge and is not in this filter. */
async function taskTaskId(handle: { harness: import("@earendil-works/pi-durable").Harness }, conversationId: number): Promise<number> {
	const tasks = await handle.harness.commit(async (tx) => {
		const out: Array<{ id: number; conversationId: number; owner: unknown }> = [];
		let cursor: unknown = undefined;
		for (;;) {
			const page = await tx.scanTasks({}, 256, cursor as never);
			for (const task of page.items) out.push({ id: task.id, conversationId: task.conversationId, owner: task.owner });
			if (page.next === undefined) break;
			cursor = page.next;
		}
		return out;
	}, context);
	const parentTasks = tasks.filter((t) => t.conversationId === conversationId && t.owner === undefined);
	expect(parentTasks).toHaveLength(1);
	return parentTasks[0].id;
}
