/**
 * H2.4 acceptance (harness side): threshold compaction, manual compaction,
 * and reset/handoff.
 *
 *  (a) a conversation whose context exceeds its configured threshold
 *      (the `config` document) gets a background threshold compaction
 *      enqueued at a turn boundary; it completes while the NEXT turn is
 *      in flight (the summary lands at the next boundary, not by
 *      interrupting anything), the active window shrinks, and the
 *      compacted entries stay readable in storage.
 *  (b) a manual `compact` issued while a turn is in flight does not
 *      interrupt the turn: the turn settles `done` with its faux answer
 *      and the summary lands at the next boundary with reason "manual".
 *  (c) `reset` with a handoff note starts a new context segment: the old
 *      entries leave the model context but stay in `durable_entries`
 *      (the `history?q=` read path), and the next turn runs on the new
 *      segment seeded by the handoff note.
 *
 * Context sizing: the synthetic conversations are far smaller than the
 * default `compaction.keepRecentTokens` (20000) floor, below which the
 * built-in `selectCut` finds no cut — so these tests boot the harness
 * with a small `keepRecentTokens` (a test-only harness setting; the
 * per-conversation TRIGGER threshold is the `config` document, the H2.4
 * move of the legacy heuristic).
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { fauxAssistantMessage, fauxToolCall, type AssistantMessage } from "@earendil-works/pi-ai";
import { Pool } from "pg";
import type { Context, EntryRecord, TaskRecord } from "@earendil-works/pi-durable";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, startTestHarness, PG_URL } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	// Drop ONLY this file's schemas: the suite runs test files in parallel
	// against the same scratch Postgres, so a wildcard drop here would
	// destroy sibling files' live schemas.
	const pool = new Pool({ connectionString: PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

/** Poll until `probe` is defined (bounded); returns its value. */
async function poll<T>(probe: () => Promise<T | undefined>, timeoutMs: number, what: string): Promise<T> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		const value = await probe();
		if (value !== undefined) return value;
		if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
}

/** One input turn: submit, wait for settlement, return the settled status. */
async function runTurn(handle: Awaited<ReturnType<typeof startTestHarness>>["handle"], conversationId: number, requestId: string, content: string): Promise<string> {
	const { submissionId } = (await handle.handlers.submit({
		conversationId,
		requestId,
		entryDraft: { type: "input", content },
	})) as { submissionId: number };
	const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
	return settled.status;
}

async function activeView(handle: Awaited<ReturnType<typeof startTestHarness>>["handle"], conversationId: number) {
	const conversation = (await handle.harness.conversation(conversationId as never, context))!;
	return conversation.context(context);
}

async function allEntries(handle: Awaited<ReturnType<typeof startTestHarness>>["handle"], conversationId: number) {
	const conversation = (await handle.harness.conversation(conversationId as never, context))!;
	return conversation.entries({}, 200, undefined, context);
}

describe("H2.4 compaction + reset (harness)", () => {
	it("(a) enqueues a BACKGROUND threshold compaction that lands without interrupting the next turn", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done } = await startTestHarness(schema, {
			compaction: { keepRecentTokens: 10 },
		});
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-compact-a",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			// Threshold: compact above 380 estimated tokens (chars/4 ⇒
			// above ~1520 chars of context). The long answer (~1465 chars)
			// alone sits just under; the 3rd short turn crosses it. The
			// tiny keepRecentTokens floor keeps the two small trailing
			// turns out of the summarized prefix, so the long answer lands
			// IN it (a cut can only sit between entries).
			await handle.handlers.documentPut({
				conversationId,
				name: "config",
				value: { compaction: { maxContextChars: 380, divisor: 4 } },
			});

			const longAnswer = "alpha-long-answer-marker " + "context-fill".repeat(120); // ~1465 chars
			faux.setResponses([
				fauxAssistantMessage(longAnswer),
				fauxAssistantMessage("short-turn-2-marker"),
				fauxAssistantMessage("short-turn-3-marker"),
				// The next two calls race for these two responses: the
				// compaction SUMMARIZER and the in-flight 4th turn. Both
				// are plain-text `stop` answers, so the queue order does
				// not matter.
				fauxAssistantMessage("S. herdsleep summary marker"),
				fauxAssistantMessage("turn-4-final"),
			]);

			expect(await runTurn(handle, conversationId, "req-compact-a-1", "please answer")).toBe("done");
			expect(await runTurn(handle, conversationId, "req-compact-a-2", "filler two")).toBe("done");
			expect(await runTurn(handle, conversationId, "req-compact-a-3", "filler three")).toBe("done");
			// After the 3rd turn the context is over the threshold: the
			// watcher (fired on each committed pi.assistant entry) has
			// enqueued the background compaction.

			// Start the 4th turn; while it is in flight the compaction
			// runs in the background (its summary write is queued and
			// lands at the next boundary — the turn is never interrupted).
			expect(await runTurn(handle, conversationId, "req-compact-a-4", "filler four")).toBe("done");

			const head = await poll(async () => {
				const view = await activeView(handle, conversationId);
				return view.head !== undefined && view.head.kind === "pi.compaction" ? view : undefined;
			}, 20_000, "the threshold compaction head entry");
			// The compacted prefix (including the long answer) is out of
			// the active model context; the active window now holds the
			// summary plus the last turn. The summarizer and the 4th
			// generation raced for the last two faux responses, so the
			// window contains exactly one of the two tail markers.
			const active = JSON.stringify(head.messages);
			expect(active).not.toContain("alpha-long-answer-marker");
			expect(active.includes("turn-4-final") || active.includes("S. herdsleep summary marker")).toBe(true);

			// Storage still holds the compacted prefix (the history?q=
			// read path searches durable_entries directly).
			const oldPage = await allEntries(handle, conversationId);
			const stored = JSON.stringify(oldPage.items.map((entry: EntryRecord) => entry.model ?? entry.data ?? {}));
			expect(stored).toContain("alpha-long-answer-marker");
			expect(stored).toContain("please answer");

			// The enqueued task record: conversation-owned, BACKGROUND,
			// reason "threshold", terminal-completed.
			const task = (await handle.harness.commit(
				async (tx) => {
					const page = await tx.scanTasks({ kind: "pi.compaction" }, 64, undefined);
					return page.items[0];
				},
				context,
			)) as TaskRecord<{ reason: string }, { phase: string }, unknown> | undefined;
			expect(task, "the threshold compaction task must exist").toBeDefined();
			expect(task!.input).toEqual({ reason: "threshold" });
			expect(task!.background).toBe(true);
			expect(task!.owner).toBeUndefined();
			expect(task!.state.status).toBe("terminal");

			// `compactionStatus` reports the placed summary + the shrunken
			// active window.
			const status = (await handle.handlers.compactionStatus({ conversationId })) as {
				compactions: unknown[];
				lastCompaction: { entryId: number; reason: string; summaryChars: number } | null;
				activeContextChars: number;
				activeEntryCount: number;
			};
			expect(status.lastCompaction?.reason).toBe("threshold");
			expect(status.lastCompaction?.summaryChars).toBeGreaterThan(0);
			expect(status.compactions).toEqual([]);
			// The active window is strictly smaller than the full
			// transcript (the compacted prefix is out of it).
			expect(status.activeContextChars).toBeGreaterThan(0);
			expect(status.activeEntryCount).toBeLessThan(oldPage.items.length);
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 90_000);

	it("(b) a manual compact while a turn is in flight does not interrupt the turn", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done } = await startTestHarness(schema, {
			compaction: { keepRecentTokens: 100 },
		});
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-compact-b",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			// Seed a multi-entry context so a cut exists when the manual
			// compaction selects its range.
			const seed = "beta-seed-answer " + "fill".repeat(100); // ~416 chars
			faux.setResponses([fauxAssistantMessage(seed), fauxAssistantMessage("short-seed-2")]);
			expect(await runTurn(handle, conversationId, "req-compact-b-1", "seed one")).toBe("done");
			expect(await runTurn(handle, conversationId, "req-compact-b-2", "seed two")).toBe("done");

			// The in-flight turn: a bash tool round, then a final answer
			// that only arrives 800ms later (a factory response) — the
			// turn stays in flight while the manual compaction runs. After
			// the tool round commits, the summarizer and the final
			// generation each take one of the two remaining plain-text
			// responses, whichever order; the assertions hold either way.
			const delayedFinal = (): Promise<AssistantMessage> =>
				new Promise((resolve) =>
					setTimeout(() => resolve(fauxAssistantMessage("coexist-final-marker")), 800),
				);
			faux.setResponses([
				fauxAssistantMessage([fauxToolCall("bash", { command: "true" })], { stopReason: "toolUse" }),
				delayedFinal,
				fauxAssistantMessage("manual-summary-marker"),
			]);

			const { submissionId } = (await handle.handlers.submit({
				conversationId,
				requestId: "req-compact-b-3",
				entryDraft: { type: "input", content: "go" },
			})) as { submissionId: number };
			const submission = (await handle.harness.submission(submissionId, context))!;

			// Wait until the tool round is committed (the toolCall response
			// is consumed), then force the manual compaction while the
			// final generation is pending.
			await poll(
				async () => {
					const page = await allEntries(handle, conversationId);
					return page.items.some(
							(entry: EntryRecord) =>
								entry.kind === "pi.assistant" && JSON.stringify(entry.model ?? {}).includes("toolCall"),
						)
						? true
						: undefined;
				},
				20_000,
				"the in-flight turn's toolCall entry",
			);
			const compactResult = (await handle.handlers.compact({ conversationId })) as { taskId: number };
			expect(compactResult.taskId).toBeTypeOf("number");

			const settled = await submission.wait(context);
			expect(settled.status).toBe("done");

			// The manual summary landed (at once when idle, or at the
			// turn's end — either way the turn was never interrupted).
			const head = await poll(async () => {
				const view = await activeView(handle, conversationId);
				return view.head !== undefined && view.head.kind === "pi.compaction" ? view : undefined;
			}, 20_000, "the manual compaction head entry");

			// Both markers are in the transcript: one is the turn's final
			// answer, the other the summary; the active window's summary
			// holds whichever the summarizer took.
			const lastPage = await allEntries(handle, conversationId);
			const serialized = JSON.stringify(lastPage.items.map((entry: EntryRecord) => entry.model ?? {}));
			expect(serialized).toContain("coexist-final-marker");
			expect(serialized).toContain("manual-summary-marker");
			const headText = JSON.stringify(head.head!.model ?? []);
			expect(headText.includes("coexist-final-marker") || headText.includes("manual-summary-marker")).toBe(true);

			const status = (await handle.handlers.compactionStatus({ conversationId })) as {
				lastCompaction: { entryId: number; reason: string } | null;
			};
			expect(status.lastCompaction?.reason).toBe("manual");
			expect(status.lastCompaction?.entryId).toBe(head.head!.id);
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 90_000);

	it("(c) reset starts a new segment: old entries stay in storage, the next turn runs from the handoff note", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-reset-c",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			const needle = "needle-herd-x7f3";
			faux.setResponses([fauxAssistantMessage(`${needle} is the answer`)]);
			expect(await runTurn(handle, conversationId, "req-reset-c-1", "what is the needle?")).toBe("done");

			// Reset with a handoff note.
			await handle.handlers.reset({ conversationId, handoffNote: "handoff-marker-7f3: keep going" });
			const head = await poll(async () => {
				const view = await activeView(handle, conversationId);
				return view.head !== undefined && view.head.kind === "pi.reset" ? view : undefined;
			}, 20_000, "the reset head entry");

			// New segment: the handoff note is in the model context, the
			// old answer is not.
			expect(JSON.stringify(head.messages)).toContain("handoff-marker-7f3");
			expect(JSON.stringify(head.messages)).not.toContain(needle);

			// But storage kept the old segment (history?q= reads it):
			const pool = new Pool({ connectionString: PG_URL });
			try {
				const rows = await pool.query(
					`SELECT count(*)::int AS n FROM "${schema}".durable_entries WHERE record LIKE '%' || $1 || '%'`,
					[needle],
				);
				expect(rows.rows[0].n).toBeGreaterThanOrEqual(1);
			} finally {
				await pool.end();
			}

			// And the next turn runs on the new segment.
			faux.setResponses([fauxAssistantMessage("after-reset-answer")]);
			expect(await runTurn(handle, conversationId, "req-reset-c-2", "continue")).toBe("done");
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 60_000);
});
