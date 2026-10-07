/**
 * In-process harness tests against a scratch Postgres schema:
 *  (a) boot + submit + wait + read the answer back from storage after reopen
 *  (b) submission dedup by requestId
 *  (c) document put/get round-trip
 *  (d) timer fires and submits a turn
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { fauxAssistantMessage, createModels } from "@earendil-works/pi-ai";
import { createRegistry, Harness, type Context } from "@earendil-works/pi-durable";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, freshStorage, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	// Drop ONLY this file's schemas: the suite runs test files in parallel
	// against the same scratch Postgres, so a wildcard drop here would
	// destroy sibling files' live schemas.
	const { Pool } = await import("pg");
	const { PG_URL } = await import("./support.js");
	const pool = new Pool({ connectionString: PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

function firstAssistantText(entry: { model?: readonly unknown[] }): string | undefined {
	for (const message of entry.model ?? []) {
		const role = (message as { role?: string }).role;
		if (role !== "assistant") continue;
		const content = (message as { content?: unknown }).content;
		if (!Array.isArray(content)) continue;
		for (const block of content) {
			if (block !== null && typeof block === "object" && (block as { type?: string }).type === "text") {
				return String((block as { text?: unknown }).text);
			}
		}
	}
	return undefined;
}

describe("harness core", () => {
	it("(a) submits a turn, waits, and reads the answer back from storage after reopen", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-a",
				agent: { provider: "faux", modelId: "faux-1", systemPrompt: "Answer in one word." },
				extraInstructions: "Be concise.",
			})) as { conversationId: number };
			expect(conversationId).toBeTypeOf("number");

			faux.setResponses([fauxAssistantMessage("Paris")]);
			const { submissionId } = (await handle.handlers.submit({
				conversationId,
				requestId: "req-a-1",
				entryDraft: { type: "input", content: "What is the capital of France?" },
			})) as { submissionId: number };
			const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
			expect(settled.status).toBe("done");

			// Close the harness cleanly, then re-open a FRESH storage + harness
			// on the same schema and read the transcript back from Postgres.
			await handle.stop();
			const reopened = await freshStorage(schema);
			try {
				const registry = createRegistry();
				const harness2 = await Harness.open(reopened.storage, { models: createModels(), registry }, context);
				const conversation = await harness2.conversation(conversationId, context);
				expect(conversation).toBeDefined();
				const page = await conversation!.entries({}, 100, undefined, context);
				const kinds = page.items.map((entry) => entry.kind);
				expect(kinds).toContain("pi.user");
				expect(kinds).toContain("pi.assistant");
				const user = page.items.find((entry) => entry.kind === "pi.user");
				expect(JSON.stringify(user?.model ?? user?.data)).toContain("What is the capital of France?");
				const assistant = page.items.find((entry) => entry.kind === "pi.assistant")!;
				expect(firstAssistantText(assistant)).toBe("Paris");
				await harness2.close(context);
			} finally {
				await done();
			}
		} finally {
			// done() may not have run on failure; stop is idempotent enough.
			await handle.stop().catch(() => {});
		}
	}, 30_000);

	it("(b) submits the same requestId twice and gets exactly one submission", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-b",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };
			const draft = { type: "input", content: "hello" };
			const first = (await handle.handlers.submit({ conversationId, requestId: "req-b-1", entryDraft: draft })) as { submissionId: number };
			const second = (await handle.handlers.submit({ conversationId, requestId: "req-b-1", entryDraft: draft })) as { submissionId: number };
			expect(first).toEqual(second);
			// Exactly one committed submission record for that request.
			const byRequest = await handle.harness.commit(
				(tx) => tx.submissionByRequest(conversationId, "req-b-1"),
				context,
			);
			expect(byRequest).toBeDefined();
			expect(byRequest!.id).toBe(first.submissionId);
			const inspection = await handle.harness.inspect(context);
			expect(inspection.submissions.filter((s) => s.requestId === "req-b-1")).toHaveLength(1);
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 30_000);

	it("(c) round-trips a named document through put/get", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-c",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			const value = { plan: [1, 2, 3], note: "keep this" };
			await handle.handlers.documentPut({ conversationId, name: "scratch", value });
			const readBack = await handle.handlers.documentGet({ conversationId, name: "scratch" });
			expect(readBack).toEqual(value);

			// A second put replaces the value.
			await handle.handlers.documentPut({ conversationId, name: "scratch", value: { plan: [4] } });
			expect(await handle.handlers.documentGet({ conversationId, name: "scratch" })).toEqual({ plan: [4] });
			// Absent document is null.
			expect(await handle.handlers.documentGet({ conversationId, name: "never-written" })).toBeNull();

			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 30_000);

	it("(d) fires a timer and submits a turn on its conversation", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, done } = await startTestHarness(schema);
		const seen: { type: string }[] = [];
		const unsubscribe = handle.events.subscribe((event) => seen.push(event as { type: string }));
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-d",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			const { timerId } = (await handle.handlers.timerSet({
				conversationId,
				at: Date.now() + 500,
				prompt: "ping",
			})) as { timerId: string };
			expect(timerId).toBeTypeOf("string");

			// Wait for the timer_fired event (fires well under the test budget).
			const deadline = Date.now() + 15_000;
			while (!seen.some((event) => event.type === "timer_fired")) {
				if (Date.now() > deadline) throw new Error("timer_fired event not observed");
				await new Promise((resolve) => setTimeout(resolve, 50));
			}

			// The fired prompt landed in the transcript as a user entry.
			const convProbe = await handle.harness.conversation(conversationId, context);
			expect(convProbe).toBeDefined();
			const page = await convProbe
				.entries({}, 100, undefined, context);
			const serialized = JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? entry));
			expect(serialized).toContain("timer fired: ping");

			// One-shot: it is gone after firing.
			expect(await handle.handlers.timerClear({ conversationId, timerId })).toEqual({ cleared: false });
			unsubscribe();
			await handle.stop();
		} finally {
			unsubscribe();
			await done().catch(() => {});
		}
	}, 30_000);
});
