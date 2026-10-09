/**
 * Herd H5.1: the research task — read-only BY CONSTRUCTION.
 *
 *  (a) `spawnResearch` mints a durable conversation whose per-task
 *      registry (what pi receives as the tool list) is EXACTLY
 *      `{read, webfetch, search, note}` — asserted through the
 *      `extensionTools` RPC against the live registry: no `bash`,
 *      no `write`, no `edit`, no `spawn_subagent`, no memory tools;
 *  (b) the research turn runs to a terminal state and a `note` call
 *      lands in the conversation's `research_notes` document while a
 *      `webfetch` call against a loopback target is refused by the
 *      guard (no network contact);
 *  (c) a restarted harness re-installs the research extension from
 *      the meta document with the same filtered registry.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { fauxAssistantMessage, fauxProvider, fauxToolCall, createModels } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/pi-durable";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { startHarness } from "../src/main.js";
import { freshSchemaName, freshStorage, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const RESEARCH_REGISTRY = ["read", "webfetch", "search", "note"];
const DENIED_TOOLS = [
	"bash",
	"write",
	"edit",
	"spawn_subagent",
	"memory_remember",
	"agent_signal",
];

const schemas: string[] = [];
function track(schema: string): void {
	schemas.push(schema);
}
afterAll(async () => {
	const pool = new Pool({ connectionString: (await import("./support.js")).PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

async function registryTools(handle: { handlers: Record<string, (p: Record<string, unknown>) => Promise<unknown>> }, conversationId: number): Promise<string[]> {
	const result = (await handle.handlers.extensionTools({ conversationId })) as { tools: string[] };
	return [...result.tools].sort();
}

describe("research tasks (H5.1)", () => {
	it("(a) the research registry is exactly {read, webfetch, search, note}", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done } = await startTestHarness(schema);
		try {
			faux.setResponses([fauxAssistantMessage("REPORT: the answer is yes.")]);
			const { conversationId } = (await handle.handlers.spawnResearch({
				forgeSessionId: "f5e1-0000-0000-0000-000000000001",
				question: "What is the capital of France?",
				provider: "faux",
				modelId: "faux-1",
				scope: "quick fact-check",
			})) as { conversationId: number };

			// The per-task registry: exactly the research surface.
			const tools = await registryTools(handle, conversationId);
			expect(tools).toEqual([...RESEARCH_REGISTRY].sort());
			// …and it LITERALLY lacks the write-class / relay-class tools.
			for (const denied of DENIED_TOOLS) {
				expect(tools, `${denied} must not be offered`).not.toContain(denied);
			}

			// …while an ordinary conversation gets the full standard
			// surface (+ spawn_subagent): the filtering is per TASK, not
			// per agent — same process, same registry.
			const { conversationId: normalId } = (await handle.handlers.createConversation({
				forgeSessionId: "f5e1-0000-0000-0000-000000000002",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };
			const normalTools = await registryTools(handle, normalId);
			expect(normalTools).toContain("bash");
			expect(normalTools).toContain("write");
			expect(normalTools).toContain("edit");
			expect(normalTools).toContain("spawn_subagent");
			expect(normalTools).not.toContain("webfetch");
			expect(normalTools).not.toContain("note");
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 90_000);

	it("(b) the research turn settles; note lands, webfetch loopback is refused", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done, timerPool } = await startTestHarness(schema);
		try {
			faux.setResponses([
				fauxAssistantMessage([fauxToolCall("note", { text: "finding: verified via source A" })], { stopReason: "toolUse" }),
				fauxAssistantMessage([fauxToolCall("webfetch", { url: "http://127.0.0.1:9/steal" })], { stopReason: "toolUse" }),
				fauxAssistantMessage("REPORT: Paris is the capital."),
			]);
			const { conversationId } = (await handle.handlers.spawnResearch({
				forgeSessionId: "f5e1-0000-0000-0000-000000000003",
				question: "Confirm the capital of France.",
				provider: "faux",
				modelId: "faux-1",
			})) as { conversationId: number };

			// Settle: every task of the conversation is terminal.
			const deadline = Date.now() + 60_000;
			for (;;) {
				const tasks = await timerPool.query<{ status: string }>(
					`SELECT status FROM "${schema}".durable_tasks WHERE conversation_id = $1`,
					[conversationId],
				);
				if (tasks.rows.length > 0 && tasks.rows.every((t) => t.status === "terminal")) break;
				if (Date.now() > deadline) throw new Error("research conversation did not settle");
				await new Promise((resolve) => setTimeout(resolve, 100));
			}

			// The note landed in the research_notes document (the H2.5
			// document surface — the same family the /documents routes
			// read).
			const notes = (await handle.handlers.documentGet({
				conversationId,
				name: "research_notes",
			})) as { entries?: string[] };
			expect(notes.entries).toEqual(["finding: verified via source A"]);

			// The transcript shows the loopback webfetch REFUSED by the
			// guard (no network contact: 127.0.0.1:9 is never dialed).
			const entries = JSON.stringify(
				(await (await handle.harness.conversation(conversationId as never, context))!.entries({}, 100, undefined, context)).items.map(
					(entry) => entry.model ?? entry.data ?? entry,
				),
			);
			expect(entries).toContain("refused");
			expect(entries).toContain("REPORT: Paris is the capital.");
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 90_000);

	it("(c) after a restart the research extension rebuilds with the filtered registry", async () => {
		const schema = freshSchemaName();
		track(schema);

		// First process: spawn the research task (no answer queued — the
		// process dies before the turn runs).
		const first = await startTestHarness(schema);
		let conversationId = 0;
		{
			({ conversationId } = (await first.handle.handlers.spawnResearch({
				forgeSessionId: "f5e1-0000-0000-0000-000000000004",
				question: "Research that settles after a restart.",
				provider: "faux",
				modelId: "faux-1",
			})) as { conversationId: number });
			expect(conversationId).toBeGreaterThan(0);
		}
		// Crash simulation: close the process WITHOUT dropping the schema
		// (the meta document is already committed; `done()` would drop the
		// schema and erase the state the restart must re-install).
		await first.handle.stop().catch(() => {});
		await first.timerPool.end().catch(() => {});

		// Second process: re-install reads the meta document.
		const storage = await freshStorage(schema);
		const faux2 = fauxProvider();
		const models2 = createModels();
		models2.setProvider(faux2.provider);
		const timerPool2 = new Pool({ connectionString: (await import("./support.js")).PG_URL, max: 4 });
		try {
			const handle2 = await startHarness({
				storage: storage.storage,
				models: models2,
				apiUrl: "http://127.0.0.1:9",
				apiKey: "test-key",
				schema,
				timerPool: timerPool2,
				log: () => {},
			});
			const tools = await registryTools(handle2, conversationId);
			expect(tools).toEqual([...RESEARCH_REGISTRY].sort());
			for (const denied of DENIED_TOOLS) expect(tools).not.toContain(denied);
			await handle2.stop();
		} finally {
			await timerPool2.end().catch(() => {});
			await storage.drop();
		}
	}, 90_000);
});
