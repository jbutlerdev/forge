/**
 * H2.5 acceptance (harness side): the malleable layer — the `before_tool`
 * allowlist hook, the `document_*` prompt sections, and the
 * profile/agent instruction flow into the conversation config.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { Context, PromptInput } from "@earendil-works/pi-durable";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, startTestHarness } from "./support.js";

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

describe("H2.5 hooks, prompt sections, documents (harness)", () => {
	it("(a) a toolsAllowlist blocks a disallowed tool: the model sees the reason, the API is never reached", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, faux, done } = await startTestHarness(schema);
		try {
			// Only `read` is allowed; the (unreachable-in-test) forge API
			// would be hit if `bash` were NOT blocked.
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-hook-a",
				agent: { provider: "faux", modelId: "faux-1" },
				toolsAllowlist: ["read"],
			})) as { conversationId: number };

			faux.setResponses([
				fauxAssistantMessage([fauxToolCall("bash", { command: "ls" })], { stopReason: "toolUse" }),
				fauxAssistantMessage("allowed-only-final"),
			]);
			const { submissionId } = (await handle.handlers.submit({
				conversationId,
				requestId: "req-hook-a-1",
				entryDraft: { type: "input", content: "run ls" },
			})) as { submissionId: number };
			const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
			expect(settled.status).toBe("done");

			const page = await (await handle.harness.conversation(conversationId as never, context))!
				.entries({}, 100, undefined, context);
			const serialized = JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? {}));
			// The block reason is what the model sees in the transcript…
			expect(serialized).toContain("Tool 'bash' is not in this agent's tool allowlist");
			expect(serialized).toContain("(allowed: read)");
			expect(serialized).toContain("was NOT executed");
			// …and the call never reached /tools/execute (the test API URL
			// is unreachable: an executed bash would surface a streaming /
			// network error result instead).
			expect(serialized).not.toContain("Streaming error");
			expect(serialized).not.toContain("Network error");
			expect(serialized).not.toContain("Command completed");
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 60_000);

	it("(b) the document_plan section renders the current document value (a mid-session put changes the next turn's prompt)", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-section-b",
				agent: { provider: "faux", modelId: "faux-1" },
			})) as { conversationId: number };

			const conversation = (await handle.harness.conversation(conversationId as never, context))!;
			const agent = await conversation.agent(context);
			// A PromptInput built the way the request preparation builds
			// it: committed reads come straight from the harness.
			const input = {
				conversationId: conversationId as never,
				agent,
				env: undefined,
				shown: {},
				read: handle.harness,
			} as unknown as PromptInput;

			const sections = handle.registry.snapshot().sections();
			const plan = sections.find((entry) => entry.section.key === "document_plan");
			expect(plan, "the document_plan section must be registered").toBeDefined();

			// Absent document ⇒ the section renders nothing.
			expect(await plan!.section.render(input, context)).toBeUndefined();

			await handle.handlers.documentPut({
				conversationId,
				name: "plan",
				value: { goal: "build the thing", steps: [1, 2] },
			});
			const first = await plan!.section.render(input, context);
			expect(first).toContain("build the thing");

			// Mid-session put: the section (rebuilt per request) renders
			// the NEW value on the next turn's prompt. (The faux provider
			// cannot echo the rendered system prompt, so this asserts the
			// section fetch directly — the per-request rebuild is
			// pi-durable's job on top of it.)
			await handle.handlers.documentPut({
				conversationId,
				name: "plan",
				value: { goal: "ship it instead" },
			});
			const second = await plan!.section.render(input, context);
			expect(second).toContain("ship it instead");
			expect(second).not.toContain("build the thing");

			// String documents render verbatim, unquoted.
			await handle.handlers.documentPut({ conversationId, name: "handoff", value: "handoff text here" });
			const handoff = sections.find((entry) => entry.section.key === "document_handoff")!;
			expect(await handoff.section.render(input, context)).toBe("handoff text here");

			// The H4 slot: registered, renders nothing for now.
			const beliefs = sections.find((entry) => entry.section.key === "memory_beliefs");
			expect(beliefs, "the memory_beliefs stub must be registered").toBeDefined();
			expect(await beliefs!.section.render(input, context)).toBeUndefined();
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 60_000);

	it("(c) the profile systemPrompt and the agent extraInstructions reach the conversation agent config", async () => {
		const schema = freshSchemaName();
		track(schema);
		const { handle, done } = await startTestHarness(schema);
		try {
			const { conversationId } = (await handle.handlers.createConversation({
				forgeSessionId: "sess-instr-c",
				agent: { provider: "faux", modelId: "faux-1", systemPrompt: "SYS-PROBE-7" },
				extraInstructions: "EXTRA-PROBE-9",
			})) as { conversationId: number };

			// pi-durable renders the conversation `instructions` as the
			// `instructions` prompt section, LAST, before every request —
			// asserting the resolved agent config is asserting the prompt
			// input of every turn.
			const conversation = (await handle.harness.conversation(conversationId as never, context))!;
			const agent = await conversation.agent(context);
			expect(agent.instructions).toContain("SYS-PROBE-7");
			expect(agent.instructions).toContain("EXTRA-PROBE-9");
			expect(agent.instructions!.indexOf("SYS-PROBE-7")).toBeLessThan(agent.instructions!.indexOf("EXTRA-PROBE-9"));
			await handle.stop();
		} finally {
			await done().catch(() => {});
		}
	}, 60_000);
});
