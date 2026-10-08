/**
 * Herd H4 acceptance (harness side): `memory_remember` tool relay +
 * the `memory_beliefs` prompt section (confidence pass, retrieval
 * pass, degradation, omission when empty).
 *
 * The forge memory API is an in-process fake `node:http` server (the
 * faux-provider pattern): the extension's `apiUrl` points at it.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import type { AddressInfo } from "node:net";
import http from "node:http";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { Context, PromptInput } from "@earendil-works/pi-durable";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const AGENT_ID = "11111111-1111-4111-8111-111111111111";

/** Recorded request + canned-response handler (status overridable). */
interface Hit {
	method: string;
	path: string;
	body: Record<string, unknown> | undefined;
	auth: string;
}

type Handler = (body: Record<string, unknown> | undefined) => { status?: number; json: unknown };

function fakeMemoryApi(handlers: Record<string, Handler>): Promise<{
	url: string;
	hits: Hit[];
	close: () => Promise<void>;
}> {
	const hits: Hit[] = [];
	const server = http.createServer((req, res) => {
		let raw = "";
		req.on("data", (c) => (raw += c));
		req.on("end", () => {
			const body = raw === "" ? undefined : (JSON.parse(raw) as Record<string, unknown>);
			hits.push({ method: req.method ?? "", path: req.url ?? "", body, auth: req.headers.authorization ?? "" });
			const handler = handlers[req.url ?? ""];
			if (handler === undefined) {
				res.writeHead(404, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: `no fake handler for ${req.url}` }));
				return;
			}
			const { status = 200, json } = handler(body);
			res.writeHead(status, { "Content-Type": "application/json" });
			res.end(JSON.stringify(json));
		});
	});
	return new Promise((resolve) => {
		server.listen(0, "127.0.0.1", () => {
			const { port } = server.address() as AddressInfo;
			resolve({ url: `http://127.0.0.1:${port}`, hits, close: () => new Promise((r) => server.close(() => r())) });
		});
	});
}

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

/** Render the conversation's `memory_beliefs` section the way request
 * preparation builds the PromptInput (committed reads straight from the
 * harness — same shape the malleable.test.ts section tests use). */
async function renderMemoryBeliefs(
	handle: Awaited<ReturnType<typeof startTestHarness>>["handle"],
	conversationId: number,
): Promise<string | undefined> {
	const conversation = (await handle.harness.conversation(conversationId as never, context))!;
	const agent = await conversation.agent(context);
	const input = {
		conversationId: conversationId as never,
		agent,
		env: undefined,
		shown: {},
		read: handle.harness,
	} as unknown as PromptInput;
	const section = handle.registry
		.snapshot()
		.sections()
		.find((entry) => entry.section.key === "memory_beliefs");
	expect(section, "the memory_beliefs section must be registered").toBeDefined();
	return section!.section.render(input, context);
}

describe("H4 memory: memory_remember + memory_beliefs section (harness)", () => {
	it("(a) memory_remember relays to POST /agents/:id/memory/beliefs and reports the recorded note", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeMemoryApi({
			[`/agents/${AGENT_ID}/memory/beliefs`]: (body) => ({
				status: 201,
				json: { status: "recorded", note: "pending your review", belief_id: "b-1", body },
			}),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const { conversationId } = (await handle.handlers.createConversation({
					forgeSessionId: "sess-mem-a",
					agent: { provider: "faux", modelId: "faux-1" },
					policyAgentId: AGENT_ID,
				})) as { conversationId: number };

				faux.setResponses([
					fauxAssistantMessage(
						[fauxToolCall("memory_remember", { content: "user prefers tabs", kind: "preference" })],
						{ stopReason: "toolUse" },
					),
					fauxAssistantMessage("noted"),
				]);
				const { submissionId } = (await handle.handlers.submit({
					conversationId,
					requestId: "req-mem-a-1",
					entryDraft: { type: "input", content: "remember that I prefer tabs" },
				})) as { submissionId: number };
				const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
				expect(settled.status).toBe("done");

				// The relay hit the fake endpoint exactly once, owner-scoped,
				// with the model's args intact.
				const post = api.hits.filter((h) => h.method === "POST" && h.path === `/agents/${AGENT_ID}/memory/beliefs`);
				expect(post.length).toBe(1);
				expect(post[0].body).toEqual({ content: "user prefers tabs", kind: "preference" });
				expect(post[0].auth).toBe("Bearer test-key");

				// The tool result the model sees carries the recorded note.
				const page = await (await handle.harness.conversation(conversationId as never, context))!
					.entries({}, 100, undefined, context);
				const serialized = JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? {}));
				expect(serialized).toContain("recorded: pending your review");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(a2) memory_remember surfaces a rejection as a tool error", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeMemoryApi({
			[`/agents/${AGENT_ID}/memory/beliefs`]: () => ({ status: 404, json: { error: "Agent not found" } }),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const { conversationId } = (await handle.handlers.createConversation({
					forgeSessionId: "sess-mem-a2",
					agent: { provider: "faux", modelId: "faux-1" },
					policyAgentId: AGENT_ID,
				})) as { conversationId: number };

				faux.setResponses([
					fauxAssistantMessage([fauxToolCall("memory_remember", { content: "x" })], { stopReason: "toolUse" }),
					fauxAssistantMessage("ok"),
				]);
				const { submissionId } = (await handle.handlers.submit({
					conversationId,
					requestId: "req-mem-a2-1",
					entryDraft: { type: "input", content: "remember x" },
				})) as { submissionId: number };
				const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
				expect(settled.status).toBe("done");
				const page = await (await handle.harness.conversation(conversationId as never, context))!
					.entries({}, 100, undefined, context);
				const serialized = JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? {}));
				expect(serialized).toContain("Error: Agent not found");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(b) memory_beliefs renders top-confidence + retrieved beliefs; degrades when the embedding search 503s; omits when empty", async () => {
		const schema = freshSchemaName();
		track(schema);
		const beliefs = [
			{ id: "b-1", kind: "preference", content: "user prefers brief replies", confidence: 0.9, source_episodes: [] },
			{ id: "b-2", kind: "fact", content: "the deploy host is 10.0.0.4", confidence: 0.7, source_episodes: ["e-1"] },
		];
		const retrieved = [beliefs[1]];
		// The search path starts with the user message; any k works.
		const searchPath = `/agents/${AGENT_ID}/memory/search?q=${encodeURIComponent("make me a commit")}&k=5`;
		let searchStatus = 200;
		const api = await fakeMemoryApi({
			[`/agents/${AGENT_ID}/memory/beliefs?status=active&limit=15`]: () => ({ json: { beliefs } }),
			[searchPath]: () => ({ status: searchStatus, json: { beliefs: retrieved, episodes: [] } }),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const { conversationId } = (await handle.handlers.createConversation({
					forgeSessionId: "sess-mem-b",
					agent: { provider: "faux", modelId: "faux-1" },
					policyAgentId: AGENT_ID,
				})) as { conversationId: number };

				// One turn so the conversation has a user message for the
				// retrieval pass's query.
				faux.setResponses([fauxAssistantMessage("committing now")]);
				const { submissionId } = (await handle.handlers.submit({
					conversationId,
					requestId: "req-mem-b-1",
					entryDraft: { type: "input", content: "make me a commit" },
				})) as { submissionId: number };
				await (await handle.harness.submission(submissionId, context))!.wait(context);

				const rendered = await renderMemoryBeliefs(handle, conversationId);
				expect(rendered).toBeDefined();
				expect(rendered).toContain("What you know about this user");
				expect(rendered).toContain("user prefers brief replies");
				expect(rendered).toContain("the deploy host is 10.0.0.4");
				expect(rendered).toContain("confidence 0.90");

				// Retrieval down (503 — the embedding endpoint is down on
				// the forge side): the section STILL renders, confidence-only.
				searchStatus = 503;
				const degraded = await renderMemoryBeliefs(handle, conversationId);
				expect(degraded).toBeDefined();
				expect(degraded).toContain("user prefers brief replies");

				// (Empty-memory omission is test (c).)
await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

		it("(c) memory_beliefs omits when the agent has no active beliefs", async () => {
			const schema = freshSchemaName();
			track(schema);
			const api = await fakeMemoryApi({
				[`/agents/${AGENT_ID}/memory/beliefs?status=active&limit=15`]: () => ({ json: { beliefs: [] } }),
			});
			try {
				const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
				try {
					const { conversationId } = (await handle.handlers.createConversation({
						forgeSessionId: "sess-mem-c",
						agent: { provider: "faux", modelId: "faux-1" },
						policyAgentId: AGENT_ID,
					})) as { conversationId: number };

					faux.setResponses([fauxAssistantMessage("hello")]);
					const { submissionId } = (await handle.handlers.submit({
						conversationId,
						requestId: "req-mem-c-1",
						entryDraft: { type: "input", content: "hi" },
					})) as { submissionId: number };
					await (await handle.harness.submission(submissionId, context))!.wait(context);

					const rendered = await renderMemoryBeliefs(handle, conversationId);
					expect(rendered).toBeUndefined();
					await handle.stop();
				} finally {
					await done().catch(() => {});
				}
			} finally {
				await api.close();
			}
		}, 60_000);
});
