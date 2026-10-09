/**
 * Herd H4.6 acceptance (harness side): the `agent_signal` tool relay
 * (body mapping incl. the documented absent-`to` ⇒ org-broadcast
 * convention) and the `memory_signals` prompt section (renders unread
 * signals; omitted when empty; omitted on API failure — never fails
 * the turn).
 *
 * The forge memory API is an in-process fake `node:http` server (the
 * faux-provider pattern; same shape as memory.test.ts).
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

const AGENT_ID = "22222222-2222-4222-8222-222222222222";
const PEER_AGENT = "33333333-3333-4333-8333-333333333333";

/** Recorded request + canned-response handler (status overridable per
 * request via the closure's mutable state). */
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
	const { Pool } = await import("pg");
	const { PG_URL } = await import("./support.js");
	const pool = new Pool({ connectionString: PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

/** Render the conversation's `memory_signals` section the way request
 * preparation builds the PromptInput (same shape as the
 * memory.test.ts section helper). */
async function renderMemorySignals(handle: Awaited<ReturnType<typeof startTestHarness>>["handle"], conversationId: number): Promise<string | undefined> {
	const conversation = (await handle.harness.conversation(conversationId as never, context))!;
	const agent = await conversation.agent(context);
	const input = {
		conversationId: conversationId as never,
		agent,
		env: undefined,
		shown: {},
		read: handle.harness,
	} as unknown as PromptInput;
	const section = handle.registry.snapshot().sections().find((entry) => entry.section.key === "memory_signals");
	expect(section, "the memory_signals section must be registered").toBeDefined();
	return section!.section.render(input, context);
}

/** Run one faux turn ending in a tool call; returns the serialized
 * conversation entries (the tool result the model sees is in there). */
async function runToolCall(
	handle: Awaited<ReturnType<typeof startTestHarness>>["handle"],
	faux: Awaited<ReturnType<typeof startTestHarness>>["faux"],
	forgeSessionId: string,
	toolName: string,
	toolArgs: Record<string, unknown>,
	prompt: string,
): Promise<{ conversationId: number; serialized: string }> {
	const { conversationId } = (await handle.handlers.createConversation({
		forgeSessionId,
		agent: { provider: "faux", modelId: "faux-1" },
		policyAgentId: AGENT_ID,
	})) as { conversationId: number };

	faux.setResponses([
		fauxAssistantMessage([fauxToolCall(toolName, toolArgs)], { stopReason: "toolUse" }),
		fauxAssistantMessage("done"),
	]);
	const { submissionId } = (await handle.handlers.submit({
		conversationId,
		requestId: `req-${forgeSessionId}-1`,
		entryDraft: { type: "input", content: prompt },
	})) as { submissionId: number };
	const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
	expect(settled.status).toBe("done");

	const page = await (await handle.harness.conversation(conversationId as never, context))!.entries({}, 100, undefined, context);
	const serialized = JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? {}));
	return { conversationId, serialized };
}

describe("H4.6 cross-agent signals: agent_signal tool + memory_signals section (harness)", () => {
	it("(a) agent_signal without `to` relays to POST /agents/:id/memory/signals with kind/payload only (absent to ⇒ org broadcast, documented)", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeMemoryApi({
			[`/agents/${AGENT_ID}/memory/signals`]: () => ({
				status: 201,
				json: { recorded: true, signal_id: "sig-1", kind: "insight", to: null },
			}),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const { serialized } = await runToolCall(handle, faux, "sess-sig-a", "agent_signal", {
					kind: "insight",
					payload: { note: "PO template changed", detail: "v2 requires the new line items" },
				}, "signal the team");

				// The relay hit the fake endpoint exactly once, owner-scoped.
				const post = api.hits.filter((h) => h.method === "POST" && h.path === `/agents/${AGENT_ID}/memory/signals`);
				expect(post.length).toBe(1);
				// Body mapping: kind + payload intact; NO `to` key at all
				// (absent ⇒ forge treats the signal as an org broadcast).
				expect(post[0].body).toEqual({
					kind: "insight",
					payload: { note: "PO template changed", detail: "v2 requires the new line items" },
				});
				expect("to" in (post[0].body ?? {})).toBe(false);
				expect(post[0].auth).toBe("Bearer test-key");

				// The tool result the model sees confirms the broadcast.
				expect(serialized).toContain("org broadcast");
				expect(serialized).toContain("insight");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(b) agent_signal with `to` relays the target agent id verbatim", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeMemoryApi({
			[`/agents/${AGENT_ID}/memory/signals`]: (body) => ({
				status: 201,
				json: { recorded: true, signal_id: "sig-2", kind: "handoff", to: PEER_AGENT },
			}),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const { serialized } = await runToolCall(handle, faux, "sess-sig-b", "agent_signal", {
					kind: "handoff",
					to: PEER_AGENT,
					payload: { note: "pick this up when you're free" },
				}, "hand off to the peer");

				const post = api.hits.filter((h) => h.method === "POST" && h.path === `/agents/${AGENT_ID}/memory/signals`);
				expect(post.length).toBe(1);
				expect(post[0].body).toEqual({
					kind: "handoff",
					to: PEER_AGENT,
					payload: { note: "pick this up when you're free" },
				});
				expect(serialized).toContain(PEER_AGENT);
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(b2) agent_signal surfaces a server rejection as a tool error", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeMemoryApi({
			[`/agents/${AGENT_ID}/memory/signals`]: () => ({ status: 400, json: { error: "invalid to (agent UUID)" } }),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				// Schema-valid args, unresolvable target: the SERVER rejects
				// and the relay must surface the 400 body to the model.
				const { serialized } = await runToolCall(handle, faux, "sess-sig-b2", "agent_signal", {
					kind: "insight",
					to: "not-a-uuid",
					payload: {},
				}, "signal");

				expect(serialized).toContain("Error: invalid to (agent UUID)");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(c) memory_signals renders unread signals, omits when empty, omits on API failure (never fails the turn)", async () => {
		const schema = freshSchemaName();
		track(schema);
		// The unread endpoint's response is mutable: the test walks
		// signals-present → empty → 500 against ONE conversation.
		const unreadPath = `/agents/${AGENT_ID}/memory/signals/unread?limit=10`;
		let unreadStatus = 200;
		let unreadBody: unknown = {
			signals: [
				{
					id: "sig-1",
					kind: "insight",
					from_agent: PEER_AGENT,
					created_at: "2026-10-06T00:00:00Z",
					payload: { note: "PO template changed" },
				},
				{
					id: "sig-2",
					kind: "handoff",
					from_agent: PEER_AGENT,
					created_at: "2026-10-06T00:01:00Z",
					// No preferred summary field: the JSON is summarized.
					payload: { detail: { steps: [1, 2, 3] }, note: "pick this up" },
				},
			],
		};
		const api = await fakeMemoryApi({
			[unreadPath]: () => ({ status: unreadStatus, json: unreadBody }),
		});
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const { conversationId } = (await handle.handlers.createConversation({
					forgeSessionId: "sess-sig-c",
					agent: { provider: "faux", modelId: "faux-1" },
					policyAgentId: AGENT_ID,
				})) as { conversationId: number };

				// One turn so the conversation is live (the section render
				// itself does not read the conversation).
				faux.setResponses([fauxAssistantMessage("hello")]);
				const { submissionId } = (await handle.handlers.submit({
					conversationId,
					requestId: "req-sig-c-1",
					entryDraft: { type: "input", content: "hi" },
				})) as { submissionId: number };
				await (await handle.harness.submission(submissionId, context))!.wait(context);

				// Signals present: the section renders a compact block with
				// one line per signal (kind + from + payload summary).
				const rendered = await renderMemorySignals(handle, conversationId);
				expect(rendered).toBeDefined();
				expect(rendered).toContain("Signals for you");
				expect(rendered).toContain(`- [insight] from ${PEER_AGENT}: PO template changed`);
				// sig-2 prefers its `note` field over the full JSON.
				expect(rendered).toContain(`- [handoff] from ${PEER_AGENT}: pick this up`);
				// Both fetches happened: the fake saw the limit param.
				expect(api.hits.filter((h) => h.path === unreadPath).length).toBeGreaterThanOrEqual(1);
				const first = api.hits.find((h) => h.path === unreadPath);
				expect(first?.path).toBe(unreadPath);

				// Empty: the section is OMITTED (stable-prompt rule).
				unreadBody = { signals: [] };
				expect(await renderMemorySignals(handle, conversationId)).toBeUndefined();

				// API failure: the section is OMITTED and the turn never
				// fails (a failed fetch consumes nothing server-side).
				unreadStatus = 500;
				expect(await renderMemorySignals(handle, conversationId)).toBeUndefined();
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);
});
