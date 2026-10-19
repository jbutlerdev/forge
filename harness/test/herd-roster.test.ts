/**
 * Herd roster prompt section (2026-10-19): the live agent list as a
 * per-turn section. Why: sticky herd conversations quoted a stale
 * `list_agents` toolResult and "couldn't see" recently-created agents
 * (Shepherd) while GET /agents was healthy. The section re-fetches
 * every render, so the roster cannot go stale; a failed fetch omits
 * the section (never fails the turn).
 */
import type { AddressInfo } from "node:net";
import http from "node:http";
import type { Context, PromptInput } from "@earendil-works/pi-durable";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const AGENT_ID = "44444444-4444-4444-8444-444444444444";

type Responder = { status?: number; json: unknown } | (() => { status?: number; json: unknown });

function fakeForge(responders: Record<string, Responder>): Promise<{
	url: string;
	close: () => Promise<void>;
}> {
	const server = http.createServer((req, res) => {
		// Drain the request first (the signals.test.ts fake pattern):
		// replying before the body lands can truncate the response.
		req.on("data", () => {});
		req.on("end", () => {
			const path = (req.url ?? "").split("?")[0] ?? "";
			const entry = responders[path];
			const hit = typeof entry === "function" ? entry() : entry;
			const payload = hit === undefined ? JSON.stringify({ error: `no fake handler for ${path}` }) : JSON.stringify(hit.json ?? {});
			res.writeHead(hit === undefined ? 404 : hit.status ?? 200, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(payload) });
			res.end(payload);
		});
	});
	return new Promise((resolve) => {
		server.listen(0, "127.0.0.1", () => {
			const { port } = server.address() as AddressInfo;
			resolve({ url: `http://127.0.0.1:${port}`, close: () => new Promise((r) => server.close(() => r())) });
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

async function renderRoster(handle: Awaited<ReturnType<typeof startTestHarness>>["handle"], conversationId: number): Promise<string | undefined> {
	const conversation = (await handle.harness.conversation(conversationId as never, context))!;
	const agent = await conversation.agent(context);
	const input = {
		conversationId: conversationId as never,
		agent,
		env: undefined,
		shown: {},
		read: handle.harness,
	} as unknown as PromptInput;
	const section = handle.registry.snapshot().sections().find((entry) => entry.section.key === "herd_roster");
	expect(section, "the herd_roster section must be registered").toBeDefined();
	return section!.section.render(input, context);
}

async function openConversation(handle: Awaited<ReturnType<typeof startTestHarness>>["handle"]): Promise<number> {
	const { conversationId } = (await handle.handlers.createConversation({
		forgeSessionId: `roster-${Math.random().toString(36).slice(2)}`,
		agent: { provider: "faux", modelId: "faux-1" },
		policyAgentId: AGENT_ID,
	})) as { conversationId: number };
	return conversationId;
}

describe("herd_roster prompt section", () => {
	it("renders the live list (names, you-marker, paused, role) and re-fetches per render", async () => {
		const schema = freshSchemaName();
		track(schema);
		let generation = 0;
		const api = await fakeForge({
			"/agents": () => ({
				json: {
					agents: [
						{ id: AGENT_ID, name: "executive assistant", home_machine: "1e22c5d7-962a-42f3-bbf4-f0fd7787556b", description: "Runs the fleet: intake, triage, follow-through." },
						{ id: "b955926b-82a8-4856-a283-b41801119977", name: "Shepherd", home_machine: "1e22c5d7-962a-42f3-bbf4-f0fd7787556b", paused: generation === 0, description: "Audits the herd for bugs, tech debt, and misbehavior; pushes fixes." },
					],
				},
			}),
		});
		const { handle } = await startTestHarness(schema, undefined, { apiUrl: api.url });
		const conversationId = await openConversation(handle);

		const first = await renderRoster(handle, conversationId);
		expect(first).toBeDefined();
		expect(first).toContain("Herd roster (live, 2 agent(s)");
		expect(first).toContain("executive assistant (you)");
		expect(first).toContain("Shepherd");
		expect(first).toContain("PAUSED"); // generation 0
		expect(first).toContain("Audits the herd");
		expect(first).toContain("home 1e22c5d7");

		// Second render reflects registry changes WITHOUT any tool call —
		// the stale-context bug in one line.
		generation = 1;
		const second = await renderRoster(handle, conversationId);
		expect(second).toBeDefined();
		expect(second).not.toContain("PAUSED");
		await api.close();
	});

	it("omits the section when the list is empty (stable prompt)", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeForge({ "/agents": () => ({ json: { agents: [] } }) });
		const { handle } = await startTestHarness(schema, undefined, { apiUrl: api.url });
		const conversationId = await openConversation(handle);
		expect(await renderRoster(handle, conversationId)).toBeUndefined();
		await api.close();
	});

	it("omits (never throws) when the control plane is down — the turn survives on the previous render", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeForge({ "/agents": () => ({ status: 500, json: { error: "boom" } }) });
		const { handle } = await startTestHarness(schema, undefined, { apiUrl: api.url });
		const conversationId = await openConversation(handle);
		expect(await renderRoster(handle, conversationId)).toBeUndefined();
		await api.close();
	});
});
