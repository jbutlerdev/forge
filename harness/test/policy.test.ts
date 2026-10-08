/**
 * Herd H3.5 acceptance (harness side): the `before_tool` POLICY hook —
 * mule `POST /api/v1/policies/evaluate` → allow / deny / ask, with the
 * ask path through forge's `POST /sessions/:id/policy-ask` relay
 * round-trip and the best-effort `POST /api/v1/policies/memo` write.
 *
 * Both the mule control plane and the forge API are in-process fake
 * `node:http` servers (the faux-provider pattern): the harness
 * extension's `apiUrl` points at the fake forge; `FORGE_POLICY_URL`
 * (set per test BEFORE `createConversation`, read once at extension
 * build) points at the fake mule.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import type { AddressInfo } from "node:net";
import http from "node:http";
import { fauxAssistantMessage, fauxToolCall, type FauxProviderHandle } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/pi-durable";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import type { HarnessHandle } from "../src/main.js";
import { freshSchemaName, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

/** One recorded HTTP request (method, path, parsed JSON body, auth). */
interface Hit {
	method: string;
	path: string;
	body: Record<string, unknown> | undefined;
	auth: string;
}

/** A minimal fake HTTP server: one JSON-response handler per path,
 * plus a full hit log. */
function fakeServer(handlers: Record<string, (body: Record<string, unknown>) => Record<string, unknown>>): Promise<{
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
			hits.push({
				method: req.method ?? "",
				path: req.url ?? "",
				body,
				auth: req.headers.authorization ?? "",
			});
			const handler = handlers[req.url ?? ""];
			if (handler === undefined) {
				res.writeHead(404, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: `no fake handler for ${req.url}` }));
				return;
			}
			res.writeHead(200, { "Content-Type": "application/json" });
			res.end(JSON.stringify(handler(body ?? {})));
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
	// Drop ONLY this file's schemas: the suite runs test files in
	// parallel against the same scratch Postgres.
	const { Pool } = await import("pg");
	const { PG_URL } = await import("./support.js");
	const pool = new Pool({ connectionString: PG_URL });
	for (const n of schemas) await pool.query(`DROP SCHEMA IF EXISTS ${n} CASCADE`).catch(() => {});
	await pool.end().catch(() => {});
});

/** Env cleanup: the policy env is read once per extension build, so
 * every test must leave both vars unset for its siblings. */
afterEach(() => {
	delete process.env.FORGE_POLICY_URL;
	delete process.env.FORGE_POLICY_API_KEY;
});

/** Drive one turn: the model calls `tool` once, then answers. Returns
 * the serialized transcript of the conversation's entries. */
async function driveToolCall(
	handle: HarnessHandle,
	faux: FauxProviderHandle,
	params: { forgeSessionId: string; policyAgentId?: string; tool: string; toolArgs: Record<string, unknown> },
): Promise<{ entries: string; conversationId: number }> {
	const { conversationId } = (await handle.handlers.createConversation({
		forgeSessionId: params.forgeSessionId,
		agent: { provider: "faux", modelId: "faux-1" },
		...((params.policyAgentId !== undefined) ? { policyAgentId: params.policyAgentId } : {}),
	})) as { conversationId: number };

	faux.setResponses([
		fauxAssistantMessage([fauxToolCall(params.tool, params.toolArgs)], { stopReason: "toolUse" }),
		fauxAssistantMessage("policy-final"),
	]);
	const { submissionId } = (await handle.handlers.submit({
		conversationId,
		requestId: `req-${params.forgeSessionId}-1`,
		entryDraft: { type: "input", content: "go" },
	})) as { submissionId: number };
	const settled = await (await handle.harness.submission(submissionId as never, context))!.wait(context);
	expect(settled.status).toBe("done");
	const page = await (await handle.harness.conversation(conversationId as never, context))!.entries(
		{},
		100,
		undefined,
		context,
	);
	return { entries: JSON.stringify(page.items.map((e) => e.model ?? e.data ?? {})), conversationId };
}

describe("H3.5 policy hook (harness)", () => {
	it("(a) mule 'deny' → the tool is blocked with the rule id, the tool body never runs", async () => {
		const schema = freshSchemaName();
		track(schema);
		const mule = await fakeServer({
			"/api/v1/policies/evaluate": () => ({
				verdict: "deny",
				rule_id: "rm-guard",
				reason: "rm -rf is blocked by operator policy",
				memoized: false,
			}),
		});
		// The fake forge records whether a tool call would have run.
		const forge = await fakeServer({
			"/tools/execute": () => ({ success: true, output: "UNREACHABLE" }),
			"/tools/execute/stream": () => ({ error: "stream endpoint must not be used by read" }),
		});
		let done: (() => Promise<void>) | undefined;
		try {
			process.env.FORGE_POLICY_URL = mule.url;
			process.env.FORGE_POLICY_API_KEY = "sk_mule_test_policy";
			const { handle, faux } = await startTestHarness(schema, undefined, { apiUrl: forge.url });
			done = handle.done;

			const { entries } = await driveToolCall(handle, faux, {
				forgeSessionId: "sess-policy-a",
				policyAgentId: "agent-policy-a",
				tool: "read",
				toolArgs: { path: "rm-guard-target.txt" },
			});

			// The model sees the deny reason with the rule id…
			expect(entries).toContain("Policy denied (rm-guard)");
			expect(entries).toContain("rm -rf is blocked by operator policy");
			expect(entries).toContain("NOT executed");
			// …and the tool body NEVER ran (no /tools/execute hit).
			expect(forge.hits.filter((h) => h.path.startsWith("/tools/execute"))).toEqual([]);
			// The evaluate call carried the agent id + tool + input.
			const ev = mule.hits.filter((h) => h.path === "/api/v1/policies/evaluate");
			expect(ev).toHaveLength(1);
			expect(ev[0].body?.agent_id).toBe("agent-policy-a");
			expect(ev[0].body?.tool).toBe("read");
			expect(ev[0].auth).toBe("Bearer sk_mule_test_policy");
			// No ask round-trip, no memo write on a plain deny.
			expect(forge.hits.filter((h) => h.path.endsWith("/policy-ask"))).toEqual([]);
			expect(mule.hits.filter((h) => h.path === "/api/v1/policies/memo")).toEqual([]);
		} finally {
			await done?.().catch(() => {});
			await mule.close();
			await forge.close();
		}
	}, 60_000);

	it("(b) mule 'ask' → forge policy-ask 'allow' → the tool runs AND the allow was memoized", async () => {
		const schema = freshSchemaName();
		track(schema);
		const mule = await fakeServer({
			"/api/v1/policies/evaluate": () => ({
				verdict: "ask",
				rule_id: "etc-guard",
				reason: "writing to /etc needs approval",
				memoized: false,
			}),
			"/api/v1/policies/memo": () => ({ recorded: true }),
		});
		const forge = await fakeServer({
			"/sessions/sess-policy-b/policy-ask": () => ({ decision: "allow" }),
			"/tools/execute": () => ({ success: true, output: "MEMOIZED-PROCEED" }),
		});
		let done: (() => Promise<void>) | undefined;
		try {
			process.env.FORGE_POLICY_URL = mule.url;
			const { handle, faux } = await startTestHarness(schema, undefined, { apiUrl: forge.url });
			done = handle.done;

			const { entries } = await driveToolCall(handle, faux, {
				forgeSessionId: "sess-policy-b",
				policyAgentId: "agent-policy-b",
				tool: "read",
				toolArgs: { path: "/etc/hosts" },
			});

			// The ask round-trip hit the fake forge with the decision
			// context (rule id + reason + the tool + its input).
			const ask = forge.hits.filter((h) => h.path === "/sessions/sess-policy-b/policy-ask");
			expect(ask).toHaveLength(1);
			expect(ask[0].body?.rule_id).toBe("etc-guard");
			expect(ask[0].body?.reason).toBe("writing to /etc needs approval");
			expect(ask[0].body?.tool).toBe("read");
			expect(ask[0].body?.input).toEqual({ path: "/etc/hosts" });
			// The approved action was memoized on mule (verdict allow,
			// the same action hash inputs).
			const memo = mule.hits.filter((h) => h.path === "/api/v1/policies/memo");
			expect(memo).toHaveLength(1);
			expect(memo[0].body?.verdict).toBe("allow");
			expect(memo[0].body?.agent_id).toBe("agent-policy-b");
			expect(memo[0].body?.rule_id).toBe("etc-guard");
			// And the tool RAN: its output is in the transcript.
			expect(entries).toContain("MEMOIZED-PROCEED");
		} finally {
			await done?.().catch(() => {});
			await mule.close();
			await forge.close();
		}
	}, 60_000);

	it("(c) mule 'ask' → forge policy-ask 'expired' → blocked fail-closed, the tool never runs", async () => {
		const schema = freshSchemaName();
		track(schema);
		const mule = await fakeServer({
			"/api/v1/policies/evaluate": () => ({
				verdict: "ask",
				rule_id: "etc-guard",
				reason: "writing to /etc needs approval",
				memoized: false,
			}),
		});
		const forge = await fakeServer({
			"/sessions/sess-policy-c/policy-ask": () => ({ decision: "expired" }),
		});
		let done: (() => Promise<void>) | undefined;
		try {
			process.env.FORGE_POLICY_URL = mule.url;
			const { handle, faux } = await startTestHarness(schema, undefined, { apiUrl: forge.url });
			done = handle.done;

			const { entries } = await driveToolCall(handle, faux, {
				forgeSessionId: "sess-policy-c",
				tool: "read",
				toolArgs: { path: "/etc/hosts" },
			});

			// Expiry is a fail-closed block naming the rule…
			expect(entries).toContain("Policy decision pending/expired (etc-guard)");
			expect(entries).toContain("NOT executed");
			// …with no tool execution and no memo write.
			expect(forge.hits.filter((h) => h.path.startsWith("/tools/execute"))).toEqual([]);
			expect(mule.hits.filter((h) => h.path === "/api/v1/policies/memo")).toEqual([]);
		} finally {
			await done?.().catch(() => {});
			await mule.close();
			await forge.close();
		}
	}, 60_000);

	it("(d) mule down (connection refused) → blocked with 'policy unavailable', fail-closed", async () => {
		const schema = freshSchemaName();
		track(schema);
		// A port that is not listening: bind + close to get one.
		const dead = await new Promise<number>((resolve) => {
			const s = http.createServer();
			s.listen(0, "127.0.0.1", () => {
				const { port } = s.address() as AddressInfo;
				s.close(() => resolve(port));
			});
		});
		const forge = await fakeServer({});
		let done: (() => Promise<void>) | undefined;
		try {
			process.env.FORGE_POLICY_URL = `http://127.0.0.1:${dead}`;
			const { handle, faux } = await startTestHarness(schema, undefined, { apiUrl: forge.url });
			done = handle.done;

			const { entries } = await driveToolCall(handle, faux, {
				forgeSessionId: "sess-policy-d",
				tool: "read",
				toolArgs: { path: "anything" },
			});

			// Fail closed: the model is told the policy engine is
			// unreachable and the call did not run.
			expect(entries).toContain("Policy unavailable");
			expect(entries).toContain("NOT executed");
			expect(forge.hits.filter((h) => h.path.startsWith("/tools/execute"))).toEqual([]);
		} finally {
			await done?.().catch(() => {});
			await forge.close();
		}
	}, 60_000);

	it("(e) FORGE_POLICY_URL unset → no mule calls, the tool runs (opt-out default)", async () => {
		const schema = freshSchemaName();
		track(schema);
		// A mule server that would RECORD any call: it must stay empty.
		const mule = await fakeServer({
			"/api/v1/policies/evaluate": () => ({ verdict: "allow", rule_id: "", reason: "", memoized: false }),
		});
		const forge = await fakeServer({
			"/tools/execute": () => ({ success: true, output: "NO-POLICY-RAN" }),
		});
		let done: (() => Promise<void>) | undefined;
		try {
			// No FORGE_POLICY_URL (afterEach guarantees a clean slate).
			const { handle, faux } = await startTestHarness(schema, undefined, { apiUrl: forge.url });
			done = handle.done;

			const { entries } = await driveToolCall(handle, faux, {
				forgeSessionId: "sess-policy-e",
				tool: "read",
				toolArgs: { path: "plain" },
			});

			// The tool ran normally…
			expect(entries).toContain("NO-POLICY-RAN");
			// …and the policy control plane was never contacted.
			expect(mule.hits).toEqual([]);
		} finally {
			await done?.().catch(() => {});
			await mule.close();
			await forge.close();
		}
	}, 60_000);

	it("(f) memoized 'allow' → the tool runs without any policy-ask round-trip", async () => {
		const schema = freshSchemaName();
		track(schema);
		const mule = await fakeServer({
			// A previously approved action: mule answers from policy_memo.
			"/api/v1/policies/evaluate": () => ({
				verdict: "allow",
				rule_id: "etc-guard",
				reason: "writing to /etc needs approval (approved)",
				memoized: true,
			}),
		});
		const forge = await fakeServer({
			"/tools/execute": () => ({ success: true, output: "MEMO-ALLOW-RAN" }),
		});
		let done: (() => Promise<void>) | undefined;
		try {
			process.env.FORGE_POLICY_URL = mule.url;
			const { handle, faux } = await startTestHarness(schema, undefined, { apiUrl: forge.url });
			done = handle.done;

			const { entries } = await driveToolCall(handle, faux, {
				forgeSessionId: "sess-policy-f",
				tool: "read",
				toolArgs: { path: "/etc/hosts" },
			});

			// One evaluate, NO ask, NO memo re-write…
			expect(mule.hits.filter((h) => h.path === "/api/v1/policies/evaluate")).toHaveLength(1);
			expect(mule.hits.filter((h) => h.path === "/api/v1/policies/memo")).toEqual([]);
			expect(forge.hits.filter((h) => h.path.endsWith("/policy-ask"))).toEqual([]);
			// …and the tool ran.
			expect(entries).toContain("MEMO-ALLOW-RAN");
		} finally {
			await done?.().catch(() => {});
			await mule.close();
			await forge.close();
		}
	}, 60_000);
});
