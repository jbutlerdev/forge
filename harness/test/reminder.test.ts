/**
 * Herd H5.2 acceptance (harness side): the `schedule_reminder` tool
 * relay — body mapping (`message` + exactly one of `in_minutes` /
 * `cron`), the owner-scoped session URL, and the tool result the
 * model sees.
 *
 * The forge reminders API is an in-process fake `node:http` server
 * (the faux-provider pattern; same shape as signals.test.ts).
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import type { AddressInfo } from "node:net";
import http from "node:http";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { Context } from "@earendil-works/pi-durable";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, startTestHarness } from "./support.js";

const context = { get: () => undefined } as unknown as Context;

const AGENT_ID = "22222222-2222-4222-8222-222222222222";
const SESSION_ID = "sess-rem-1";

interface Hit {
	method: string;
	path: string;
	body: Record<string, unknown> | undefined;
	auth: string;
}

/** Recorded-request fake forge API: answers the reminders POST with a
 * canned 201 and records every hit. */
async function fakeForgeApi(canned: Record<string, unknown> = {
	scheduled: true,
	session_id: SESSION_ID,
	timer_id: "timer_test_1",
	when: "2026-12-31T00:00:00+00:00",
}): Promise<{ url: string; hits: Hit[]; close: () => Promise<void> }> {
	const hits: Hit[] = [];
	const server = http.createServer((req, res) => {
		let raw = "";
		req.on("data", (c) => (raw += c));
		req.on("end", () => {
			const body = raw === "" ? undefined : (JSON.parse(raw) as Record<string, unknown>);
			hits.push({ method: req.method ?? "", path: req.url ?? "", body, auth: req.headers.authorization ?? "" });
			res.writeHead(201, { "Content-Type": "application/json" });
			res.end(JSON.stringify(canned));
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

/** Run one faux turn ending in a schedule_reminder tool call; returns
 * the serialized conversation entries (the tool result the model sees
 * is in there). */
async function runReminderCall(
	handle: Awaited<ReturnType<typeof startTestHarness>>["handle"],
	faux: Awaited<ReturnType<typeof startTestHarness>>["faux"],
	toolArgs: Record<string, unknown>,
	prompt: string,
): Promise<string> {
	const { conversationId } = (await handle.handlers.createConversation({
		forgeSessionId: SESSION_ID,
		agent: { provider: "faux", modelId: "faux-1" },
		policyAgentId: AGENT_ID,
	})) as { conversationId: number };

	faux.setResponses([
		fauxAssistantMessage([fauxToolCall("schedule_reminder", toolArgs)], { stopReason: "toolUse" }),
		fauxAssistantMessage("done"),
	]);
	const { submissionId } = (await handle.handlers.submit({
		conversationId,
		requestId: `req-${SESSION_ID}-1`,
		entryDraft: { type: "input", content: prompt },
	})) as { submissionId: number };
	const settled = await (await handle.harness.submission(submissionId, context))!.wait(context);
	expect(settled.status).toBe("done");

	const page = await (await handle.harness.conversation(conversationId as never, context))!.entries({}, 100, undefined, context);
	return JSON.stringify(page.items.map((entry) => entry.model ?? entry.data ?? {}));
}

describe("H5.2 schedule_reminder tool relay (harness)", () => {
	it("(a) in_minutes relays POST /sessions/:id/reminders with the session id and body, and confirms to the model", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeForgeApi();
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const serialized = await runReminderCall(
					handle,
					faux,
					{ message: "check the build status", in_minutes: 45 },
					"remind me later",
				);

				const post = api.hits.filter((h) => h.method === "POST" && h.path === `/sessions/${SESSION_ID}/reminders`);
				expect(post.length).toBe(1);
				// Body mapping: message + in_minutes; NO cron key at all.
				expect(post[0].body).toEqual({ message: "check the build status", in_minutes: 45 });
				expect("cron" in (post[0].body ?? {})).toBe(false);
				expect(post[0].auth).toBe("Bearer test-key");

				// The tool result the model sees confirms the schedule.
				expect(serialized).toContain("Reminder scheduled");
				expect(serialized).toContain("timer_test_1");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(b) cron relays the recurring spec (no in_minutes)", async () => {
		const schema = freshSchemaName();
		track(schema);
		const api = await fakeForgeApi({ scheduled: true, session_id: SESSION_ID, timer_id: "timer_test_2", when: "0 6 * * *" });
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, { apiUrl: api.url });
			try {
				const serialized = await runReminderCall(
					handle,
					faux,
					{ message: "morning report", cron: "0 6 * * *" },
					"recur this daily",
				);

				const post = api.hits.filter((h) => h.method === "POST" && h.path === `/sessions/${SESSION_ID}/reminders`);
				expect(post.length).toBe(1);
				expect(post[0].body).toEqual({ message: "morning report", cron: "0 6 * * *" });
				expect("in_minutes" in (post[0].body ?? {})).toBe(false);
				expect(serialized).toContain("0 6 * * *");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await api.close();
		}
	}, 60_000);

	it("(c) a server rejection surfaces as a tool error to the model", async () => {
		const schema = freshSchemaName();
		track(schema);
		const hits: Hit[] = [];
		const server = http.createServer((req, res) => {
			let raw = "";
			req.on("data", (c) => (raw += c));
			req.on("end", () => {
				hits.push({ method: req.method ?? "", path: req.url ?? "", body: undefined, auth: req.headers.authorization ?? "" });
				res.writeHead(400, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: "exactly one of in_minutes or cron must be set" }));
			});
		});
		await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
		const { port } = server.address() as AddressInfo;
		try {
			const { handle, faux, done } = await startTestHarness(schema, undefined, {
				apiUrl: `http://127.0.0.1:${port}`,
			});
			try {
				const serialized = await runReminderCall(
					handle,
					faux,
					{ message: "both-or-neither", in_minutes: 10, cron: "0 6 * * *" },
					"try an invalid spec",
				);
				// The 400 body must reach the model as an error.
				expect(serialized).toContain("exactly one of in_minutes or cron must be set");
				await handle.stop();
			} finally {
				await done().catch(() => {});
			}
		} finally {
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	}, 60_000);
});
