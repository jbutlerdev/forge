/**
 * Herd H6.5: the `web_login` / `web_login_done` in-process tools
 * (the "web-auth tool", PLAN-HERD §H6.5). Unit-tested against a fake
 * forge that mimics the two endpoint shapes:
 *
 *   - POST /sessions/:id/web-login    → {status: "approved"|"denied"|"expired", handoff_url?}
 *   - GET  /sessions/:id/web-login?slot=… → {status: "pending"|"signed_in"|"none"}
 *
 * The invariant these tests guard: the MODEL-VISIBLE OUTPUT NEVER
 * contains credential material — the tool's outputs are status
 * strings only (the cookie value never crosses the forge → harness
 * boundary at all; the fake server proves it by never sending one).
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { type AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { Context } from "@earendil-works/chord";
import {
	createWebLoginDoneTool,
	createWebLoginTool,
	type ForgeToolOptions,
} from "../src/forge-ext.js";

interface SeenRequest {
	method: string;
	url: string;
	authorization: string | null;
	body: Record<string, unknown>;
}

interface ToolResult {
	content?: Array<{ type?: string; text?: string }>;
	isError?: boolean;
}

let server: Server;
let baseUrl = "";
let seen: SeenRequest[];
/** The fake forge's "secret store": the cookie value it would hand
 * the sandbox — NEVER included in any response body. */
const STORED_COOKIE = "sk-live-cookie-h65-should-never-reach-the-model";

function readBody(req: IncomingMessage): Promise<Record<string, unknown>> {
	return new Promise((resolve) => {
		let data = "";
		req.on("data", (c) => (data += c));
		req.on("end", () => resolve(data ? JSON.parse(data) : {}));
	});
}

beforeAll(async () => {
	seen = [];
	server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
		const url = req.url ?? "";
		const body = req.method === "POST" ? await readBody(req) : {};
		seen.push({
			method: req.method ?? "",
			url,
			authorization: req.headers.authorization ?? null,
			body,
		});
		res.setHeader("content-type", "application/json");
		if (req.method === "POST" && url.startsWith("/sessions/sess-1/web-login")) {
			if (body.url === "https://denied.example.com/login") {
				res.end(JSON.stringify({ status: "denied" }));
				return;
			}
			if (body.url === "https://slow.example.com/login") {
				res.end(JSON.stringify({ status: "expired" }));
				return;
			}
			res.end(
				JSON.stringify({
					status: "approved",
					handoff_url: `${baseUrl}/auth/browser-handoff?token=one-time-abc&next=${encodeURIComponent(String(body.url ?? ""))}`,
					slot: body.slot,
				}),
			);
			return;
		}
		if (req.method === "GET" && url.startsWith("/sessions/sess-1/web-login?")) {
			const slot = new URL(url, "http://x").searchParams.get("slot");
			res.end(JSON.stringify({ status: slot === "PENDING_SLOT" ? "pending" : "signed_in" }));
			return;
		}
		res.statusCode = 404;
		res.end(JSON.stringify({ error: "not found" }));
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const address = server.address() as AddressInfo;
	baseUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(() => {
	server.close();
});

/** A fake ToolExecutionApi bound to the fake forge: the session id
 * comes from the conversation's `forge.meta` document, exactly like
 * the real extension reads it. */
function fakeMetaApi() {
	return {
		conversationId: "conv-1",
		callId: "tc-1",
		snapshot: async () => ({ value: { forgeSessionId: "sess-1" } }),
		commit: async () => {},
	} as unknown;
}

function noMetaApi() {
	return {
		conversationId: "conv-x",
		callId: "tc-x",
		snapshot: async () => undefined,
		commit: async () => {},
	} as unknown;
}

function exec(tool: unknown, args: Record<string, unknown>, api: unknown): Promise<ToolResult> {
	return (tool as { execute: (a: unknown, b: unknown, c: unknown) => Promise<ToolResult> }).execute(
		args,
		api,
		{} as Context,
	);
}

function textOf(result: ToolResult): string {
	return (result.content ?? []).map((c) => c.text ?? "").join("");
}

function baseOptions(overrides: Partial<ForgeToolOptions>): ForgeToolOptions {
	return { ...overrides, apiUrl: baseUrl, apiKey: "sk-forge-test-key" };
}

describe("web_login (H6.5)", () => {
	it("approved: relays {url, slot} with the Bearer key; the output is a status string (no credential material)", async () => {
		const tool = createWebLoginTool(baseOptions({}));
		const result = await exec(
			tool,
			{ url: "https://billing.example.com/login", slot: "BILLING_COOKIE" },
			fakeMetaApi(),
		);
		expect(result.isError).not.toBe(true);
		const text = textOf(result);
		expect(text).toContain("Sign-in approved");
		expect(text).toContain("one-time-abc"); // the handoff url is relayed so the user can open it
		// THE invariant: no credential material in the model-visible output.
		expect(text).not.toContain(STORED_COOKIE);

		const call = seen.at(-1);
		expect(call?.method).toBe("POST");
		expect(call?.url).toBe("/sessions/sess-1/web-login");
		expect(call?.authorization).toBe("Bearer sk-forge-test-key");
		expect(call?.body).toEqual({ url: "https://billing.example.com/login", slot: "BILLING_COOKIE" });
	});

	it("denied: the model sees a soft error telling it not to retry", async () => {
		const tool = createWebLoginTool(baseOptions({}));
		const result = await exec(
			tool,
			{ url: "https://denied.example.com/login", slot: "SLOT_A" },
			fakeMetaApi(),
		);
		expect(result.isError).toBe(true);
		expect(textOf(result)).toContain("declined");
		expect(textOf(result)).not.toContain(STORED_COOKIE);
	});

	it("expired: surfaces as a retry-able soft error, not a network error", async () => {
		const tool = createWebLoginTool(baseOptions({}));
		const result = await exec(
			tool,
			{ url: "https://slow.example.com/login", slot: "SLOT_A" },
			fakeMetaApi(),
		);
		expect(result.isError).toBe(true);
		expect(textOf(result)).toContain("timed out");
	});

	it("validates the slot BEFORE any network call", async () => {
		const before = seen.length;
		const tool = createWebLoginTool(baseOptions({}));
		const result = await exec(tool, { url: "https://x.io/login", slot: "lower-case" }, fakeMetaApi());
		expect(result.isError).toBe(true);
		expect(seen.length).toBe(before); // no request left the harness
	});

	it("no session id: fails without touching forge", async () => {
		const before = seen.length;
		const tool = createWebLoginTool(baseOptions({}));
		const result = await exec(tool, { url: "https://x.io/login", slot: "SLOT_A" }, noMetaApi());
		expect(result.isError).toBe(true);
		expect(textOf(result)).toContain("no forge session id");
		expect(seen.length).toBe(before);
	});
});

describe("web_login_done (H6.5)", () => {
	it("pending: still waiting for the user", async () => {
		const tool = createWebLoginDoneTool(baseOptions({}));
		const result = await exec(tool, { slot: "PENDING_SLOT" }, fakeMetaApi());
		expect(textOf(result)).toContain("Still waiting");
		expect(textOf(result)).not.toContain(STORED_COOKIE);
		const call = seen.at(-1);
		expect(call?.url).toBe("/sessions/sess-1/web-login?slot=PENDING_SLOT");
	});

	it("signed_in: reports the env var name and the no-print invariant", async () => {
		const tool = createWebLoginDoneTool(baseOptions({}));
		const result = await exec(tool, { slot: "BILLING_COOKIE" }, fakeMetaApi());
		const text = textOf(result);
		expect(text).toContain("Signed in");
		expect(text).toContain("environment variable BILLING_COOKIE");
		expect(text).toContain("Never print");
		expect(text).not.toContain(STORED_COOKIE);
	});

	it("blank slot: clean validation error, no crash", async () => {
		const tool = createWebLoginDoneTool(baseOptions({}));
		const result = await exec(tool, { slot: "" }, fakeMetaApi());
		expect(result.isError).toBe(true);
		expect(textOf(result)).toContain("requires a `slot`");
	});
});
