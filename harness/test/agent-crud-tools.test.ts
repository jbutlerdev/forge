/**
 * Unit tests for the herd agent CRUD tools' pure helpers (H6.5d):
 * normalizeWakeCron (create_agent schedule), buildAgentEditBody
 * (edit_agent PATCH diff). The fetch paths are exercised live in the
 * lab (Bob create→edit→delete round-trip).
 */
import { describe, expect, it } from "vitest";
import { buildAgentEditBody, normalizeWakeCron } from "../src/forge-ext.js";

describe("normalizeWakeCron (create_agent)", () => {
	it("converts HH:MM to 5-field daily", () => {
		expect(normalizeWakeCron("07:00")).toBe("0 7 * * *");
		expect(normalizeWakeCron(" 23:59 ")).toBe("59 23 * * *");
	});
	it("passes through 5-field cron", () => {
		expect(normalizeWakeCron("*/30 * * * *")).toBe("*/30 * * * *");
		expect(normalizeWakeCron("0 3 * * 1-5")).toBe("0 3 * * 1-5");
	});
	it("rejects junk", () => {
		expect(normalizeWakeCron("every day")).toBeUndefined();
		expect(normalizeWakeCron("24:00")).toBeUndefined();
		expect(normalizeWakeCron("* * *")).toBeUndefined();
		expect(normalizeWakeCron("0 0 * * rm")).toBeUndefined();
	});
});

describe("buildAgentEditBody (edit_agent)", () => {
	it("includes ONLY supplied keys (PATCH = keep absent)", () => {
		const body = buildAgentEditBody({ agent: "x", visibility: "org" }, null);
		expect(body).toEqual({ visibility: "org" });
	});
	it("maps profile_id → primary_profile_id; skips blanks", () => {
		const body = buildAgentEditBody(
			{ agent: "x", profile_id: "p-1", name: "", home_machine: undefined },
			null,
		);
		expect(body).toEqual({ primary_profile_id: "p-1" });
	});
	it("tools_allowlist [] IS a change (means: all tools)", () => {
		const body = buildAgentEditBody({ agent: "x", tools_allowlist: [] }, null);
		expect(body).toEqual({ tools_allowlist: [] });
	});
	it("extra_instructions REPLACES; system_prompt_append APPENDS to current", () => {
		const current = { extra_instructions: "Original mission." };
		const replace = buildAgentEditBody({ agent: "x", extra_instructions: "New mission." }, current);
		expect(replace.extra_instructions).toBe("New mission.");
		const append = buildAgentEditBody({ agent: "x", system_prompt_append: "Also be terse." }, current);
		expect(append.extra_instructions).toBe("Original mission.\n\nAlso be terse.");
		const appendEmpty = buildAgentEditBody({ agent: "x", system_prompt_append: "Hi." }, null);
		expect(appendEmpty.extra_instructions).toBe("Hi.");
	});
	it("empty args → empty body (tool turns it into a model error)", () => {
		expect(buildAgentEditBody({ agent: "x" }, null)).toEqual({});
	});
});

import { summarizeSandboxRequest } from "../src/forge-ext.js";

describe("summarizeSandboxRequest", () => {
	it("renders requested + skipped lines", () => {
		const out = summarizeSandboxRequest(
			[{ id: "abc", package: "nodejs", status: "pending" }],
			[{ package: "jq", why: "already pending or approved" }],
		);
		expect(out).toContain("nodejs");
		expect(out).toContain("request abc");
		expect(out).toContain("jq");
		expect(out).toContain("already pending");
		expect(out).toContain("PERSIST");
	});
	it("empty everything still gives an actionable line", () => {
		expect(summarizeSandboxRequest([], [])).toContain("Nothing requested");
	});
});
