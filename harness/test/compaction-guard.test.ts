/**
 * H2.7: bounded, fail-safe compaction (the harness `beforeCompact` hook).
 *
 * Production evidence (durable_tasks, conversation 5080, `pi.compaction`
 * tasks 7887/7896/7898, input {"reason":"threshold"}): the built-in
 * summarizer request — the serialized transcript — itself exceeded the
 * model context ("Summarization failed: 400: … maximum context length
 * is 262144 tokens …"), every turn re-tripped the threshold, compaction
 * failed again, SILENTLY, forever.
 *
 * These tests drive `createBeforeCompactHook` directly (no network, no
 * Postgres) with a mocked summarizer (`models.completeSimple`) and an
 * injected ⚠-row notifier, proving:
 *
 *  (a) an OVERSIZED transcript is trimmed from the OLDEST side into the
 *      model-context budget and the summarizer then SUCCEEDS — outcome
 *      `compacted-truncated`, warn log, no ⚠ row;
 *  (b) a summarizer that STILL fails (provider 400) falls back to the
 *      head+tail truncation compaction with the explicit
 *      "[oldest N entries truncated — summary unavailable]" marker and
 *      emits the ⚠ row callback exactly once — outcome
 *      `summary-failed-truncated`;
 *  (c) a NORMAL-SIZE compaction is unaffected: the full transcript
 *      reaches the summarizer, outcome `compacted`, no ⚠ row.
 */
import type { Context } from "@earendil-works/chord";
import type { AssistantMessage, Message } from "@earendil-works/pi-ai";
import { AgentDoc, type HookApi } from "@earendil-works/pi-durable";
import { describe, expect, it } from "vitest";
import {
	CHARS_PER_TOKEN,
	SUMMARIZER_ATTEMPTS,
	createBeforeCompactHook,
	summarizerBudgetChars,
	transcriptParts,
	trimTranscriptOldest,
	truncationSummary,
} from "../src/compaction.js";
import { ForgeMeta } from "../src/docs.js";

const context = { get: () => undefined } as unknown as Context;

/** A small "model": budget = (8192 − 1024 output − 4096 margin) × 4 ≈ 12k chars. */
const FAUX_MODEL = {
	provider: "faux",
	id: "summa-1",
	name: "summa-1",
	api: "faux",
	contextWindow: 8_192,
	maxTokens: 1_024,
};

function userMsg(tag: string, filler = ""): Message {
	return { role: "user", content: `[User marker ${tag}] ${filler}`, timestamp: 1 } as unknown as Message;
}

function assistantMsg(tag: string, filler = ""): Message {
	return {
		role: "assistant",
		content: [{ type: "text", text: `[Assistant marker ${tag}] ${filler}` }],
		api: "faux",
		provider: "faux",
		model: "faux-1",
		usage: {},
		stopReason: "stop",
		timestamp: 1,
	} as unknown as Message;
}

function summaryResponse(text: string): AssistantMessage {
	return {
		role: "assistant",
		content: [{ type: "text", text }],
		api: "faux",
		provider: "faux",
		model: "summa-1",
		usage: {},
		stopReason: "stop",
		timestamp: 1,
	} as unknown as AssistantMessage;
}

function errorResponse(): AssistantMessage {
	return {
		role: "assistant",
		content: [],
		api: "faux",
		provider: "faux",
		model: "summa-1",
		usage: {},
		stopReason: "error",
		errorMessage: "400: BadRequestError … This model's maximum context length is 262144 tokens. However, you requested 16 out…",
		timestamp: 1,
	} as unknown as AssistantMessage;
}

interface GuardedHookResult {
	decision: { summary?: string; decline?: true } | undefined;
	prompts: string[];
	notifyTexts: string[];
	logs: Record<string, unknown>[];
	completeCalls: number;
}

async function runHook(
	messages: readonly Message[],
	respond: (prompt: string, attempt: number) => AssistantMessage,
	options: { failModels?: boolean } = {},
): Promise<GuardedHookResult> {
	const prompts: string[] = [];
	const notifyTexts: string[] = [];
	const logs: Record<string, unknown>[] = [];
	let completeCalls = 0;
	const models = {
		getModel: (provider: string, modelId: string) =>
			!options.failModels && provider === "faux" && modelId === "summa-1"
				? ({ ...FAUX_MODEL } as never)
				: undefined,
		completeSimple: async (_model: never, ctx: { messages: Message[] }, _opts: never) => {
			completeCalls++;
			const user = ctx.messages[1] as unknown as { content: [{ text: string }] };
			const prompt = user.content[0]!.text;
			prompts.push(prompt);
			return respond(prompt, completeCalls);
		},
	} as never;
	const api = {
		conversationId: 5080,
		taskId: 7887,
		models,
		snapshot: async (doc: unknown, _id: never, keyOrContext?: unknown, maybeContext?: unknown) => {
			if (doc === AgentDoc && maybeContext === undefined) return { model: { provider: "faux", modelId: "summa-1" } };
			if (doc === ForgeMeta) return { value: { forgeSessionId: "sess-compaction-guard" } };
			return undefined;
		},
		memo: async () => undefined,
	} as unknown as HookApi;
	const hook = createBeforeCompactHook({
		apiUrl: "http://127.0.0.1:9", // never hit: notify is injected below
		apiKey: "test-key",
		models: models as never,
		notify: async (text) => {
			notifyTexts.push(text);
		},
		log: (record) => {
			logs.push(record);
		},
	});
	const decision = await hook(
		{ reason: "threshold", entries: [], messages, firstKept: 1 } as never,
		api,
		context,
	);
	return { decision, prompts, notifyTexts, logs, completeCalls };
}

function outcomeLogs(logs: Record<string, unknown>[]): Record<string, unknown>[] {
	return logs.filter((record) => record.msg === "compaction outcome");
}

describe("H2.7 bounded, fail-safe compaction (beforeCompact hook)", () => {
	it("(a) trims an OVERSIZED transcript from the oldest side and the summarizer succeeds", async () => {
		const messages: Message[] = [];
		for (let i = 0; i < 40; i++) {
			const filler = `filler-${i}-`.padEnd(1_200, "x");
			messages.push(userMsg(`old-${i}`, i === 0 ? "OLDEST-NEEDLE" : filler));
			messages.push(assistantMsg(`ans-${i}`, i === 39 ? "NEWEST-NEEDLE" : filler));
		}
		const { decision, prompts, notifyTexts, logs } = await runHook(messages, () =>
			summaryResponse("STRUCTURED-SUMMARY-MARKER"),
		);

		// The hook OWNS the summary (the vendored summarize phase never runs).
		expect(decision).toBeDefined();
		expect(decision!.decline).toBeUndefined();
		const summary = decision!.summary!;
		expect(summary).toContain("STRUCTURED-SUMMARY-MARKER");
		// The trimming is RECORDED in the compaction result.
		expect(summary).toContain("summarizer input trimmed");

		// The summarizer input was BOUNDED: the prompt fits the budget
		// (plus the fixed summarizer prompt overhead), and the oldest
		// content was dropped while the newest survived.
		const budget = summarizerBudgetChars(FAUX_MODEL);
		expect(prompts.length).toBe(1);
		expect(prompts[0]!.length).toBeLessThan(budget + 4_096);
		expect(prompts[0]!.includes("OLDEST-NEEDLE")).toBe(false);
		expect(prompts[0]!.includes("NEWEST-NEEDLE")).toBe(true);

		// Outcome `compacted-truncated` at WARN; NO ⚠ row on this path.
		const outcomes = outcomeLogs(logs);
		expect(outcomes.length).toBe(1);
		expect(outcomes[0]!.level).toBe("warn");
		expect(outcomes[0]!.outcome).toBe("compacted-truncated");
		expect(outcomes[0]!.trimmedCount).toBeGreaterThan(0);
		expect(notifyTexts).toEqual([]);
	}, 20_000);

	it("(b) a still-failing summarizer falls back to truncation and emits the ⚠ row", async () => {
		const messages: Message[] = [];
		for (let i = 0; i < 30; i++) {
			messages.push(userMsg(`old-${i}`, `filler-${i}-`.padEnd(900, "y")));
			messages.push(assistantMsg(`ans-${i}`, `answer-${i}-`.padEnd(900, "z")));
		}
		const { decision, notifyTexts, logs, completeCalls } = await runHook(messages, () => errorResponse());

		// The conversation STILL becomes compactable: the hook returns a
		// truncation summary with the explicit marker.
		expect(decision).toBeDefined();
		const summary = decision!.summary!;
		expect(summary).toMatch(/\[oldest \d+ entries truncated — summary unavailable\]/);
		expect(summary).toContain("[oldest ");
		// Head+tail window: the newest content survives verbatim.
		expect(summary).toContain("[Assistant marker ans-29]");
		// The provider error is recorded for the audit trail.
		expect(summary).toContain("summarizer error");
		expect(completeCalls).toBe(SUMMARIZER_ATTEMPTS);

		// Outcome `summary-failed-truncated` at warn, and EXACTLY ONE ⚠
		// row callback (the human/phone notification).
		const outcomes = outcomeLogs(logs);
		expect(outcomes.length).toBe(1);
		expect(outcomes[0]!.level).toBe("warn");
		expect(outcomes[0]!.outcome).toBe("summary-failed-truncated");
		expect(outcomes[0]!.truncatedCount).toBeGreaterThan(0);
		expect(notifyTexts.length).toBe(1);
		expect(notifyTexts[0]!.startsWith("⚠ ")).toBe(true);
		expect(notifyTexts[0]).toContain("could not summarize");
	}, 20_000);

	it("(c) a normal-size compaction is unaffected (full transcript, no trim, no ⚠)", async () => {
		const messages = [
			userMsg("hello", "OLDEST-SMALL-NEEDLE"),
			assistantMsg("hi", "how can I help"),
			userMsg("task", "do the thing"),
			assistantMsg("done", "NEWEST-SMALL-NEEDLE"),
		];
		const { decision, prompts, notifyTexts, logs } = await runHook(messages, () =>
			summaryResponse("SMALL-STRUCTURED-SUMMARY"),
		);

		expect(decision!.summary).toBe("SMALL-STRUCTURED-SUMMARY");
		// The FULL transcript reached the summarizer (nothing trimmed).
		expect(prompts[0]).toContain("OLDEST-SMALL-NEEDLE");
		expect(prompts[0]).toContain("NEWEST-SMALL-NEEDLE");
		const outcomes = outcomeLogs(logs);
		expect(outcomes.length).toBe(1);
		expect(outcomes[0]!.outcome).toBe("compacted");
		expect(outcomes[0]!.trimmedCount).toBe(0);
		expect(notifyTexts).toEqual([]);
	}, 20_000);

	it("(d) no summarizer available ⇒ truncation fallback (never uncompacting)", async () => {
		const messages: Message[] = [];
		for (let i = 0; i < 20; i++) {
			messages.push(userMsg(`a${i}`, `u-${i}-`.padEnd(20_000, "u")));
			messages.push(assistantMsg(`b${i}`, `a-${i}-`.padEnd(20_000, "v")));
		}
		const { decision, notifyTexts, completeCalls } = await runHook(messages, () => summaryResponse("unused"), {
			failModels: true,
		});
		expect(completeCalls).toBe(0);
		expect(decision).toBeDefined();
		expect(decision!.summary).toContain("summary unavailable");
		expect(notifyTexts.length).toBe(1);
	});

	it("(e) pure helpers: budget, oldest-side trim, truncation marker", () => {
		// Budget: conservative chars/4 with margin + output reserve.
		expect(summarizerBudgetChars({ contextWindow: 262_144, maxTokens: 8_192 })).toBe(
			(262_144 - 2_048 - Math.floor(262_144 * 0.25)) * CHARS_PER_TOKEN,
		);
		// Trim drops WHOLE blocks from the OLDEST side, keeps the newest.
		const parts = ["a".repeat(100), "b".repeat(100), "c".repeat(10)];
		const trimmed = trimTranscriptOldest(parts, 60);
		expect(trimmed.trimmedCount).toBe(2);
		expect(trimmed.text).toBe("c".repeat(10));
		// Truncation: head + marker + tail, count of the dropped middle.
		const many = Array.from({ length: 10 }, (_, i) => `${i}-`.padEnd(500, "m"));
		const cut = truncationSummary(many, 1_500);
		expect(cut.text).toContain("[oldest ");
		expect(cut.text).toContain("entries truncated — summary unavailable]");
		expect(cut.truncatedCount).toBeGreaterThan(0);
		expect(cut.text.endsWith(many[9]!.slice(-10))).toBe(true);
		// transcriptParts serializes user/assistant/tool-result shapes.
		const parts2 = transcriptParts([
			userMsg("u1"),
			{
				role: "toolResult",
				toolCallId: "t1",
				content: [{ type: "text", text: "tool-output-marker" }],
				timestamp: 1,
			} as unknown as Message,
		]);
		expect(parts2.length).toBe(2);
		expect(parts2[1]).toContain("[Tool result]: tool-output-marker");
	});
});
