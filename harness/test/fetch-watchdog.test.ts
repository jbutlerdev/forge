/**
 * Provider fetch watchdog (drover "silent deaths" #9/#10, 2026-11-12):
 * self-hosted streams (sglang / llama.cpp) can go permanently silent and
 * the vendored provider clients set no timeout — the task waits forever.
 * The watchdog aborts idle streams and over-long requests so pi's normal
 * provider-error retry takes over.
 */
import http from "node:http";
import type { AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { installFetchWatchdog } from "../src/fetch-watchdog.js";

const IDLE_MS = 300;
const MAX_MS = 2_500;

let server: http.Server;
let base = "";
const logs: Record<string, unknown>[] = [];

beforeAll(async () => {
	server = http.createServer((req, res) => {
		const url = req.url ?? "";
		if (url.startsWith("/healthy-stream")) {
			res.writeHead(200, { "content-type": "text/event-stream" });
			res.write("data: a\n\n");
			setTimeout(() => res.write("data: b\n\n"), 60);
			setTimeout(() => res.end("data: done\n\n"), 140);
			return;
		}
		if (url.startsWith("/dead-stream")) {
			// Headers + one chunk, then forever silence (dead socket).
			res.writeHead(200, { "content-type": "text/event-stream" });
			res.write("data: a\n\n");
			// never end
			return;
		}
		if (url.startsWith("/trickle")) {
			// Never idle (chunk every 80 ms) but never finishes: only the
			// hard cap can save us.
			res.writeHead(200, { "content-type": "text/event-stream" });
			const t = setInterval(() => res.write("data: x\n\n"), 80);
			req.on("close", () => clearInterval(t));
			return;
		}
		res.writeHead(404).end();
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const addr = server.address() as AddressInfo;
	base = `http://127.0.0.1:${addr.port}`;
	const installed = installFetchWatchdog({ idleMs: IDLE_MS, maxMs: MAX_MS, log: (r) => logs.push(r) });
	expect(installed).toBe(true);
});

afterAll(async () => {
	await new Promise<void>((resolve) => server.close(() => resolve()));
});

describe("installFetchWatchdog", () => {
	it("is idempotent", () => {
		expect(installFetchWatchdog({ log: () => {} })).toBe(false);
	});

	it("passes healthy streams through intact", async () => {
		const res = await fetch(`${base}/healthy-stream`);
		expect(res.status).toBe(200);
		const text = await res.text();
		expect(text).toContain("data: a");
		expect(text).toContain("data: done");
	});

	it("aborts a stream that goes silent (idle cap)", async () => {
		const started = Date.now();
		await expect(fetch(`${base}/dead-stream`).then((r) => r.text())).rejects.toThrow();
		const elapsed = Date.now() - started;
		expect(elapsed).toBeLessThan(1_500); // well under MAX_MS
		expect(logs.some((l) => String(l.msg ?? "").includes("aborted mid-stream"))).toBe(true);
	});

	it("aborts a forever-trickling request (hard cap)", async () => {
		const started = Date.now();
		await expect(fetch(`${base}/trickle`).then((r) => r.text())).rejects.toThrow();
		expect(Date.now() - started).toBeGreaterThanOrEqual(MAX_MS - 200);
	});
});
