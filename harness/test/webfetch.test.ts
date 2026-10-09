/**
 * Herd H5.1: the `webfetch` / `search` guards — the read-only-by-
 * construction guarantees, unit-tested:
 *
 *   - method locked to GET; no `Authorization` / `Cookie` headers ever
 *     attached (a local server echoes what it saw);
 *   - the SSRF guard (`assertFetchable`): http(s) only, no embedded
 *     credentials, loopback/private/link-local/metadata targets
 *     refused — IP literals and `localhost` included;
 *   - the 32 KB response cap (truncation flag + note);
 *   - redirects reported, never followed;
 *   - non-http schemes refused.
 */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { AddressFamily, type AddressInfo } from "node:net";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	assertFetchable,
	httpGet,
	isPrivateAddress,
	renderSearchHit,
	WEBFETCH_MAX_BYTES,
	WebfetchResult,
	webfetch,
} from "../src/webfetch.js";

interface SeenRequest {
	method: string;
	authorization: string | null;
	cookie: string | null;
}

let server: Server;
let baseUrl: string;
let seen: SeenRequest[];

beforeAll(async () => {
	seen = [];
	server = createServer((req: IncomingMessage, res: ServerResponse) => {
		seen.push({
			method: req.method ?? "",
			authorization: req.headers.authorization ?? null,
			cookie: req.headers.cookie ?? null,
		});
		if (req.url === "/text") {
			res.setHeader("content-type", "text/plain");
			res.end("hello from the test server");
		} else if (req.url === "/big") {
			res.end("a".repeat(WEBFETCH_MAX_BYTES + 512));
		} else if (req.url === "/redirect") {
			res.statusCode = 302;
			res.setHeader("location", "/text");
			res.end();
		} else {
			res.statusCode = 404;
			res.end("nope");
		}
	});
	await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
	const address = server.address() as AddressInfo;
	baseUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
	await new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())));
});

describe("httpGet guarantees", () => {
	it("sends GET and no Authorization/Cookie headers", async () => {
		seen = [];
		const result = await httpGet(`${baseUrl}/text`);
		expect(result.status).toBe(200);
		expect(result.body).toBe("hello from the test server");
		expect(result.truncated).toBe(false);
		expect(seen).toHaveLength(1);
		expect(seen[0].method).toBe("GET");
		expect(seen[0].authorization).toBeNull();
		expect(seen[0].cookie).toBeNull();
	});

	it("truncates at 32 KB and flags it", async () => {
		const result: WebfetchResult = await httpGet(`${baseUrl}/big`);
		expect(result.truncated).toBe(true);
		expect(result.body.length).toBeLessThanOrEqual(WEBFETCH_MAX_BYTES);
		expect(result.totalBytes).toBe(WEBFETCH_MAX_BYTES + 512);
	});

	it("reports 3xx with Location and never follows", async () => {
		const result = await httpGet(`${baseUrl}/redirect`);
		expect(result.status).toBe(302);
		expect(result.location).toBe("/text");
		expect(result.body).toBe("");
	});
});

describe("assertFetchable (the SSRF guard)", () => {
	it("accepts public IP literals", async () => {
		await expect(assertFetchable("http://93.184.216.34:8080/page")).resolves.toContain("93.184.216.34");
		await expect(assertFetchable("http://8.8.8.8/")).resolves.toContain("8.8.8.8");
	});

	it("refuses loopback names and literals", async () => {
		await expect(assertFetchable("http://localhost/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://127.0.0.1/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://[::1]/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://0.0.0.0:8080/x")).rejects.toThrow(/refused/);
	});

	it("refuses private, link-local, and metadata ranges", async () => {
		await expect(assertFetchable("http://10.1.2.3/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://172.16.0.9/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://192.168.0.10/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://100.64.0.1/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://169.254.169.254/latest/meta-data/")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://[fc00::1]/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://[fe80::1]/x")).rejects.toThrow(/refused/);
		await expect(assertFetchable("http://[::ffff:10.0.0.1]/x")).rejects.toThrow(/refused/);
	});

	it("refuses embedded credentials", async () => {
		await expect(assertFetchable("http://user:secret@93.184.216.34/x")).rejects.toThrow(/credentials/i);
	});

	it("refuses non-http(s) schemes", async () => {
		await expect(assertFetchable("ftp://93.184.216.34/x")).rejects.toThrow(/only http\(s\)/);
		await expect(assertFetchable("file:///etc/passwd")).rejects.toThrow(/only http\(s\)/);
		await expect(assertFetchable("gopher://example.org/")).rejects.toThrow(/only http\(s\)/);
	});

	it("refuses malformed URLs", async () => {
		await expect(assertFetchable("not a url")).rejects.toThrow(/invalid URL/);
	});
});

describe("isPrivateAddress", () => {
	it("classifies public addresses as fetchable", () => {
		for (const ip of [
			"93.184.216.34",
			"8.8.8.8",
			"2606:4700:4700::1111"
		]) {
			expect(isPrivateAddress(ip), ip).toBe(false);
		}
	});

	it("classifies the reserved ranges", () => {
		for (const ip of [
			"0.0.0.0",
			"10.0.0.1",
			"100.64.0.1",
			"100.127.255.255",
			"127.0.0.1",
			"169.254.0.1",
			"172.16.0.1",
			"172.31.255.255",
			"192.168.1.1",
			"192.0.2.1",
			"198.51.100.7",
			"203.0.113.9",
			"224.0.0.1",
			"255.255.255.255",
			"::",
			"::1",
			"fc00::1",
			"fd12:3456::1",
			"fe80::1",
			"ff02::1",
			"::ffff:10.0.0.1",
			"::ffff:a00:1",
			"2001::1",
		]) {
			expect(isPrivateAddress(ip), ip).toBe(true);
		}
		// 100.63.x and 172.15.x are OUTSIDE the CGNAT / private ranges.
		expect(isPrivateAddress("100.63.0.1")).toBe(false);
		expect(isPrivateAddress("172.15.0.1")).toBe(false);
	});
});

describe("webfetch end-to-end (against a public-looking literal)", () => {
	it("refuses at the tool level, not just the guard", async () => {
		await expect(webfetch("http://127.0.0.1/x")).rejects.toThrow(/refused/);
	});
});

describe("renderSearchHit", () => {
	it("renders title, URL, and snippet", () => {
		expect(renderSearchHit({ title: "T", url: "https://x.test/1", snippet: "s" })).toBe(
			"- T\n  https://x.test/1 — s",
		);
		expect(renderSearchHit({ title: "T", url: "https://x.test/1", snippet: "   " })).toBe(
			"- T\n  https://x.test/1",
		);
	});
});
