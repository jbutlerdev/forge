/**
 * Herd H5.1: `webfetch` + `search` — the research task's read-only web
 * surface.
 *
 * Both are in-process GET-only tools offered ONLY to research
 * conversations (see `createForgeExtension`'s `research` flag). The
 * guards are the point of H5.1's "read-only by construction":
 *
 *   - **Method locked to GET** (`httpGet` never sends anything else).
 *   - **No credential forwarding**: no `Authorization` and no
 *     `Cookie` headers are ever attached (the forge API key stays
 *     inside the harness), and URLs with embedded
 *     `user:password@` credentials are refused.
 *   - **SSRF guard** (`assertFetchable`): only `http:`/`https:`
 *     schemes; `localhost`, IP literals, and names whose DNS
 *     resolution includes ANY loopback / private / link-local /
 *     multicast / reserved address are refused (this also covers the
 *     cloud metadata endpoint `169.254.169.254`). A name resolving to
 *     one public and one private address is refused too — a name that
 *     CAN point at a private address is not fetchable.
 *   - **Redirects are NOT followed** (`redirect: "manual"`): a 3xx is
 *     reported to the model with its `Location` instead of being
 *     chased, so a public redirector cannot launder a fetch through
 *     to a private target (the fetch of the destination, if the model
 *     does it, re-runs the guards).
 *   - **Bounded**: a 30 s timeout and a 32 KB response cap.
 *
 * `search` (PLAN-HERD H5.1) is the SearXNG HTTP API through the same
 * guarded GET path. The sandbox SearXNG CLI
 * (`docs/SEARCH-TOOL.md`) is a BASH tool — research tasks have no
 * `bash` by construction — so the HTTP API is the read-only
 * equivalent: the instance base comes from `FORGE_SEARCH_INSTANCE`
 * (the same env var the sandbox CLI is threaded with; optional
 * `FORGE_SEARCH_API_KEY`).
 */
import { lookup } from "node:dns/promises";
import { isIP } from "node:net";

/** The research task's fetch timeout (PLAN-HERD H5.1: 30 s). */
export const WEBFETCH_TIMEOUT_MS = 30_000;
/** The research task's response cap (PLAN-HERD H5.1: ~32 KB). */
export const WEBFETCH_MAX_BYTES = 32 * 1024;

/** One guarded GET, without the SSRF pre-check (see `assertFetchable`). */
export interface WebfetchResult {
	/** The HTTP status of the response (or of the first hop for 3xx). */
	readonly status: number;
	/** The `Location` header for 3xx responses (never followed). */
	readonly location: string | null;
	/** The response body as text, capped at [`WEBFETCH_MAX_BYTES`] ("" for 3xx). */
	readonly body: string;
	/** The response body's full size in bytes. */
	readonly totalBytes: number;
	/** True when the body was cut at the cap. */
	readonly truncated: boolean;
}

/**
 * Parse an IPv6 literal into its 8 groups of 16 bits (or null when it
 * is not one). Handles `::` compression, zone ids, and the dotted-IPv4
 * tail (`::ffff:10.0.0.1`). Node's `URL` normalizes IPv4-mapped
 * literals to 8 hex groups (`::ffff:a00:1`), so the numeric checks
 * see the mapped form either way.
 */
function parseIPv6Groups(ip: string): number[] | null {
	let s = ip.toLowerCase();
	const zone = s.indexOf("%");
	if (zone !== -1) s = s.slice(0, zone);
	let v4tail: string | null = null;
	const m = s.match(/:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
	if (m !== null && m.index !== undefined) {
		s = s.slice(0, m.index);
		v4tail = m[1];
	}
	const compressed = s.split("::");
	if (compressed.length > 2) return null;
	const left = compressed[0] === "" ? [] : compressed[0].split(":");
	const right = compressed.length === 2 ? (compressed[1] === "" ? [] : compressed[1].split(":")) : [];
	const okGroup = (p: string): boolean => /^[0-9a-f]{1,4}$/.test(p);
	if ([...left, ...right].some((p) => !okGroup(p))) return null;
	let v4pairs: [number, number] | null = null;
	if (v4tail !== null) {
		const octets = v4tail.split(".").map(Number);
		if (octets.some((p) => p > 255)) return null;
		v4pairs = [((octets[0] as number) << 8) | (octets[1] as number), ((octets[2] as number) << 8) | (octets[3] as number)];
	}
	const missing = 8 - left.length - right.length - (v4pairs !== null ? 2 : 0);
	if (missing < 0) return null;
	if (compressed.length === 1 && missing !== 0) return null; // no `::` ⇒ exactly 8 groups
	const groups = [...left, ...Array<string>(missing).fill("0"), ...right];
	const nums = groups.map((g) => Number.parseInt(g, 16));
	if (v4pairs !== null) nums.push(v4pairs[0], v4pairs[1]);
	if (nums.length !== 8) return null;
	return nums;
}

/** True when the IP address is loopback, private, link-local, multicast,
 * or otherwise not fetchable by a research task. */
export function isPrivateAddress(ip: string): boolean {
	const v4 = ip.split(".").map(Number);
	if (v4.length === 4 && v4.every((p) => Number.isInteger(p) && p >= 0 && p <= 255)) {
		const [a, b] = v4;
		if (a === 0) return true; // 0.0.0.0/8
		if (a === 10 || a === 127) return true; // 10/8, 127/8
		if (a === 100 && b >= 64 && b <= 127) return true; // 100.64/10 (CGNAT)
		if (a === 169 && b === 254) return true; // 169.254/16 (link-local: the metadata endpoint)
		if (a === 172 && b >= 16 && b <= 31) return true; // 172.16/12
		if (a === 192 && b === 168) return true; // 192.168/16
		if (a === 192 && b === 0 && v4[2] === 2) return true; // 192.0.2/24
		if (a === 198 && b === 51 && v4[2] === 100) return true; // 198.51.100/24
		if (a === 203 && b === 0 && v4[2] === 113) return true; // 203.0.113/24
		if (a >= 224) return true; // 224/4 multicast + 240/4 reserved
		return false;
	}
	const groups = parseIPv6Groups(ip);
	if (groups !== null) {
		const [g0, g1, g2, g3, g4, g5, g6, g7] = groups;
		if (groups.every((g) => g === 0)) return true; // :: unspecified
		if (g0 === 0 && g1 === 0 && g2 === 0 && g3 === 0 && g4 === 0 && g5 === 0 && g6 === 0 && g7 === 1) {
			return true; // ::1 loopback
		}
		if ((g0 & 0xfe00) === 0xfc00) return true; // fc00::/7 ULA
		if ((g0 & 0xffc0) === 0xfe80) return true; // fe80::/10 link-local
		if (g0 === 0 && g1 === 0 && g2 === 0 && g3 === 0 && g4 === 0 && g5 === 0xffff) {
			// ::ffff:0:0/96 — IPv4-mapped: check the embedded IPv4.
			const a = Math.floor(g6 / 256);
			const b = g6 % 256;
			const c = Math.floor(g7 / 256);
			const d = g7 % 256;
			return isPrivateAddress(`${a}.${b}.${c}.${d}`);
		}
		if ((g0 & 0xff00) === 0xff00) return true; // ff00::/8 multicast
		if (g0 === 0x2001 && g1 === 0) return true; // 2001::/32 Teredo (tunnels arbitrary destinations)
		return false;
	}
	return false;
}

/**
 * The SSRF guard. Resolves the URL's host (DNS for names) and REFUSES
 * loopback/private/link-local/multicast/reserved targets — see the
 * module header for the policy. Returns the validated URL string when
 * the fetch may proceed; throws (the tool surfaces the reason to the
 * model) when it must not.
 */
export async function assertFetchable(rawUrl: string): Promise<string> {
	let url: URL;
	try {
		url = new URL(rawUrl);
	} catch {
		throw new Error(`invalid URL: ${rawUrl}`);
	}
	if (url.protocol !== "http:" && url.protocol !== "https:") {
		throw new Error(`only http(s) URLs may be fetched (got ${url.protocol})`);
	}
	if (url.username !== "" || url.password !== "") {
		throw new Error("URLs with embedded credentials are refused (no credential forwarding)");
	}
	const host = url.hostname;
	if (host === "localhost") {
		throw new Error("localhost is refused (private target)");
	}
	// `URL` lowercases hostnames; IPv6 literals keep their brackets.
	const bare = host.startsWith("[") && host.endsWith("]") ? host.slice(1, -1) : host;
	if (isIP(bare) !== 0) {
		// IP literal: check it directly.
		if (isPrivateAddress(bare)) {
			throw new Error(`${host} is a loopback/private/link-local address and is refused`);
		}
		return url.toString();
	}
	// A name: every resolved address must be public.
	let addresses: readonly { address: string }[];
	try {
		addresses = await lookup(host, { all: true });
	} catch (error) {
		throw new Error(`DNS lookup failed for ${host}: ${error instanceof Error ? error.message : String(error)}`);
	}
	if (addresses.length === 0) {
		throw new Error(`${host} resolved to no addresses`);
	}
	for (const addr of addresses) {
		if (isPrivateAddress(addr.address)) {
			throw new Error(`${host} resolves to the private address ${addr.address} and is refused`);
		}
	}
	return url.toString();
}

/**
 * One GET with the research-task guarantees: method locked to GET, no
 * `Authorization`/`Cookie` headers ever attached, redirects NOT
 * followed, 30 s timeout, body capped at 32 KB.
 */
export async function httpGet(url: string, timeoutMs = WEBFETCH_TIMEOUT_MS): Promise<WebfetchResult> {
	// NOTE: no `headers` option at all — that is how the "no credential
	// forwarding" guarantee holds (there is no header to leak through).
	const response = await fetch(url, {
		method: "GET",
		redirect: "manual",
		signal: AbortSignal.timeout(timeoutMs),
	});
	const isRedirect = response.status >= 300 && response.status < 400;
	const buf = Buffer.from(await response.arrayBuffer());
	const cap = Math.min(buf.byteLength, WEBFETCH_MAX_BYTES);
	const total = buf.byteLength;
	const body = isRedirect ? "" : buf.toString("utf8", 0, cap);
	return {
		status: response.status,
		location: isRedirect ? response.headers.get("location") : null,
		body,
		totalBytes: total,
		truncated: total > WEBFETCH_MAX_BYTES,
	};
}

/**
 * The `webfetch` tool body: guard + GET + render. Throws on refusal /
 * network failure (the tool wrapper surfaces the message to the model
 * as a failed tool result).
 */
export async function webfetch(rawUrl: string, timeoutMs = WEBFETCH_TIMEOUT_MS): Promise<string> {
	const url = await assertFetchable(rawUrl);
	const result = await httpGet(url, timeoutMs);
	if (result.location !== null) {
		return (
			`HTTP ${result.status}: ${url} redirects to ${result.location}. ` +
			`Redirects are not followed (SSRF guard). Fetch the destination explicitly if it is public.`
		);
	}
	const header = `HTTP ${result.status} ${url}\n`;
	if (result.truncated) {
		return `${header}${result.body}\n[truncated: first ${WEBFETCH_MAX_BYTES} of ${result.totalBytes} bytes]`;
	}
	return `${header}${result.body.length > 0 ? result.body : "(no body)"}`;
}

/** One SearXNG search hit, rendered line-wise for the model. */
export interface SearchHit {
	readonly title: string;
	readonly url: string;
	readonly snippet: string;
}

/** One SearXNG result rendered as a compact text line. */
export function renderSearchHit(hit: SearchHit): string {
	const snippet = hit.snippet.trim().length > 0 ? ` — ${hit.snippet}` : "";
	return `- ${hit.title}\n  ${hit.url}${snippet}`;
}

/**
 * The `search` tool body: one SearXNG `format=json` query through the
 * same guarded GET path. The instance base is `FORGE_SEARCH_INSTANCE`
 * (the sandbox CLI's env var; the CLI itself needs `bash`, which the
 * research registry lacks by construction); an optional key rides as
 * SearXNG's `key` query parameter.
 */
export async function searxngSearch(query: string, count = 5, timeoutMs = WEBFETCH_TIMEOUT_MS): Promise<SearchHit[]> {
	const base = process.env.FORGE_SEARCH_INSTANCE?.trim();
	if (base === undefined || base.length === 0) {
		throw new Error("search is not configured: the harness has no FORGE_SEARCH_INSTANCE (SearXNG instance URL)");
	}
	const url = new URL(`search?q=${encodeURIComponent(query)}&format=json`, `${base.replace(/\/+$/, "")}/`);
	const apiKey = process.env.FORGE_SEARCH_API_KEY?.trim();
	if (apiKey !== undefined && apiKey.length > 0) {
		url.searchParams.set("key", apiKey);
	}
	const validated = await assertFetchable(url.toString());
	const result = await httpGet(validated, timeoutMs);
	if (result.status !== 200) {
		throw new Error(`SearXNG returned HTTP ${result.status}: ${result.body.slice(0, 300)}`);
	}
	if (result.location !== null || result.truncated) {
		throw new Error(`SearXNG response was not a usable 200 (redirect or over the size cap)`);
	}
	const parsed: unknown = JSON.parse(result.body);
	const rows = Array.isArray((parsed as { results?: unknown }).results) ? (parsed as { results: unknown[] }).results : [];
	const hits: SearchHit[] = [];
	for (const row of rows.slice(0, count)) {
		if (typeof row !== "object" || row === null) continue;
		const r = row as Record<string, unknown>;
		if (typeof r.url !== "string" || r.url.length === 0) continue;
		hits.push({
			title: typeof r.title === "string" && r.title.length > 0 ? r.title.slice(0, 200) : r.url,
			url: r.url,
			snippet: typeof r.content === "string" ? r.content.slice(0, 200) : "",
		});
	}
	if (hits.length === 0) {
		throw new Error("SearXNG returned no results for that query");
	}
	return hits;
}
