/**
 * Provider fetch watchdog (2026-11-12, drover "silent deaths" #9/#10).
 *
 * The vendored pi-ai provider clients only set a request timeout when the
 * caller passes `timeoutMs`, and pi-durable never does. The herd's models
 * are self-hosted (sglang @ .174, llama.cpp on vram @ .146), where a
 * stream can go permanently silent under GPU contention — and the whole
 * agent task then waits forever on a stalled socket. Observed shape:
 * 22–30 minute gaps with zero provider traffic, zero errors, and hourly
 * RESUME nudges piling up as "deaths".
 *
 * `installFetchWatchdog()` wraps `globalThis.fetch` with two guards:
 *
 *  1. **Idle cap** — for responses with a streaming body, abort when no
 *     chunk arrives for `idleMs` (default 180 s). A live SSE stream
 *     resets the timer on every chunk; a dead one aborts cleanly.
 *  2. **Hard cap** — abort the whole exchange (connect + body drain) at
 *     `maxMs` (default 20 min), so no request can hang unbounded.
 *
 * An abort surfaces to the caller as a fetch/stream rejection, which pi
 * treats as a provider error and runs its normal auto-retry — exactly
 * what a stalled stream should have done all along.
 *
 * Timeouts are env-overridable (`FORGE_FETCH_IDLE_MS`, `FORGE_FETCH_MAX_MS`)
 * and the install is idempotent.
 */

export interface WatchdogOptions {
	/** Abort a response whose stream goes silent this long. Default 180 s. */
	idleMs?: number;
	/** Abort the whole request after this long. Default 20 min. */
	maxMs?: number;
	/** Inject the log line (defaults to console.error JSON). */
	log?: (rec: Record<string, unknown>) => void;
}

const installedKey = Symbol.for("forge.fetchWatchdog");

interface FetchGlobals {
	[key: symbol]: unknown;
}

function positiveInt(raw: string | undefined, fallback: number): number {
	const n = raw === undefined ? NaN : Number.parseInt(raw, 10);
	return Number.isFinite(n) && n > 0 ? n : fallback;
}

export function installFetchWatchdog(opts: WatchdogOptions = {}): boolean {
	const g = globalThis as unknown as FetchGlobals & typeof globalThis;
	if (g[installedKey]) return false; // idempotent
	const idleMs = opts.idleMs ?? positiveInt(process.env.FORGE_FETCH_IDLE_MS, 180_000);
	const maxMs = opts.maxMs ?? positiveInt(process.env.FORGE_FETCH_MAX_MS, 2_700_000);
	const log =
		opts.log ??
		((rec: Record<string, unknown>) => {
			console.error(JSON.stringify({ level: "warn", ...rec }));
		});

	const original = globalThis.fetch.bind(globalThis);

	const wrapped: typeof fetch = async (input, init) => {
		const started = Date.now();
		const url = describeRequest(input);
		const controller = new AbortController();
		let abortReason = "";
		const abortWith = (reason: string) => {
			if (!controller.signal.aborted) {
				abortReason = reason;
				controller.abort(new Error(`forge fetch watchdog: ${reason}`));
			}
		};

		// Link the caller's signal (if any) into our controller so
		// caller-initiated cancels keep working.
		const callerSignal = init?.signal ?? (input instanceof Request ? input.signal : null);
		if (callerSignal) {
			if (callerSignal.aborted) {
				abortWith("caller signal already aborted");
			} else {
				callerSignal.addEventListener("abort", () => abortWith("caller aborted"), {
					once: true,
				});
			}
		}

		const hardTimer = setTimeout(() => abortWith(`request exceeded ${maxMs}ms`), maxMs);

		let res: Response;
		try {
			res = await original(input, { ...(init ?? {}), signal: controller.signal });
		} catch (err) {
			clearTimeout(hardTimer);
			if (abortReason) {
				log({ msg: "fetch watchdog: aborted before response", url, reason: abortReason });
			}
			throw err;
		}

		// No body (HEAD, 204, etc.) — hand it straight back.
		if (!res.body) {
			clearTimeout(hardTimer);
			return res;
		}

		let idleTimer: ReturnType<typeof setTimeout> | undefined;
		const resetIdle = () => {
			if (idleTimer !== undefined) clearTimeout(idleTimer);
			idleTimer = setTimeout(() => abortWith(`stream idle > ${idleMs}ms`), idleMs);
		};
		resetIdle();

		const source = res.body.getReader();
		const body = new ReadableStream<Uint8Array>({
			async pull(branch) {
				try {
					const { done, value } = await source.read();
					if (done) {
						if (idleTimer !== undefined) clearTimeout(idleTimer);
						clearTimeout(hardTimer);
						branch.close();
						return;
					}
					resetIdle();
					branch.enqueue(value);
				} catch (err) {
					if (idleTimer !== undefined) clearTimeout(idleTimer);
					clearTimeout(hardTimer);
					if (abortReason) {
						log({
							msg: "fetch watchdog: aborted mid-stream",
							url,
							reason: abortReason,
							ms: Date.now() - started,
						});
					}
					branch.error(err);
				}
			},
			cancel(reason) {
				if (idleTimer !== undefined) clearTimeout(idleTimer);
				clearTimeout(hardTimer);
				return source.cancel(reason);
			},
		});

		const wrappedRes = new Response(body, {
			status: res.status,
			statusText: res.statusText,
			headers: res.headers,
		});
		// Response.url/redirected are read-only ctor fields from input;
		// copy them so SDKs that log/dedupe by url keep working.
		try {
			Object.defineProperty(wrappedRes, "url", { value: res.url, configurable: true });
			Object.defineProperty(wrappedRes, "redirected", {
				value: res.redirected,
				configurable: true,
			});
		} catch {
			/* non-fatal — url metadata only */
		}
		return wrappedRes;
	};

	g.fetch = wrapped;
	g[installedKey] = true;
	log({ msg: "fetch watchdog installed", idleMs, maxMs });
	return true;
}

function describeRequest(input: RequestInfo | URL): string {
	try {
		if (typeof input === "string") return input.slice(0, 160);
		if (input instanceof URL) return input.toString().slice(0, 160);
		if (input instanceof Request) return input.url.slice(0, 160);
		return String(input).slice(0, 160);
	} catch {
		return "<unknown>";
	}
}
