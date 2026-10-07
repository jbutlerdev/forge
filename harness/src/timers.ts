/**
 * Harness timers: the durable-timer surface (H2.3) in its H2.0 form.
 *
 * A timer belongs to a conversation. When it fires, the harness submits a
 * turn with `timer fired: <prompt>` on that conversation and pushes a
 * `timer_fired` event. `at` timers fire once; `cron` timers re-arm on the
 * next matching wall-clock minute.
 *
 * KNOWN LIMIT (H2.0): timers live in process memory. They are NOT durable
 * across a harness crash — pi-durable has no built-in durable timer task in
 * 1.0.4, and the durable primitive lands in H2.3. Documented in README.
 */
import { nextCronTime, parseCron, type CronSpec } from "./cron.js";

export interface TimerFire {
	readonly timerId: string;
	readonly conversationId: number;
	readonly prompt: string;
}

export interface TimerBackend {
	/** Submit the fired prompt as a turn on the conversation. */
	submit: (conversationId: number, content: string) => Promise<void>;
	/** Push a `timer_fired` event. */
	onFire: (fire: TimerFire) => void;
	/** Clock override for tests. */
	now?: () => number;
}

interface LiveTimer {
	conversationId: number;
	prompt: string;
	at?: number;
	cron?: CronSpec;
	handle?: NodeJS.Timeout;
}

export interface TimerSpec {
	/** Absolute epoch ms. Exactly one of `at` / `cron` is required. */
	at?: number;
	/** 5-field cron expression (UTC). */
	cron?: string;
	prompt: string;
}

export class TimerRegistry {
	#timers = new Map<string, LiveTimer>();
	#counter = 0;
	readonly #backend: TimerBackend;

	constructor(backend: TimerBackend) {
		this.#backend = backend;
	}

	/** Add a timer; returns its id. Throws on invalid/ambiguous specs. */
	set(conversationId: number, spec: TimerSpec): string {
		if (typeof spec.prompt !== "string" || spec.prompt.length === 0) {
			throw new Error("timer prompt must be a non-empty string");
		}
		const hasAt = spec.at !== undefined;
		const hasCron = spec.cron !== undefined;
		if (hasAt === hasCron) {
			throw new Error("timer requires exactly one of `at` (epoch ms) or `cron`");
		}
		const timerId = `timer_${++this.#counter}_${Date.now().toString(36)}`;
		const live: LiveTimer = { conversationId, prompt: spec.prompt, at: spec.at };
		if (hasCron) live.cron = parseCron(spec.cron!);
		this.#timers.set(timerId, live);
		this.arm(timerId, live, this.#backend.now?.() ?? Date.now());
		return timerId;
	}

	/** Remove a timer owned by the conversation; false when not found. */
	clear(conversationId: number, timerId: string): boolean {
		const live = this.#timers.get(timerId);
		if (live === undefined || live.conversationId !== conversationId) return false;
		if (live.handle !== undefined) clearTimeout(live.handle);
		this.#timers.delete(timerId);
		return true;
	}

	/** Count of live timers (optionally scoped to one conversation). */
	size(conversationId?: number): number {
		if (conversationId === undefined) return this.#timers.size;
		let n = 0;
		for (const live of this.#timers.values()) if (live.conversationId === conversationId) n++;
		return n;
	}

	/** Cancel every pending timer (shutdown). */
	dispose(): void {
		for (const live of this.#timers.values()) {
			if (live.handle !== undefined) clearTimeout(live.handle);
		}
		this.#timers.clear();
	}

	#armLater(timerId: string, live: LiveTimer, atMs: number): void {
		const delay = Math.max(0, atMs - (this.#backend.now?.() ?? Date.now()));
		live.handle = setTimeout(() => {
			void this.fire(timerId, live);
		}, delay);
		// Never keep the process alive for a timer alone.
		(live.handle as { unref?: () => void }).unref?.();
	}

	arm(timerId: string, live: LiveTimer, fromMs: number): void {
		if (live.at !== undefined) {
			this.#armLater(timerId, live, live.at);
			return;
		}
		const next = nextCronTime(live.cron!, fromMs);
		if (next === undefined) {
			// Impossible cron combination: drop it, tell the operator.
			this.#timers.delete(timerId);
			console.error(
				JSON.stringify({
					level: "error",
					msg: "cron timer has no matching time within 4 years",
					timerId,
				}),
			);
			return;
		}
		this.#armLater(timerId, live, next);
	}

	async fire(timerId: string, live: LiveTimer): Promise<void> {
		const still = this.#timers.get(timerId);
		if (still !== live) return; // cleared in the meantime
		try {
			await this.#backend.submit(live.conversationId, `timer fired: ${live.prompt}`);
		} catch (error) {
			// The conversation is gone or the harness is closing. The timer is
			// done either way (it is not durable).
			console.error(
				JSON.stringify({
					level: "error",
					msg: "timer submit failed; timer discarded",
					timerId,
					error: error instanceof Error ? error.message : String(error),
				}),
			);
			this.#timers.delete(timerId);
			return;
		}
		this.#backend.onFire({
			timerId,
			conversationId: live.conversationId,
			prompt: live.prompt,
		});
		if (live.cron !== undefined) {
			this.#armLater(timerId, live, Date.now());
		} else {
			this.#timers.delete(timerId);
		}
	}
}
