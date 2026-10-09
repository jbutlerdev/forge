/**
 * Harness timers (H2.3): the DURABLE surface.
 *
 * A timer belongs to a conversation and is persisted in the harness's
 * Postgres schema (`harness_timers`, see `timer-store.ts`). When a timer
 * fires, the harness submits a turn with `timer fired: <prompt>` on that
 * conversation and pushes a `timer_fired` event. `at` timers fire once;
 * `cron` timers re-arm on the next matching wall-clock minute.
 *
 * Durability: on boot, `reload()` reads every live row back and re-arms
 * the in-memory handles, firing anything overdue. The fire itself claims
 * the row atomically (`UPDATE … WHERE fired_at IS NULL RETURNING` — see
 * `TimerStore.claim`), so a timer that was pending when a process
 * crashed (armed in memory, un-fired) fires EXACTLY ONCE on the next
 * boot: the claim is the only write, and the fired submission carries
 * the request id `timer-fired:<timerId>`, which pi-durable dedupes
 * (submissionByRequest) on top of the row claim.
 */
import { nextCronTime, parseCron, type CronSpec } from "./cron.js";
import type { TimerRow, TimerStore } from "./timer-store.js";

export interface TimerFire {
	readonly timerId: string;
	readonly conversationId: number;
	readonly prompt: string;
}

export interface TimerBackend {
	/** Submit the fired prompt as a turn on the conversation. */
	submit: (conversationId: number, content: string, requestId: string) => Promise<void>;
	/** Push a `timer_fired` event. */
	onFire: (fire: TimerFire) => void;
	/**
	 * Herd H5.3 kill switch: `true` when the timer's conversation
	 * belongs to a PAUSED agent. A skipped fire is a no-op at the
	 * admission seam: the row is already claimed (its tick is lost —
	 * see `fire`), no prompt is submitted, and no `timer_fired` event
	 * is pushed. Omitted → timers always fire (in-process consumers
	 * with no api schema to check).
	 */
	agentPaused?: (conversationId: number) => Promise<boolean>;
	/** Clock override for tests. */
	now?: () => number;
	/**
	 * Clock multiplier for tests: when `now()` runs `scale` times faster
	 * than the wall clock, armed delays are divided by it so a timer
	 * scheduled `D` fake-ms out waits `D/scale` real ms.
	 */
	scale?: number;
}

interface LiveTimer {
	readonly row: TimerRow;
	handle?: NodeJS.Timeout;
}

export interface TimerSpec {
	/** Absolute epoch ms. Exactly one of `at` / `cron` is required. */
	at?: number;
	/** 5-field cron expression (UTC). */
	cron?: string;
	prompt: string;
}

/**
 * In-memory handles over the Postgres-backed timer rows. The store is
 * the source of truth; this class only arms `setTimeout` handles and
 * routes fires through the atomic claim.
 */
export class TimerRegistry {
	#timers = new Map<string, LiveTimer>();
	#counter = 0;
	readonly #store: TimerStore;
	readonly #backend: TimerBackend;

	constructor(store: TimerStore, backend: TimerBackend) {
		this.#store = store;
		this.#backend = backend;
	}

	/** Boot step: re-arm every live timer (fire anything overdue now).
	 * Must run before the scheduler is allowed to run. */
	async reload(): Promise<number> {
		const rows = await this.#store.list();
		for (const row of rows) {
			this.arm(row);
		}
		return rows.length;
	}

	/** Add a timer; returns its id. Throws on invalid/ambiguous specs. */
	async set(conversationId: number, spec: TimerSpec): Promise<string> {
		if (typeof spec.prompt !== "string" || spec.prompt.length === 0) {
			throw new Error("timer prompt must be a non-empty string");
		}
		const hasAt = spec.at !== undefined;
		const hasCron = spec.cron !== undefined;
		if (hasAt === hasCron) {
			throw new Error("timer requires exactly one of `at` (epoch ms) or `cron`");
		}
		const timerId = `timer_${(++this.#counter).toString(36)}${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
		let atMs: number;
		let cron: string | undefined;
		if (hasAt) {
			atMs = spec.at!;
		} else {
			cron = spec.cron;
			const next = nextCronTime(parseCron(cron!), this.now());
			if (next === undefined) {
				throw new Error("cron timer has no matching time within 4 years");
			}
			atMs = next;
		}
		await this.#store.create(conversationId, timerId, atMs, cron, spec.prompt);
		this.arm({
			timerId,
			conversationId,
			at: atMs,
			cron,
			prompt: spec.prompt,
			createdAt: this.now(),
			firedAt: undefined,
		});
		return timerId;
	}

	/** Remove a timer owned by the conversation; false when not found. */
	async clear(conversationId: number, timerId: string): Promise<boolean> {
		const live = this.#timers.get(timerId);
		if (live !== undefined && live.handle !== undefined) clearTimeout(live.handle);
		const cleared = await this.#store.clear(conversationId, timerId);
		if (cleared) this.#timers.delete(timerId);
		return cleared;
	}

	/** Live timers (optionally scoped to one conversation), from Postgres. */
	list(conversationId?: number): Promise<TimerRow[]> {
		return this.#store.list(conversationId);
	}

	/** Count of live timers (optionally scoped to one conversation). */
	async size(conversationId?: number): Promise<number> {
		if (conversationId === undefined) return this.#store.count();
		return (await this.#store.list(conversationId)).length;
	}

	/** Cancel every pending handle (shutdown). Rows stay live in Postgres
	 * — the next boot re-arms them. */
	dispose(): void {
		for (const live of this.#timers.values()) {
			if (live.handle !== undefined) clearTimeout(live.handle);
		}
		this.#timers.clear();
	}

	now(): number {
		return this.#backend.now?.() ?? Date.now();
	}

	#armLater(timerId: string, atMs: number): void {
		const delay = Math.max(0, (atMs - this.now()) / (this.#backend.scale ?? 1));
		const handle = setTimeout(() => {
			void this.fire(timerId);
		}, delay);
		// Never keep the process alive for a timer alone.
		(handle as { unref?: () => void }).unref?.();
		const existing = this.#timers.get(timerId);
		if (existing !== undefined) existing.handle = handle;
	}

	arm(row: TimerRow): void {
		const atMs = row.at;
		if (atMs === undefined) {
			// A live row without a scheduled time cannot happen (the
			// create path always computes one); drop the handle, log it.
			console.error(
				JSON.stringify({ level: "error", msg: "live timer has no scheduled time; not armed", timerId: row.timerId }),
			);
			return;
		}
		this.#timers.set(row.timerId, { row });
		this.#armLater(row.timerId, atMs);
	}

	/** Fire the timer: claim the row first (exactly-once), then submit. */
	async fire(timerId: string): Promise<void> {
		const live = this.#timers.get(timerId);
		if (live === undefined) return;
		const row = live.row;
		// Cron timers re-arm at the next matching wall-clock minute in the
		// claim itself; one-shots keep their (already passed) `at`.
		const nextAtMs = row.cron === undefined ? null : (nextCronTime(parseCron(row.cron), this.now()) ?? null);
		const claimed = await this.#store.claim(timerId, nextAtMs).catch((error) => {
			console.error(
				JSON.stringify({
					level: "error",
					msg: "timer claim failed",
					timerId,
					error: error instanceof Error ? error.message : String(error),
				}),
			);
			return null;
		});
		if (claimed === null) return; // cleared, already fired, or lost a race
		// Herd H5.3 kill switch: check the owning agent AT THE ADMISSION
		// SEAM (after the exactly-once claim, before the submit). A
		// paused agent enqueues nothing new: no prompt lands, no event
		// is pushed. The claim already ran, so the row stays
		// claimed/fired — a one-shot timer simply loses that tick (it
		// can never be claimed again), and a cron row was re-armed to
		// the next boundary by the claim itself, so the schedule
		// continues and re-checks the pause on its next fire (no
		// catch-up: a tick that lands while paused is gone, matching
		// "enqueues nothing new").
		const paused = this.#backend.agentPaused
			? await this.#backend.agentPaused(claimed.conversationId).catch(() => false)
			: false;
		if (paused) {
			console.error(
				JSON.stringify({
					level: "info",
					msg: "timer fire skipped: agent paused (H5.3 kill switch)",
					timerId,
					conversationId: claimed.conversationId,
				}),
			);
			if (row.cron !== undefined && claimed.at !== undefined) {
				this.#timers.set(timerId, { row: { ...row, at: claimed.at, firedAt: claimed.firedAt } });
				this.#armLater(timerId, claimed.at);
			} else {
				this.#timers.delete(timerId);
			}
			return;
		}
		try {
			// One request id per FIRE: the claim's fired_at makes a cron
			// timer's successive fires distinct submissions (a bare
			// per-timer id would get deduped to the first fire by
			// submissionByRequest). The id also dedupes a re-submission of
			// the SAME fire (e.g. submit accepted, harness died before the
			// event push; the row claim already prevented the refire, but
			// the submission dedup is the backstop).
			await this.#backend.submit(
				claimed.conversationId,
				`timer fired: ${claimed.prompt}`,
				`timer-fired:${claimed.timerId}:${claimed.firedAt ?? this.now()}`,
			);
		} catch (error) {
			// The conversation is gone or the harness is closing. The claim
			// already ran: the prompt was admitted exactly once (or the
			// submission never landed — logged; a cron timer will retry on
			// its next fire, a one-shot is lost, matching the H2.0 behavior).
			console.error(
				JSON.stringify({
					level: "error",
					msg: "timer submit failed; timer not re-queued",
					timerId,
					error: error instanceof Error ? error.message : String(error),
				}),
			);
			return;
		}
		this.#backend.onFire({
			timerId: claimed.timerId,
			conversationId: claimed.conversationId,
			prompt: claimed.prompt,
		});
		if (row.cron !== undefined && claimed.at !== undefined) {
			this.#timers.set(timerId, { row: { ...row, at: claimed.at, firedAt: claimed.firedAt } });
			this.#armLater(timerId, claimed.at);
		} else {
			this.#timers.delete(timerId);
		}
	}
}
