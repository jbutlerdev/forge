/**
 * Harness → forge-api event push.
 *
 * The harness translates pi-durable commit publications into a small, stable
 * JSON-line event vocabulary and pushes them to the single connected
 * forge-api client on the events socket. There is NO replay on reconnect:
 * forge-api resynchronizes through its own API after (re)connecting
 * (see README, "Events contract").
 */
import type {
	ConversationId,
	CommitPublication,
	EntryRecord,
	EntryId,
	TaskId,
} from "@earendil-works/pi-durable";

export type HarnessEvent =
	/** A task moved to `started` (first running), or to a terminal status
	 * mapped to done/failed/aborted. */
	| {
			type: "task_state";
			taskId: TaskId;
			conversationId: ConversationId;
			status: "started" | "done" | "failed" | "aborted";
			/** Raw pi-durable outcome status for terminal tasks. */
			outcomeStatus?: string;
	  }
	/** A generation-produced assistant message was committed: the end of a
	 * turn (or of a turn's current answer segment). */
	| {
			type: "turn_end";
			conversationId: ConversationId;
			entryId: EntryId;
			/** First ≤200 chars of the assistant text; empty for tool-only. */
			summary: string;
	  }
	/** A harness document (forge.meta / forge.document) changed. */
	| {
			type: "document_changed";
			conversationId: ConversationId;
			name: string;
	  }
	/** A durable harness timer fired and submitted its prompt. */
	| {
			type: "timer_fired";
			timerId: string;
			conversationId: ConversationId;
			prompt: string;
	  }
	/** The `spawn_subagent` tool created (or re-found on a replayed call)
	 * a child conversation owned by the calling task: forge-api maps the
	 * child to a new forge session row under the parent (H2.2). */
	| {
			type: "subagent_spawned";
			parentConversationId: ConversationId;
			childConversationId: ConversationId;
			/** Pre-minted forge session id for the child (its `forge.meta`
			 * carries the same value; forge-api reuses it as the session row
			 * id so the two sides never disagree). */
			childForgeSessionId: string;
			/** The `task` argument of the spawn tool call. */
			task: string;
			/** Detached subagents are owned by a background anchor task:
			 * they survive the parent's abort and idle waits. */
			detached: boolean;
	  };

export type EventListener = (event: HarnessEvent) => void;

/** Fan-out to subscribers; emit() is fire-and-forget per listener, and a
 * listener failure is contained (the pi-durable commit listener must never
 * throw). */
export class EventBus {
	#listeners = new Set<EventListener>();

	subscribe(listener: EventListener): () => void {
		this.#listeners.add(listener);
		return () => {
			this.#listeners.delete(listener);
		};
	}

	emit(event: HarnessEvent): void {
		for (const listener of [...this.#listeners]) {
			try {
				listener(event);
			} catch {
				// Contained: a broken event sink must never take down commits.
			}
		}
	}
}

const ASSISTANT_KIND = "pi.assistant";

function assistantSummary(entry: EntryRecord): string {
	for (const message of entry.model ?? []) {
		if (message.role !== "assistant") continue;
		const content = (message as { content?: unknown }).content;
		if (!Array.isArray(content)) continue;
		for (const block of content) {
			if (block !== null && typeof block === "object" && (block as { type?: unknown }).type === "text") {
				const text = (block as { text?: unknown }).text;
				if (typeof text === "string" && text.length > 0) return text.slice(0, 200);
			}
		}
	}
	return "";
}

/**
 * Wire an EventBus to pi-durable commit publications: task state
 * transitions, assistant turn-ends, and harness document changes.
 * Returns the unsubscribe function.
 */
export function watchCommits(
	commits: { subscribeCommits(listener: (publication: CommitPublication, context: unknown) => void): () => void },
	events: EventBus,
): () => void {
	// Last observed durable status per task, for transition detection.
	const lastTaskStatus = new Map<TaskId, string>();

	return commits.subscribeCommits((publication) => {
		for (const change of publication.changes) {
			try {
				if (change.type === "task") {
					const record = change.value;
					const status = record.state.status;
					const previous = lastTaskStatus.get(record.id);
					lastTaskStatus.set(record.id, status);
					if (status === "running") {
						// First entry to running (or a re-run after
						// pending) is a start.
						if (previous === undefined || previous !== "running") {
							events.emit({
								type: "task_state",
								taskId: record.id,
								conversationId: record.conversationId,
								status: "started",
							});
						}
					} else if (status === "terminal") {
						const outcome = record.state.outcome;
						const mapped =
							outcome.status === "completed" ? "done" : outcome.status === "failed" ? "failed" : "aborted";
						events.emit({
							type: "task_state",
							taskId: record.id,
							conversationId: record.conversationId,
							status: mapped,
							outcomeStatus: outcome.status,
						});
					}
				} else if (change.type === "entry" && change.value.kind === ASSISTANT_KIND) {
					events.emit({
						type: "turn_end",
						conversationId: change.value.conversationId,
						entryId: change.value.id,
						summary: assistantSummary(change.value),
					});
				} else if (change.type === "document" && change.conversationId !== undefined) {
					// Only harness documents; pi.* documents are harness internals.
					if (change.record.kind.startsWith("forge.")) {
						events.emit({
							type: "document_changed",
							conversationId: change.conversationId,
							name: change.record.key ?? change.record.kind,
						});
					}
				}
			} catch {
				// One malformed change must not break later ones.
			}
		}
	});
}
