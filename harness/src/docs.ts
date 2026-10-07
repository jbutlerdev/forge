/**
 * Harness-owned pi-durable documents (conversation-scoped document families).
 *
 * Both families store one opaque JSON `value` — the durable shape is the same
 * for the per-conversation meta document and for the generic named documents
 * exposed through the `documentGet`/`documentPut` IPC methods.
 */
import type { JsonValue } from "@earendil-works/chord";
import { defineDocFamily } from "@earendil-works/pi-durable";

/** Value shape of every harness document. */
export type DocValue = { readonly value: JsonValue };

/**
 * The meta document value shape: `createConversation` writes
 * `{ forgeSessionId, extensionName, replaySafeTools }`; subagent
 * children (H2.2) add `tools` (their spawn args' tool subset). The boot
 * re-install pass reads `extensionName`/`replaySafeTools`/`tools`.
 */
export type MetaValue = {
	readonly forgeSessionId: string;
	readonly extensionName: string;
	readonly replaySafeTools: readonly string[];
	/** Subagent children only: the tool subset they were spawned with. */
	readonly tools?: readonly string[];
	/** Whether this conversation offers `spawn_subagent` (subagent
	 * children: false; everything else: true). */
	readonly subagent?: boolean;
};

export const ForgeMeta = defineDocFamily<DocValue, JsonValue>({
	kind: "forge.meta",
	version: 1,
	scope: "conversation",
	history: "latest",
	fork: "current",
	family: true,
	initial: (seed) => ({ value: seed ?? null }),
});

/** Generic named documents, one family member per `name` per conversation. */
export const ForgeDocument = defineDocFamily<DocValue, JsonValue>({
	kind: "forge.document",
	version: 1,
	scope: "conversation",
	history: "latest",
	fork: "current",
	family: true,
	initial: (seed) => ({ value: seed }),
});

/** Family key of the meta document. */
export const META_KEY = "meta";
