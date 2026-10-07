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
 * The meta document: written once in the conversation's creating commit by
 * `createConversation`, holds `{ forgeSessionId, extensionName, replaySafeTools }`.
 * The forge tool extension reads `forgeSessionId` from it on every tool call;
 * the boot recovery pass reads `extensionName`/`replaySafeTools` to re-install
 * per-conversation extensions.
 */
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
