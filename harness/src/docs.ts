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
 * The per-conversation config document (H2.4/H2.5): a `forge.document`
 * family member under the name `config`, edited through the ordinary
 * document surface (`documentPut` / `PUT /sessions/:id/documents/config`).
 * Shape:
 * ```json
 * { "compaction": { "maxContextChars": 300000, "divisor": 4 } }
 * ```
 * Absent document or fields ⇒ the defaults in `compaction.ts` (today's
 * forge-api heuristic: compact above ~300k estimated tokens, where the
 * estimate is context chars / 4).
 */
export const CONFIG_KEY = "config";

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
	/** Herd H2.5: the agent's `tools_allowlist` (H1.1 column) this
	 * conversation enforces with the `before_tool` hook. Empty/absent =
	 * no allowlist = every offered tool runs (non-breaking). */
	readonly toolsAllowlist?: readonly string[];
	/** Herd H3.5: the mule policy engine's agent id for this
	 * conversation. v1 convention: the FORGE agent id (`agents.id`
	 * string) — mule policy authors create rules with that same
	 * `agent_id`, so forge and mule share the agent identity value.
	 * Absent (session without an agent) ⇒ the policy hook evaluates as
	 * `session:<forgeSessionId>` (documented in CLIENT.md
	 * "Policy hook (H3.5)"). */
	readonly policyAgentId?: string;
	/** Herd H5.1: this conversation is a research task — its registry is
	 * the read-only-by-construction surface `{read, webfetch, search,
	 * note}` (the boot re-install reads this to rebuild the research
	 * extension). `tools` stays `["read"]` and `subagent` stays false. */
	readonly research?: boolean;
	/** Herd H5.1: the research question (for display/re-derivation). */
	readonly question?: string;
	/** Herd H5.1: an optional research scope note. */
	readonly scope?: string;
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
