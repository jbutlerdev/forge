"use strict";
/**
 * ranch-tools: Ranch tool provider extension for pi (forge side).
 *
 * Registers ranch_spawn / ranch_send / ranch_status / ranch_read /
 * ranch_close on forge-sandboxed agents. Calls forward to forge's
 * `/tools/execute` (the same door forge-tools uses), where forge's
 * ranch-tool relay (see forge `api/ranch_tools.rs`) queues the call,
 * publishes a `ranch_tool_request` event on the session's SSE stream,
 * and long-polls for the answer that ranchd's forge worker POSTs back
 * to `/ranch-tools/{id}/result`. The agent's turn stays synchronous.
 *
 * Load alongside forge-tools:
 *   pi --no-builtin-tools --extension forge-tools/dist/index.js \
 *      --extension ranch-tools/dist/index.js
 *
 * ranchd ownership semantics carry over: a forge agent may only touch
 * panes it spawned (ranchd keys ownership off the forge pane uuid,
 * which travels in the relay payload as the caller).
 */
Object.defineProperty(exports, "__esModule", { value: true });
const typebox_1 = require("typebox");
let forgeApiUrl = process.env.FORGE_API_URL || "http://localhost:8080";
let sessionId = process.env.FORGE_SESSION_ID || "";
// forge-api spawns pi with FORGE_API_KEY (real key or process-scoped
// tool token); /tools/execute requires it.
let forgeApiKey = process.env.FORGE_API_KEY || "";
const SpawnInputSchema = typebox_1.Type.Object({
    kind: typebox_1.Type.Optional(typebox_1.Type.String({ description: "agent kind: forge (default) or pi" })),
    name: typebox_1.Type.Optional(typebox_1.Type.String({ description: "optional session name (mode=session)" })),
    prompt: typebox_1.Type.String({ description: "first prompt for the new agent" }),
    mode: typebox_1.Type.Optional(typebox_1.Type.String({ description: "\"split\" (default) anchors a new pane next to you; \"session\" creates a named session", enum: ["split", "session"] })),
    profile_id: typebox_1.Type.Optional(typebox_1.Type.String({ description: "forge agent profile to use (kind=forge)" })),
});
const SendInputSchema = typebox_1.Type.Object({
    pane: typebox_1.Type.String({ description: "target pane id (from ranch_spawn)" }),
    text: typebox_1.Type.String({ description: "the message" }),
    delivery: typebox_1.Type.Optional(typebox_1.Type.String({ description: "\"steer\" redirects the agent ASAP; \"queue\" delivers after its turn", enum: ["steer", "queue"] })),
});
const PaneInputSchema = typebox_1.Type.Object({
    pane: typebox_1.Type.String({ description: "target pane id" }),
});
const ReadInputSchema = typebox_1.Type.Object({
    pane: typebox_1.Type.String({ description: "target pane id" }),
    since_seq: typebox_1.Type.Optional(typebox_1.Type.Number({ description: "only rows after this seq" })),
    limit: typebox_1.Type.Optional(typebox_1.Type.Number({ description: "max rows (default 50)" })),
});
async function callRanchTool(tool, params) {
    const resp = await fetch(`${forgeApiUrl}/tools/execute`, {
        method: "POST",
        headers: {
            "content-type": "application/json",
            ...(forgeApiKey ? { "X-API-Key": forgeApiKey } : {}),
        },
        body: JSON.stringify({
            session_id: sessionId,
            tool,
            input: params,
        }),
    });
    const body = await resp.json().catch(() => null);
    if (!resp.ok) {
        const msg = body?.error ?? `HTTP ${resp.status}`;
        return { content: [{ type: "text", text: `${tool} failed: ${msg}` }], is_error: true };
    }
    // forge returns {success, output, error}; output is the control-API
    // reply frames value (AgentStatusOk / AgentReadOk / ...)
    if (body?.success === false) {
        return { content: [{ type: "text", text: `${tool} failed: ${body?.error ?? "unknown error"}` }], is_error: true };
    }
    const out = body?.output;
    return { content: [{ type: "text", text: typeof out === "string" ? out : JSON.stringify(out) }] };
}
async function ranchToolsExtension(pi) {
    if (pi.config?.forgeApiUrl)
        forgeApiUrl = pi.config.forgeApiUrl;
    if (pi.config?.sessionId)
        sessionId = pi.config.sessionId;
    const defs = [
        {
            name: "ranch_spawn",
            description: "Spawn a sub-agent in a new ranch pane and delegate a task to it. " +
                "Returns the pane id; use ranch_send/ranch_read/ranch_status/ranch_close to manage it. " +
                "The human sees the spawned pane live.",
            parameters: SpawnInputSchema,
            execute: async (_id, params) => callRanchTool("ranch_spawn", params),
        },
        {
            name: "ranch_send",
            description: "Send a message to an agent in another ranch pane (one you spawned). " +
                "delivery=\"steer\" redirects the agent as soon as possible; \"queue\" delivers after its current turn.",
            parameters: SendInputSchema,
            execute: async (_id, params) => callRanchTool("ranch_send", params),
        },
        {
            name: "ranch_status",
            description: "Check an agent pane's state (working/idle) and model. Cheap; use before ranch_read.",
            parameters: PaneInputSchema,
            execute: async (_id, params) => callRanchTool("ranch_status", params),
        },
        {
            name: "ranch_read",
            description: "Read recent conversation rows from an agent pane you spawned (newest last).",
            parameters: ReadInputSchema,
            execute: async (_id, params) => callRanchTool("ranch_read", params),
        },
        {
            name: "ranch_close",
            description: "Close an agent pane you spawned when its work is done (the human can also close it). " +
                "Returns the pane's final output summary.",
            parameters: PaneInputSchema,
            execute: async (_id, params) => callRanchTool("ranch_close", params),
        },
    ];
    for (const tool of defs) {
        pi.registerTool({
            name: tool.name,
            label: tool.name.replace("ranch_", "Ranch "),
            description: tool.description,
            parameters: tool.parameters,
            execute: tool.execute,
        });
    }
    console.error(`[ranch-tools] Registered ${defs.length} tools with pi`);
}
module.exports = ranchToolsExtension;
