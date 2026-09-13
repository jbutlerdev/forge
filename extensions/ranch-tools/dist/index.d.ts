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
export {};
