"use strict";
/**
 * forge-tools: Forge tool provider extension for pi
 *
 * This extension intercepts tool requests from the LLM and forwards
 * them to Forge's Tool API for execution in the sandbox.
 *
 * IMPORTANT: pi's built-in tools must be disabled when using this extension.
 * Use: pi --no-builtin-tools --extension ./forge-tools/dist/index.js
 *
 * Supports SSE streaming for real-time output on bash commands.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.default = forgeToolsExtension;
const typebox_1 = require("typebox");
// Tool input schemas
const BashInputSchema = typebox_1.Type.Object({
    command: typebox_1.Type.String({ description: "The shell command to execute" }),
    timeout_ms: typebox_1.Type.Optional(typebox_1.Type.Integer({ description: "Timeout in milliseconds", default: 30000 })),
});
const ReadInputSchema = typebox_1.Type.Object({
    path: typebox_1.Type.String({ description: "Path to the file to read" }),
    offset: typebox_1.Type.Optional(typebox_1.Type.Integer({ description: "Line to start reading from (1-indexed)", default: 1 })),
    limit: typebox_1.Type.Optional(typebox_1.Type.Integer({ description: "Maximum lines to read", default: 100 })),
});
const WriteInputSchema = typebox_1.Type.Object({
    path: typebox_1.Type.String({ description: "Path to the file to write" }),
    content: typebox_1.Type.String({ description: "Content to write to the file" }),
});
const EditInputSchema = typebox_1.Type.Object({
    path: typebox_1.Type.String({ description: "Path to the file to edit" }),
    old_text: typebox_1.Type.String({ description: "Exact text to find and replace" }),
    new_text: typebox_1.Type.String({ description: "Replacement text" }),
});
// Global state
let forgeApiUrl = process.env.FORGE_API_URL || "http://localhost:8080/api/v1";
let sessionId = process.env.FORGE_SESSION_ID || "";
let useStreaming = process.env.FORGE_USE_STREAMING !== "false"; // Default to true
/**
 * Parse SSE stream from response
 */
async function parseSSEStream(response, toolCallId) {
    const reader = response.body?.getReader();
    if (!reader) {
        return {
            content: [{ type: "text", text: "Error: No response body" }],
            is_error: true,
        };
    }
    const decoder = new TextDecoder();
    let buffer = "";
    let output = "";
    let errorOutput = "";
    let success = true;
    let durationMs = 0;
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done)
                break;
            buffer += decoder.decode(value, { stream: true });
            // Process complete SSE events
            const lines = buffer.split("\n");
            buffer = lines.pop() || ""; // Keep incomplete line in buffer
            for (const line of lines) {
                if (line.startsWith("event:")) {
                    // Event type
                }
                else if (line.startsWith("data:")) {
                    const data = line.slice(5).trim();
                    if (data) {
                        try {
                            const parsed = JSON.parse(data);
                            await processSSEEvent(parsed, (chunk) => {
                                output += chunk;
                                process.stdout.write(chunk);
                            }, (chunk) => {
                                errorOutput += chunk;
                                process.stderr.write(chunk);
                            }, (result) => {
                                success = result.success;
                                durationMs = result.duration_ms || 0;
                            });
                        }
                        catch {
                            // Ignore parse errors for incomplete data
                        }
                    }
                }
            }
        }
    }
    finally {
        reader.releaseLock();
    }
    console.log(`\n[forge-tools] Tool completed in ${durationMs}ms, success=${success}`);
    if (success) {
        return {
            content: [{ type: "text", text: output || "Command completed successfully" }],
        };
    }
    else {
        return {
            content: [{ type: "text", text: errorOutput || "Command failed" }],
            is_error: true,
        };
    }
}
/**
 * Process a single SSE event
 */
async function processSSEEvent(event, onStdout, onStderr, onComplete) {
    const data = JSON.parse(event.data);
    switch (event.event || event.data.startsWith("{") ? undefined : event.data) {
        case "tool_start":
            console.log(`[forge-tools] Tool started: ${data.tool}`);
            break;
        case "stdout":
            onStdout(data.chunk || "");
            break;
        case "stderr":
            onStderr(data.chunk || "");
            break;
        case "tool_end":
            onComplete({
                success: data.success,
                duration_ms: data.duration_ms || 0,
            });
            break;
        case "error":
            console.error(`[forge-tools] Tool error: ${data.error}`);
            onStderr(`Error: ${data.error}\n`);
            break;
        case "done":
            // Stream complete
            break;
        default:
            // Handle events without explicit event type
            if (data.chunk) {
                onStdout(data.chunk);
            }
            if (data.success !== undefined) {
                onComplete({ success: data.success, duration_ms: data.duration_ms || 0 });
            }
            if (data.error) {
                onStderr(`Error: ${data.error}\n`);
            }
    }
}
/**
 * Execute tool with SSE streaming (for bash commands)
 */
async function executeToolStreaming(toolName, toolInput, toolCallId) {
    console.log(`[forge-tools] Streaming tool call: ${toolName}`);
    try {
        const response = await fetch(`${forgeApiUrl}/tools/execute/stream`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
            },
            body: JSON.stringify({
                session_id: sessionId,
                tool: toolName,
                input: toolInput,
                tool_call_id: toolCallId,
            }),
        });
        if (!response.ok) {
            const errorText = await response.text();
            console.error(`[forge-tools] Forge SSE API error: ${response.status} ${errorText}`);
            // Fall back to non-streaming
            return executeToolNonStreaming(toolName, toolInput, toolCallId);
        }
        return await parseSSEStream(response, toolCallId);
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error(`[forge-tools] SSE streaming error: ${errorMessage}`);
        console.log(`[forge-tools] Falling back to non-streaming`);
        return executeToolNonStreaming(toolName, toolInput, toolCallId);
    }
}
/**
 * Execute tool without streaming (fallback)
 */
async function executeToolNonStreaming(toolName, toolInput, toolCallId) {
    console.log(`[forge-tools] Non-streaming tool call: ${toolName}`);
    try {
        const response = await fetch(`${forgeApiUrl}/tools/execute`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({
                session_id: sessionId,
                tool: toolName,
                input: toolInput,
                tool_call_id: toolCallId,
            }),
        });
        if (!response.ok) {
            const errorText = await response.text();
            console.error(`[forge-tools] Forge API error: ${response.status} ${errorText}`);
            return {
                content: [{ type: "text", text: `Error: ${response.status} ${errorText}` }],
                is_error: true,
            };
        }
        const result = await response.json();
        if (result.success) {
            console.log(`[forge-tools] Tool success: ${toolName}`);
            return {
                content: [{ type: "text", text: result.output || "" }],
            };
        }
        else {
            console.error(`[forge-tools] Tool error: ${result.error}`);
            return {
                content: [{ type: "text", text: result.error || "Unknown error" }],
                is_error: true,
            };
        }
    }
    catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error(`[forge-tools] Network error: ${errorMessage}`);
        return {
            content: [{ type: "text", text: `Network error: ${errorMessage}` }],
            is_error: true,
        };
    }
}
/**
 * Main extension factory
 *
 * Called by pi when loading extensions.
 */
function forgeToolsExtension(pi) {
    console.log("[forge-tools] Initializing Forge tools extension");
    console.log("[forge-tools] Forge API URL:", forgeApiUrl);
    console.log("[forge-tools] Session ID:", sessionId);
    console.log("[forge-tools] SSE Streaming:", useStreaming ? "enabled" : "disabled");
    // Override API URL if provided via pi's --config or similar
    if (pi.config?.forgeApiUrl) {
        forgeApiUrl = pi.config.forgeApiUrl;
        console.log("[forge-tools] Using configured API URL:", forgeApiUrl);
    }
    if (pi.config?.sessionId) {
        sessionId = pi.config.sessionId;
        console.log("[forge-tools] Using configured session ID:", sessionId);
    }
    if (pi.config?.useStreaming !== undefined) {
        useStreaming = pi.config.useStreaming;
        console.log("[forge-tools] Streaming configured:", useStreaming ? "enabled" : "disabled");
    }
    // Register as a tool provider
    const toolProvider = {
        name: "forge",
        tools: [
            {
                name: "bash",
                description: "Execute a shell command and return stdout/stderr. Output is streamed in real-time for long-running commands.",
                parameters: BashInputSchema,
            },
            {
                name: "read",
                description: "Read file contents",
                parameters: ReadInputSchema,
            },
            {
                name: "write",
                description: "Write content to a file (creates or overwrites)",
                parameters: WriteInputSchema,
            },
            {
                name: "edit",
                description: "Apply a targeted text replacement to a file",
                parameters: EditInputSchema,
            },
        ],
        async execute(toolName, toolInput, toolCallId) {
            // Use streaming for bash commands if enabled
            if (useStreaming && toolName === "bash") {
                return executeToolStreaming(toolName, toolInput, toolCallId);
            }
            return executeToolNonStreaming(toolName, toolInput, toolCallId);
        },
    };
    // Check if pi supports registerToolProvider
    if (typeof pi.registerToolProvider === "function") {
        pi.registerToolProvider(toolProvider);
        console.log("[forge-tools] Registered tool provider with pi");
    }
    else if (typeof pi.registerTool === "function") {
        // Fallback: register individual tools
        console.log("[forge-tools] registerToolProvider not found, using registerTool");
        for (const tool of toolProvider.tools) {
            pi.registerTool({
                name: tool.name,
                description: tool.description,
                parameters: tool.parameters,
                execute: (input, toolCallId) => toolProvider.execute(tool.name, input, toolCallId),
            });
        }
        console.log(`[forge-tools] Registered ${toolProvider.tools.length} tools with pi`);
    }
    else {
        console.error("[forge-tools] No tool registration method found on pi");
    }
}
// Module exports for CommonJS compatibility
module.exports = forgeToolsExtension;
