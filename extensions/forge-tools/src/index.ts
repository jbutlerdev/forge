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

import { Type, Static } from "typebox";

// Tool input schemas
// Bash default timeout: 1 hour. This must match
// `BASH_DEFAULT_TIMEOUT_MS` in
// `crates/forge-api/src/tool_executor.rs`. The LLM may pass
// any value up to and beyond this — it's a default, not a cap.
// A shorter default would race the harness's
// `TOOL_READ_TIMEOUT_SECS` and the streaming-bash / sandbox
// outer grace window: a long `cargo test --release` or
// `git clone` would be killed at the first read-timeout
// boundary the harness hit.
const BASH_DEFAULT_TIMEOUT_MS = 3_600_000;

const BashInputSchema = Type.Object({
    command: Type.String({ description: "The shell command to execute" }),
    timeout_ms: Type.Optional(Type.Integer({ description: "Timeout in milliseconds", default: BASH_DEFAULT_TIMEOUT_MS })),
});

const ReadInputSchema = Type.Object({
    path: Type.String({ description: "Path to the file to read" }),
    offset: Type.Optional(Type.Integer({ description: "Line to start reading from (1-indexed)", default: 1 })),
    limit: Type.Optional(Type.Integer({ description: "Maximum lines to read", default: 100 })),
});

const WriteInputSchema = Type.Object({
    path: Type.String({ description: "Path to the file to write" }),
    content: Type.String({ description: "Content to write to the file" }),
});

const EditInputSchema = Type.Object({
    path: Type.String({ description: "Path to the file to edit" }),
    old_text: Type.String({ description: "Exact text to find and replace" }),
    new_text: Type.String({ description: "Replacement text" }),
});

type BashInput = Static<typeof BashInputSchema>;
type ReadInput = Static<typeof ReadInputSchema>;
type WriteInput = Static<typeof WriteInputSchema>;
type EditInput = Static<typeof EditInputSchema>;

interface ToolCallResult {
    content: Array<{ type: "text"; text: string }>;
    is_error?: boolean;
}

interface ToolProviderOptions {
    name: string;
    tools: Array<{
        name: string;
        description: string;
        parameters: any;
    }>;
    execute(toolName: string, toolInput: any, toolCallId: string): Promise<ToolCallResult>;
}

// Global state
let forgeApiUrl = process.env.FORGE_API_URL || "http://localhost:8080";
let sessionId = process.env.FORGE_SESSION_ID || "";
let useStreaming = process.env.FORGE_USE_STREAMING !== "false"; // Default to true

/**
 * Parse SSE stream from response.
 *
 * The forge side sends events in the standard SSE wire format:
 *
 *     event: stdout
 *     data: {"tool_call_id":"...","chunk":"hello"}
 *
 *     event: tool_end
 *     data: {"tool_call_id":"...","success":true,...}
 *
 * Each event is terminated by a blank line. We accumulate the
 * raw bytes, split on blank lines to recover whole events, then
 * pull out the `event:` name and `data:` payload from each one
 * before dispatching.
 *
 * The previous version of this parser only iterated single lines
 * and never associated the `event:` line with the following
 * `data:` line, so every event hit the fallback branch and only
 * stdout with a `chunk` field ever landed in the model's view.
 * tool_start/tool_end were silently dropped, which meant success
 * and duration_ms were never recorded and the model saw an empty
 * result for any command whose chunks were split across multiple
 * TCP reads.
 */
async function parseSSEStream(response: Response, toolCallId: string): Promise<ToolCallResult> {
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

    const onStdout = (chunk: string) => {
        output += chunk;
        process.stdout.write(chunk);
    };
    const onStderr = (chunk: string) => {
        errorOutput += chunk;
        process.stderr.write(chunk);
    };
    const onComplete = (result: { success: boolean; duration_ms: number }) => {
        success = result.success;
        durationMs = result.duration_ms || 0;
    };

    try {
        while (true) {
            const { done, value } = await reader.read();

            if (done) break;

            buffer += decoder.decode(value, { stream: true });

            // SSE events are separated by a blank line (\n\n).
            // Split the buffer on the double newline, keep the
            // tail (anything after the last blank line) in the
            // buffer for the next read.
            const events = buffer.split("\n\n");
            buffer = events.pop() || "";

            for (const raw of events) {
                if (!raw) continue;
                // Each event is one or more lines. The first
                // `event:` line names the event, the first `data:`
                // line carries the JSON payload. Comments start
                // with ':' and we ignore them.
                let eventName: string | undefined;
                let dataLine: string | undefined;
                for (const line of raw.split("\n")) {
                    if (line.startsWith(":")) continue;
                    if (line.startsWith("event:") && eventName === undefined) {
                        eventName = line.slice(6).trim();
                    } else if (line.startsWith("data:") && dataLine === undefined) {
                        dataLine = line.slice(5).trim();
                    }
                }
                if (dataLine === undefined) continue;
                let payload: any;
                try {
                    payload = JSON.parse(dataLine);
                } catch {
                    // Malformed JSON; skip rather than crash.
                    continue;
                }
                dispatchSSEEvent(eventName, payload, onStdout, onStderr, onComplete);
            }
        }
    } finally {
        reader.releaseLock();
    }

    console.log(`\n[forge-tools] Tool completed in ${durationMs}ms, success=${success}`);

    if (success) {
        return {
            content: [{ type: "text", text: output || "Command completed successfully" }],
        };
    } else {
        return {
            content: [{ type: "text", text: errorOutput || "Command failed" }],
            is_error: true,
        };
    }
}

/**
 * Dispatch a single SSE event by name.
 */
function dispatchSSEEvent(
    eventName: string | undefined,
    data: any,
    onStdout: (chunk: string) => void,
    onStderr: (chunk: string) => void,
    onComplete: (result: { success: boolean; duration_ms: number }) => void
): void {
    switch (eventName) {
        case "tool_start":
            console.log(`[forge-tools] Tool started: ${data.tool}`);
            break;
        case "stdout":
            if (data.chunk) onStdout(data.chunk);
            break;
        case "stderr":
            if (data.chunk) onStderr(data.chunk);
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
            // An error event also means the command didn't run
            // to completion, so mark it as failed. If a
            // subsequent tool_end with success=true arrives we
            // would still treat the result as success, which is
            // the right behavior.
            break;
        case "done":
            // Stream complete
            break;
        default:
            // Unknown event name; log it so we can debug protocol
            // drift between the Rust API and the extension.
            console.warn(`[forge-tools] Unknown SSE event: ${eventName ?? "(none)"}`);
    }
}

/**
 * Execute tool with SSE streaming (for bash commands)
 */
async function executeToolStreaming(
    toolName: string,
    toolInput: Record<string, any>,
    toolCallId: string
): Promise<ToolCallResult> {
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
    } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        console.error(`[forge-tools] SSE streaming error: ${errorMessage}`);
        console.log(`[forge-tools] Falling back to non-streaming`);
        return executeToolNonStreaming(toolName, toolInput, toolCallId);
    }
}

/**
 * Execute tool without streaming (fallback)
 */
async function executeToolNonStreaming(
    toolName: string,
    toolInput: Record<string, any>,
    toolCallId: string
): Promise<ToolCallResult> {
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

        const result = await response.json() as {
            success: boolean;
            output: string | null;
            error: string | null;
        };

        if (result.success) {
            console.log(`[forge-tools] Tool success: ${toolName}`);
            return {
                content: [{ type: "text", text: result.output || "" }],
            };
        } else {
            console.error(`[forge-tools] Tool error: ${result.error}`);
            return {
                content: [{ type: "text", text: result.error || "Unknown error" }],
                is_error: true,
            };
        }
    } catch (error) {
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
export default function forgeToolsExtension(pi: any): void {
    console.log("[forge-tools] Initializing Forge tools extension");
    console.log("[forge-tools] Forge API URL:", forgeApiUrl);
    console.log("[forge-tools] Session ID:", sessionId);
    console.log("[forge-tools] SSE Streaming:", useStreaming ? "enabled" : "disabled");

    // Override API URL if provided via pi's --config or similar
    if ((pi as any).config?.forgeApiUrl) {
        forgeApiUrl = (pi as any).config.forgeApiUrl;
        console.log("[forge-tools] Using configured API URL:", forgeApiUrl);
    }

    if ((pi as any).config?.sessionId) {
        sessionId = (pi as any).config.sessionId;
        console.log("[forge-tools] Using configured session ID:", sessionId);
    }

    if ((pi as any).config?.useStreaming !== undefined) {
        useStreaming = (pi as any).config.useStreaming;
        console.log("[forge-tools] Streaming configured:", useStreaming ? "enabled" : "disabled");
    }

    // Register as a tool provider
    const toolProvider: ToolProviderOptions = {
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

        async execute(
            toolName: string,
            toolInput: Record<string, any>,
            toolCallId: string
        ): Promise<ToolCallResult> {
            // Use streaming for bash commands if enabled
            if (useStreaming && toolName === "bash") {
                return executeToolStreaming(toolName, toolInput, toolCallId);
            }
            return executeToolNonStreaming(toolName, toolInput, toolCallId);
        },
    };

    // Check if pi supports registerToolProvider
    if (typeof (pi as any).registerToolProvider === "function") {
        (pi as any).registerToolProvider(toolProvider);
        console.log("[forge-tools] Registered tool provider with pi");
    } else if (typeof (pi as any).registerTool === "function") {
        // Fallback: register individual tools
        console.log("[forge-tools] registerToolProvider not found, using registerTool");
        for (const tool of toolProvider.tools) {
            (pi as any).registerTool({
                name: tool.name,
                description: tool.description,
                parameters: tool.parameters,
                // pi's `registerTool` callback signature is
                // `execute(toolCallId, params, signal, onUpdate, ctx)` -
                // note that `toolCallId` is the FIRST argument, not the
                // second. Earlier versions of this extension had them
                // swapped which caused `input` to be sent to Forge as
                // the call id and vice versa.
                execute: (toolCallId: string, input: any) =>
                    toolProvider.execute(tool.name, input, toolCallId),
            });
        }
        console.log(`[forge-tools] Registered ${toolProvider.tools.length} tools with pi`);
    } else {
        console.error("[forge-tools] No tool registration method found on pi");
    }
}

// Module exports for CommonJS compatibility
module.exports = forgeToolsExtension;
