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
/**
 * Main extension factory
 *
 * Called by pi when loading extensions.
 */
export default function forgeToolsExtension(pi: any): void;
