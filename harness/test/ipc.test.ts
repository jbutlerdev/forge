/**
 * End-to-end IPC test: spawn the real `src/main.ts` as a child process over
 * a real scratch Postgres schema (faux provider via FORGE_HARNESS_FAUX=1),
 * talk to it through a raw net.Socket on the RPC socket, and assert that
 * the events socket pushes at least a task `started` line for a submitted
 * turn.
 *
 * Environment: HARNESS_TEST_PG (default
 * postgres://postgres:forge@127.0.0.1:5432/postgres).
 */
import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { connect, type Socket } from "node:net";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
import { freshSchemaName, freshStorage } from "./support.js";

const HERE = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = join(HERE, "..");

const cleanups: Array<() => Promise<void> | void> = [];

afterAll(async () => {
	for (const cleanup of cleanups.splice(0)) await cleanup().catch(() => {});
});

function connectSocket(path: string): Promise<Socket> {
	return new Promise((resolve, reject) => {
		const socket = connect(path, () => resolve(socket));
		socket.on("error", reject);
	});
}

/** Read JSON lines off a socket until `take` accepts one; timeout guards. */
function readLine(socket: Socket, take: (line: Record<string, unknown>) => boolean, timeoutMs: number): Promise<Record<string, unknown>> {
	const deadline = Date.now() + timeoutMs;
	return new Promise((resolve, reject) => {
		let buffer = "";
		const onData = (chunk: Buffer) => {
			buffer += chunk.toString("utf8");
			let newline = buffer.indexOf("\n");
			while (newline !== -1) {
				const line = buffer.slice(0, newline);
				buffer = buffer.slice(newline + 1);
				newline = buffer.indexOf("\n");
				if (line.length === 0) continue;
				let parsed: Record<string, unknown>;
				try {
					parsed = JSON.parse(line);
				} catch {
					continue;
				}
				if (take(parsed)) {
					socket.off("data", onData);
					resolve(parsed);
					return;
				}
			}
		};
		socket.on("data", onData);
		setTimeout(() => {
			socket.off("data", onData);
			reject(new Error(`timed out after ${timeoutMs}ms waiting for event`));
		}, timeoutMs);
	});
}

function rpcRequest(
	socket: Socket,
	id: number,
	method: string,
	params: Record<string, unknown> = {},
): Promise<{ result?: unknown; error?: { code: string; message: string } }> {
	socket.write(`${JSON.stringify({ id, method, params })}\n`);
	return new Promise((resolve, reject) => {
		const onData = (chunk: Buffer) => {
			const text = chunk.toString("utf8");
			const idx = text.indexOf("\n");
			if (idx === -1) return;
			const line = text.slice(0, idx);
			// We issue one request at a time in this test; answer the first
			// reply that carries our id.
			let parsed: unknown;
			try {
				parsed = JSON.parse(line);
			} catch {
				return;
			}
			const reply = parsed as { id?: unknown };
			if (reply.id !== id) return;
			socket.off("data", onData);
			resolve(parsed as { result?: unknown; error?: { code: string; message: string } });
		};
		socket.on("data", onData);
		setTimeout(() => {
			socket.off("data", onData);
			reject(new Error(`RPC ${method} timed out`));
		}, 20_000);
	});
}

describe("IPC over the real process", () => {
	it("serves importEntries: bulk entry import lands durably and the imported context turns", async () => {
		const dir = await mkdtemp(join(tmpdir(), "harness-ipc-"));
		const schema = freshSchemaName();
		const rpcSocket = join(dir, "harness.sock");
		const eventsSocket = join(dir, "harness-events.sock");
		const storage = await freshStorage(schema);
		const { Pool } = await import("pg");
		const probe = new Pool({
			connectionString: process.env.HARNESS_TEST_PG ?? "postgres://postgres:forge@127.0.0.1:5432/postgres",
			max: 2,
		});

		const child = spawn("npx", ["tsx", "src/main.ts"], {
			cwd: PACKAGE_ROOT,
			env: {
				...process.env,
				FORGE_DATABASE_URL: process.env.HARNESS_TEST_PG ?? "postgres://postgres:forge@127.0.0.1:5432/postgres",
				FORGE_HARNESS_SCHEMA: schema,
				FORGE_API_URL: "http://127.0.0.1:9",
				FORGE_API_KEY: "ipc-test-key",
				FORGE_HARNESS_SOCKET: rpcSocket,
				FORGE_HARNESS_EVENTS_SOCKET: eventsSocket,
				FORGE_HARNESS_FAUX: "1",
				FORGE_HARNESS_FAUX_RESPONSES: '["imported context works"]',
			},
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stderr = "";
		child.stderr!.on("data", (chunk) => (stderr += String(chunk)));
		cleanups.push(async () => {
			if (!child.killed) child.kill("SIGKILL");
			await new Promise((r) => child.on("exit", () => r()));
			await storage.drop();
			await probe.end().catch(() => {});
			await rm(dir, { recursive: true, force: true });
		});

		// Wait for the harness to finish booting (it logs "listening").
		const listening = await new Promise<boolean>((resolve) => {
			const check = () => {
				if (stderr.includes('"listening"') || stderr.includes("failed to start")) {
					clearInterval(interval);
					resolve(stderr.includes('"listening"'));
				}
			};
			const interval = setInterval(check, 50);
			setTimeout(() => {
				clearInterval(interval);
				resolve(false);
			}, 30_000);
			check();
		});
		expect(listening, `harness did not start. stderr:\n${stderr}`).toBe(true);

		const eventsSocketClient = await connectSocket(eventsSocket);
		await readLine(eventsSocketClient, (line) => line.type === "hello", 10_000);
		const rpc = await connectSocket(rpcSocket);

		const created = await rpcRequest(rpc, 1, "createConversation", {
			forgeSessionId: "ipc-sess-import",
			agent: { provider: "faux", modelId: "faux-1" },
		});
		expect(created.error, JSON.stringify(created.error)).toBeUndefined();
		const conversationId = (created.result as { conversationId: number }).conversationId;

		// The legacy-transcript shapes forge-api's lazy migration imports
		// (H2.6): a pi.user prompt and a pi.assistant answer — in one
		// commit. (The tool-call/result mapping is covered on the
		// forge-api side by `messages_to_entries` + the migration E2E.)
		const entries = [
			{
				kind: "pi.user",
				model: [{ role: "user", content: "seed prompt", timestamp: 1 }],
			},
			{
				kind: "pi.assistant",
				model: [
					{
						role: "assistant",
						content: [{ type: "text", text: "seed answer" }],
						api: "faux",
						provider: "faux",
						model: "faux-1",
						usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
						stopReason: "stop",
						timestamp: 2,
					},
				],
			}
		];

		// Bad params: missing kind / unknown conversation.
		const badKind = await rpcRequest(rpc, 2, "importEntries", {
			conversationId,
			entries: [{ model: [] }],
		});
		expect(badKind.error?.code).toBe("invalid_params");
		const unknownConversation = await rpcRequest(rpc, 3, "importEntries", {
			conversationId: 999999,
			entries,
		});
		expect(unknownConversation.error?.code).toBe("unknown_conversation");

		// The real import: one round trip, one commit.
		const imported = await rpcRequest(rpc, 4, "importEntries", { conversationId, entries });
		expect(imported.error, JSON.stringify(imported.error)).toBeUndefined();
		expect((imported.result as { imported: number }).imported).toBe(2);

		const rows = await probe.query(
			`SELECT (record::jsonb)->>'kind' AS kind FROM "${schema}".durable_entries WHERE conversation_id = $1 ORDER BY id`,
			[conversationId],
		);
		expect(rows.rows.map((r) => r.kind)).toEqual(["pi.user", "pi.assistant"]);

		// The imported context is valid: a turn on the conversation
		// completes against it (faux answer queued).
		const submitted = await rpcRequest(rpc, 5, "submit", {
			conversationId,
			requestId: "req-import-turn",
			entryDraft: { type: "input", content: "hello after import" },
		});
		expect(submitted.error, JSON.stringify(submitted.error)).toBeUndefined();
		const done = await readLine(
			eventsSocketClient,
			(line) =>
				line.type === "task_state" &&
				(line as { status?: string }).status === "done" &&
				(line as { conversationId?: number }).conversationId === conversationId,
			30_000,
		);
		expect(done.taskId).toBeTypeOf("number");

		rpc.destroy();
		eventsSocketClient.destroy();
	}, 90_000);

	it("serves status/createConversation/submit on the RPC socket and pushes events", async () => {
		const dir = await mkdtemp(join(tmpdir(), "harness-ipc-"));
		const schema = freshSchemaName();
		const rpcSocket = join(dir, "harness.sock");
		const eventsSocket = join(dir, "harness-events.sock");
		const storage = await freshStorage(schema);

		const child = spawn("npx", ["tsx", "src/main.ts"], {
			cwd: PACKAGE_ROOT,
			env: {
				...process.env,
				FORGE_DATABASE_URL: process.env.HARNESS_TEST_PG ?? "postgres://postgres:forge@127.0.0.1:5432/postgres",
				FORGE_HARNESS_SCHEMA: schema,
				FORGE_API_URL: "http://127.0.0.1:9",
				FORGE_API_KEY: "ipc-test-key",
				FORGE_HARNESS_SOCKET: rpcSocket,
				FORGE_HARNESS_EVENTS_SOCKET: eventsSocket,
				FORGE_HARNESS_FAUX: "1",
			},
			stdio: ["ignore", "pipe", "pipe"],
		});
		let stderr = "";
		child.stderr!.on("data", (chunk) => (stderr += String(chunk)));
		cleanups.push(async () => {
			if (!child.killed) child.kill("SIGKILL");
			await new Promise((r) => child.on("exit", () => r()));
			await storage.drop();
			await rm(dir, { recursive: true, force: true });
		});

		// Wait for the harness to finish booting (it logs "listening").
		const listening = await new Promise<boolean>((resolve) => {
			const check = () => {
				if (stderr.includes('"listening"') || stderr.includes("failed to start")) {
					clearInterval(interval);
					resolve(stderr.includes('"listening"'));
				}
			};
			const interval = setInterval(check, 50);
			setTimeout(() => {
				clearInterval(interval);
				resolve(false);
			}, 30_000);
			check();
		});
		expect(listening, `harness did not start. stderr:\n${stderr}`).toBe(true);

		// Connect the events client BEFORE submitting, so the first events land.
		const eventsSocketClient = await connectSocket(eventsSocket);
		const hello = await readLine(eventsSocketClient, (line) => line.type === "hello", 10_000);
		expect(hello.version).toBe("0.1.0");

		const rpc = await connectSocket(rpcSocket);

		const status = await rpcRequest(rpc, 1, "status");
		expect(status.error).toBeUndefined();
		expect((status.result as { version: string }).version).toBe("0.1.0");
		expect(((status.result as { activeTasks: number; conversations: number }).conversations)).toBe(0);

		const created = await rpcRequest(rpc, 2, "createConversation", {
			forgeSessionId: "ipc-sess-1",
			agent: { provider: "faux", modelId: "faux-1" },
		});
		expect(created.error, JSON.stringify(created.error)).toBeUndefined();
		const conversationId = (created.result as { conversationId: number }).conversationId;
		expect(conversationId).toBeTypeOf("number");

		// An unknown method and an unknown conversation must come back as
		// typed errors, not crashes.
		const unknown = await rpcRequest(rpc, 3, "noSuchMethod");
		expect(unknown.error?.code).toBe("unknown_method");
		const unknownConversation = await rpcRequest(rpc, 4, "submit", {
			conversationId: 999999,
			requestId: "req-x",
			entryDraft: { type: "input", content: "hi" },
		});
		expect(unknownConversation.error?.code).toBe("unknown_conversation");

		const submitted = await rpcRequest(rpc, 5, "submit", {
			conversationId,
			requestId: "req-ipc-1",
			entryDraft: { type: "input", content: "hello from IPC" },
		});
		expect(submitted.error, JSON.stringify(submitted.error)).toBeUndefined();
		expect((submitted.result as { submissionId: number }).submissionId).toBeTypeOf("number");

		// The turn starts: the events socket pushes a task_state started line
		// for the generation task of our conversation.
		const started = await readLine(
			eventsSocketClient,
			(line) =>
				line.type === "task_state" &&
				(line as { status?: string }).status === "started" &&
				(line as { conversationId?: number }).conversationId === conversationId,
			20_000,
		);
		expect(started.taskId).toBeTypeOf("number");

		// Second client displaces the first on the events socket.
		const eventsSocketClient2 = await connectSocket(eventsSocket);
		const hello2 = await readLine(eventsSocketClient2, (line) => line.type === "hello", 10_000);
		expect(hello2.version).toBe("0.1.0");

		rpc.destroy();
		eventsSocketClient.destroy();
		eventsSocketClient2.destroy();
	}, 90_000);
});
