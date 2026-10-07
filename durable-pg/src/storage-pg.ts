/**
 * durable-pg — Postgres Storage backend for @earendil-works/pi-durable.
 *
 * A faithful port of the reference SQLite backend
 * (packages/durable/src/storage/sqlite/storage.ts in the pinned vendor)
 * to node-postgres. Same schema shape, same ID/sequence semantics, same
 * write-time checks — only the driver differs. See migrations/001_initial.sql
 * for the schema and the ID-model notes.
 */
import { Pool, type PoolClient } from "pg";
import type { Context, JsonValue } from "@earendil-works/chord";
import { apply, type Op } from "@earendil-works/chord/delta";
import { StorageRejected } from "@earendil-works/pi-durable";
import type {
	ConversationId,
	ConversationQuery,
	ConversationRecord,
	Cursor,
	DocumentAddress,
	DocumentContent,
	DocumentCreate,
	DocumentId,
	DocumentPoint,
	DocumentQuery,
	DocumentRecord,
	EntryId,
	EntryQuery,
	EntryRecord,
	Id,
	JsonObject,
	Page,
	Seq,
	Storage,
	StorageWrite,
	StoredDocument,
	SubmissionId,
	SubmissionQuery,
	SubmissionRecord,
	TaskId,
	TaskQuery,
	TaskRecord,
} from "@earendil-works/pi-durable";

type StoredTask = TaskRecord<JsonValue, JsonValue, JsonValue>;
type TableName = "conversation" | "entry" | "task" | "submission" | "document";

type RecordIdRow = { record_type: TableName };
type JsonRow = { record: string | JsonObject };
type EntryJsonRow = { record: string | JsonObject; commit_seq: number | string };
type RevisionRow = { seq: number; kind: DocumentContent["kind"]; version: number; content: string | JsonObject };
type MetadataRow = { next_id: string; next_seq: number };
type DocumentAction = {
	create?: DocumentCreate;
	copy?: Extract<StorageWrite, { readonly type: "document.copy" }>["source"];
	content?: DocumentContent;
	retire: boolean;
};

const encodeJson = (value: unknown): string => JSON.stringify(value) as string;
const parseJson = <T>(value: string | JsonObject): T =>
	typeof value === "string" ? (JSON.parse(value) as T) : (value as T);
// Some drivers normalize lone surrogates; JSON encoding keeps indexed string identities lossless.
const encodeIndexedString = (value: string): string => JSON.stringify(value);

// pi-durable's npm entry point does not export these tiny helpers, so
// they are mirrored here (verbatim semantics from src/ids.ts + storage/scan.ts).
const id = <I extends Id<string>>(value: number): I => value as I;
const seqOf = (value: number): Seq => value as Seq;

type Executor = {
	query<T extends object>(sql: string, params?: unknown[]): Promise<{ rows: T[] }>;
};

/** pg's PoolOptions (subset we touch). */
type PoolOptionsWithSchema = { schema?: string };

/** Last ID a previous page returned, from a caller-round-tripped cursor. */
const cursorId = (cursor: Cursor | undefined): number | undefined => {
	const after = (cursor as { after?: unknown } | undefined)?.after;
	if (after === undefined) return undefined;
	if (typeof after !== "number" || !Number.isSafeInteger(after)) throw new TypeError("Invalid storage cursor");
	return after;
};

function page<T extends { readonly id: Id<string> }>(values: readonly T[], limit: number): Page<T, Cursor> {
	const items = values.slice(0, limit);
	if (values.length <= limit) return { items };
	return { items, next: { after: items[items.length - 1]!.id as unknown as number } } as Page<T, Cursor>;
}

/** Ordered-ID WHERE clause + bind params for an id-keyed scan page. */
const ascendingScan = (after: number | undefined) => ({
	clause: after === undefined ? "TRUE" : "id > $1",
	params: after === undefined ? [] : [after],
	direction: "ASC" as const,
});

type ScopeColumns = { scopeKind: DocumentRecord["scope"]["kind"]; ownerId: number };
const scopeColumns = (scope: DocumentRecord["scope"]): ScopeColumns => {
	switch (scope.kind) {
		case "session":
			return { scopeKind: "session", ownerId: 0 };
		case "conversation":
			return { scopeKind: "conversation", ownerId: scope.conversationId };
		case "task":
			return { scopeKind: "task", ownerId: scope.taskId };
	}
};

const addressParts = (address: DocumentAddress | DocumentCreate | DocumentRecord) => {
	const scope = scopeColumns(address.scope);
	return {
		kind: encodeIndexedString(address.kind),
		...scope,
		family: address.key === undefined ? 0 : 1,
		keyValue: encodeIndexedString(address.key ?? ""),
	};
};

const addressKey = (address: DocumentAddress | DocumentCreate | DocumentRecord): string => {
	const parts = addressParts(address);
	return JSON.stringify([parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue]);
};

const isAliveAt = (record: DocumentRecord, at: DocumentPoint): boolean => {
	if (at === "current") return record.retiredAt === undefined;
	return record.createdAt <= at && (record.retiredAt === undefined || at < record.retiredAt);
};

const isCurrentOnly = (record: DocumentRecord): boolean =>
	record.scope.kind !== "conversation" || record.history === "latest";

const writeId = (write: StorageWrite): Id<string> | undefined => {
	switch (write.type) {
		case "conversation":
		case "entry":
		case "task":
		case "submission":
			return write.value.id;
		case "document.create":
		case "document.copy":
			return write.record.id;
		case "document.change":
		case "document.retire":
			return undefined;
	}
};

export type PgStorageOptions = {
	/** Existing pool to wrap (close() then does NOT close it — tests share pools). */
	readonly pool?: Pool;
	/** Or connection details for a pool this storage owns. */
	readonly connectionString?: string;
	/**
	 * Schema the durable_* tables live in. Applied per-connection via the
	 * connection `options` startup parameter (race-free, unlike a `connect`
	 * event SET). Defaults to the server default (public).
	 */
	readonly schema?: string;
};

/** Run `fn` with a dedicated client, rolling back on throw. */
async function withTransaction<T>(client: PoolClient, fn: (tx: Executor) => Promise<T>): Promise<T> {
	await client.query("BEGIN");
	try {
		const result = await fn({
			query: (sql, params) => client.query(sql, params as unknown[]),
		});
		await client.query("COMMIT");
		return result;
	} catch (error) {
		try {
			await client.query("ROLLBACK");
		} catch {
			// Preserve the original error; a failed rollback is surfaced by the pool's error state.
		}
		throw error;
	}
}

export class PgStorage implements Storage {
	private readonly pool: Pool;
	private readonly ownsPool: boolean;
	private closed = false;

	private readonly schema: string | undefined;

	private constructor(pool: Pool, ownsPool: boolean, schema: string | undefined) {
		this.pool = pool;
		this.ownsPool = ownsPool;
		this.schema = schema;
		// Pin every connection this storage acquires to the schema. node-postgres
		// pools have no per-pool defaults reachable after construction, and a SET
		// in the pool's connect event races the first query — so route all
		// acquisitions through a wrapper that SETs once per fresh connection.
		if (schema !== undefined) {
			const acquire = pool.connect.bind(pool) as (cb?: unknown) => unknown;
			const done = new WeakSet<object>();
			const connect = (cb?: unknown): unknown => {
				// node-postgres supports callback or promise acquisition; mirror both.
				if (typeof cb === "function") {
					return acquire((error: Error | undefined, client: PoolClient) => {
						if (error || client === undefined) {
							(cb as (e: Error | undefined, c?: PoolClient) => void)(error, client);
							return;
						}
						if (!done.has(client)) {
							void client
								.query("SET search_path TO " + schema + ", public")
								.then(() => {
									done.add(client);
									(cb as (e: Error | undefined, c?: PoolClient) => void)(undefined, client);
								})
								.catch((queryError: Error) => (cb as (e: Error | undefined, c?: PoolClient) => void)(queryError));
						} else {
							(cb as (e: Error | undefined, c?: PoolClient) => void)(undefined, client);
						}
					});
				}
				return (acquire() as Promise<PoolClient>).then(async (client) => {
					if (!done.has(client)) {
						await client.query("SET search_path TO " + schema + ", public");
						done.add(client);
					}
					return client;
				});
			};
			pool.connect = connect as unknown as Pool["connect"];
		}
	}

	/** Initialize storage over a Postgres database, applying pending migrations. */
	static async open(options: PgStorageOptions = {}): Promise<PgStorage> {
		const ownsPool = options.pool === undefined;
		const pool = options.pool ?? new Pool({ connectionString: options.connectionString });
		const storage = new PgStorage(pool, ownsPool, options.schema);
		await storage.applyMigrations();
		return storage;
	}

	/** Apply pending migrations from migrations/*.sql (append-only history). */
	private async applyMigrations(): Promise<void> {
		const client = await this.pool.connect();
		try {
			await client.query("BEGIN");
			await client.query(
				`CREATE TABLE IF NOT EXISTS durable_schema (
					singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
					version INTEGER NOT NULL CHECK (version >= 0)
				)`,
			);
			await client.query("INSERT INTO durable_schema (singleton, version) VALUES (1, 0) ON CONFLICT (singleton) DO NOTHING");
			const current = Number(
				(await client.query<{ version: number }>("SELECT version FROM durable_schema WHERE singleton = 1")).rows[0]!
					.version,
			);
			if (current > PG_MIGRATIONS.at(-1)!.version) {
				throw new Error(`durable-pg: database schema v${current} is newer than supported v${PG_MIGRATIONS.at(-1)!.version}`);
			}
			for (const migration of PG_MIGRATIONS) {
				if (migration.version <= current) continue;
				await client.query(migration.sql);
				await client.query("UPDATE durable_schema SET version = $1 WHERE singleton = 1", [migration.version]);
			}
			await client.query("COMMIT");
		} catch (error) {
			await client.query("ROLLBACK").catch(() => {});
			throw error;
		} finally {
			client.release();
		}
	}

	async commit(writes: readonly StorageWrite[], _context: Context): Promise<Seq> {
		this.assertOpen();
		const client = await this.pool.connect();
		try {
			return await withTransaction(client, async (tx) => {
				// Allocate the commit sequence: single-row, locked to this transaction.
				const meta = (
					await tx.query<MetadataRow>("SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1 FOR UPDATE")
				).rows[0];
				if (meta === undefined) throw new Error("durable-pg: metadata row is missing");
				const committedSeq = seqOf(Number(meta.next_seq));
				await this.checkGlobalIds(tx, writes);
				const documentActions = this.prepareDocumentActions(writes);
				await this.checkDocumentActions(tx, documentActions);
				for (const write of writes) await this.applyTableWrite(tx, write, committedSeq);
				await this.applyDocumentActions(tx, documentActions, committedSeq);
				const candidateNextId = this.candidateNextId(writes);
				await tx.query("UPDATE durable_metadata SET next_id = $1, next_seq = $2 WHERE singleton = 1", [
					String(Math.max(Number(meta.next_id), candidateNextId)),
					committedSeq + 1,
				]);
				return committedSeq;
			});
		} finally {
			client.release();
		}
	}

	async mintId<I extends Id<string>>(): Promise<I> {
		this.assertOpen();
		const client = await this.pool.connect();
		try {
			return await withTransaction(client, async (tx) => {
				const meta = (
					await tx.query<MetadataRow>("SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1 FOR UPDATE")
				).rows[0]!;
				const next = Number(meta.next_id);
				if (!Number.isSafeInteger(next)) throw new Error("ID space is exhausted");
				await tx.query("UPDATE durable_metadata SET next_id = $1 WHERE singleton = 1", [String(next + 1)]);
				return id<I>(next);
			});
		} finally {
			client.release();
		}
	}

	async conversation(idValue: ConversationId, _context: Context): Promise<ConversationRecord | undefined> {
		this.assertOpen();
		const row = (
			await this.pool.query<JsonRow>("SELECT record FROM durable_conversations WHERE id = $1", [idValue as unknown as number])
		).rows[0];
		return row === undefined ? undefined : parseJson<ConversationRecord>(row.record);
	}

	async scanConversations(
		query: ConversationQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<ConversationRecord, Cursor>> {
		this.assertOpen();
		const after = cursorId(cursor);
		const scan = ascendingScan(after);
		const clauses = [scan.clause];
		const params: unknown[] = scan.params;
		if (query.ownerConversationId !== undefined) {
			clauses.push(`owner_conversation_id = $${params.push(query.ownerConversationId)}`);
		}
		if (query.ownerTaskId !== undefined) {
			clauses.push(`owner_task_id = $${params.push(query.ownerTaskId)}`);
		}
		params.push(limit + 1);
		const rows = (
			await this.pool.query<JsonRow>(
				`SELECT record FROM durable_conversations WHERE ${clauses.join(" AND ")} ORDER BY id ${scan.direction} LIMIT $${params.length}`,
				params,
			)
		).rows;
		return page(rows.map((row) => parseJson<ConversationRecord>(row.record)), limit);
	}

	async entry(idValue: EntryId, context: Context): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined>;
	async entry(
		conversationId: ConversationId,
		idValue: EntryId,
		context: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined>;
	async entry(
		idOrConversationId: EntryId | ConversationId,
		idOrContext: EntryId | Context,
		context?: Context,
	): Promise<{ readonly entry: EntryRecord; readonly commitSeq: Seq } | undefined> {
		this.assertOpen();
		const entryId =
			context === undefined
				? id<EntryId>(idOrConversationId as EntryId)
				: typeof idOrContext === "number"
					? id<EntryId>(idOrContext)
					: undefined;
		if (entryId === undefined) throw new TypeError("Storage.entry() requires an entry ID");
		let conversation: ConversationRecord | undefined;
		if (context !== undefined) {
			const conversationId = id<ConversationId>(idOrConversationId as ConversationId);
			conversation = await this.readConversation(conversationId);
			if (conversation === undefined) throw new Error(`Unknown conversation: ${conversationId}`);
		}
		const row = (
			await this.pool.query<EntryJsonRow>("SELECT record, commit_seq FROM durable_entries WHERE id = $1", [entryId as unknown as number])
		).rows[0];
		if (row === undefined) return undefined;
		const entry = parseJson<EntryRecord>(row.record);
		if (conversation !== undefined) {
			let upperEntryId = Number.POSITIVE_INFINITY;
			while (conversation.id !== entry.conversationId) {
				if (conversation.parent === undefined) return undefined;
				upperEntryId = Math.min(upperEntryId, conversation.parent.at);
				conversation = (await this.readConversation(conversation.parent.conversationId))!;
			}
			if (entry.id > upperEntryId) return undefined;
		}
		return { entry, commitSeq: seqOf(Number(row.commit_seq)) };
	}

	async findLatestHeadMarker(
		conversationId: ConversationId,
		atOrBeforeEntryId: EntryId | undefined,
		_context: Context,
	): Promise<(EntryRecord & { readonly head: EntryId }) | undefined> {
		this.assertOpen();
		let conversation = await this.readConversation(conversationId);
		if (conversation === undefined) throw new Error(`Unknown conversation: ${conversationId}`);
		let upper: number | undefined = atOrBeforeEntryId as number | undefined;
		while (true) {
			const row = (
				await this.pool.query<JsonRow>(
					upper === undefined
						? "SELECT record FROM durable_entries WHERE conversation_id = $1 AND head IS NOT NULL ORDER BY id DESC LIMIT 1"
						: "SELECT record FROM durable_entries WHERE conversation_id = $1 AND head IS NOT NULL AND id <= $2 ORDER BY id DESC LIMIT 1",
					upper === undefined ? [conversation.id] : [conversation.id, upper],
				)
			).rows[0];
			if (row !== undefined) return parseJson<EntryRecord & { readonly head: EntryId }>(row.record);
			if (conversation.parent === undefined) return undefined;
			upper = upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
	}

	async scanEntries(
		query: EntryQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<EntryRecord, Cursor>> {
		this.assertOpen();
		const after = cursorId(cursor);
		let conversation = await this.readConversation(query.conversationId);
		if (conversation === undefined) throw new Error(`Unknown conversation: ${query.conversationId}`);
		let upper: number | undefined = query.maxEntryId as number | undefined;
		if (after !== undefined) upper = Math.min(upper ?? Number.MAX_SAFE_INTEGER, after - 1);
		const values: EntryRecord[] = [];
		while (true) {
			const clauses = ["conversation_id = $1"];
			const params: unknown[] = [conversation.id];
			if (query.minEntryId !== undefined) clauses.push(`id >= $${params.push(query.minEntryId)}`);
			if (upper !== undefined) clauses.push(`id <= $${params.push(upper)}`);
			params.push(limit + 1 - values.length);
			const rows = (
				await this.pool.query<JsonRow>(
					`SELECT record FROM durable_entries WHERE ${clauses.join(" AND ")} ORDER BY id DESC LIMIT $${params.length}`,
					params,
				)
			).rows;
			values.push(...rows.map((row) => parseJson<EntryRecord>(row.record)));
			if (values.length > limit || conversation.parent === undefined) break;
			upper = upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
			if (query.minEntryId !== undefined && upper < query.minEntryId) break;
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
		return page(values, limit);
	}

	/** Oldest first: the fork chain's segments from the root conversation forward, each up to its fork point. */
	private async readEntriesAscending(query: EntryQuery, limit: number, after: number | undefined): Promise<Page<EntryRecord, Cursor>> {
		const segments: { readonly conversationId: ConversationId; readonly upper: number | undefined }[] = [];
		let conversation = await this.readConversation(query.conversationId);
		if (conversation === undefined) throw new Error(`Unknown conversation: ${query.conversationId}`);
		let upper: number | undefined = query.maxEntryId as number | undefined;
		while (true) {
			segments.push({ conversationId: conversation.id, upper });
			if (conversation.parent === undefined) break;
			upper = upper === undefined ? conversation.parent.at : Math.min(upper, conversation.parent.at);
			if (query.minEntryId !== undefined && upper < query.minEntryId) break;
			conversation = (await this.readConversation(conversation.parent.conversationId))!;
		}
		let lower: number | undefined = query.minEntryId as number | undefined;
		if (after !== undefined) lower = Math.max(lower ?? after + 1, after + 1);
		const values: EntryRecord[] = [];
		for (const segment of segments.reverse()) {
			const clauses = ["conversation_id = $1"];
			const params: unknown[] = [segment.conversationId];
			if (lower !== undefined) clauses.push(`id >= $${params.push(lower)}`);
			if (segment.upper !== undefined) clauses.push(`id <= $${params.push(segment.upper)}`);
			params.push(limit + 1 - values.length);
			const rows = (
				await this.pool.query<JsonRow>(
					`SELECT record FROM durable_entries WHERE ${clauses.join(" AND ")} ORDER BY id ASC LIMIT $${params.length}`,
					params,
				)
			).rows;
			values.push(...rows.map((row) => parseJson<EntryRecord>(row.record)));
			if (values.length > limit) break;
		}
		return page(values, limit);
	}

	async task(idValue: TaskId, _context: Context): Promise<StoredTask | undefined> {
		this.assertOpen();
		const row = (await this.pool.query<JsonRow>("SELECT record FROM durable_tasks WHERE id = $1", [idValue as unknown as number])).rows[0];
		return row === undefined ? undefined : parseJson<StoredTask>(row.record);
	}

	async scanTasks(
		query: TaskQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<StoredTask, Cursor>> {
		this.assertOpen();
		const after = cursorId(cursor);
		const scan = ascendingScan(after);
		const clauses = [scan.clause];
		const params: unknown[] = scan.params;
		if (query.conversationId !== undefined) clauses.push(`conversation_id = $${params.push(query.conversationId)}`);
		if (query.kind !== undefined) clauses.push(`kind = $${params.push(encodeIndexedString(query.kind))}`);
		if (query.status !== undefined) clauses.push(`status = $${params.push(query.status)}`);
		if (query.abortRequested !== undefined) clauses.push(`abort_requested = $${params.push(query.abortRequested)}`);
		if (query.background !== undefined) clauses.push(`background = $${params.push(query.background)}`);
		params.push(limit + 1);
		const rows = (
			await this.pool.query<JsonRow>(
				`SELECT record FROM durable_tasks WHERE ${clauses.join(" AND ")} ORDER BY id ${scan.direction} LIMIT $${params.length}`,
				params,
			)
		).rows;
		return page(rows.map((row) => parseJson<StoredTask>(row.record)), limit);
	}

	async submission(idValue: SubmissionId, _context: Context): Promise<SubmissionRecord | undefined> {
		this.assertOpen();
		const row = (
			await this.pool.query<JsonRow>("SELECT record FROM durable_submissions WHERE id = $1", [idValue as unknown as number])
		).rows[0];
		return row === undefined ? undefined : parseJson<SubmissionRecord>(row.record);
	}

	async scanSubmissions(
		query: SubmissionQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<SubmissionRecord, Cursor>> {
		this.assertOpen();
		const after = cursorId(cursor);
		const scan = ascendingScan(after);
		const clauses = [scan.clause];
		const params: unknown[] = scan.params;
		if (query.conversationId !== undefined) clauses.push(`conversation_id = $${params.push(query.conversationId)}`);
		if (query.status !== undefined) clauses.push(`status = $${params.push(query.status)}`);
		params.push(limit + 1);
		const rows = (
			await this.pool.query<JsonRow>(
				`SELECT record FROM durable_submissions WHERE ${clauses.join(" AND ")} ORDER BY id ${scan.direction} LIMIT $${params.length}`,
				params,
			)
		).rows;
		return page(rows.map((row) => parseJson<SubmissionRecord>(row.record)), limit);
	}

	async submissionByRequest(
		conversationId: ConversationId,
		requestId: string,
		_context: Context,
	): Promise<SubmissionRecord | undefined> {
		this.assertOpen();
		const row = (
			await this.pool.query<JsonRow>(
				"SELECT record FROM durable_submissions WHERE conversation_id = $1 AND request_id = $2",
				[conversationId as unknown as number, encodeIndexedString(requestId)],
			)
		).rows[0];
		return row === undefined ? undefined : parseJson<SubmissionRecord>(row.record);
	}

	async findDocument(address: DocumentAddress, at: DocumentPoint, _context: Context): Promise<DocumentRecord | undefined> {
		this.assertOpen();
		const parts = addressParts(address);
		const sql =
			at === "current"
				? `SELECT record FROM durable_documents
					WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND family = $4 AND key_value = $5
					AND retired_at IS NULL ORDER BY created_at DESC LIMIT 1`
				: `SELECT record FROM durable_documents
					WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND family = $4 AND key_value = $5
					AND created_at <= $6 AND (retired_at IS NULL OR retired_at > $6)
					ORDER BY created_at DESC LIMIT 1`;
		const params: unknown[] = [parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue];
		if (at !== "current") params.push(at);
		const row = (await this.pool.query<JsonRow>(sql, params)).rows[0];
		return row === undefined ? undefined : parseJson<DocumentRecord>(row.record);
	}

	async document(idValue: DocumentId, at: DocumentPoint, _context: Context): Promise<StoredDocument | undefined> {
		this.assertOpen();
		// The record and revision queries must observe one committed state; a commit between them can replace the base.
		const client = await this.pool.connect();
		try {
			return await withTransaction(client, (tx) => this.materializeDocument(tx, idValue as unknown as number, at));
		} finally {
			client.release();
		}
	}

	async scanDocuments(
		query: DocumentQuery,
		limit: number,
		cursor: Cursor | undefined,
		_context: Context,
	): Promise<Page<DocumentRecord, Cursor>> {
		this.assertOpen();
		const scope = scopeColumns(query.scope);
		const clauses = ["scope_kind = $1", "owner_id = $2", `id > $3`];
		const params: unknown[] = [scope.scopeKind, scope.ownerId, cursorId(cursor) ?? -1];
		if (query.kind !== undefined) clauses.push(`kind = $${params.push(encodeIndexedString(query.kind))}`);
		if (query.at === "current") {
			clauses.push("retired_at IS NULL");
		} else {
			params.push(query.at);
			clauses.push(`created_at <= $${params.length}`, `(retired_at IS NULL OR retired_at > $${params.length})`);
		}
		params.push(limit + 1);
		const rows = (
			await this.pool.query<JsonRow>(
				`SELECT record FROM durable_documents WHERE ${clauses.join(" AND ")} ORDER BY id LIMIT $${params.length}`,
				params,
			)
		).rows;
		return page(rows.map((row) => parseJson<DocumentRecord>(row.record)), limit);
	}

	async close(_context: Context): Promise<void> {
		this.closed = true;
		if (this.ownsPool) await this.pool.end();
	}

	private assertOpen(): void {
		if (this.closed) throw new Error("PgStorage is closed");
	}

	private async readConversation(idValue: ConversationId): Promise<ConversationRecord | undefined> {
		const row = (
			await this.pool.query<JsonRow>("SELECT record FROM durable_conversations WHERE id = $1", [idValue as unknown as number])
		).rows[0];
		return row === undefined ? undefined : parseJson<ConversationRecord>(row.record);
	}

	private async materializeDocument(tx: Executor, idValue: number, at: DocumentPoint): Promise<StoredDocument | undefined> {
		const row = (await tx.query<JsonRow>("SELECT record FROM durable_documents WHERE id = $1", [idValue])).rows[0];
		if (row === undefined) return undefined;
		const record = parseJson<DocumentRecord>(row.record);
		if (at !== "current" && isCurrentOnly(record)) {
			throw new Error(`Document ${idValue} does not retain historical content`);
		}
		if (!isAliveAt(record, at)) return undefined;
		// "current" reads every revision; the sentinel for "no upper bound" is the
		// bigint column domain's max (2^63-1), safely above any stored seq.
		const upper = at === "current" ? 0x7fffffffffffffffn : at;
		const base = (
			await tx.query<RevisionRow>(
				`SELECT seq, kind, version, content FROM durable_document_revisions
				WHERE document_id = $1 AND kind = 'base' AND seq <= $2 ORDER BY seq DESC LIMIT 1`,
				[idValue, upper],
			)
		).rows[0];
		if (base === undefined) throw new Error(`Document ${idValue} is missing a required base`);
		let value = parseJson<JsonObject>(base.content);
		const tail = (
			await tx.query<RevisionRow>(
				`SELECT seq, kind, version, content FROM durable_document_revisions
				WHERE document_id = $1 AND seq > $2 AND seq <= $3 ORDER BY seq`,
				[idValue, base.seq, upper],
			)
		).rows;
		for (const revision of tail) {
			if (revision.kind !== "delta" || revision.version !== base.version) {
				throw new Error(`Document ${idValue} crosses a stored version boundary without a base`);
			}
			value = apply(value, parseJson<readonly Op[]>(revision.content)) as JsonObject;
		}
		return { record, version: base.version, value, deltasSinceBase: tail.length };
	}

	private candidateNextId(writes: readonly StorageWrite[]): number {
		let nextId = 1;
		for (const write of writes) {
			const idValue = writeId(write);
			if (idValue !== undefined) nextId = Math.max(nextId, (idValue as unknown as number) + 1);
		}
		return nextId;
	}

	private async checkGlobalIds(tx: Executor, writes: readonly StorageWrite[]): Promise<void> {
		const claimed = new Map<number, TableName>();
		for (const write of writes) {
			if (write.type === "document.change" || write.type === "document.retire") continue;
			const document = write.type === "document.create" || write.type === "document.copy";
			const table: TableName = document ? "document" : write.type;
			const idValue = document ? (write.record.id as unknown as number) : (write.value.id as unknown as number);
			const existing = (
				await tx.query<RecordIdRow>("SELECT record_type FROM durable_record_ids WHERE id = $1", [idValue])
			).rows[0]?.record_type;
			const earlier = claimed.get(idValue);
			if (table === "conversation" || table === "entry" || table === "document") {
				if (existing !== undefined) throw new Error(`ID ${idValue} already belongs to ${existing}`);
				if (earlier !== undefined) throw new Error(`ID ${idValue} is written more than once`);
			} else {
				if (existing !== undefined && existing !== table) throw new Error(`ID ${idValue} already belongs to ${existing}`);
				if (earlier !== undefined && earlier !== table) throw new Error(`ID ${idValue} is written as two record types`);
			}
			claimed.set(idValue, table);
		}
	}

	private prepareDocumentActions(writes: readonly StorageWrite[]): Map<DocumentId, DocumentAction> {
		const actions = new Map<DocumentId, DocumentAction>();
		for (const write of writes) {
			if (
				write.type !== "document.create" &&
				write.type !== "document.copy" &&
				write.type !== "document.change" &&
				write.type !== "document.retire"
			) {
				continue;
			}
			const idValue = write.type === "document.create" || write.type === "document.copy" ? write.record.id : write.id;
			let action = actions.get(idValue);
			if (action === undefined) {
				action = { retire: false };
				actions.set(idValue, action);
			}
			switch (write.type) {
				case "document.create":
					if (action.create !== undefined || action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${idValue} has more than one content command`);
					}
					action.create = write.record;
					action.content = write.content;
					break;
				case "document.copy":
					if (action.create !== undefined || action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${idValue} has more than one content command`);
					}
					action.create = write.record;
					action.copy = write.source;
					break;
				case "document.change":
					if (action.content !== undefined || action.copy !== undefined) {
						throw new Error(`Document ${idValue} has more than one content command`);
					}
					action.content = write.content;
					break;
				case "document.retire":
					if (action.retire) throw new Error(`Document ${idValue} is retired more than once`);
					action.retire = true;
					break;
			}
		}
		return actions;
	}

	private async checkDocumentActions(tx: Executor, actions: ReadonlyMap<DocumentId, DocumentAction>): Promise<void> {
		const liveCounts = new Map<string, number>();
		for (const [idValue, action] of actions) {
			if (action.copy !== undefined && actions.has(action.copy.id)) {
				throw new StorageRejected(`Document copy ${idValue} source is changed in the copy batch`);
			}
			const idNumber = idValue as unknown as number;
			const existingRow = (await tx.query<JsonRow>("SELECT record FROM durable_documents WHERE id = $1", [idNumber])).rows[0];
			const existing = existingRow === undefined ? undefined : parseJson<DocumentRecord>(existingRow.record);
			if (action.create === undefined && existing === undefined) throw new Error(`Unknown document: ${idValue}`);
			if (action.create !== undefined && existing !== undefined) throw new Error(`Document ${idValue} already exists`);
			if (existing?.retiredAt !== undefined) throw new Error(`Document ${idValue} is retired`);
			if (action.content?.kind === "delta") {
				const previous = (
					await tx.query<{ version: number }>(
						"SELECT version FROM durable_document_revisions WHERE document_id = $1 ORDER BY seq DESC LIMIT 1",
						[idNumber],
					)
				).rows[0];
				if (previous === undefined) throw new Error(`Document ${idValue} delta has no base`);
				if (previous.version !== action.content.version) {
					throw new Error(`Document ${idValue} version transition requires a base`);
				}
			}
			const record = action.create ?? existing!;
			const key = addressKey(record);
			let live = liveCounts.get(key);
			if (live === undefined) live = (await this.currentDocumentId(tx, record)) === undefined ? 0 : 1;
			if (action.retire && existing !== undefined) live--;
			if (action.create !== undefined && !action.retire) live++;
			liveCounts.set(key, live);
		}
		for (const live of liveCounts.values()) {
			if (live > 1) throw new Error("Document address already has a current incarnation");
		}
	}

	private async currentDocumentId(
		tx: Executor,
		address: DocumentAddress | DocumentCreate | DocumentRecord,
	): Promise<DocumentId | undefined> {
		const parts = addressParts(address);
		const row = (
			await tx.query<{ id: number }>(
				`SELECT id FROM durable_documents
				WHERE kind = $1 AND scope_kind = $2 AND owner_id = $3 AND family = $4 AND key_value = $5 AND retired_at IS NULL
				LIMIT 1`,
				[parts.kind, parts.scopeKind, parts.ownerId, parts.family, parts.keyValue],
			)
		).rows[0];
		return row === undefined ? undefined : id<DocumentId>(row.id);
	}

	private async applyTableWrite(tx: Executor, write: StorageWrite, seq: Seq): Promise<void> {
		switch (write.type) {
			case "conversation":
				await this.claimId(tx, write.value.id, "conversation");
				await tx.query(
					"INSERT INTO durable_conversations (id, owner_conversation_id, owner_task_id, record) VALUES ($1, $2, $3, $4)",
					[
						write.value.id as unknown as number,
						write.value.owner?.conversationId ?? null,
						write.value.owner?.taskId ?? null,
						encodeJson(write.value),
					],
				);
				break;
			case "entry":
				await this.claimId(tx, write.value.id, "entry");
				await tx.query(
					"INSERT INTO durable_entries (id, conversation_id, head, commit_seq, record) VALUES ($1, $2, $3, $4, $5)",
					[
						write.value.id as unknown as number,
						write.value.conversationId as unknown as number,
						write.value.head ?? null,
						seq,
						encodeJson(write.value),
					],
				);
				break;
			case "task":
				await this.claimId(tx, write.value.id, "task");
				await tx.query(
					`INSERT INTO durable_tasks (id, conversation_id, kind, status, abort_requested, background, record)
					VALUES ($1, $2, $3, $4, $5, $6, $7)
					ON CONFLICT (id) DO UPDATE SET conversation_id = EXCLUDED.conversation_id, kind = EXCLUDED.kind,
					status = EXCLUDED.status, abort_requested = EXCLUDED.abort_requested,
					background = EXCLUDED.background, record = EXCLUDED.record`,
					[
						write.value.id as unknown as number,
						write.value.conversationId as unknown as number,
						encodeIndexedString(write.value.kind),
						write.value.state.status,
						write.value.abortRequested,
						write.value.background,
						encodeJson(write.value),
					],
				);
				break;
			case "submission":
				await this.claimId(tx, write.value.id, "submission");
				await tx.query(
					`INSERT INTO durable_submissions (id, conversation_id, request_id, status, record) VALUES ($1, $2, $3, $4, $5)
					ON CONFLICT (id) DO UPDATE SET conversation_id = EXCLUDED.conversation_id,
					request_id = EXCLUDED.request_id, status = EXCLUDED.status, record = EXCLUDED.record`,
					[
						write.value.id as unknown as number,
						write.value.conversationId as unknown as number,
						write.value.requestId === undefined ? null : encodeIndexedString(write.value.requestId),
						write.value.status,
						encodeJson(write.value),
					],
				);
				break;
			case "document.create":
			case "document.copy":
			case "document.change":
			case "document.retire":
				break;
		}
	}

	private async claimId(tx: Executor, idValue: Id<string>, table: TableName): Promise<void> {
		// The reference backend uses INSERT OR IGNORE: the real
		// duplicate/exclusive check is checkGlobalIds above.
		await tx.query("INSERT INTO durable_record_ids (id, record_type) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING", [
			idValue as unknown as number,
			table,
		]);
	}

	private async applyDocumentActions(tx: Executor, actions: ReadonlyMap<DocumentId, DocumentAction>, seq: Seq): Promise<void> {
		for (const [idValue, action] of actions) {
			const idNumber = idValue as unknown as number;
			let content = action.content;
			if (action.copy !== undefined) {
				try {
					const stored = await this.materializeDocument(tx, action.copy.id as unknown as number, action.copy.at);
					if (stored === undefined) throw new Error(`Fork source document ${action.copy.id} cannot be read`);
					const create = action.create!;
					if (
						stored.record.scope.kind !== "conversation" ||
						create.scope.kind !== "conversation" ||
						stored.record.kind !== create.kind ||
						stored.record.key !== create.key ||
						stored.record.history !== create.history ||
						stored.record.fork !== create.fork
					) {
						throw new Error(`Fork source document ${action.copy.id} does not match the copied record`);
					}
					content = { kind: "base", version: stored.version, value: stored.value };
				} catch (error) {
					if (error instanceof StorageRejected) throw error;
					throw new StorageRejected(`Document copy ${idValue} was rejected`, { cause: error });
				}
			}
			let record: DocumentRecord;
			if (action.create !== undefined) {
				record = {
					...action.create,
					createdAt: seq,
					...(action.retire ? { retiredAt: seq } : {}),
				};
				const parts = addressParts(record);
				await this.claimId(tx, idValue, "document");
				await tx.query(
					`INSERT INTO durable_documents
					(id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
					[
						idNumber,
						parts.kind,
						parts.family,
						parts.keyValue,
						parts.scopeKind,
						parts.ownerId,
						seq,
						action.retire ? seq : null,
						encodeJson(record),
					],
				);
			} else {
				const row = (await tx.query<JsonRow>("SELECT record FROM durable_documents WHERE id = $1", [idNumber])).rows[0]!;
				record = parseJson<DocumentRecord>(row.record);
			}

			if (content !== undefined) {
				if (content.kind === "base" && isCurrentOnly(record)) {
					await tx.query("DELETE FROM durable_document_revisions WHERE document_id = $1", [idNumber]);
				}
				const encodedContent = content.kind === "base" ? encodeJson(content.value) : encodeJson(content.ops);
				await tx.query(
					"INSERT INTO durable_document_revisions (document_id, seq, kind, version, content) VALUES ($1, $2, $3, $4, $5)",
					[idNumber, seq, content.kind, content.version, encodedContent],
				);
			}

			if (action.retire) {
				if (action.create === undefined) {
					record = { ...record, retiredAt: seq };
					await tx.query("UPDATE durable_documents SET retired_at = $1, record = $2 WHERE id = $3", [
						seq,
						encodeJson(record),
						idNumber,
					]);
				}
				if (isCurrentOnly(record)) {
					await tx.query("DELETE FROM durable_document_revisions WHERE document_id = $1", [idNumber]);
				}
			}
		}
	}
}

import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

/** Migration history loaded from ./migrations/*.sql at import time (append-only). */
const PG_MIGRATIONS: readonly { readonly version: number; readonly sql: string }[] = (() => {
	const dir = join(dirname(fileURLToPath(import.meta.url)), "..", "migrations");
	return [{ version: 1, sql: readFileSync(join(dir, "001_initial.sql"), "utf-8") }];
})();
