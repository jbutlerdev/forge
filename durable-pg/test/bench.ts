/**
 * Storage benchmark harness (uses the vendor's benchmark definitions).
 * Run: DURABLE_PG_URL=... npm run bench
 * Optionally compare against the same suite over the vendor's SQLite storage
 * with DURABLE_PG_BENCH_SQLITE=1.
 */
import pg from "pg";
import {
	STORAGE_READ_BENCHMARKS,
	STORAGE_WRITE_BENCHMARKS,
	TIMING_SCALE,
	seedStorageBenchmark,
	seedStorageWriteBenchmark,
	storageBenchmarkPrimaryRecordCount,
} from "@earendil-works/pi-durable/testing";
import type { Storage } from "@earendil-works/pi-durable";
import { PgStorage } from "../src/storage-pg.ts";

const CONNECTION_STRING =
	process.env.DURABLE_PG_URL ?? "postgres://postgres:forge@127.0.0.1:5432/postgres";
const WITH_SQLITE = process.env.DURABLE_PG_BENCH_SQLITE === "1";

const PACKAGE_VENDOR = "/home/jbutler/src/forge/vendor/pi-durable";

const context = { get: () => undefined } as never;

async function withPg(fn: (storage: Storage) => Promise<void>): Promise<void> {
	const { Pool } = pg;
	const schema = `durable_bench_${Date.now().toString(36)}`;
	const pool = new Pool({ connectionString: CONNECTION_STRING, max: 4 });
	await pool.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`);
	const storage = await PgStorage.open({ pool, schema });
	try {
		await fn(storage);
	} finally {
		await storage.close(context).catch(() => {});
		await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
		await pool.end();
	}
}

async function withSqlite(fn: (storage: Storage) => Promise<void>): Promise<void> {
	const { mkdtemp, rm } = await import("node:fs/promises");
	const { tmpdir } = await import("node:os");
	const { join } = await import("node:path");
	const { openNodeSqliteStorage } = await import(
		join(PACKAGE_VENDOR, "packages/durable/src/storage/sqlite/node.ts")
	);
	const dir = await mkdtemp(join(tmpdir(), "durable-bench-sqlite-"));
	const storage = await openNodeSqliteStorage(join(dir, "bench.sqlite"));
	try {
		await fn(storage as Storage);
	} finally {
		await storage.close(context).catch(() => {});
		await rm(dir, { recursive: true, force: true });
	}
}

type Sample = { readonly backend: string; readonly name: string; readonly perOp: number };

async function runReads(backend: string, open: (fn: (s: Storage) => Promise<void>) => Promise<void>): Promise<Sample[]> {
	const samples: Sample[] = [];
	await open(async (storage) => {
		const dataset = await seedStorageBenchmark(storage, TIMING_SCALE);
		// Warm then time a loop — single runs are sub-millisecond.
		for (const benchmark of STORAGE_READ_BENCHMARKS) {
			await benchmark.run(storage, dataset);
			const iterations = 200;
			const t0 = performance.now();
			for (let i = 0; i < iterations; i++) await benchmark.run(storage, dataset);
			samples.push({ backend, name: benchmark.name, perOp: (performance.now() - t0) / iterations });
		}
	});
	return samples;
}

async function runWrites(backend: string, open: (fn: (s: Storage) => Promise<void>) => Promise<void>): Promise<Sample[]> {
	const samples: Sample[] = [];
	await open(async (storage) => {
		await seedStorageWriteBenchmark(storage);
		for (const benchmark of STORAGE_WRITE_BENCHMARKS) {
			const t0 = performance.now();
			await benchmark.run(storage);
			samples.push({ backend, name: benchmark.name, perOp: performance.now() - t0 });
		}
	});
	return samples;
}

const reads = await runReads("postgres", withPg);
const writes = await runWrites("postgres", withPg);
const sqlite = WITH_SQLITE
	? [
			...(await runReads("sqlite", withSqlite)),
			...(await runWrites("sqlite", withSqlite)),
		]
	: [];

console.log(`records per dataset: ${storageBenchmarkPrimaryRecordCount(TIMING_SCALE)}`);
const columns = ["backend", "name", "ms"];
console.log(columns.join("\t"));
for (const sample of [...reads, ...writes, ...sqlite]) {
	console.log([sample.backend, sample.name, sample.perOp.toFixed(2)].join("\t"));
}
