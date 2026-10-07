import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    // The tests hit a real (scratch) Postgres, spawn real harness child
    // processes, and wait on wall-clock timers; running the files in
    // parallel workers starves them (flaky 60s timeouts under
    // contention). One file at a time.
    fileParallelism: false,
    hookTimeout: 60_000,
    testTimeout: 60_000,
  },
});
