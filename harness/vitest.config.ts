import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    // The tests hit a real (scratch) Postgres and wait on wall-clock timers.
    hookTimeout: 60_000,
    testTimeout: 60_000,
  },
});
