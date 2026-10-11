/**
 * H2.4 follow-up — the machine-level model catalog (pi `models.json`).
 *
 * buildModels registers the providers declared in the machine's
 * models.json (default /root/.pi/agent/models.json, overridable via
 * FORGE_HARNESS_MODELS) so profiles referencing non-builtin providers —
 * e.g. the lab LLM proxy's "proxy" provider — resolve at turn start
 * instead of failing with no_model.
 *
 *  (a) a models.json provider registers and its model resolves;
 *  (b) the provider's auth resolves to the declared key ("not-needed"
 *      for keyless endpoints);
 *  (c) providers with an unknown wire API are skipped (warned, not
 *      thrown); malformed files are ignored; a missing file is a no-op;
 *  (d) builtin providers remain available alongside.
 */
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { applyModelsJson, buildModels, MACHINE_MODELS_DEFAULT_PATH } from "../src/main.js";

const COST = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 };

function modelsJsonPath(json: unknown): string {
	const dir = mkdtempSync(join(tmpdir(), "forge-models-json-"));
	const file = join(dir, "models.json");
	writeFileSync(file, JSON.stringify(json, null, 2));
	return file;
}

describe("machine models.json (H2.4 follow-up)", () => {
	it("registers a models.json provider and its model resolves", () => {
		const file = modelsJsonPath({
			providers: {
				proxy: {
					baseUrl: "http://127.0.0.1:9999/v1",
					api: "openai-completions",
					apiKey: "test-key",
					compat: { maxTokensField: "max_tokens" },
					models: [
						{
							id: "llamacpp/laguna-xs-2.1",
							name: "Laguna",
							reasoning: false,
							input: ["text"],
							contextWindow: 4096,
							maxTokens: 512,
							cost: COST,
						},
					],
				},
			},
		});
		const models = buildModels({ FORGE_HARNESS_MODELS: file });
		const model = models.getModel("proxy", "llamacpp/laguna-xs-2.1");
		expect(model).toBeDefined();
		expect(model?.baseUrl).toBe("http://127.0.0.1:9999/v1");
		expect(model?.contextWindow).toBe(4096);
		// provider-level compat applies to models that carry none
		expect((model as { compat?: unknown } | undefined)?.compat).toEqual({ maxTokensField: "max_tokens" });
	});

	it("provider auth resolves to the declared key (or not-needed)", async () => {
		const withKey = modelsJsonPath({
			providers: {
				proxy: {
					baseUrl: "http://127.0.0.1:9999/v1",
					apiKey: "secret-key",
					models: [
						{ id: "m", name: "M", reasoning: false, input: ["text"], contextWindow: 1000, maxTokens: 100, cost: COST },
					],
				},
			},
		});
		const models = buildModels({ FORGE_HARNESS_MODELS: withKey });
		const auth = await models.getAuth("proxy");
		expect(auth?.auth.apiKey).toBe("secret-key");

		const keyless = modelsJsonPath({
			providers: {
				local: {
					baseUrl: "http://127.0.0.1:9998/v1",
					models: [
						{ id: "m", name: "M", reasoning: false, input: ["text"], contextWindow: 1000, maxTokens: 100, cost: COST },
					],
				},
			},
		});
		const models2 = buildModels({ FORGE_HARNESS_MODELS: keyless });
		const auth2 = await models2.getAuth("local");
		expect(auth2?.auth.apiKey).toBe("not-needed");
	});

	it("skips providers with an unknown wire api; ignores malformed and missing files", () => {
		const badApi = modelsJsonPath({
			providers: {
				bad: {
					api: "warpdrive",
					models: [
						{ id: "m", name: "M", reasoning: false, input: ["text"], contextWindow: 1000, maxTokens: 100, cost: COST },
					],
				},
				good: {
					api: "openai-completions",
					baseUrl: "http://127.0.0.1:9997/v1",
					models: [
						{ id: "m", name: "M", reasoning: false, input: ["text"], contextWindow: 1000, maxTokens: 100, cost: COST },
					],
				},
			},
		});
		const models = buildModels({ FORGE_HARNESS_MODELS: badApi });
		expect(models.getModel("bad", "m")).toBeUndefined();
		expect(models.getModel("good", "m")).toBeDefined();

		// malformed file: no throw
		const dir = mkdtempSync(join(tmpdir(), "forge-models-json-"));
		const malformed = join(dir, "models.json");
		writeFileSync(malformed, "{ not json");
		expect(() => buildModels({ FORGE_HARNESS_MODELS: malformed })).not.toThrow();

		// missing file: no-op (the builtin-only machine shape)
		expect(() => buildModels({ FORGE_HARNESS_MODELS: join(dir, "nope.json") })).not.toThrow();

		rmSync(dir, { recursive: true, force: true });
	});

	it("builtin providers remain available alongside models.json providers", () => {
		const models = buildModels({ FORGE_HARNESS_MODELS: "/definitely/not/here.json" });
		expect(models.getProvider("openai")).toBeDefined();
	});

	it("applyModelsJson ignores non-objects and empty provider maps", () => {
		const models = buildModels({});
		expect(() => applyModelsJson(models, null)).not.toThrow();
		expect(() => applyModelsJson(models, { providers: {} })).not.toThrow();
		expect(() => applyModelsJson(models, { providers: { x: { models: [] } } })).not.toThrow();
	});

	it("exposes the default path for the deployment units", () => {
		expect(MACHINE_MODELS_DEFAULT_PATH).toBe("/root/.pi/agent/models.json");
	});
});
