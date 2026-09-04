// Unit tests for the forge-tools SSE parsing helpers.
//
// The extension's `parseSSEStream` is coupled to the fetch response
// reader and process.stdout writes, so we unit-test the pure helpers
// it is built from: `splitSSEEvents` (event-boundary detection) and
// `parseSSEEventBlock` (event:/data: line parsing). These are the
// pieces that previously had the "every event hit the fallback
// branch" bug (tool_start/tool_end silently dropped), so they get
// direct coverage here.
//
// Run with: `npm test` from `extensions/forge-tools/` (builds the
// extension first, then runs this file with the built-in
// `node --test` runner — no external test deps).

import { test } from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
// The built CommonJS entry point: the extension factory with the
// SSE helpers attached via `module.exports = Object.assign(...)`.
const { splitSSEEvents, parseSSEEventBlock } = require("../dist/index.js");

// ------------------------------------------------------------------
// splitSSEEvents: event-boundary detection
// ------------------------------------------------------------------

test("splitSSEEvents: two complete events, no tail", () => {
    const s =
        "event: stdout\ndata: {\"chunk\":\"hi\"}\n\n" +
        "event: tool_end\ndata: {\"success\":true}\n\n";
    const { events, tail } = splitSSEEvents(s);
    assert.equal(events.length, 2);
    assert.equal(tail, "");
    assert.match(events[0], /^event: stdout/);
    assert.match(events[1], /^event: tool_end/);
});

test("splitSSEEvents: incomplete final event stays in the tail", () => {
    // A chunk boundary lands mid-event: no trailing blank line, so
    // the second event is not complete yet.
    const s =
        "event: stdout\ndata: {\"chunk\":\"hi\"}\n\n" +
        'event: stderr\ndata: {"chunk":"wo';
    const { events, tail } = splitSSEEvents(s);
    assert.equal(events.length, 1);
    assert.equal(tail, 'event: stderr\ndata: {"chunk":"wo');
});

test("splitSSEEvents: empty buffer produces no events and empty tail", () => {
    const { events, tail } = splitSSEEvents("");
    assert.deepEqual(events, []);
    assert.equal(tail, "");
});

test("splitSSEEvents: single event with no trailing blank line is a tail", () => {
    // Until the server sends the terminating blank line, an event
    // cannot be dispatched.
    const s = "event: tool_end\ndata: {\"success\":false}";
    const { events, tail } = splitSSEEvents(s);
    assert.equal(events.length, 0);
    assert.equal(tail, s);
});

test("splitSSEEvents: consecutive blank lines yield empty blocks", () => {
    // The empty blocks are harmless: parseSSEEventBlock returns null
    // for them (no data line).
    const s =
        "event: stdout\ndata: {}\n\n\n\nevent: done\ndata: {}\n\n";
    const { events, tail } = splitSSEEvents(s);
    assert.equal(tail, "");
    const real = events.filter((e) => e.trim().length > 0);
    assert.equal(real.length, 2);
});

// ------------------------------------------------------------------
// parseSSEEventBlock: event:/data: line parsing
// ------------------------------------------------------------------

test("parseSSEEventBlock: event + data lines", () => {
    const block =
        'event: stdout\ndata: {"tool_call_id":"abc","chunk":"hello"}';
    const p = parseSSEEventBlock(block);
    assert.equal(p.eventName, "stdout");
    assert.equal(JSON.parse(p.data).chunk, "hello");
    assert.equal(JSON.parse(p.data).tool_call_id, "abc");
});

test("parseSSEEventBlock: data without an event line has undefined name", () => {
    const p = parseSSEEventBlock('data: {"ok":true}');
    assert.notEqual(p, null);
    assert.equal(p.eventName, undefined);
    assert.deepEqual(JSON.parse(p.data), { ok: true });
});

test("parseSSEEventBlock: comment-only or empty block is null", () => {
    assert.equal(parseSSEEventBlock(": keep-alive comment"), null);
    assert.equal(parseSSEEventBlock(""), null);
});

test("parseSSEEventBlock: first event:/data: wins; comments ignored", () => {
    const block = ": c1\nevent: stdout\nevent: later-ignored\ndata: 1\ndata: 2\n: c2";
    const p = parseSSEEventBlock(block);
    assert.equal(p.eventName, "stdout");
    assert.equal(p.data, "1");
});

test("parseSSEEventBlock: unknown lines (id/retry) are ignored", () => {
    const block =
        'id: 42\nretry: 5000\nevent: tool_end\ndata: {"success":true,"duration_ms":7}';
    const p = parseSSEEventBlock(block);
    assert.equal(p.eventName, "tool_end");
    assert.deepEqual(JSON.parse(p.data), { success: true, duration_ms: 7 });
});

// ------------------------------------------------------------------
// Simulated stream assembly (mirrors parseSSEStream's read loop)
// ------------------------------------------------------------------

test("assembling chunks across reads recovers every event in order", () => {
    // A realistic forge tool stream, deliberately split at awkward
    // byte offsets so the tail-carry logic has to work.
    const full =
        "event: tool_start\ndata: {\"tool\":\"bash\"}\n\n" +
        'event: stdout\ndata: {"chunk":"line1\\n"}\n\n' +
        'event: stdout\ndata: {"chunk":"line2\\n"}\n\n' +
        'event: stderr\ndata: {"chunk":"warn\\n"}\n\n' +
        'event: tool_end\ndata: {"success":true,"duration_ms":42}\n\n' +
        "event: done\ndata: {}\n\n";
    const chunks = [full.slice(0, 30), full.slice(30, 80), full.slice(80)];

    let buffer = "";
    const seen = [];
    for (const chunk of chunks) {
        const { events, tail } = splitSSEEvents((buffer += chunk));
        buffer = tail;
        for (const raw of events) {
            const p = parseSSEEventBlock(raw);
            if (p !== null) seen.push([p.eventName ?? null, p.data]);
        }
    }

    assert.equal(buffer, "", "no unconsumed tail after the final event");
    assert.equal(seen.length, 6);
    assert.deepEqual(seen[0], ["tool_start", '{"tool":"bash"}']);
    assert.deepEqual(seen[3], ["stderr", '{"chunk":"warn\\n"}']);
    assert.deepEqual(seen[5], ["done", "{}"]);
    const toolEnd = seen[4];
    assert.equal(toolEnd[0], "tool_end");
    assert.deepEqual(JSON.parse(toolEnd[1]), { success: true, duration_ms: 42 });
});
