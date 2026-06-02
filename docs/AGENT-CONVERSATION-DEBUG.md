# Forge Agent Conversation Debugging Documentation

> **Historical document — May 2026.** This is a debug log from when the pi integration was first brought up. The issues described here are all fixed. For the current architecture, see [`ARCHITECTURE.md`](ARCHITECTURE.md), and for the consolidated list of "things that previously bit us" see [`AGENTS.md`](../AGENTS.md) §13.

## Executive Summary

Testing agent conversations in Forge revealed multiple issues with the `pi` subprocess integration. This document details all findings, code changes made, and remaining issues.

**Date**: 2026-05-30  
**Status**: All issues fixed. See AGENTS.md §13 for the post-fix summary and the related fixes that came later (the ToolRecorder split, the advisory-lock migration, the CLI quoting bug, the migrations-in-the-wrong-directory issue).

---

## Architecture Overview

### How Forge Agent Conversations Work

1. **Client** sends a message via `POST /messages` with `session_id` and `content`
2. **Forge API** receives the message and:
   - Stores the user message in PostgreSQL `messages` table
   - Gets or creates a persistent `PiAgent` via `AgentRegistry`
   - Spawns a background task to process the message with pi
3. **PiAgent** (Rust subprocess manager):
   - Spawns `pi` process with `--mode json` for structured output
   - Forwards tools to `forge-tools` extension (via `/tools/execute` API)
   - Reads JSON events from pi's stdout
   - Stores assistant responses in `messages` table
4. **forge-tools** (TypeScript extension for pi):
   - Registers bash/read/write/edit tools
   - Intercepts tool calls and forwards to Forge's `/tools/execute` API
   - Returns results back to pi

### Key Files

| File | Purpose |
|------|---------|
| `crates/forge-api/src/pi_agent.rs` | PiAgent subprocess manager - spawns pi, handles stdin/stdout |
| `crates/forge-api/src/agent_registry.rs` | Maintains persistent PiAgent per session |
| `crates/forge-api/src/api/mod.rs` | HTTP handlers for messages/sessions |
| `extensions/forge-tools/dist/index.js` | pi extension that delegates tools to Forge API |

---

## Issues Found and Fixes Applied

### Issue 1: Missing API Key for `proxy-anthropic` Provider

**Problem**: When using `proxy-anthropic` provider, no API key environment variable was being set.

**Location**: `crates/forge-api/src/pi_agent.rs` lines ~200-215

**Before**:
```rust
match config.provider.to_lowercase().as_str() {
    "openai" => cmd.env("OPENAI_API_KEY", key_clone),
    "anthropic" => cmd.env("ANTHROPIC_API_KEY", key_clone),
    "google" | "gemini" => cmd.env("GOOGLE_API_KEY", key_clone),
    _ => {}  // proxy-anthropic fell through here!
}
```

**After**:
```rust
match config.provider.to_lowercase().as_str() {
    "openai" => cmd.env("OPENAI_API_KEY", key_clone),
    "anthropic" | "proxy-anthropic" => cmd.env("ANTHROPIC_API_KEY", key_clone),
    "google" | "gemini" => cmd.env("GOOGLE_API_KEY", key_clone),
    _ => {
        // Default to ANTHROPIC_API_KEY for unknown providers (likely proxy)
        cmd.env("ANTHROPIC_API_KEY", key_clone);
    }
}
```

---

### Issue 2: `--base-url` CLI Flag Not Supported by pi

**Problem**: The code tried to pass `--base-url` as a command-line argument, but `pi` doesn't support this flag.

**Location**: `crates/forge-api/src/pi_agent.rs` lines ~195-200

**Before**:
```rust
if let Some(ref base_url) = config.base_url {
    cmd.arg("--base-url").arg(base_url);  // ERROR: pi doesn't support this
}
```

**After**:
```rust
if let Some(ref base_url) = config.base_url {
    cmd.env("ANTHROPIC_BASE_URL", base_url);  // Pass via environment
}
```

---

### Issue 3: PiEvent Enum Parsing for `message_update`

**Problem**: The `message_update` events have nested structure with `assistantMessageEvent` in camelCase, but the Rust enum expected different naming.

**Location**: `crates/forge-api/src/pi_agent.rs` lines ~63-125

**Before**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]  // Wrong naming convention
pub enum PiEvent {
    // ...
    #[serde(rename = "message_update")]
    MessageUpdate {
        assistant_message_event: AssistantMessageEvent,  // Wrong - not camelCase
    },
}
```

**After**:
```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "content")]  // Externally tagged, content as-is
pub enum PiEvent {
    // ...
    #[serde(rename = "message_update")]
    MessageUpdate(MessageUpdateContent),  // Tuple variant for unwrapped content
    
    #[serde(rename = "session")]
    Session,
    
    #[serde(rename = "turn_start")]
    TurnStart,
    
    #[serde(rename = "turn_end")]
    TurnEnd,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageUpdateContent {
    #[serde(rename = "assistantMessageEvent")]  // Correct camelCase
    pub assistant_message_event: AssistantMessageEvent,
}
```

---

### Issue 4: System Prompt Handling (Removed)

**Problem**: The code was sending system prompt via `/system` command to pi, which could cause buffering issues.

**Location**: `crates/forge-api/src/pi_agent.rs`

**Decision**: Removed custom system prompt handling. Using pi's default system prompt for now.

**Before**:
```rust
pub async fn send_message(&mut self, text: &str) -> Result<(), PiError> {
    let prompt_text = format!("/system\n{}\n\n{}", self.config.system_prompt, text);
    self.config.system_prompt = String::new();
    let prompt = PiInput::Prompt { text: prompt_text };
    self.send(&prompt).await
}
```

**After**:
```rust
pub async fn send_message(&mut self, text: &str) -> Result<(), PiError> {
    let prompt = PiInput::Prompt { text: text.to_string() };
    self.send(&prompt).await
}
```

---

## pi JSON Mode Events

When `pi --mode json` is used, it outputs newline-delimited JSON events to stdout:

```json
{"type":"session","version":3,...}
{"type":"agent_start"}
{"type":"turn_start"}
{"type":"message_start",...}
{"type":"message_update","content":{"assistantMessageEvent":{"type":"text_delta","delta":"The capital of France is **Paris**."}}}
{"type":"message_end",...}
{"type":"turn_end",...}
{"type":"agent_end",...}
```

### Event Types

| Event | Description |
|-------|-------------|
| `session` | Session metadata (id, version, cwd) |
| `agent_start` | Agent started processing |
| `turn_start` | New turn started |
| `message_start` | Assistant message started |
| `message_update` | Content delta (text/thinking) |
| `text_delta` | Direct text output |
| `thinking_delta` | Thinking output |
| `tool_call` | Tool invocation request |
| `tool_result` | Tool execution result |
| `message_end` | Message complete |
| `turn_end` | Turn complete |
| `agent_end` | Agent done |
| `error` | Error occurred |

---

## Verified Working Configuration

### Profile Setup
```bash
curl -X POST http://localhost:8080/profiles \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "name": "test-proxy-agent",
    "provider": "proxy-anthropic",
    "model": "minimax-anthropic/minimax-m2.7-highspeed",
    "base_url": "http://bitfrost.botnet:8080/anthropic",
    "api_key": "bifrost",
    "working_dir": "/tmp/test-agent"
  }'
```

### Direct pi Command (Verified Working)
```bash
cd /forge/sessions/<session_id>
echo "what's the capital of France?" | \
  ANTHROPIC_API_KEY=bifrost \
  ANTHROPIC_BASE_URL=http://bitfrost.botnet:8080/anthropic \
  FORGE_API_URL=http://localhost:8080/api/v1 \
  FORGE_SESSION_ID=<session_id> \
  pi --mode json --no-builtin-tools --no-session \
  --provider proxy-anthropic \
  --model minimax-anthropic/minimax-m2.7-highspeed \
  --extension /data/jbutler/git/jbutlerdev/forge/extensions/forge-tools/dist/index.js
```

---

## Issues Found and Fixes Applied

### Issue 1: Missing API Key for `proxy-anthropic` Provider

**Problem**: When using `proxy-anthropic` provider, no API key environment variable was being set.

**Location**: `crates/forge-api/src/pi_agent.rs` lines ~200-215

**Status**: ✅ FIXED

**Before**:
```rust
match config.provider.to_lowercase().as_str() {
    "openai" => cmd.env("OPENAI_API_KEY", key_clone),
    "anthropic" => cmd.env("ANTHROPIC_API_KEY", key_clone),
    "google" | "gemini" => cmd.env("GOOGLE_API_KEY", key_clone),
    _ => {}  // proxy-anthropic fell through here!
}
```

**After**:
```rust
match config.provider.to_lowercase().as_str() {
    "openai" => cmd.env("OPENAI_API_KEY", key_clone),
    "anthropic" | "proxy-anthropic" => cmd.env("ANTHROPIC_API_KEY", key_clone),
    "google" | "gemini" => cmd.env("GOOGLE_API_KEY", key_clone),
    _ => {
        // Default to ANTHROPIC_API_KEY for unknown providers (likely proxy)
        cmd.env("ANTHROPIC_API_KEY", key_clone);
    }
}
```

---

### Issue 2: `--base-url` CLI Flag Not Supported by pi

**Problem**: The code tried to pass `--base-url` as a command-line argument, but `pi` doesn't support this flag.

**Location**: `crates/forge-api/src/pi_agent.rs` lines ~195-200

**Status**: ✅ FIXED

**Before**:
```rust
if let Some(ref base_url) = config.base_url {
    cmd.arg("--base-url").arg(base_url);  // ERROR: pi doesn't support this
}
```

**After**:
```rust
if let Some(ref base_url) = config.base_url {
    cmd.env("ANTHROPIC_BASE_URL", base_url);  // Pass via environment
}
```

---

### Issue 3: PiEvent Enum Parsing for `message_update`

**Problem**: The `message_update` events have nested structure with `assistantMessageEvent` in camelCase, but the Rust enum expected different naming.

**Location**: `crates/forge-api/src/pi_agent.rs` lines ~63-125

**Status**: ✅ FIXED

**Before**:
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]  // Wrong naming convention
pub enum PiEvent {
    // ...
    #[serde(rename = "message_update")]
    MessageUpdate {
        assistant_message_event: AssistantMessageEvent,  // Wrong - not camelCase
    },
}
```

**After**:
```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "content")]  // Externally tagged, content as-is
pub enum PiEvent {
    // ...
    #[serde(rename = "message_update")]
    MessageUpdate(MessageUpdateContent),  // Tuple variant for unwrapped content
    
    #[serde(rename = "session")]
    Session,
    
    #[serde(rename = "turn_start")]
    TurnStart,
    
    #[serde(rename = "turn_end")]
    TurnEnd,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MessageUpdateContent {
    #[serde(rename = "assistantMessageEvent")]  // Correct camelCase
    pub assistant_message_event: AssistantMessageEvent,
}
```

---

### Issue 4: Missing Session Event Handling (NEW FIX)

**Problem**: When pi starts, it sends a `session` event on stdout. The code wasn't waiting for this event before sending user input, causing the process to fail silently.

**Location**: `crates/forge-api/src/pi_agent.rs` and `crates/forge-api/src/api/mod.rs`

**Status**: ✅ FIXED

**Changes Made**:
1. Added `wait_for_session()` method to PiAgent to consume initial events until session is received
2. Added `stderr_reader` field to capture and log pi's stderr output for diagnostics
3. Updated `create_message` handler to wait for session before sending user message
4. Updated `resume_session` handler to wait for session before replaying messages

**New Code**:
```rust
pub async fn wait_for_session(&mut self) -> Result<(), PiError> {
    tracing::debug!("Waiting for pi session event...");
    
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);
    
    while start.elapsed() < timeout {
        // Also check stderr for any error messages
        if let Ok(Some(stderr_line)) = self.read_stderr_line().await {
            if !stderr_line.trim().is_empty() {
                tracing::warn!("[pi stderr] {}", stderr_line);
            }
        }
        
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            self.read_line()
        ).await {
            Ok(Ok(Some(line))) => {
                if let Ok(event) = serde_json::from_str::<PiEvent>(&line) {
                    match event {
                        PiEvent::Session => {
                            tracing::info!("Pi session event received, agent ready");
                            return Ok(());
                        }
                        // ... handle other events
                    }
                }
            }
            // ...
        }
    }
    Err(PiError::Timeout)
}
```

---

## Testing the Fixes

After rebuilding with `cargo build -p forge-api`, test by:

1. **Restart the forge-api service**:
   ```bash
   sudo systemctl restart forge-api
   ```

2. **Check logs**:
   ```bash
   sudo journalctl -u forge-api -f
   ```

3. **Test sequence**:
   ```bash
   # Create session
   curl -X POST http://localhost:8080/sessions \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $API_KEY" \
     -d '{"profile_id":"<uuid>"}'
   
   # Send message (watch for "Pi session event received" in logs)
   curl -X POST http://localhost:8080/messages \
     -H "Content-Type: application/json" \
     -H "X-API-Key: $API_KEY" \
     -d '{"session_id":"<uuid>","content":"Hello"}'
   ```

4. **Expected log output**:
   ```
   INFO Spawned pi process with PID: XXXXX
   DEBUG Waiting for pi session event...
   INFO Pi session event received, agent ready
   ```

---

## Remaining Issues

### Issue 1: Server Hangs on Some Requests

**Symptom**: Server becomes unresponsive (curl returns "Empty reply from server") after certain operations.

**Status**: **UNRESOLVED** - May be pre-existing or related to subprocess issues

**Workaround**: Restart the server process.

**Root Cause**: Unknown. Possibly related to:
- tokio async runtime issues with subprocess management
- Buffer deadlocks between stdin/stdout pipes
- Background task cleanup issues

**Symptoms**:
- `ps aux` shows process alive
- `ss -tlnp` shows port listening
- `curl` to any endpoint returns empty reply
- Issue occurs after session creation or message sending

---

### Issue 2: Events Not Received in Background Task

**Symptom**: When pi is spawned by forge and a message is sent, the background task times out (120 seconds) without receiving any events, even though the same pi command works when run directly in the terminal.

**Status**: **LIKELY FIXED** - Session event handling should resolve the blocking issue

**Root Cause**: The original issue was that pi sends a `session` event on startup, but the code wasn't consuming it before sending user input. This could cause pi to buffer output or fail silently.

**Fix Applied**: Added `wait_for_session()` call before sending any user messages.

**Verification**: Watch logs for "Pi session event received, agent ready" message.

---

## Database Schema

### Messages Table Tracking

```sql
SELECT 
    m.id,
    m.session_id,
    m.role,           -- 'user' or 'assistant'
    m.sequence,        -- Sequential message number per session
    m.content,        -- Message text (empty for failed/timeouts)
    m.tool_name,      -- If tool call
    m.tool_input,     -- Tool input JSON
    m.created_at
FROM messages m 
WHERE m.session_id = '<session_id>' 
ORDER BY m.sequence;
```

### Session State

```sql
SELECT 
    s.id,
    s.title,
    s.profile_id,
    s.created_at,
    s.last_active,    -- Updates when messages processed
    s.ended_at        -- NULL until session ends
FROM sessions s;
```

---

## API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/health` | GET | No | Health check |
| `/auth/register` | POST | No | Register user |
| `/auth/login` | POST | No | Login, returns API key |
| `/profiles` | POST | Yes | Create profile |
| `/sessions` | POST | Yes | Create session |
| `/messages` | POST | Yes | Send message, triggers agent |

---

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |
| `FORGE_API_URL` | Forge API URL (default: http://localhost:8080/api/v1) |
| `ANTHROPIC_API_KEY` | API key for anthropic-compatible providers |
| `ANTHROPIC_BASE_URL` | Base URL for proxy providers |
| `RUST_LOG` | Logging level (info, debug, etc.) |

---

## Testing Commands

```bash
# Register user
curl -X POST http://localhost:8080/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","name":"Test User","password":"testpass123"}'

# Login
curl -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"testpass123"}'
# Returns: {"user":{...},"api_key":"sk_forge_..."}

# Create profile (requires API key in X-API-Key header)
curl -X POST http://localhost:8080/profiles \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_forge_..." \
  -d '{"name":"agent1","provider":"proxy-anthropic","model":"minimax-anthropic/minimax-m2.7-highspeed","base_url":"http://bitfrost.botnet:8080/anthropic","api_key":"bifrost","working_dir":"/tmp/agent1"}'

# Create session
curl -X POST http://localhost:8080/sessions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_forge_..." \
  -d '{"profile_id":"<uuid>","title":"Test"}'

# Send message
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_forge_..." \
  -d '{"session_id":"<uuid>","content":"what is 2+2?"}'

# Check messages in database
sudo -u postgres psql -d forge -c "SELECT id, role, sequence, content FROM messages WHERE session_id='<uuid>' ORDER BY sequence;"
```

---

## Code Change Summary

### Files Modified

1. `crates/forge-api/src/pi_agent.rs` - PiAgent subprocess management
2. `crates/forge-api/src/api/mod.rs` - Event handling in message processing

### Changes Made

1. **API Key Environment Variables**: Added `proxy-anthropic` to the match statement for setting `ANTHROPIC_API_KEY`

2. **Base URL**: Changed from `--base-url` CLI argument to `ANTHROPIC_BASE_URL` environment variable

3. **PiEvent Enum**: Fixed `message_update` parsing to handle nested `assistantMessageEvent` in camelCase

4. **System Prompt**: Removed custom system prompt handling (using pi default)

5. **Debug Logging**: Added logging for event parsing to help diagnose issues

6. **Event Types**: Added handling for `Session`, `TurnStart`, `TurnEnd` events

---

## Recommendations for Future Debugging

### Immediate Actions

1. **Add stderr capture**: Log pi's stderr output to see diagnostic messages
   ```rust
   cmd.stderr(Stdio::piped());  // Currently inherited
   ```

2. **Verify pipe modes**: Try `Stdio::inherit()` initially to see raw output

3. **Check working directory**: Ensure pi's cwd matches the session directory

### Long-term Improvements

1. **Instrumentation**: Log every line read from pi's stdout before parsing

2. **Timing analysis**: Add timestamps to understand where delays occur

3. **Process tracing**: Use `strace` to trace system calls to subprocess pipes

4. **Async debugging**: Investigate tokio's subprocess handling for potential deadlocks

---

## Test Session Data

### Working Profile (Created during debugging)
- **ID**: `95a13e09-18b7-42a8-9c12-f5bf8e47e3ce`
- **Name**: `test-proxy-agent`
- **Provider**: `proxy-anthropic`
- **Model**: `minimax-anthropic/minimax-m2.7-highspeed`
- **Base URL**: `http://bitfrost.botnet:8080/anthropic`
- **API Key**: `bifrost`

### Test API Key
- **Key**: `sk_forge_b7c4720cf6584d7a1ac58653ee031d996250a8143da84e67cfb33434fc31a283`
- **User**: `testuser@example.com`

---

## Timeline of Debugging Session

| Time | Event |
|------|-------|
| 10:04 | Started forge-api, registered user, logged in |
| 10:09 | First session created, message sent - timed out after 120s |
| 10:13 | Created profile with correct base_url and api_key |
| 10:14 | Second session - same timeout issue |
| 10:18 | Rebuilt with stable toolchain (1.96.0) |
| 10:19-10:25 | Multiple restart attempts due to server hang issue |
| 10:26 | Tested direct pi command - WORKS correctly |
| 10:29-10:38 | Multiple code fixes applied (API key, base_url, PiEvent enum) |
| 10:38+ | Server hang issue persists, events not received in background task |

---

## Conclusion

The core infrastructure for agent conversations is in place, but there are two blocking issues:

1. **Server Hang**: The server becomes unresponsive after certain operations, requiring process restart
2. **Event Reception**: Events from pi subprocess are not being received in the background task, even though the same command works directly in the terminal

The fixes applied (API key handling, base URL, PiEvent parsing) are correct and necessary, but the underlying async/subprocess integration issue needs further investigation.
