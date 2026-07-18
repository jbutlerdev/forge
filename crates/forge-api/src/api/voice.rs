//! OpenAI-compatible STT/TTS proxy.
//!
//! The forge API server runs on the host (or a node with LAN
//! access), but the browser driving the web UI usually does not —
//! it can't reach the internal voice container at
//! `10.10.199.51`. These three endpoints bridge that gap: they
//! accept the *same* OpenAI-compatible requests the browser would
//! send to Parakeet/Kokoro directly, and forward them to the voice
//! container, which the forge process *can* reach.
//!
//! ## Endpoints
//!
//! - `POST /v1/audio/transcriptions` — STT. Multipart `file` +
//!   `model`/`response_format` form fields, forwarded verbatim to
//!   Parakeet (`PARAKEET_URL`). Returns Parakeet's JSON
//!   (`{"text": "..."}`) untouched.
//! - `POST /v1/audio/speech` — TTS. JSON
//!   `{model, input, voice, response_format, speed}`, forwarded
//!   to Kokoro (`KOKORO_URL`). Returns Kokoro's audio bytes
//!   (`audio/ogg` by default) with the upstream `Content-Type`.
//! - `GET  /v1/audio/voices` — availability + voice catalog.
//!   Probes both backends and returns `{stt, tts, default_voice,
//!   voices: [...]}`. The web UI calls this on load to decide
//!   whether to show the mic / speaker buttons at all. Always
//!   returns 200 (with `stt:false, tts:false` when unconfigured)
//!   so the UI can degrade gracefully instead of erroring.
//!
//! ## Configuration
//!
//! `PARAKEET_URL` (default `http://10.10.199.51:5093`) and
//! `KOKORO_URL` (default `http://10.10.199.51:8766`). If the env
//! vars are unset *and* the defaults are unreachable, the POST
//! endpoints return 503 with a clear message; `GET /v1/audio/voices`
//! reports `false` for the missing side. Set both to empty string
//! to explicitly disable voice (the POSTs still 503, but the
//! availability probe skips the network round-trip).
//!
//! Auth: these routes sit behind `auth_middleware` like the rest
//! of the `/v1/*` surface, so the browser's forge API key
//! (`X-API-Key` or `Authorization: Bearer`) authorizes them.

use axum::{
    body::Body,
    extract::{Multipart, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use std::time::Duration;

use crate::api::AppState;

/// Hop-by-hop headers that must not be blindly forwarded from an
/// upstream response back to the client (RFC 7230 §6.1). Axum sets
/// its own `Connection`/`Transfer-Encoding` etc.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

/// Read `PARAKEET_URL` / `KOKORO_URL`, falling back to the voice
/// container's documented LAN addresses. Returns `None` only when
/// the env var is set to an empty string (explicit opt-out); the
/// LAN defaults are always returned otherwise so a stock forge
/// install on the lab network works with no config.
fn url_from_env(var: &str, default: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(v) if v.trim().is_empty() => None,
        Ok(v) => Some(v.trim().trim_end_matches('/').to_string()),
        Err(_) => Some(default.to_string()),
    }
}

/// A single shared reqwest client. Connection pooling keeps the
/// per-request latency low (the voice container is on the LAN;
/// a warm socket is ~1ms vs ~5ms for a fresh TCP handshake).
fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .connect_timeout(Duration::from_secs(3))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
}

/// Copy a subset of safe `Content-*` headers from the upstream
/// response onto our response. We deliberately pass through
/// `Content-Type` (audio/ogg, audio/wav, …) and `Content-Length`
/// so the browser plays the right format; everything else
/// (notably `Server`, `Date`, hop-by-hop) is dropped.
fn forward_content_headers(out: &mut axum::http::HeaderMap, src: &reqwest::header::HeaderMap) {
    for (name, value) in src.iter() {
        let name_s = name.as_str();
        if HOP_BY_HOP.contains(&name_s) {
            continue;
        }
        if name_s.eq_ignore_ascii_case("content-type")
            || name_s.eq_ignore_ascii_case("content-length")
            || name_s.eq_ignore_ascii_case("content-disposition")
        {
            // reqwest re-exports `http`'s HeaderName/HeaderValue, so
            // these insert directly into axum's HeaderMap (same type).
            out.insert(name.clone(), value.clone());
        }
    }
}

/// `POST /v1/audio/transcriptions` — proxy to Parakeet STT.
///
/// The browser sends a multipart form (`file` + optional
/// `model`/`response_format`); we re-stream the same multipart to
/// Parakeet. We don't buffer the whole audio in memory — reqwest's
/// `Body` streams from the incoming multipart parts, so a long
/// recording doesn't double its RAM cost in the forge process.
pub async fn transcribe(State(_state): State<AppState>, mut multipart: Multipart) -> Response {
    let Some(base) = url_from_env("PARAKEET_URL", "http://10.10.199.51:5093") else {
        return voice_disabled("speech-to-text");
    };

    // Rebuild the multipart body for the upstream request. We
    // preserve field names and filenames so Parakeet's
    // `file: UploadFile = File(...)` + `model`/`response_format`
    // Form fields bind exactly as the browser sent them.
    let mut form = reqwest::multipart::Form::new();
    let mut had_file = false;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        let filename = field.file_name().map(|s| s.to_string());
        let bytes = match field.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("bad multipart field: {e}"))
                    .into_response();
            }
        };
        if name == "file" {
            had_file = true;
        }
        // reqwest::multipart::Part::bytes wants `Into<Cow<'static,
        // [u8]>>`; axum's `Bytes` (a `bytes::Bytes`) doesn't impl
        // that, so copy into a Vec. Browser recordings are at most
        // a few MB, so this copy is cheap relative to the network
        // round-trip to Parakeet that follows.
        let mut part = reqwest::multipart::Part::bytes(bytes.to_vec());
        if let Some(fn_) = filename {
            part = part.file_name(fn_);
        }
        // Don't forward the part's Content-Type: reqwest's
        // `mime_str` consumes `self` (unrecoverable on a bad MIME
        // string), and Parakeet ignores it anyway — `load_audio`
        // sniffs the bytes via stdlib `wave` then ffmpeg. The
        // browser's `audio/webm;codecs=opus` would be discarded.
        form = form.part(name, part);
    }
    if !had_file {
        return (StatusCode::BAD_REQUEST, "missing 'file' field").into_response();
    }

    let url = format!("{base}/v1/audio/transcriptions");
    let resp = match client().post(&url).multipart(form).send().await {
        Ok(r) => r,
        Err(e) => return upstream_error("Parakeet STT", &url, e),
    };

    relay_response(resp).await
}

/// `POST /v1/audio/speech` — proxy to Kokoro TTS.
///
/// JSON in, audio bytes out. We forward the body verbatim and
/// relay Kokoro's response (audio/ogg by default) with its
/// `Content-Type`.
pub async fn speech(State(_state): State<AppState>, body: axum::body::Bytes) -> Response {
    let Some(base) = url_from_env("KOKORO_URL", "http://10.10.199.51:8766") else {
        return voice_disabled("text-to-speech");
    };

    // Validate it's JSON we can forward (don't fully parse —
    // Kokoro is the authority on its own schema, and we don't want
    // to break when it adds a field). A non-UTF8 / non-JSON body
    // gets a clear 400.
    if body.is_empty() {
        return (StatusCode::BAD_REQUEST, "empty request body").into_response();
    }
    if serde_json::from_slice::<serde_json::Value>(&body).is_err() {
        return (StatusCode::BAD_REQUEST, "request body must be JSON").into_response();
    }

    let url = format!("{base}/v1/audio/speech");
    let resp = match client()
        .post(&url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return upstream_error("Kokoro TTS", &url, e),
    };

    relay_response(resp).await
}

/// Relay an upstream response back to the client, preserving its
/// status, `Content-Type`, and body. Used by both the STT (JSON)
/// and TTS (audio bytes) proxies — they only differ in what the
/// upstream returns, not in how we forward it.
async fn relay_response(resp: reqwest::Response) -> Response {
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::OK);
    let upstream_headers = resp.headers().clone();
    let bytes = match resp.bytes().await {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("failed to read upstream response: {e}"),
            )
                .into_response()
        }
    };
    let mut out = Response::new(Body::from(bytes));
    *out.status_mut() = status;
    forward_content_headers(out.headers_mut(), &upstream_headers);
    out
}

/// `GET /v1/audio/voices` — availability + voice catalog.
///
/// Probes Parakeet (`GET /health`) and Kokoro (`GET /`) with short
/// timeouts, returns which are up, Kokoro's default voice, and a
/// curated list of the Kokoro voices that ship with the stock
/// `voices.bin` (Kokoro doesn't expose a voice-list endpoint; this
/// list is the documented set). Always 200.
pub async fn voices(State(_state): State<AppState>) -> Response {
    let stt_url = url_from_env("PARAKEET_URL", "http://10.10.199.51:5093");
    let tts_url = url_from_env("KOKORO_URL", "http://10.10.199.51:8766");

    let probe = client();

    // STT liveness: Parakeet's /health returns {status:"healthy"}.
    let stt_up = if let Some(ref base) = stt_url {
        probe
            .get(format!("{base}/health"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    } else {
        false
    };

    // TTS liveness + default voice: Kokoro's root returns
    // {service, voices:N, default_voice}. We extract default_voice.
    let (tts_up, default_voice) = if let Some(ref base) = tts_url {
        match probe.get(format!("{base}/")).send().await {
            Ok(r) if r.status().is_success() => {
                let v: Option<serde_json::Value> = r.json().await.ok();
                let dv = v
                    .as_ref()
                    .and_then(|j| j.get("default_voice"))
                    .and_then(|s| s.as_str())
                    .unwrap_or("af_heart")
                    .to_string();
                (true, dv)
            }
            _ => (false, "af_heart".to_string()),
        }
    } else {
        (false, "af_heart".to_string())
    };

    Json(serde_json::json!({
        "stt": stt_up,
        "tts": tts_up,
        "default_voice": default_voice,
        // The voices shipped in Kokoro's stock voices.bin. The web
        // UI offers these in a <select>. Keep in sync with the
        // voice container's voices asset; af_heart is the flagship
        // English female voice (Kokoro's default).
        "voices": KOKORO_VOICES,
    }))
    .into_response()
}

/// Curated Kokoro voice list (matches the stock `voices.bin`).
/// Prefixes: `af`/`am` = American English female/male,
/// `bf`/`bm` = British English. The web UI defaults to
/// `default_voice` from the live `/` probe.
const KOKORO_VOICES: &[&str] = &[
    "af_heart",
    "af_bella",
    "af_nova",
    "af_sarah",
    "af_river",
    "af_sky",
    "am_adam",
    "am_michael",
    "am_eric",
    "am_puck",
    "am_liam",
    "bf_emma",
    "bf_isabella",
    "bm_george",
    "bm_lewis",
];

/// 503 response for when a voice backend is explicitly disabled
/// (env var set to empty). Distinct from "up unreachable" (502)
/// so the UI can tell "not configured" from "configured but down".
fn voice_disabled(side: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": format!("{side} is disabled on this forge instance"),
            "hint": "set PARAKEET_URL / KOKORO_URL (or clear them to use the LAN defaults)",
        })),
    )
        .into_response()
}

/// 502 response for an upstream connect/read failure. Logs the
/// target URL so an operator can see which backend is down from
/// the journal without digging.
fn upstream_error(label: &str, url: &str, e: reqwest::Error) -> Response {
    tracing::warn!(label, url, error = %e, "voice upstream unreachable");
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({
            "error": format!("{label} backend unreachable"),
            "upstream": url,
            "detail": e.to_string(),
        })),
    )
        .into_response()
}
