//! OpenAI-compatible STT/TTS proxy.
//!
//! The forge API server runs on the host, but the browser driving
//! the web UI may not be able to reach your voice backend (Parakeet
//! STT / Kokoro TTS) directly. These three endpoints bridge that gap: they
//! accept the *same* OpenAI-compatible requests the browser would
//! send to Parakeet/Kokoro directly, and forward them to the voice
//! backend, which the forge process *can* reach.
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
//! `PARAKEET_URL` and `KOKORO_URL` (no defaults; voice is disabled
//! unless you set them to your STT/TTS host). When unset (or set to
//! an empty string), the POST endpoints return 503 with a clear
//! message; `GET /v1/audio/voices` reports `false` for the missing
//! side and skips the network round-trip.
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

/// Read `PARAKEET_URL` / `KOKORO_URL`. Returns `None` when the
/// resolved value is empty — i.e. when the var is unset (there is no
/// default; voice is opt-in) or explicitly set to an empty string.
/// The caller turns that into a 503 "voice disabled" response, so a
/// stock install without voice backends degrades gracefully.
fn url_from_env(var: &str, default: &str) -> Option<String> {
    let v = std::env::var(var).unwrap_or_else(|_| default.to_string());
    let trimmed = v.trim().trim_end_matches('/');
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// A single shared reqwest client, built once and reused for the
/// life of the process. Connection pooling keeps the per-request
/// latency low (the voice container is on the LAN; a warm socket is
/// ~1ms vs ~5ms for a fresh TCP handshake).
static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

fn client() -> &'static reqwest::Client {
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

/// Cap on the total inbound multipart body (all parts combined).
/// Exceeding it is a 413 — audio uploads are a few MB at most.
const MAX_INBOUND_BYTES: u64 = 25 * 1024 * 1024;

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
/// `model`/`response_format`); we rebuild the same multipart for
/// Parakeet. Only the known fields (`file`, `model`,
/// `response_format`) are forwarded upstream — anything else
/// (e.g. `language`, `temperature`) is dropped so extra browser
/// fields can't leak into the upstream request. Inbound size is
/// capped at `MAX_INBOUND_BYTES` (413 when exceeded); the rebuild
/// still fully buffers each part in memory, but that's bounded and
/// cheap for browser-sized recordings.
pub async fn transcribe(State(_state): State<AppState>, mut multipart: Multipart) -> Response {
    // Rebuild the multipart body for the upstream request. We
    // preserve field names and filenames so Parakeet's
    // `file: UploadFile = File(...)` + `model`/`response_format`
    // Form fields bind exactly as the browser sent them.
    let mut form = reqwest::multipart::Form::new();
    let mut had_file = false;
    let mut total_bytes: u64 = 0;
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        // Forward only the fields Parakeet binds; drop the rest.
        if !matches!(name.as_str(), "file" | "model" | "response_format") {
            continue;
        }
        let filename = field.file_name().map(|s| s.to_string());
        let bytes = match field.bytes().await {
            Ok(b) => b,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, format!("bad multipart field: {e}"))
                    .into_response();
            }
        };
        // Cap the total inbound body across all parts.
        match total_bytes.checked_add(bytes.len() as u64) {
            Some(sum) if sum > MAX_INBOUND_BYTES => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("request body exceeds {} bytes", MAX_INBOUND_BYTES),
                )
                    .into_response();
            }
            Some(sum) => total_bytes = sum,
            None => {
                return (StatusCode::PAYLOAD_TOO_LARGE, "request body too large").into_response()
            }
        }
        if name == "file" {
            had_file = true;
        }
        // reqwest::multipart::Part::bytes wants `Into<Cow<'static,
        // [u8]>>`; axum's `Bytes` (a `bytes::Bytes`) doesn't impl
        // that, so copy into a Vec.
        let mut part = reqwest::multipart::Part::bytes(bytes.to_vec());
        if let Some(fn_) = filename {
            part = part.file_name(fn_);
        }
        // Don't forward the part's Content-Type: Parakeet sniffs
        // the audio bytes itself (stdlib `wave` then ffmpeg).
        form = form.part(name, part);
    }
    if !had_file {
        return (StatusCode::BAD_REQUEST, "missing 'file' field").into_response();
    }

    // Validate input (400) before reporting service availability
    // (503), so a malformed request is never masked by a disabled
    // backend.
    let Some(base) = url_from_env("PARAKEET_URL", "") else {
        return voice_disabled("speech-to-text");
    };

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

    // Validate input (400) before reporting service availability
    // (503), so a malformed request is never masked by a disabled
    // backend.
    let Some(base) = url_from_env("KOKORO_URL", "") else {
        return voice_disabled("text-to-speech");
    };

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
    let stt_url = url_from_env("PARAKEET_URL", "");
    let tts_url = url_from_env("KOKORO_URL", "");

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Distinct env-var name per test so the tests can run in
    /// parallel without clobbering each other's process-global env
    /// state.
    const VAR_UNSET: &str = "FORGE_TEST_VOICE_URL_UNSET";
    const VAR_SET: &str = "FORGE_TEST_VOICE_URL_SET";
    const VAR_EMPTY: &str = "FORGE_TEST_VOICE_URL_EMPTY";
    const VAR_DEF: &str = "FORGE_TEST_VOICE_URL_DEF";

    #[test]
    fn url_from_env_unset_with_empty_default_is_none() {
        std::env::remove_var(VAR_UNSET);
        assert_eq!(url_from_env(VAR_UNSET, ""), None);
    }

    #[test]
    fn url_from_env_set_value_is_trimmed_of_trailing_slash() {
        std::env::set_var(VAR_SET, "http://voice.lan:8081/");
        assert_eq!(
            url_from_env(VAR_SET, ""),
            Some("http://voice.lan:8081".to_string())
        );
    }

    #[test]
    fn url_from_env_explicitly_empty_is_none_even_with_default() {
        // A var explicitly set to "" disables the backend; the
        // default must NOT fill in when the var is present but empty.
        std::env::set_var(VAR_EMPTY, "");
        assert_eq!(url_from_env(VAR_EMPTY, "http://default.lan"), None);
    }

    #[test]
    fn url_from_env_unset_falls_back_to_default() {
        std::env::remove_var(VAR_DEF);
        assert_eq!(
            url_from_env(VAR_DEF, "http://fallback.lan"),
            Some("http://fallback.lan".to_string())
        );
    }

    /// Hop-by-hop headers (RFC 7230 §6.1) and other upstream
    /// metadata must not leak into the relayed response; only
    /// Content-* headers are forwarded.
    #[test]
    fn forward_content_headers_filters_hop_by_hop() {
        let mut src = reqwest::header::HeaderMap::new();
        src.insert(reqwest::header::CONNECTION, "keep-alive".parse().unwrap());
        src.insert(
            reqwest::header::TRANSFER_ENCODING,
            "chunked".parse().unwrap(),
        );
        src.insert(reqwest::header::SERVER, "kokoro/1.0".parse().unwrap());
        src.insert(
            reqwest::header::DATE,
            "Mon, 01 Jan 2027 00:00:00 GMT".parse().unwrap(),
        );
        src.insert(
            reqwest::header::HeaderName::from_static("content-type"),
            "audio/ogg".parse().unwrap(),
        );
        src.insert(
            reqwest::header::HeaderName::from_static("content-length"),
            "123".parse().unwrap(),
        );
        src.insert(
            reqwest::header::HeaderName::from_static("content-disposition"),
            "inline".parse().unwrap(),
        );

        let mut out = axum::http::HeaderMap::new();
        forward_content_headers(&mut out, &src);

        assert_eq!(
            out.get("content-type").and_then(|v| v.to_str().ok()),
            Some("audio/ogg")
        );
        assert_eq!(
            out.get("content-length").and_then(|v| v.to_str().ok()),
            Some("123")
        );
        assert_eq!(
            out.get("content-disposition").and_then(|v| v.to_str().ok()),
            Some("inline")
        );
        assert!(out.get("connection").is_none());
        assert!(out.get("transfer-encoding").is_none());
        assert!(out.get("server").is_none());
        assert!(out.get("date").is_none());
    }

    #[test]
    fn forward_content_headers_empty_src_is_noop() {
        let src = reqwest::header::HeaderMap::new();
        let mut out = axum::http::HeaderMap::new();
        forward_content_headers(&mut out, &src);
        assert!(out.is_empty());
    }
}
