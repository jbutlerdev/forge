//! Compile-time-embedded web UI assets + SPA fallback handler.
//!
//! The repo's `web/` directory is the source of truth for the UI,
//! and `cargo run` serves it from disk (live edits, no rebuild) via
//! `ServeDir` in `api::build_app`. But a *deployed* binary at
//! `/opt/forge/forge-api` has no `CARGO_MANIFEST_DIR` (that's a
//! cargo-build-time var) and the host's `/etc/forge/forge.env`
//! doesn't set `FORGE_WEB_DIR`, so `resolve_web_dir()` returns
//! `None` and the disk path is skipped. Without this module, a
//! deployed binary would serve the API but 404 at `GET /`.
//!
//! The fix: embed the UI into the binary with `include_str!` so the
//! deployed binary is self-contained. `build_app` falls back to
//! [`embedded_spa`] (this handler) when no disk dir is resolved.
//! Zero new dependencies, zero external files on the host.
//!
//! The assets are small (~70 KB of text); embedding them costs
//! nothing at runtime. When a disk dir *is* resolved (dev /
//! `FORGE_WEB_DIR` override), `ServeDir` wins and these embeddings
//! are unused — so editing `web/` and reloading in dev still works
//! without a rebuild. Bump the binary when you want the embedded
//! copy updated for a deploy.
//!
//! `include_str!` paths are resolved relative to `CARGO_MANIFEST_DIR`
//! (`crates/forge-api`) so they're stable regardless of where this
//! file sits in the tree. The repo `web/` dir is two levels up:
//! `crates/forge-api/../../web`.

use axum::{
    body::Body,
    extract::Request,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};

// --- Embedded assets (compile-time, via CARGO_MANIFEST_DIR) ---
static INDEX_HTML: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/index.html"));
static STYLES_CSS: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/styles.css"));
static APP_JS: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/app.js"));
static SW_JS: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/sw.js"));
static MANIFEST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/manifest.webmanifest"
));
static ICON_SVG: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/icon.svg"));
static ICON_MASKABLE_SVG: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../web/icon-maskable.svg"
));

/// Content-Type for an asset path, inferred from the extension.
/// Manual (no `mime_guess` dep) since the UI is 7 fixed files.
fn mime_for(path: &str) -> &'static str {
    let lower = path.rsplit('/').next().unwrap_or(path).to_ascii_lowercase();
    if lower.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if lower.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if lower.ends_with(".js") {
        "application/javascript; charset=utf-8"
    } else if lower.ends_with(".webmanifest") {
        "application/manifest+json; charset=utf-8"
    } else if lower.ends_with(".svg") {
        "image/svg+xml"
    } else if lower.ends_with(".json") {
        "application/json; charset=utf-8"
    } else {
        "application/octet-stream"
    }
}

/// Look up an embedded asset by path. Returns `(bytes, mime)`.
fn asset(path: &str) -> Option<(&'static str, &'static str)> {
    // Normalize: strip a leading slash and any trailing slash, and
    // collapse `./` so `/./styles.css` and `styles.css` match.
    let p = path.trim_start_matches('/').trim_end_matches('/');
    let p = p.strip_prefix("./").unwrap_or(p);
    let asset = match p {
        "" | "/" => return Some((INDEX_HTML, mime_for("index.html"))),
        "index.html" => INDEX_HTML,
        "styles.css" => STYLES_CSS,
        "app.js" => APP_JS,
        "sw.js" => SW_JS,
        "manifest.webmanifest" | "manifest.json" => MANIFEST,
        "icon.svg" => ICON_SVG,
        "icon-maskable.svg" => ICON_MASKABLE_SVG,
        _ => return None,
    };
    Some((asset, mime_for(p)))
}

/// SPA fallback handler for the embedded web UI.
///
/// Serves the requested asset if it's one of the embedded files;
/// otherwise serves `index.html` (HTTP 200) so the client-side
/// router can take over deep links like `/chat/<id>`. Mirrors the
/// disk `ServeDir::fallback(ServeFile::new(index))` behavior —
/// including the 200 (not 404) on deep links, which browsers and
/// the service worker rely on.
pub async fn embedded_spa(req: Request) -> Response {
    let path = req.uri().path();
    if let Some((body, mime)) = asset(path) {
        return serve(body, mime);
    }
    // SPA fallback: any unmatched path -> index.html (200), so
    // deep links resolve client-side. Matches the disk path's
    // `.fallback(ServeFile)` semantics (200, not 404).
    if let Some((body, mime)) = asset("index.html") {
        return serve(body, mime);
    }
    // Should be unreachable (index.html is always embedded); if
    // somehow it's gone, fall back to a plain 404 rather than
    // panic.
    (StatusCode::NOT_FOUND, "not found").into_response()
}

fn serve(body: &'static str, mime: &'static str) -> Response {
    let mut resp = Response::new(Body::from(body));
    *resp.status_mut() = StatusCode::OK;
    if let Ok(val) = HeaderValue::from_str(mime) {
        resp.headers_mut().insert(header::CONTENT_TYPE, val);
    }
    // The UI has no sensitive content, but caching the immutable
    // asset files and never caching index.html (so deploys are
    // picked up immediately) is the right policy. Keep it simple
    // here; the service worker handles offline caching.
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embeds_are_nonempty() {
        // Guard against a path typo in include_str! silently
        // embedding an empty file. All 7 assets have known
        // minimum content.
        assert!(
            INDEX_HTML.contains("<!doctype html>"),
            "index.html missing doctype"
        );
        assert!(
            STYLES_CSS.contains("--bg"),
            "styles.css missing theme tokens"
        );
        assert!(APP_JS.contains("forge.apiKey"), "app.js missing the LS key");
        assert!(
            SW_JS.contains("forge-shell"),
            "sw.js missing the cache name"
        );
        assert!(
            MANIFEST.contains("\"Forge\""),
            "manifest missing the app name"
        );
        assert!(ICON_SVG.contains("<svg"), "icon.svg missing svg root");
        assert!(
            ICON_MASKABLE_SVG.contains("<svg"),
            "icon-maskable.svg missing svg root"
        );
    }

    #[test]
    fn asset_lookup_and_mime() {
        assert_eq!(asset("styles.css").unwrap().1, "text/css; charset=utf-8");
        assert_eq!(
            asset("/app.js").unwrap().1,
            "application/javascript; charset=utf-8"
        );
        assert_eq!(
            asset("manifest.webmanifest").unwrap().1,
            "application/manifest+json; charset=utf-8"
        );
        assert_eq!(asset("icon.svg").unwrap().1, "image/svg+xml");
        assert_eq!(asset("does-not-exist.png"), None);
        // root -> index.html
        let (body, mime) = asset("").unwrap();
        assert_eq!(mime, "text/html; charset=utf-8");
        assert!(body.contains("<!doctype html>"));
    }
}
