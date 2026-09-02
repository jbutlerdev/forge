// Forge web UI service worker — app-shell cache for installability
// and offline relaunch. Network-first for the API + voice proxies
// (never cache dynamic responses); cache-first for the static
// assets that make up the shell.
const CACHE = "forge-shell-v2";
// Relative entries used by caches.addAll() during install
// (they resolve against the SW's scope).
const SHELL = [
  "./",
  "./index.html",
  "./styles.css",
  "./app.js",
  "./manifest.webmanifest",
  "./icon.svg",
  "./icon-maskable.svg",
  "./icon-192.png",
  "./icon-512.png",
  "./icon-maskable-512.png",
  "./icon-180.png",
];
// URL pathnames (leading slash) corresponding to the SHELL entries,
// for cache matching in the fetch handler. The old check
// `SHELL.includes(url.pathname)` (`"/app.js"` vs `"./app.js"`) and
// `SHELL.includes("./" + url.pathname)` (`".//app.js"`) could never
// match, so the cache-first branch was dead code and the
// "offline relaunch" promise failed (index.html loaded, scripts
// didn't).
const SHELL_PATHNAMES = new Set([
  "/",
  "/index.html",
  "/styles.css",
  "/app.js",
  "/manifest.webmanifest",
  "/icon.svg",
  "/icon-maskable.svg",
  "/icon-192.png",
  "/icon-512.png",
  "/icon-maskable-512.png",
  "/icon-180.png",
]);

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});

self.addEventListener("activate", (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (e) => {
  const req = e.request;
  const url = new URL(req.url);
  // Only handle same-origin GET. POST (API/voice) always goes to
  // the network. Non-GET or cross-origin is passthrough.
  if (req.method !== "GET" || url.origin !== self.location.origin) return;

  // SPA navigations -> serve index.html (cache-first, so the app
  // opens offline even on a deep link).
  if (req.mode === "navigate") {
    e.respondWith(caches.match("./index.html").then((r) => r || fetch(req)));
    return;
  }
  // Static shell assets -> cache-first with background update.
  if (SHELL_PATHNAMES.has(url.pathname)) {
    e.respondWith(
      caches.match(req).then((cached) => {
        const network = fetch(req).then((r) => {
          if (r.ok) caches.open(CACHE).then((c) => c.put(req, r.clone()));
          return r;
        }).catch(() => cached);
        return cached || network;
      })
    );
    return;
  }
  // Everything else (e.g. SSE event streams) -> network only.
});
