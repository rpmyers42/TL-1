/* ─────────────────────────────────────────────────────────────────────────
 * TL-1 receiver, service worker
 *
 * One job: make the page open with no signal. There is no network traffic
 * in this app beyond loading itself, so there is nothing clever to do. The
 * strategy is network-first with a cache fallback, which means:
 *
 *   - at home on wifi you always get the newest version you pushed
 *   - at the velodrome with no bars it loads instantly from cache
 *
 * Cache-first would be faster but would serve a stale page after you edit
 * the HTML, and chasing a phantom bug that was really a cached old build
 * is a bad afternoon.
 *
 * Bump CACHE when you change the file list. Old caches are deleted on
 * activate, so nothing accumulates.
 * ───────────────────────────────────────────────────────────────────────── */

const CACHE = "tl1-v5";

const ASSETS = [
  "./",
  "./index.html",
  "./manifest.webmanifest",
  "./icon-180.png",
  "./icon-192.png",
  "./icon-512.png",
  "./icon.svg",
];

self.addEventListener("install", ev=>{
  ev.waitUntil(
    caches.open(CACHE)
      // addAll rejects the whole batch if any single item 404s, which would
      // leave the worker uninstalled and the app with no offline support at
      // all. Individual puts let a missing icon pass quietly.
      .then(c=>Promise.all(ASSETS.map(u=>c.add(u).catch(()=>{}))))
      .then(()=>self.skipWaiting())
  );
});

self.addEventListener("activate", ev=>{
  ev.waitUntil(
    caches.keys()
      .then(keys=>Promise.all(keys.filter(k=>k !== CACHE).map(k=>caches.delete(k))))
      .then(()=>self.clients.claim())
  );
});

self.addEventListener("fetch", ev=>{
  const req = ev.request;
  if(req.method !== "GET") return;

  // Same origin only. Nothing else should be requested, but if a future
  // version does reach out, it should not be silently cached.
  if(new URL(req.url).origin !== self.location.origin) return;

  ev.respondWith(
    fetch(req)
      .then(res=>{
        // Opaque or error responses are not worth storing.
        if(res && res.ok && res.type === "basic"){
          const copy = res.clone();
          caches.open(CACHE).then(c=>c.put(req, copy)).catch(()=>{});
        }
        return res;
      })
      .catch(async ()=>{
        const hit = await caches.match(req, { ignoreSearch:true });
        if(hit) return hit;
        // A navigation that misses the cache still has to render something,
        // so fall back to the app shell rather than the browser error page.
        if(req.mode === "navigate"){
          const shell = await caches.match("./index.html");
          if(shell) return shell;
        }
        return new Response("Offline and not cached yet.", {
          status:503, headers:{ "Content-Type":"text/plain" },
        });
      })
  );
});
