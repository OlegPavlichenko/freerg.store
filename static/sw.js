const CACHE = "freerg-v1";
const STATIC = [
  "/",
  "/static/manifest.webmanifest",
];

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(STATIC)));
});

self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);

  // Только свой домен
  if (url.origin !== location.origin) return;

  // Статика: cache-first
  if (url.pathname.startsWith("/static/")) {
    e.respondWith(
      caches.match(e.request).then((hit) => hit || fetch(e.request))
    );
    return;
  }

  // Страницы: network-first, fallback на cache
  e.respondWith(
    fetch(e.request)
      .then((resp) => {
        const copy = resp.clone();
        caches.open(CACHE).then((c) => c.put(e.request, copy));
        return resp;
      })
      .catch(() => caches.match(e.request).then((hit) => hit || caches.match("/")))
  );
});