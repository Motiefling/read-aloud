/**
 * Service Worker for offline caching of audio files and app shell.
 */

const CACHE_NAME = "audiobook-cache-v4";
const APP_SHELL = ["/", "/index.html", "/app.js", "/styles.css", "/manifest.json"];

// ===================== Install =====================

self.addEventListener("install", (event) => {
    event.waitUntil(
        caches.open(CACHE_NAME).then((cache) => {
            return cache.addAll(APP_SHELL);
        })
    );
    self.skipWaiting();
});

// ===================== Activate =====================

self.addEventListener("activate", (event) => {
    event.waitUntil(
        caches.keys().then((keys) => {
            return Promise.all(
                keys
                    .filter((key) => key !== CACHE_NAME)
                    .map((key) => caches.delete(key))
            );
        })
    );
    self.clients.claim();
});

// ===================== Fetch =====================

self.addEventListener("fetch", (event) => {
    const url = new URL(event.request.url);

    // Cache audio files on demand (when played or explicitly downloaded)
    if (url.pathname.includes("/audio")) {
        event.respondWith(
            caches.match(event.request).then((cached) => {
                if (cached) return cached;
                return fetch(event.request).then((response) => {
                    // Only cache successful responses
                    if (response.ok) {
                        const clone = response.clone();
                        caches.open(CACHE_NAME).then((cache) => {
                            cache.put(event.request, clone);
                        });
                    }
                    return response;
                });
            })
        );
        return;
    }

    // App shell: network-first with cache fallback
    event.respondWith(
        fetch(event.request)
            .then((response) => {
                if (response.ok) {
                    const clone = response.clone();
                    caches.open(CACHE_NAME).then((cache) => {
                        cache.put(event.request, clone);
                    });
                }
                return response;
            })
            .catch(() => {
                return caches.match(event.request);
            })
    );
});

// ===================== Messages =====================
// Handle messages from the app (e.g., explicit download requests)

self.addEventListener("message", (event) => {
    if (event.data && event.data.type === "CACHE_AUDIO") {
        // TODO: Pre-cache a list of audio URLs for offline listening
        // const urls = event.data.urls;
        // caches.open(CACHE_NAME).then(cache => cache.addAll(urls));
    }
});
