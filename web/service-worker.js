const CACHE_NAME = "woladen-shell-20260701";

const APP_SHELL = [
  "/",
  "/index.html",
  "/styles.css?v=20260701-charge-plan1",
  "/app-install-promo.js?v=20260620-eu-i18n9",
  "/pwa-register.js?v=20260701-pwa1",
  "/app.js?v=20260701-charge-plan1",
  "/filtering.mjs?v=20260626-routing-web1",
  "/filter-settings.mjs?v=20260626-routing-web1",
  "/favorite-metadata.mjs?v=20260626-favorites-web1",
  "/opening-hours.mjs?v=20260620-i18n",
  "/location.mjs?v=20260630-map-location1",
  "/live-api.mjs?v=20260630-live-eu1",
  "/geocoding.mjs?v=20260618-commercial-merge",
  "/routing.mjs?v=20260626-routing-web1",
  "/charge-plan.mjs?v=20260701-charge-plan1",
  "/open-static-ui.mjs?v=20260618-commercial-merge",
  "/map-keyboard.mjs?v=20260618-keyboard-restore",
  "/rating.mjs",
  "/note.mjs",
  "/i18n.mjs?v=20260701-charge-plan1",
  "/styles.css",
  "/app.js",
  "/config.js",
  "/i18n.mjs",
  "/charge-plan.mjs",
  "/favicon-32x32.png",
  "/favicon-512.png",
  "/img/touch-icon.png",
  "/manifest.webmanifest",
];

const DATA_PATH_PREFIXES = [
  "/api/",
  "/data/",
  "/live/",
  "/daily-occupancy/",
];

const NEVER_CACHE_HOSTS = new Set([
  "live.woladen.de",
  "live-eu.woladen.de",
]);

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then((cache) => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(
        keys
          .filter((key) => key !== CACHE_NAME)
          .map((key) => caches.delete(key)),
      ))
      .then(() => self.clients.claim()),
  );
});

function shouldBypassCache(request) {
  if (request.method !== "GET") {
    return true;
  }
  const url = new URL(request.url);
  if (NEVER_CACHE_HOSTS.has(url.hostname)) {
    return true;
  }
  if (url.origin !== self.location.origin) {
    return true;
  }
  return DATA_PATH_PREFIXES.some((prefix) => url.pathname.startsWith(prefix));
}

function isStaticAssetRequest(request) {
  if (shouldBypassCache(request)) {
    return false;
  }
  return [
    "font",
    "image",
    "manifest",
    "script",
    "style",
  ].includes(request.destination);
}

function isCacheableResponse(response) {
  return response && response.ok && response.type === "basic";
}

async function networkFirst(request) {
  const cache = await caches.open(CACHE_NAME);
  try {
    const response = await fetch(request);
    if (isCacheableResponse(response)) {
      await cache.put(request, response.clone());
    }
    return response;
  } catch (error) {
    const cached = await cache.match(request);
    return cached || cache.match("/index.html");
  }
}

async function staleWhileRevalidate(request) {
  const cache = await caches.open(CACHE_NAME);
  const cached = await cache.match(request);
  const network = fetch(request)
    .then((response) => {
      if (isCacheableResponse(response)) {
        cache.put(request, response.clone());
      }
      return response;
    })
    .catch(() => cached);

  return cached || network;
}

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (shouldBypassCache(request)) {
    return;
  }
  if (request.mode === "navigate") {
    event.respondWith(networkFirst(request));
    return;
  }
  if (isStaticAssetRequest(request)) {
    event.respondWith(staleWhileRevalidate(request));
  }
});
