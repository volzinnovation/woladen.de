window.WOLADEN_OPEN_STATIC_API_BASE_URL =
  window.WOLADEN_OPEN_STATIC_API_BASE_URL || "./api/open-static";

window.WOLADEN_OPEN_STATIC_FEATURE_LIMIT =
  window.WOLADEN_OPEN_STATIC_FEATURE_LIMIT || "";

window.WOLADEN_GEOCODER_API_BASE_URL =
  window.WOLADEN_GEOCODER_API_BASE_URL || "";

window.WOLADEN_OPENING_HOURS_MODULE_URL =
  window.WOLADEN_OPENING_HOURS_MODULE_URL || "";

window.WOLADEN_LIVE_API_BASE_URL =
  window.WOLADEN_LIVE_API_BASE_URL || "./api/live";

window.WOLADEN_COMMERCIAL_API_BASE_URL =
  window.WOLADEN_COMMERCIAL_API_BASE_URL || "./api/commercial";

window.WOLADEN_OCCUPANCY_API_BASE_URL =
  window.WOLADEN_OCCUPANCY_API_BASE_URL || "./api/commercial/v1/mobile";

// Management analytics use the bounded public QA proxy during acceptance
// testing. The proxy is origin-limited and never exposes the shared server
// token; an approved customer session proxy must replace this public mode
// before customer rollout. Static cache remains an outage fallback.
window.WOLADEN_MANAGEMENT_API_BASE_URL =
  window.WOLADEN_MANAGEMENT_API_BASE_URL ||
  "https://live-eu.woladen.de/v1/management";
window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED =
  window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED !== false;
