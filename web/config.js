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

// Management analytics stay private. Point this at an approved same-origin
// entitlement/session proxy ending in /v1/management; never embed a shared API
// token in browser configuration. Empty means use the rebuildable static cache.
window.WOLADEN_MANAGEMENT_API_BASE_URL =
  window.WOLADEN_MANAGEMENT_API_BASE_URL || "";
window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED =
  window.WOLADEN_MANAGEMENT_STATIC_FALLBACK_ENABLED !== false;
