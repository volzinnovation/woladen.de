window.WOLADEN_OPEN_STATIC_SUMMARY_URL =
  window.WOLADEN_OPEN_STATIC_SUMMARY_URL ||
  "https://live-eu.woladen.de/data/open_static_summary.json";

window.WOLADEN_GEOCODER_API_BASE_URL =
  window.WOLADEN_GEOCODER_API_BASE_URL || "";

window.WOLADEN_OPENING_HOURS_MODULE_URL =
  window.WOLADEN_OPENING_HOURS_MODULE_URL || "";

window.WOLADEN_LIVE_API_BASE_URL =
  window.WOLADEN_LIVE_API_BASE_URL || "https://live-eu.woladen.de";

window.WOLADEN_COMMERCIAL_API_BASE_URL =
  window.WOLADEN_COMMERCIAL_API_BASE_URL || "https://live-eu.woladen.de/commercial";

window.WOLADEN_OCCUPANCY_API_BASE_URL =
  window.WOLADEN_OCCUPANCY_API_BASE_URL || "https://live-eu.woladen.de/commercial/v1/mobile";

// Management analytics use the bounded public QA proxy during acceptance
// testing. The proxy is origin-limited and never exposes the shared server
// token; an approved customer session proxy must replace this public mode
// before customer rollout. Static cache remains an outage fallback.
window.WOLADEN_MANAGEMENT_API_BASE_URL =
  window.WOLADEN_MANAGEMENT_API_BASE_URL ||
  "https://live-eu.woladen.de/v1/management";
