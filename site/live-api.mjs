const LIVE_LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "0.0.0.0", "::1", "[::1]"]);
const LIVE_REMOTE_HOSTS = new Map([
  ["woladen.de", "https://live.woladen.de"],
  ["www.woladen.de", "https://live.woladen.de"],
  ["live.woladen.de", "https://live.woladen.de"],
]);
const LIVE_API_QUERY_PARAM = "liveApiBaseUrl";

export function normalizeLiveApiBaseUrl(value) {
  const candidate = String(value || "").trim();
  if (!candidate) {
    return "";
  }
  try {
    const url = new URL(candidate);
    return url.toString().replace(/\/+$/, "");
  } catch (error) {
    return "";
  }
}

export function queryLiveApiBaseUrl(locationHref) {
  const href = String(locationHref || "").trim();
  if (!href) {
    return "";
  }
  try {
    const url = new URL(href);
    return normalizeLiveApiBaseUrl(url.searchParams.get(LIVE_API_QUERY_PARAM) || "");
  } catch (error) {
    return "";
  }
}

export function resolveLiveApiBaseUrl({
  configuredValue = "",
  locationHref = "",
  locationHostname = "",
} = {}) {
  const queryOverride = queryLiveApiBaseUrl(locationHref);
  if (queryOverride) {
    return queryOverride;
  }

  const configured = normalizeLiveApiBaseUrl(configuredValue);
  if (configured) {
    return configured;
  }

  const hostname = String(locationHostname || "").trim();
  if (LIVE_LOCAL_HOSTS.has(hostname)) {
    return normalizeLiveApiBaseUrl("https://live.woladen.de");
  }
  if (LIVE_REMOTE_HOSTS.has(hostname)) {
    return normalizeLiveApiBaseUrl(LIVE_REMOTE_HOSTS.get(hostname) || "");
  }
  return "";
}
