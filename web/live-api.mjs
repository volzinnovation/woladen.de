const LIVE_LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "0.0.0.0", "::1", "[::1]"]);
const LIVE_EU_API_BASE_URL = "https://live-eu.woladen.de";
const LIVE_REMOTE_HOSTS = new Map([
  ["woladen.de", LIVE_EU_API_BASE_URL],
  ["www.woladen.de", LIVE_EU_API_BASE_URL],
  ["live-eu.woladen.de", LIVE_EU_API_BASE_URL],
]);
const LIVE_API_QUERY_PARAM = "liveApiBaseUrl";
const LIVE_DE_API_QUERY_PARAM = "deLiveApiBaseUrl";

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

function queryApiBaseUrl(locationHref, queryParam) {
  const href = String(locationHref || "").trim();
  if (!href) {
    return "";
  }
  try {
    const url = new URL(href);
    return normalizeLiveApiBaseUrl(url.searchParams.get(queryParam) || "");
  } catch (error) {
    return "";
  }
}

export function queryLiveApiBaseUrl(locationHref) {
  return queryApiBaseUrl(locationHref, LIVE_API_QUERY_PARAM);
}

export function queryGermanLiveApiBaseUrl(locationHref) {
  return queryApiBaseUrl(locationHref, LIVE_DE_API_QUERY_PARAM);
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
    return normalizeLiveApiBaseUrl(LIVE_EU_API_BASE_URL);
  }
  if (LIVE_REMOTE_HOSTS.has(hostname)) {
    return normalizeLiveApiBaseUrl(LIVE_REMOTE_HOSTS.get(hostname) || "");
  }
  return "";
}

export function resolveGermanLiveApiBaseUrl({
  configuredValue = "",
  locationHref = "",
  locationHostname = "",
  primaryBaseUrl = "",
} = {}) {
  const germanQueryOverride = queryGermanLiveApiBaseUrl(locationHref);
  if (germanQueryOverride) {
    return germanQueryOverride;
  }

  const configured = normalizeLiveApiBaseUrl(configuredValue);
  if (configured) {
    return configured;
  }

  const primaryQueryOverride = queryLiveApiBaseUrl(locationHref);
  if (primaryQueryOverride) {
    return primaryQueryOverride;
  }

  return normalizeLiveApiBaseUrl(primaryBaseUrl) || resolveLiveApiBaseUrl({
    configuredValue: "",
    locationHref,
    locationHostname,
  });
}
