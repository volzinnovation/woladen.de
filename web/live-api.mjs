const LIVE_LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "0.0.0.0", "::1", "[::1]"]);
const LIVE_EU_API_BASE_URL = "https://live-eu.woladen.de";
const LIVE_REMOTE_HOSTS = new Map([
  ["woladen.de", LIVE_EU_API_BASE_URL],
  ["www.woladen.de", LIVE_EU_API_BASE_URL],
  ["live-eu.woladen.de", LIVE_EU_API_BASE_URL],
]);
const LIVE_API_QUERY_PARAM = "liveApiBaseUrl";
const LIVE_DE_API_QUERY_PARAM = "deLiveApiBaseUrl";
const LEGACY_GERMAN_STATION_ID_RE = /^[0-9a-f]{16}$/i;
const NAMESPACED_GERMAN_STATION_ID_RE = /^DE:([0-9a-f]{16})$/i;
const COUNTRY_STATION_ID_RE = /^([A-Z]{2}):(.+)$/i;

export function normalizeLiveStationId(value) {
  const stationId = String(value || "").trim();
  if (!stationId) {
    return "";
  }
  if (LEGACY_GERMAN_STATION_ID_RE.test(stationId)) {
    return `DE:${stationId.toLowerCase()}`;
  }
  const germanMatch = stationId.match(NAMESPACED_GERMAN_STATION_ID_RE);
  if (germanMatch) {
    return `DE:${germanMatch[1].toLowerCase()}`;
  }
  const countryMatch = stationId.match(COUNTRY_STATION_ID_RE);
  if (countryMatch?.[1]?.toUpperCase() === "DE") {
    return `DE:${countryMatch[2]}`;
  }
  if (countryMatch) {
    return `${countryMatch[1].toLowerCase()}:${countryMatch[2]}`;
  }
  return stationId;
}

export function buildLiveStationDetailPath(stationId) {
  const normalizedStationId = normalizeLiveStationId(stationId);
  return normalizedStationId
    ? `/v1/stations/${encodeURIComponent(normalizedStationId)}`
    : "";
}

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

function normalizeLiveDynamicField(value) {
  return String(value ?? "").trim();
}

export function readLiveDynamicFields(record = {}) {
  const source = record && typeof record === "object" ? record : {};
  return {
    operationalStatus: normalizeLiveDynamicField(source.operational_status),
    availabilityStatus: normalizeLiveDynamicField(source.availability_status),
    priceDisplay: normalizeLiveDynamicField(source.price_display),
  };
}

function queryApiBaseUrl(locationHref, queryParam) {
  const href = String(locationHref || "").trim();
  if (!href) {
    return "";
  }
  try {
    const url = new URL(href);
    if (!LIVE_LOCAL_HOSTS.has(url.hostname)) {
      return "";
    }
    return normalizeLiveApiBaseUrl(url.searchParams.get(queryParam) || "");
  } catch (error) {
    return "";
  }
}

function locationHostnameFromHref(locationHref) {
  try {
    return new URL(String(locationHref || "")).hostname;
  } catch (error) {
    return "";
  }
}

function liveApiOverrideHost(locationHref, locationHostname) {
  return String(locationHostname || locationHostnameFromHref(locationHref)).trim();
}

function allowsQueryApiBaseUrl(locationHref, locationHostname) {
  return LIVE_LOCAL_HOSTS.has(liveApiOverrideHost(locationHref, locationHostname));
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
  const hostname = liveApiOverrideHost(locationHref, locationHostname);
  if (allowsQueryApiBaseUrl(locationHref, hostname)) {
    const queryOverride = queryLiveApiBaseUrl(locationHref);
    if (queryOverride) {
      return queryOverride;
    }
  }

  const configured = normalizeLiveApiBaseUrl(configuredValue);
  if (configured) {
    return configured;
  }

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
  const hostname = liveApiOverrideHost(locationHref, locationHostname);
  const allowQueryOverride = allowsQueryApiBaseUrl(locationHref, hostname);
  if (allowQueryOverride) {
    const germanQueryOverride = queryGermanLiveApiBaseUrl(locationHref);
    if (germanQueryOverride) {
      return germanQueryOverride;
    }
  }

  const configured = normalizeLiveApiBaseUrl(configuredValue);
  if (configured) {
    return configured;
  }

  if (allowQueryOverride) {
    const primaryQueryOverride = queryLiveApiBaseUrl(locationHref);
    if (primaryQueryOverride) {
      return primaryQueryOverride;
    }
  }

  return normalizeLiveApiBaseUrl(primaryBaseUrl) || resolveLiveApiBaseUrl({
    configuredValue: "",
    locationHref,
    locationHostname: hostname,
  });
}
