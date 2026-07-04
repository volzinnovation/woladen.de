const GEOCODER_API_QUERY_PARAM = "geocoderApiBaseUrl";
const GEOCODER_LOCAL_HOSTS = new Set([
  "127.0.0.1",
  "localhost",
  "0.0.0.0",
  "::1",
  "[::1]",
]);
const GEOCODER_REMOTE_BASE_URL = "https://live-eu.woladen.de/v1/geocode";
const GEOCODER_REMOTE_HOSTS = new Map([
  ["woladen.de", GEOCODER_REMOTE_BASE_URL],
  ["www.woladen.de", GEOCODER_REMOTE_BASE_URL],
  ["live-eu.woladen.de", GEOCODER_REMOTE_BASE_URL],
  ["volz.hs-pforzheim.de", GEOCODER_REMOTE_BASE_URL],
]);

export function normalizeGeocoderApiBaseUrl(
  value,
  baseHref = "http://127.0.0.1/",
) {
  const candidate = String(value || "").trim();
  if (!candidate) {
    return "";
  }
  try {
    const url = new URL(candidate, baseHref);
    if (!["http:", "https:"].includes(url.protocol)) {
      return "";
    }
    return url.toString().replace(/\/+$/, "");
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

export function queryGeocoderApiBaseUrl(locationHref) {
  const href = String(locationHref || "").trim();
  if (!href) {
    return "";
  }
  try {
    const url = new URL(href);
    if (!GEOCODER_LOCAL_HOSTS.has(url.hostname)) {
      return "";
    }
    return normalizeGeocoderApiBaseUrl(
      url.searchParams.get(GEOCODER_API_QUERY_PARAM) || "",
      url.href,
    );
  } catch (error) {
    return "";
  }
}

export function resolveGeocoderApiBaseUrl({
  configuredValue = "",
  locationHref = "",
  locationHostname = "",
} = {}) {
  const hostname = String(
    locationHostname || locationHostnameFromHref(locationHref),
  ).trim();
  if (GEOCODER_LOCAL_HOSTS.has(hostname)) {
    const queryOverride = queryGeocoderApiBaseUrl(locationHref);
    if (queryOverride) {
      return queryOverride;
    }
  }

  const configured = normalizeGeocoderApiBaseUrl(
    configuredValue,
    locationHref || "http://127.0.0.1/",
  );
  if (configured) {
    return configured;
  }

  if (GEOCODER_LOCAL_HOSTS.has(hostname)) {
    return GEOCODER_REMOTE_BASE_URL;
  }
  if (GEOCODER_REMOTE_HOSTS.has(hostname)) {
    return normalizeGeocoderApiBaseUrl(GEOCODER_REMOTE_HOSTS.get(hostname));
  }
  return "";
}

export function buildGeocoderApiUrl(baseUrl, path, params = {}) {
  const normalizedBase = normalizeGeocoderApiBaseUrl(baseUrl);
  if (!normalizedBase) {
    return "";
  }
  const cleanPath = String(path || "").replace(/^\/+/, "");
  const url = new URL(`${normalizedBase}/${cleanPath}`);
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

export function normalizeGeocodeResult(result) {
  if (!result || typeof result !== "object") {
    return null;
  }
  const lat = Number(result.lat);
  const lon = Number(result.lon);
  const label = String(result.label || result.name || "").trim();
  if (!label || !Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  return {
    id: String(result.id || label).trim(),
    label,
    name: String(result.name || label).trim(),
    lat,
    lon,
    country: String(result.country || "").trim(),
    countryCode: String(
      result.country_code || result.countryCode || "",
    ).trim().toUpperCase(),
    region: String(result.region || "").trim(),
    locality: String(result.locality || "").trim(),
    postalCode: String(result.postal_code || result.postalCode || "").trim(),
    confidence: Number.isFinite(Number(result.confidence))
      ? Number(result.confidence)
      : null,
    source: String(result.source || "").trim(),
    layer: String(result.layer || "").trim(),
    bbox: Array.isArray(result.bbox) && result.bbox.length === 4
      ? result.bbox
      : null,
  };
}

export function normalizeGeocodePayload(payload) {
  const results = Array.isArray(payload?.results)
    ? payload.results.map((result) => normalizeGeocodeResult(result)).filter(Boolean)
    : [];
  return {
    ok: payload?.ok === true,
    provider: String(payload?.provider || "").trim(),
    error: String(payload?.error || "").trim(),
    message: String(payload?.message || "").trim(),
    attribution: String(payload?.attribution || "").trim(),
    sourceUrl: String(payload?.source_url || payload?.sourceUrl || "").trim(),
    results,
  };
}
