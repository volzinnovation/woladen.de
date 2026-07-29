const LEVELS = ["country", "provider", "operator", "location", "point"];
const FILTER_KEYS = [
  "country_code",
  "provider_uid",
  "operator_id",
  "location_id",
  "station_id",
  "point_id",
  "durable_entity_key",
];

export function normalizeAfirApiBaseUrl(value) {
  return String(value || "").trim().replace(/\/+$/, "");
}

export function nextAfirLevel(level) {
  const index = LEVELS.indexOf(String(level || "").toLowerCase());
  return index >= 0 && index < LEVELS.length - 1 ? LEVELS[index + 1] : "";
}

export function previousAfirLevel(level) {
  const index = LEVELS.indexOf(String(level || "").toLowerCase());
  return index > 0 ? LEVELS[index - 1] : "";
}

export function scopeFromAfirDimensions(dimensions = {}) {
  return Object.fromEntries(
    FILTER_KEYS
      .map((key) => [key, String(dimensions?.[key] || "").trim()])
      .filter(([, value]) => value),
  );
}

export function afirStationDetailUrl(dimensions = {}) {
  const stationId = String(dimensions?.station_id || "").trim();
  return stationId ? `./?station=${encodeURIComponent(stationId)}` : "";
}

export function buildAfirCurrentUrl(
  baseUrl,
  {
    level = "country",
    scope = {},
    limit = 100,
    offset = 0,
    fieldId = "",
  } = {},
) {
  const normalizedBase = normalizeAfirApiBaseUrl(baseUrl);
  if (!normalizedBase) {
    throw new Error("AFIR API base URL is missing.");
  }
  if (!LEVELS.includes(level)) {
    throw new Error(`Unknown AFIR hierarchy level: ${level}`);
  }
  const query = new URLSearchParams({
    group_by: level,
    limit: String(limit),
    offset: String(offset),
  });
  for (const key of FILTER_KEYS) {
    const value = String(scope?.[key] || "").trim();
    if (value) query.set(key, value);
  }
  if (String(fieldId || "").trim()) {
    query.set("field_id", String(fieldId).trim().toUpperCase());
  }
  return `${normalizedBase}/current?${query.toString()}`;
}

async function fetchJson(fetchImpl, url) {
  const response = await fetchImpl(url, {
    cache: "no-store",
    headers: { Accept: "application/json" },
  });
  if (!response.ok) {
    let detail = "";
    try {
      detail = String((await response.json())?.detail || "");
    } catch {
      // The HTTP status is sufficient when the response is not JSON.
    }
    throw new Error(
      detail
        ? `AFIR API: ${detail}`
        : `AFIR API returned HTTP ${response.status}.`,
    );
  }
  return await response.json();
}

export function createAfirDataSource({
  baseUrl = "https://live-eu.woladen.de/v1/afir-compliance",
  fetchImpl = globalThis.fetch,
} = {}) {
  const normalizedBase = normalizeAfirApiBaseUrl(baseUrl);
  return {
    baseUrl: normalizedBase,
    async loadMeta() {
      return await fetchJson(fetchImpl, `${normalizedBase}/meta`);
    },
    async loadGroups(options = {}) {
      return await fetchJson(
        fetchImpl,
        buildAfirCurrentUrl(normalizedBase, options),
      );
    },
  };
}

export const AFIR_HIERARCHY_LEVELS = Object.freeze([...LEVELS]);
