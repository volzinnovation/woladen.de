import { queryStationPagePath } from "./station-detail.mjs";

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

const FIELD_SCOPE_KEYS = [...FILTER_KEYS];

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
  const stationId = String(dimensions?.detail_station_id || "").trim();
  return stationId ? queryStationPagePath(stationId) : "";
}

export function afirAggregateFieldsUrl(level = "country", dimensions = {}) {
  const safeLevel = LEVELS.includes(String(level || "").toLowerCase())
    ? String(level || "").toLowerCase()
    : "country";
  const query = new URLSearchParams({
    level: safeLevel,
    view: "fields",
  });
  for (const key of FIELD_SCOPE_KEYS) {
    const value = String(dimensions?.[key] || "").trim();
    if (value) {
      query.set(key, value);
    }
  }
  return `./afir.html?${query.toString()}`;
}

export function afirCountryDisplayName(
  countryCode,
  displayNames = null,
) {
  const code = String(countryCode || "").trim().toUpperCase();
  if (!/^[A-Z]{2}$/.test(code)) return code;
  if (!displayNames || typeof displayNames.of !== "function") return code;
  try {
    return displayNames.of(code) || code;
  } catch {
    return code;
  }
}

export function afirChargingPointCount(group = {}) {
  const counts = group?.entity_counts || {};
  const value = Number(
    counts.charging_point_count ?? counts.point_count ?? 0,
  );
  return Number.isFinite(value) && value >= 0 ? value : 0;
}

export function afirPointCoverage(summary = {}) {
  const pointCoverage = summary?.charging_point_coverage;
  return pointCoverage && typeof pointCoverage === "object"
    ? pointCoverage
    : summary;
}

export function buildAfirCurrentUrl(
  baseUrl,
  {
    level = "country",
    scope = {},
    limit = 100,
    offset = 0,
    fieldId = "",
    sort = "identity",
    direction = "asc",
    search = "",
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
    sort: String(sort || "identity"),
    direction: String(direction || "asc"),
  });
  for (const key of FILTER_KEYS) {
    const value = String(scope?.[key] || "").trim();
    if (value) query.set(key, value);
  }
  if (String(fieldId || "").trim()) {
    query.set("field_id", String(fieldId).trim().toUpperCase());
  }
  const normalizedSearch = String(search || "").trim();
  if (normalizedSearch.length >= 3) {
    query.set("search", normalizedSearch);
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
