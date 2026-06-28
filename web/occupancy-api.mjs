const OCCUPANCY_API_QUERY_PARAM = "occupancyApiBaseUrl";
const OUT_OF_ORDER_PROBABILITY_THRESHOLDS = [
  [0.5, "mostly_broken"],
  [0.25, "often_broken"],
  [0.01, "sometimes_broken"],
];
const OUT_OF_ORDER_PROBABILITY_STATUSES = new Set(
  OUT_OF_ORDER_PROBABILITY_THRESHOLDS.map(([, status]) => status),
);

export function normalizeOutOfOrderProbability(value) {
  const probability = Number(value);
  if (!Number.isFinite(probability) || probability < 0) {
    return null;
  }
  return Math.min(probability, 1);
}

export function classifyOutOfOrderProbability(value) {
  const probability = normalizeOutOfOrderProbability(value);
  if (probability === null) {
    return "";
  }
  for (const [threshold, status] of OUT_OF_ORDER_PROBABILITY_THRESHOLDS) {
    if (probability > threshold) {
      return status;
    }
  }
  return "";
}

export function normalizeOutOfOrderProbabilityStatus(status, probability = null) {
  const normalized = String(status || "").trim();
  if (OUT_OF_ORDER_PROBABILITY_STATUSES.has(normalized)) {
    return normalized;
  }
  return classifyOutOfOrderProbability(probability);
}

export function normalizeOccupancyApiBaseUrl(value, baseHref = "") {
  const candidate = String(value || "").trim();
  if (!candidate) {
    return "";
  }
  try {
    const url = baseHref ? new URL(candidate, baseHref) : new URL(candidate);
    if (!["http:", "https:"].includes(url.protocol)) {
      return "";
    }
    return url.toString().replace(/\/+$/, "");
  } catch (error) {
    return "";
  }
}

export function queryOccupancyApiBaseUrl(locationHref) {
  const href = String(locationHref || "").trim();
  if (!href) {
    return "";
  }
  try {
    const url = new URL(href);
    return normalizeOccupancyApiBaseUrl(url.searchParams.get(OCCUPANCY_API_QUERY_PARAM) || "", href);
  } catch (error) {
    return "";
  }
}

export function resolveOccupancyApiBaseUrl({
  configuredValue = "",
  locationHref = "",
} = {}) {
  const queryOverride = queryOccupancyApiBaseUrl(locationHref);
  if (queryOverride) {
    return queryOverride;
  }
  return normalizeOccupancyApiBaseUrl(configuredValue, locationHref);
}

export function buildOccupancyApiUrl(baseUrl, path, params = {}) {
  const normalizedBase = normalizeOccupancyApiBaseUrl(baseUrl);
  if (!normalizedBase) {
    return "";
  }
  const normalizedPath = String(path || "").replace(/^\/+/, "");
  const url = normalizedPath
    ? new URL(`${normalizedBase}/${normalizedPath}`)
    : new URL(normalizedBase);
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

export function normalizeOccupancyProfile(payload) {
  if (!payload || typeof payload !== "object" || payload.data_available === false) {
    return null;
  }
  const stationId = String(payload.station_id || "").trim();
  const hourlyValues = payload.hourly_average_occupied;
  if (!stationId || !hourlyValues || typeof hourlyValues !== "object") {
    return null;
  }
  const hourly = {};
  for (let hour = 0; hour < 24; hour += 1) {
    const key = `${String(hour).padStart(2, "0")}:00`;
    const value = Number(hourlyValues[key]);
    hourly[key] = Number.isFinite(value) && value > 0 ? value : 0;
  }
  const probability = normalizeOutOfOrderProbability(
    payload.out_of_order_probability ?? payload.out_of_order_share,
  );
  const probabilityStatus = normalizeOutOfOrderProbabilityStatus(
    payload.out_of_order_probability_status,
    probability,
  );
  return {
    ...payload,
    station_id: stationId,
    out_of_order_probability: probability,
    out_of_order_probability_status: probabilityStatus,
    hourly_average_occupied: hourly,
  };
}
