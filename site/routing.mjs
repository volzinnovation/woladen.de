const ROUTE_FILTER_DEFAULTS = Object.freeze({
  operator: "",
  minPower: 50,
  minAmenityCount: 0,
  amenities: [],
  amenityNameQuery: "",
  availableOnly: false,
  currentlyOpenOnly: false,
});

function finiteNumber(value, fallback = Number.NaN) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function stringValue(value) {
  return String(value ?? "").trim();
}

function normalizeAmenityKeys(value) {
  const source = value instanceof Set
    ? Array.from(value)
    : Array.isArray(value)
      ? value
      : [];
  return Array.from(
    new Set(
      source
        .map((item) => stringValue(item))
        .filter((item) => /^amenity_[a-z0-9_]+$/.test(item)),
    ),
  ).sort();
}

export function routeFiltersPayload(filters = {}) {
  const raw = filters && typeof filters === "object" ? filters : {};
  const source = {
    ...ROUTE_FILTER_DEFAULTS,
    ...raw,
  };
  const minPowerKw = finiteNumber(
    raw.minPower ?? raw.min_power_kw ?? ROUTE_FILTER_DEFAULTS.minPower,
    ROUTE_FILTER_DEFAULTS.minPower,
  );
  const minAmenityCount = finiteNumber(
    raw.minAmenityCount ?? raw.min_amenities_total ?? ROUTE_FILTER_DEFAULTS.minAmenityCount,
    ROUTE_FILTER_DEFAULTS.minAmenityCount,
  );
  return {
    operator: stringValue(source.operator),
    min_power_kw: Math.max(0, Math.round(minPowerKw)),
    min_amenities_total: Math.max(0, Math.round(minAmenityCount)),
    selected_amenities: normalizeAmenityKeys(
      raw.amenities ?? raw.selected_amenities ?? ROUTE_FILTER_DEFAULTS.amenities,
    ),
    amenity_name_query: stringValue(
      raw.amenityNameQuery ?? raw.amenity_name_query ?? ROUTE_FILTER_DEFAULTS.amenityNameQuery,
    ),
    available_only: typeof raw.availableOnly === "boolean"
      ? raw.availableOnly
      : typeof raw.available_only === "boolean"
        ? raw.available_only
        : ROUTE_FILTER_DEFAULTS.availableOnly,
    currently_open_only: typeof raw.currentlyOpenOnly === "boolean"
      ? raw.currentlyOpenOnly
      : typeof raw.currently_open_only === "boolean"
        ? raw.currently_open_only
        : ROUTE_FILTER_DEFAULTS.currentlyOpenOnly,
  };
}

export function normalizeRouteEndpoint(value) {
  const lat = finiteNumber(value?.lat);
  const lon = finiteNumber(value?.lon ?? value?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  return {
    lat,
    lon,
    label: stringValue(value?.label || value?.name),
  };
}

function normalizeRouteGeometry(geometry) {
  const coordinates = Array.isArray(geometry?.coordinates)
    ? geometry.coordinates
        .map((point) => {
          const lon = finiteNumber(point?.[0]);
          const lat = finiteNumber(point?.[1]);
          return Number.isFinite(lat) && Number.isFinite(lon) ? [lon, lat] : null;
        })
        .filter(Boolean)
    : [];
  return {
    type: "LineString",
    coordinates,
  };
}

function normalizeNearestRoutePoint(value) {
  const lat = finiteNumber(value?.lat);
  const lon = finiteNumber(value?.lon ?? value?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  return { lat, lon };
}

function normalizeRouteMetadata(value) {
  const nearestRoutePoint = normalizeNearestRoutePoint(value?.nearest_route_point);
  return {
    drive_distance_to_route_m: Math.max(0, Math.round(finiteNumber(value?.drive_distance_to_route_m, 0))),
    route_detour_m: Math.max(0, Math.round(finiteNumber(value?.route_detour_m, 0))),
    straight_line_distance_to_route_m: Math.max(0, Math.round(finiteNumber(value?.straight_line_distance_to_route_m, 0))),
    route_position_m: Math.max(0, Math.round(finiteNumber(value?.route_position_m, 0))),
    ...(nearestRoutePoint ? { nearest_route_point: nearestRoutePoint } : {}),
  };
}

function normalizeRouteStation(value) {
  const station = value?.station;
  if (!station || typeof station !== "object") {
    return null;
  }
  return {
    station,
    route: normalizeRouteMetadata(value?.route || {}),
  };
}

export function normalizeRouteChargerResponse(payload) {
  if (!payload || typeof payload !== "object") {
    throw new Error("Unexpected route charger response");
  }
  const route = payload.route && typeof payload.route === "object" ? payload.route : {};
  const geometry = normalizeRouteGeometry(route.geometry);
  return {
    route: {
      source: stringValue(route.source),
      profile: stringValue(route.profile),
      distance_m: Math.max(0, Math.round(finiteNumber(route.distance_m, 0))),
      duration_s: Math.max(0, Math.round(finiteNumber(route.duration_s, 0))),
      geometry,
    },
    stations: (Array.isArray(payload.stations) ? payload.stations : [])
      .map(normalizeRouteStation)
      .filter(Boolean),
    query: payload.query && typeof payload.query === "object" ? { ...payload.query } : {},
    source: stringValue(payload.source),
  };
}
