import { hasOpenAmenity } from "./opening-hours.mjs";

const COMBINING_MARKS = /[\u0300-\u036f]/g;
const NON_ALPHANUMERIC = /[^\p{L}\p{N}]+/gu;

export function compareOperatorNames(left, right, locale) {
  return String(left).localeCompare(String(right), locale, { sensitivity: "accent" });
}

export function normalizeAmenityNameQuery(value = "") {
  return String(value)
    .trim()
    .normalize("NFD")
    .replace(COMBINING_MARKS, "")
    .toLowerCase()
    .replace(/ß/g, "ss")
    .replace(NON_ALPHANUMERIC, "");
}

export function matchesAmenityNameQuery(properties, query) {
  const normalizedQuery = normalizeAmenityNameQuery(query);
  if (!normalizedQuery) {
    return true;
  }

  const examples = Array.isArray(properties?.amenity_examples)
    ? properties.amenity_examples
    : [];

  return examples.some((example) => {
    if (!example || typeof example.name !== "string") {
      return false;
    }
    return normalizeAmenityNameQuery(example.name).includes(normalizedQuery);
  });
}

export function countActiveFilters(filters) {
  const minPower = Number(filters?.minPower ?? 50);
  const minAmenityCount = Number(filters?.minAmenityCount ?? 0);
  const selectedAmenities =
    filters?.amenities instanceof Set
      ? filters.amenities.size
      : Array.isArray(filters?.amenities)
        ? filters.amenities.length
        : 0;

  return (
    (filters?.operator ? 1 : 0) +
    (Number.isFinite(minPower) && minPower > 0 ? 1 : 0) +
    (Number.isFinite(minAmenityCount) && minAmenityCount > 0 ? 1 : 0) +
    selectedAmenities +
    (filters?.availableOnly ? 1 : 0) +
    (filters?.currentlyOpenOnly ? 1 : 0) +
    (normalizeAmenityNameQuery(filters?.amenityNameQuery).length > 0 ? 1 : 0)
  );
}

function numericCount(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number : 0;
}

function hasLiveAvailabilitySummary(properties) {
  const total = numericCount(properties?.live_total_evses);
  const fetchedAt = String(
    properties?.live_source_observed_at ||
      properties?.live_fetched_at ||
      properties?.live_ingested_at ||
      "",
  ).trim();
  return Boolean(fetchedAt) || total > 0;
}

function availabilityCounts(properties) {
  if (hasLiveAvailabilitySummary(properties)) {
    return {
      total: numericCount(properties?.live_total_evses),
      available: numericCount(properties?.live_available_evses),
    };
  }
  return {
    total: numericCount(properties?.occupancy_total_evses),
    available: numericCount(properties?.occupancy_available_evses),
  };
}

export function hasAvailableChargingPoint(properties) {
  const counts = availabilityCounts(properties);
  return counts.total > 0 && counts.available > 0;
}

export function matchesFeatureFilters(feature, filters, options = {}) {
  const properties = feature?.properties ?? {};
  const getDisplayedMaxPowerKw =
    options.getDisplayedMaxPowerKw ??
    ((current) =>
      Number(current.max_individual_power_kw ?? current.max_power_kw ?? 0));

  if (filters?.operator && properties.operator !== filters.operator) {
    return false;
  }

  if (Number(getDisplayedMaxPowerKw(properties)) < Number(filters?.minPower ?? 50)) {
    return false;
  }

  const minAmenityCount = Number(filters?.minAmenityCount ?? 0);
  if (
    Number.isFinite(minAmenityCount) &&
    minAmenityCount > 0 &&
    numericCount(properties?.amenities_total) < minAmenityCount
  ) {
    return false;
  }

  if (filters?.currentlyOpenOnly && !hasOpenAmenity(properties, options.now ?? new Date())) {
    return false;
  }

  if (filters?.availableOnly && !hasAvailableChargingPoint(properties)) {
    return false;
  }

  for (const key of filters?.amenities ?? []) {
    if (Number(properties[key] ?? 0) <= 0) {
      return false;
    }
  }

  return matchesAmenityNameQuery(properties, filters?.amenityNameQuery);
}
