const COMBINING_MARKS = /[\u0300-\u036f]/g;
const NON_ALPHANUMERIC = /[^\p{L}\p{N}]+/gu;

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
  const selectedAmenities =
    filters?.amenities instanceof Set
      ? filters.amenities.size
      : Array.isArray(filters?.amenities)
        ? filters.amenities.length
        : 0;

  return (
    (filters?.operator ? 1 : 0) +
    (Number.isFinite(minPower) && minPower !== 50 ? 1 : 0) +
    selectedAmenities +
    (normalizeAmenityNameQuery(filters?.amenityNameQuery).length > 0 ? 1 : 0)
  );
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

  for (const key of filters?.amenities ?? []) {
    if (Number(properties[key] ?? 0) <= 0) {
      return false;
    }
  }

  return matchesAmenityNameQuery(properties, filters?.amenityNameQuery);
}
