export const DEFAULT_FILTER_SETTINGS = Object.freeze({
  operator: "",
  minPower: 50,
  minAmenityCount: 0,
  amenities: [],
  amenityNameQuery: "",
  availableOnly: true,
  currentlyOpenOnly: false,
});

const MAX_MIN_POWER_KW = 350;
const POWER_STEP_KW = 10;
const MAX_MIN_AMENITY_COUNT = 25;
const MAX_TEXT_LENGTH = 120;
const AMENITY_KEY_PATTERN = /^amenity_[a-z0-9_]+$/;

function normalizeText(value) {
  return typeof value === "string" ? value.trim().slice(0, MAX_TEXT_LENGTH) : "";
}

function normalizeMinPower(value, fallback = DEFAULT_FILTER_SETTINGS.minPower) {
  const fallbackValue = Number(fallback);
  const numericFallback = Number.isFinite(fallbackValue)
    ? fallbackValue
    : DEFAULT_FILTER_SETTINGS.minPower;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return numericFallback;
  }
  const clamped = Math.max(0, Math.min(MAX_MIN_POWER_KW, numeric));
  return Math.round(clamped / POWER_STEP_KW) * POWER_STEP_KW;
}

function normalizeMinAmenityCount(value, fallback = DEFAULT_FILTER_SETTINGS.minAmenityCount) {
  const fallbackValue = Number(fallback);
  const numericFallback = Number.isFinite(fallbackValue)
    ? fallbackValue
    : DEFAULT_FILTER_SETTINGS.minAmenityCount;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return numericFallback;
  }
  const clamped = Math.max(0, Math.min(MAX_MIN_AMENITY_COUNT, numeric));
  return Math.round(clamped);
}

function normalizeAmenities(value) {
  const list = value instanceof Set
    ? Array.from(value)
    : Array.isArray(value)
      ? value
      : [];
  return Array.from(
    new Set(
      list
        .map((item) => String(item || "").trim())
        .filter((item) => AMENITY_KEY_PATTERN.test(item)),
    ),
  ).sort();
}

export function normalizeStoredFilterSettings(value, defaults = DEFAULT_FILTER_SETTINGS) {
  const source = value && typeof value === "object" ? value : {};
  const fallback = {
    ...DEFAULT_FILTER_SETTINGS,
    ...(defaults && typeof defaults === "object" ? defaults : {}),
  };

  return {
    operator: normalizeText(source.operator ?? fallback.operator),
    minPower: normalizeMinPower(source.minPower ?? source.min_power_kw, fallback.minPower),
    minAmenityCount: normalizeMinAmenityCount(
      source.minAmenityCount ?? source.min_amenities_total,
      fallback.minAmenityCount,
    ),
    amenities: normalizeAmenities(source.amenities ?? fallback.amenities),
    amenityNameQuery: normalizeText(source.amenityNameQuery ?? source.amenity_name_query ?? fallback.amenityNameQuery),
    availableOnly: typeof source.availableOnly === "boolean"
      ? source.availableOnly
      : Boolean(fallback.availableOnly),
    currentlyOpenOnly: typeof source.currentlyOpenOnly === "boolean"
      ? source.currentlyOpenOnly
      : Boolean(fallback.currentlyOpenOnly),
  };
}

export function parseStoredFilterSettings(raw, defaults = DEFAULT_FILTER_SETTINGS) {
  if (!raw || typeof raw !== "string") {
    return null;
  }
  try {
    return normalizeStoredFilterSettings(JSON.parse(raw), defaults);
  } catch (error) {
    return null;
  }
}

export function serializeStoredFilterSettings(filters, defaults = DEFAULT_FILTER_SETTINGS) {
  return JSON.stringify(normalizeStoredFilterSettings(filters, defaults));
}
