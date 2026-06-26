export const FAVORITE_METADATA_VERSION = 2;
export const FAVORITES_V2_STORAGE_KEY = "woladen_favorites_v2";
export const FAVORITES_LEGACY_STORAGE_KEY = "woladen_favs";
export const FAVORITE_SOURCE_MANUAL = "manual";
export const FAVORITE_SOURCE_ROUTE = "route";
export const FAVORITE_SOURCE_MIGRATION = "migration";
export const FAVORITE_CATEGORY_UNCATEGORIZED = "__uncategorized__";
export const FAVORITE_FILTER_ALL = "__all__";
export const MAX_FAVORITE_CATEGORY_LENGTH = 48;
export const MAX_FAVORITE_CATEGORIES = 12;
export const MAX_FAVORITE_CATEGORY_SUGGESTIONS = 6;

const VALID_SOURCES = new Set([
  FAVORITE_SOURCE_MANUAL,
  FAVORITE_SOURCE_ROUTE,
  FAVORITE_SOURCE_MIGRATION,
]);

function defaultNormalizeStationId(value) {
  return String(value || "").trim();
}

function nowIso(now = new Date()) {
  const date = now instanceof Date ? now : new Date(now);
  return Number.isNaN(date.getTime()) ? new Date().toISOString() : date.toISOString();
}

function cleanTimestamp(value, fallback) {
  const text = String(value || "").trim();
  if (!text) {
    return fallback;
  }
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? fallback : text;
}

function normalizeSource(value, fallback = FAVORITE_SOURCE_MANUAL) {
  const source = String(value || "").trim();
  return VALID_SOURCES.has(source) ? source : fallback;
}

export function normalizeFavoriteCategoryLabel(value) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, MAX_FAVORITE_CATEGORY_LENGTH);
}

export function normalizeFavoriteCategories(value, knownCategories = []) {
  const displayByKey = new Map();
  knownCategories.forEach((category) => {
    const label = normalizeFavoriteCategoryLabel(category);
    const key = label.toLocaleLowerCase();
    if (label && !displayByKey.has(key)) {
      displayByKey.set(key, label);
    }
  });

  const source = Array.isArray(value) ? value : [];
  const categories = [];
  const seen = new Set();
  source.forEach((item) => {
    const label = normalizeFavoriteCategoryLabel(item);
    const key = label.toLocaleLowerCase();
    if (!label || seen.has(key)) {
      return;
    }
    categories.push(displayByKey.get(key) || label);
    seen.add(key);
  });
  return categories.slice(0, MAX_FAVORITE_CATEGORIES);
}

export function createEmptyFavoriteMetadata() {
  return {
    version: FAVORITE_METADATA_VERSION,
    items: new Map(),
  };
}

export function getFavoriteCategories(metadata) {
  const categories = [];
  const seen = new Set();
  for (const item of metadata?.items?.values?.() || []) {
    normalizeFavoriteCategories(item.categories, categories).forEach((category) => {
      const key = category.toLocaleLowerCase();
      if (!seen.has(key)) {
        categories.push(category);
        seen.add(key);
      }
    });
  }
  return categories;
}

export function normalizeFavoriteItem(value, options = {}) {
  const normalizeStationId = options.normalizeStationId || defaultNormalizeStationId;
  const fallbackNow = nowIso(options.now);
  if (!value || typeof value !== "object") {
    return null;
  }
  const stationId = normalizeStationId(value.station_id ?? value.stationId ?? value.id);
  if (!stationId) {
    return null;
  }
  const createdAt = cleanTimestamp(value.created_at ?? value.createdAt, fallbackNow);
  const updatedAt = cleanTimestamp(value.updated_at ?? value.updatedAt, createdAt);
  return {
    station_id: stationId,
    categories: normalizeFavoriteCategories(value.categories, options.knownCategories || []),
    created_at: createdAt,
    updated_at: updatedAt,
    source: normalizeSource(value.source),
  };
}

export function normalizeFavoriteMetadata(value, options = {}) {
  const metadata = createEmptyFavoriteMetadata();
  if (!value || typeof value !== "object" || Number(value.version) !== FAVORITE_METADATA_VERSION) {
    return metadata;
  }

  const sourceItems = value.items && typeof value.items === "object" && !Array.isArray(value.items)
    ? Object.values(value.items)
    : [];
  let knownCategories = [];
  sourceItems.forEach((item) => {
    const normalized = normalizeFavoriteItem(item, { ...options, knownCategories });
    if (!normalized) {
      return;
    }
    knownCategories = getFavoriteCategories({
      items: new Map([...metadata.items, [normalized.station_id, normalized]]),
    });
    metadata.items.set(normalized.station_id, normalized);
  });
  return metadata;
}

export function parseStoredFavoriteMetadata(raw, options = {}) {
  if (!raw || typeof raw !== "string") {
    return null;
  }
  try {
    const payload = JSON.parse(raw);
    if (!payload || typeof payload !== "object" || Number(payload.version) !== FAVORITE_METADATA_VERSION) {
      return null;
    }
    return normalizeFavoriteMetadata(payload, options);
  } catch {
    return null;
  }
}

export function migrateLegacyFavorites(raw, options = {}) {
  const normalizeStationId = options.normalizeStationId || defaultNormalizeStationId;
  const timestamp = nowIso(options.now);
  const metadata = createEmptyFavoriteMetadata();
  let values = [];
  try {
    values = raw && typeof raw === "string" ? JSON.parse(raw) : [];
  } catch {
    values = [];
  }
  if (!Array.isArray(values)) {
    return metadata;
  }
  values.forEach((value) => {
    const stationId = normalizeStationId(value);
    if (!stationId || metadata.items.has(stationId)) {
      return;
    }
    metadata.items.set(stationId, {
      station_id: stationId,
      categories: [],
      created_at: timestamp,
      updated_at: timestamp,
      source: FAVORITE_SOURCE_MIGRATION,
    });
  });
  return metadata;
}

export function serializeFavoriteMetadata(metadata) {
  const items = {};
  const entries = Array.from(metadata?.items?.entries?.() || [])
    .sort(([leftId], [rightId]) => leftId.localeCompare(rightId));
  entries.forEach(([stationId, item]) => {
    items[stationId] = {
      station_id: stationId,
      categories: normalizeFavoriteCategories(item.categories, getFavoriteCategories(metadata)),
      created_at: item.created_at,
      updated_at: item.updated_at,
      source: normalizeSource(item.source),
    };
  });
  return JSON.stringify({
    version: FAVORITE_METADATA_VERSION,
    items,
  });
}

export function getFavoriteStationIds(metadata) {
  return new Set(Array.from(metadata?.items?.keys?.() || []));
}

export function ensureFavoriteItem(metadata, stationId, options = {}) {
  const id = defaultNormalizeStationId(stationId);
  if (!id) {
    return null;
  }
  const timestamp = nowIso(options.now);
  const existing = metadata.items.get(id);
  if (existing) {
    return existing;
  }
  const item = {
    station_id: id,
    categories: [],
    created_at: timestamp,
    updated_at: timestamp,
    source: normalizeSource(options.source, FAVORITE_SOURCE_MANUAL),
  };
  metadata.items.set(id, item);
  return item;
}

export function removeFavoriteItem(metadata, stationId) {
  return metadata.items.delete(defaultNormalizeStationId(stationId));
}

export function setFavoriteCategories(metadata, stationId, categories, options = {}) {
  const item = options.create === false
    ? metadata.items.get(defaultNormalizeStationId(stationId))
    : ensureFavoriteItem(metadata, stationId, options);
  if (!item) {
    return null;
  }
  item.categories = normalizeFavoriteCategories(categories, getFavoriteCategories(metadata));
  item.updated_at = nowIso(options.now);
  return item;
}

export function addFavoriteCategory(metadata, stationId, category, options = {}) {
  const item = ensureFavoriteItem(metadata, stationId, options);
  if (!item) {
    return null;
  }
  return setFavoriteCategories(
    metadata,
    stationId,
    [...item.categories, category],
    options,
  );
}

export function removeFavoriteCategory(metadata, stationId, category, options = {}) {
  const item = metadata.items.get(defaultNormalizeStationId(stationId));
  if (!item) {
    return null;
  }
  const key = normalizeFavoriteCategoryLabel(category).toLocaleLowerCase();
  item.categories = item.categories.filter(
    (label) => label.toLocaleLowerCase() !== key,
  );
  item.updated_at = nowIso(options.now);
  return item;
}

export function favoriteCategorySuggestions(metadata, query, options = {}) {
  const limit = Math.max(1, Number(options.limit || MAX_FAVORITE_CATEGORY_SUGGESTIONS));
  const currentKeys = new Set(
    normalizeFavoriteCategories(options.exclude || []).map((category) => category.toLocaleLowerCase()),
  );
  const normalizedQuery = normalizeFavoriteCategoryLabel(query).toLocaleLowerCase();
  const categories = getFavoriteCategories(metadata)
    .filter((category) => !currentKeys.has(category.toLocaleLowerCase()))
    .sort((left, right) => left.localeCompare(right, options.locale || undefined));
  if (!normalizedQuery) {
    return categories.slice(0, limit);
  }
  const prefix = [];
  const substring = [];
  categories.forEach((category) => {
    const key = category.toLocaleLowerCase();
    if (key.startsWith(normalizedQuery)) {
      prefix.push(category);
    } else if (key.includes(normalizedQuery)) {
      substring.push(category);
    }
  });
  return [...prefix, ...substring].slice(0, limit);
}
