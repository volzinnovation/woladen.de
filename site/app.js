import { countActiveFilters, matchesFeatureFilters } from "./filtering.mjs?v=20260626-routing-web1";
import {
  DEFAULT_FILTER_SETTINGS,
  parseStoredFilterSettings,
  serializeStoredFilterSettings,
} from "./filter-settings.mjs?v=20260626-routing-web1";
import {
  FAVORITES_LEGACY_STORAGE_KEY,
  FAVORITES_V2_STORAGE_KEY,
  FAVORITE_CATEGORY_UNCATEGORIZED,
  FAVORITE_FILTER_ALL,
  FAVORITE_SOURCE_MANUAL,
  FAVORITE_SOURCE_ROUTE,
  addFavoriteCategory,
  createEmptyFavoriteMetadata,
  ensureFavoriteItem,
  favoriteCategorySuggestions,
  getFavoriteCategories,
  getFavoriteStationIds,
  migrateLegacyFavorites,
  normalizeFavoriteCategoryLabel,
  parseStoredFavoriteMetadata,
  removeFavoriteCategory,
  removeFavoriteItem,
  serializeFavoriteMetadata,
} from "./favorite-metadata.mjs?v=20260626-favorites-web1";
import {
  formatOpeningHoursForDisplay,
  getAmenityOpenStatus,
} from "./opening-hours.mjs?v=20260620-i18n";
import {
  LOCATION_ERROR_PERMISSION_DENIED,
  LOCATION_PERMISSION_DENIED,
  LOCATION_PERMISSION_GRANTED,
  LOCATION_PERMISSION_UNKNOWN,
  LOCATION_PERMISSION_UNSUPPORTED,
  LOCATION_REQUEST_ERROR,
  LOCATION_REQUEST_IDLE,
  LOCATION_REQUEST_PENDING,
  LOCATION_REQUEST_READY,
  getLocationLookupViewModel,
  normalizeLocationPermissionState,
  requestBrowserLocation,
} from "./location.mjs?v=20260620-eu-i18n8";
import {
  resolveGermanLiveApiBaseUrl as computeGermanLiveApiBaseUrl,
  resolveLiveApiBaseUrl as computeLiveApiBaseUrl,
} from "./live-api.mjs?v=20260618-split-live-routing";
import {
  buildGeocoderApiUrl,
  normalizeGeocodePayload,
  resolveGeocoderApiBaseUrl as computeGeocoderApiBaseUrl,
} from "./geocoding.mjs?v=20260618-commercial-merge";
import {
  normalizeRouteChargerResponse,
  normalizeRouteEndpoint,
  routeFiltersPayload,
} from "./routing.mjs?v=20260626-routing-web1";
import {
  formatBundleSourceTitle,
  normalizeBundleSources,
  normalizeMappedCountries,
} from "./open-static-ui.mjs?v=20260618-commercial-merge";
import {
  getMapKeyboardAction,
  performMapKeyboardAction,
} from "./map-keyboard.mjs?v=20260618-keyboard-restore";
import {
  formatRatingValue,
  getUserRating,
  normalizeRatingSummary,
  normalizeRating,
  parseStoredRatings,
  serializeStoredRatings,
} from "./rating.mjs";
import {
  getUserNote,
  normalizeNote,
  parseStoredNotes,
  serializeStoredNotes,
} from "./note.mjs";
import {
  applyDocumentTranslations,
  formatDate,
  formatDateTime,
  formatInteger as formatLocalizedInteger,
  getLocale,
  initI18n,
  onLanguageChange,
  populateLanguageSelect,
  setLanguage,
  t,
} from "./i18n.mjs?v=20260627-route-actions1";

/**
 * woladen.de - Modern Frontend Logic
 */

/* --- CONFIGURATION & CONSTANTS --- */
const MAX_DISPLAY_POWER_KW = 400;
const DEFAULT_MIN_POWER_KW = DEFAULT_FILTER_SETTINGS.minPower;
const RATINGS_STORAGE_KEY = "woladen_ratings_v1";
const RATING_CLIENT_STORAGE_KEY = "woladen_rating_client_v1";
const NOTES_STORAGE_KEY = "woladen_notes_v1";
const FILTERS_STORAGE_KEY = "woladen_filters_v1";
const SHARED_RATINGS_ENABLED = window.WOLADEN_ENABLE_SHARED_RATINGS === true ||
  window.WOLADEN_ENABLE_SHARED_RATINGS === "true";
const RATING_SUMMARY_REFRESH_MS = 60000;
const RATING_API_TIMEOUT_MS = 3500;
const CATALOG_LIST_MAX_STATIONS = 1000;
const FAVORITE_SORT_DISTANCE = "distance";
const FAVORITE_SORT_RATING = "rating";
const LIVE_SUMMARY_REFRESH_MS = 15000;
const LIVE_API_TIMEOUT_MS = 3500;
const LIVE_DETAIL_TIMEOUT_MS = 4000;
const GEOCODER_API_TIMEOUT_MS = 3500;
const GEOCODER_SUGGESTION_DEBOUNCE_MS = 250;
const ROUTE_API_TIMEOUT_MS = 120000;
const ROUTE_SUGGESTION_DEBOUNCE_MS = 250;
const ROUTE_FILTER_MODE = "route_calculation";
const LIVE_STATION_LOOKUP_BATCH_SIZE = 20;
const RATING_LOOKUP_BATCH_SIZE = 50;
const CATALOG_SEARCH_RADIUS_M = 20000;
const CATALOG_SEARCH_LIMIT = 100;
const CATALOG_ACCUMULATED_FEATURE_LIMIT = 1000;
const CATALOG_DETAIL_TIMEOUT_MS = 4500;
const CATALOG_MAP_MOVE_DEBOUNCE_MS = 450;
const CATALOG_MIN_RELOAD_DISTANCE_M = 1000;
const MAP_GPS_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
const MAP_GPS_REFRESH_MAX_LOCATION_AGE_MS = 60 * 1000;
const STATIC_FALLBACK_LIST_LIMIT = 20;
const MAP_UNCLUSTERED_MARKER_LIMIT = 350;
const MAP_UNCLUSTERED_FULL_RENDER_ZOOM = 9;
const EASTER_EGG_AUDIO_SECONDS = 1.75;
const EASTER_EGG_AUDIO_RATE = 8192;
const EASTER_EGG_MEMORY_SIZE = 65536;
const EASTER_EGG_TEXT_COLUMNS = 40;
const EASTER_EGG_TEXT_ROWS = 25;
const EASTER_EGG_VISIBLE_BYTES = EASTER_EGG_TEXT_COLUMNS * EASTER_EGG_TEXT_ROWS * 2;
const EASTER_EGG_SUBTRACT_STEP = 57;
const EASTER_EGG_ITERATIONS_PER_FRAME = 1200;
const EASTER_EGG_CHARS = "E0101101#*";
const LIVE_OUT_OF_ORDER_MARKER_SIZE = 22;
const LIVE_FULLY_OCCUPIED_MARKER_SIZE = 18;
const FAVORITE_MARKER_SIZE = 28;
const STATION_ID_NAMESPACE = "DE:";
const LEGACY_STATION_ID_RE = /^[0-9a-f]{16}$/i;
const NAMESPACED_STATION_ID_RE = /^DE:([0-9a-f]{16})$/i;
const COUNTRY_STATION_ID_RE = /^([A-Z]{2}):(.+)$/i;
const DE_MOBILITHEK_SOURCE = {
  label: "Mobilithek",
  url: "https://mobilithek.info/offers/842113170303512576",
};
const LIVE_STATION_FIELDS = [
  "availability_status",
  "available_evses",
  "occupied_evses",
  "out_of_order_evses",
  "unknown_evses",
  "total_evses",
  "price_display",
  "price_currency",
  "price_energy_eur_kwh_min",
  "price_energy_eur_kwh_max",
  "price_time_eur_min_min",
  "price_time_eur_min_max",
  "price_complex",
  "source_observed_at",
  "fetched_at",
  "ingested_at",
];
const LIVE_DYNAMIC_KEY_LABELS = {
  expectedAvailableFromTime: "Ab",
  expectedAvailableToTime: "Bis",
  expectedAvailableUntilTime: "Bis",
  startTime: "Ab",
  endTime: "Bis",
  lastUpdated: "Seit",
  value: "",
};
const LIVE_STATUS_MARKER_CONFIGS = {
  outOfOrder: {
    size: LIVE_OUT_OF_ORDER_MARKER_SIZE,
    svg: `<svg xmlns="http://www.w3.org/2000/svg" width="${LIVE_OUT_OF_ORDER_MARKER_SIZE}" height="${LIVE_OUT_OF_ORDER_MARKER_SIZE}" viewBox="0 0 ${LIVE_OUT_OF_ORDER_MARKER_SIZE} ${LIVE_OUT_OF_ORDER_MARKER_SIZE}">
      <circle cx="11" cy="11" r="10.25" fill="#ef4444" stroke="#ffffff" stroke-width="1.5"/>
      <path d="M7.25 7.25L14.75 14.75M14.75 7.25L7.25 14.75" stroke="#ffffff" stroke-width="2.2" stroke-linecap="round"/>
    </svg>`,
  },
  fullyOccupied: {
    size: LIVE_FULLY_OCCUPIED_MARKER_SIZE,
    svg: `<svg xmlns="http://www.w3.org/2000/svg" width="${LIVE_FULLY_OCCUPIED_MARKER_SIZE}" height="${LIVE_FULLY_OCCUPIED_MARKER_SIZE}" viewBox="0 0 ${LIVE_FULLY_OCCUPIED_MARKER_SIZE} ${LIVE_FULLY_OCCUPIED_MARKER_SIZE}">
      <circle cx="9" cy="9" r="8" fill="#ffffff" stroke="#f59e0b" stroke-width="2"/>
    </svg>`,
  },
};
const liveStatusMarkerIcons = new Map();
let favoriteStationMarkerIcon = null;
const AMENITY_MAPPING = {
  amenity_restaurant: { label: "Restaurant", icon: "amenity_restaurant.png" },
  amenity_cafe: { label: "Café", icon: "amenity_cafe.png" },
  amenity_fast_food: { label: "Fast Food", icon: "amenity_fast_food.png" },
  amenity_toilets: { label: "Toiletten", icon: "amenity_toilets.png" },
  amenity_supermarket: { label: "Supermarkt", icon: "shop_supermarket.png" },
  amenity_bakery: { label: "Bäckerei", icon: "shop_bakery.png" },
  amenity_convenience: { label: "Kiosk", icon: "shop_convenience.png" },
  amenity_pharmacy: { label: "Apotheke", icon: "amenity_pharmacy.png" },
  amenity_hotel: { label: "Hotel", icon: "tourism_hotel.png" }, // tourism_hotel.png also avail
  amenity_museum: { label: "Museum", icon: "tourism_museum.png" },
  amenity_playground: { label: "Spielplatz", icon: "leisure_playground.png" },
  amenity_park: { label: "Park", icon: "leisure_park.png" },
  amenity_ice_cream: { label: "Eis", icon: "amenity_cafe.png" }, // Not found, maybe generic?
  amenity_bbq: { label: "Grillplatz", icon: "amenity_bbq.png" },
  amenity_biergarten: { label: "Biergarten", icon: "amenity_biergarten.png" },
  amenity_cinema: { label: "Kino", icon: "amenity_cinema.png" },
  amenity_library: { label: "Bibliothek", icon: "amenity_library.png" },
  amenity_theatre: { label: "Theater", icon: "amenity_theatre.png" },
  amenity_atm: { label: "Geldautomat", icon: "amenity_atm.png" },
  amenity_bank: { label: "Bank", icon: "amenity_bank.png" },
  amenity_bench: { label: "Bank (Sitz)", icon: "amenity_bench.png" },
  amenity_bicycle_rental: { label: "Fahrradverleih", icon: "amenity_bicycle_rental.png" },
  amenity_car_sharing: { label: "Car Sharing", icon: "amenity_car_sharing.png" },
  amenity_fuel: { label: "Tankstelle", icon: "amenity_fuel.png" },
  amenity_hospital: { label: "Krankenhaus", icon: "amenity_hospital.png" },
  amenity_police: { label: "Polizei", icon: "amenity_police.png" },
  amenity_post_box: { label: "Briefkasten", icon: "amenity_post_box.png" },
  amenity_post_office: { label: "Post", icon: "amenity_post_office.png" },
  amenity_pub: { label: "Kneipe", icon: "amenity_pub.png" },
  amenity_school: { label: "Schule", icon: "amenity_school.png" },
  amenity_taxi: { label: "Taxi", icon: "amenity_taxi.png" },
  amenity_waste_basket: { label: "Mülleimer", icon: "amenity_waste_basket.png" },
  amenity_swimming: { label: "Schwimmbad", icon: "sport_swimming.png" },
  amenity_gym: { label: "Fitness", icon: "leisure_sports_centre.png" },
  amenity_camp_site: { label: "Camping", icon: "tourism_camp_site.png" },
  amenity_viewpoint: { label: "Aussichtspunkt", icon: "tourism_viewpoint.png" },
  amenity_zoo: { label: "Zoo", icon: "tourism_zoo.png" },
  shop_mall: { label: "Einkaufszentrum", icon: "shop_mall_.png" },
  shop_doityourself: { label: "Baumarkt", icon: "shop_doityourself.png" },
  shop_electronics: { label: "Elektronik", icon: "shop_electronics.png" },
};

const AMENITY_TRANSLATION_KEYS = {
  amenity_restaurant: "restaurant",
  amenity_cafe: "cafe",
  amenity_fast_food: "fast_food",
  amenity_toilets: "toilets",
  amenity_supermarket: "supermarket",
  amenity_bakery: "bakery",
  amenity_convenience: "convenience",
  amenity_pharmacy: "pharmacy",
  amenity_hotel: "hotel",
  amenity_museum: "museum",
  amenity_playground: "playground",
  amenity_park: "park",
  amenity_ice_cream: "ice_cream",
  amenity_bbq: "bbq",
  amenity_biergarten: "biergarten",
  amenity_cinema: "cinema",
  amenity_library: "library",
  amenity_theatre: "theatre",
  amenity_atm: "atm",
  amenity_bank: "bank",
  amenity_bench: "bench",
  amenity_bicycle_rental: "bicycle_rental",
  amenity_car_sharing: "car_sharing",
  amenity_fuel: "fuel",
  amenity_hospital: "hospital",
  amenity_police: "police",
  amenity_post_box: "post_box",
  amenity_post_office: "post_office",
  amenity_pub: "pub",
  amenity_school: "school",
  amenity_taxi: "taxi",
  amenity_waste_basket: "waste_basket",
  amenity_swimming: "swimming",
  amenity_gym: "gym",
  amenity_camp_site: "camp_site",
  amenity_viewpoint: "viewpoint",
  amenity_zoo: "zoo",
  shop_mall: "mall",
  shop_doityourself: "doityourself",
  shop_electronics: "electronics",
};

const AMENITY_GROUPS = [
  {
    label: "Essen & Trinken",
    labelKey: "amenity.groups.food",
    categories: ["restaurant", "cafe", "fast_food", "ice_cream", "bakery"],
  },
  {
    label: "Einkaufsmöglichkeiten",
    labelKey: "amenity.groups.shopping",
    categories: ["supermarket", "convenience", "pharmacy"],
  },
  {
    label: "Freizeit & Natur",
    labelKey: "amenity.groups.leisure",
    categories: ["museum", "playground", "park"],
  },
  {
    label: "Unterkunft",
    labelKey: "amenity.groups.lodging",
    categories: ["hotel"],
  },
  {
    label: "Sonstiges",
    labelKey: "amenity.groups.other",
    categories: [],
  },
];

const AMENITY_GROUP_BY_CATEGORY = new Map(
  AMENITY_GROUPS.flatMap((group) =>
    group.categories.map((category) => [category, group]),
  ),
);

// Fallback for missing icons or just generic usage
function getAmenityIconPath(key) {
  const config = AMENITY_MAPPING[key];
  if (config && config.icon) {
    return `./img/${config.icon}`;
  }
  return null;
}

function formatAmenityCount(count) {
  const numeric = Number(count || 0);
  const rounded = Number.isFinite(numeric) ? Math.round(numeric) : 0;
  return t(rounded === 1 ? "amenity.one" : "amenity.many", { count: rounded });
}

function getAmenityLabel(keyOrCategory) {
  const raw = String(keyOrCategory || "").trim();
  const key = AMENITY_TRANSLATION_KEYS[raw] ||
    AMENITY_TRANSLATION_KEYS[`amenity_${raw}`] ||
    raw.replace(/^amenity_/, "");
  const translated = t(`amenity.labels.${key}`);
  if (translated && translated !== `amenity.labels.${key}`) {
    return translated;
  }
  return AMENITY_MAPPING[raw]?.label || raw.replace(/_/g, " ") || t("amenity.generic");
}

function getAmenityGroupLabel(category) {
  const group = AMENITY_GROUP_BY_CATEGORY.get(category || "");
  return group ? t(group.labelKey) : t("amenity.groups.other");
}
function getAmenityDistance(item) {
  const distance = Number(item?.distance_m);
  return Number.isFinite(distance) ? distance : Number.POSITIVE_INFINITY;
}
function compareAmenityExamples(a, b) {
  const distanceDiff = getAmenityDistance(a) - getAmenityDistance(b);
  if (distanceDiff !== 0) return distanceDiff;
  const categoryDiff = String(a?.category || "").localeCompare(String(b?.category || ""));
  if (categoryDiff !== 0) return categoryDiff;
  return String(a?.name || "").localeCompare(String(b?.name || ""));
}
function formatAmenityOpenStatus(item, date = new Date()) {
  const status = getAmenityOpenStatus(item, date).state;
  if (status === "open") {
    return { label: t("amenity.open"), className: "open" };
  }
  if (status === "closed") {
    return { label: t("amenity.closed"), className: "closed" };
  }
  return { label: t("amenity.unknownHours"), className: "unknown" };
}
function formatAmenityDistance(item) {
  const distance = Number(item?.distance_m);
  if (!Number.isFinite(distance)) return "";
  return `${Math.round(distance)} m`;
}
function openAmenityDetailSheet(item, categoryLabel, now = new Date()) {
  const name = item.name || categoryLabel || t("amenity.generic");
  const openStatus = formatAmenityOpenStatus(item, now);
  const openingHoursText = formatOpeningHoursForDisplay(item.opening_hours, getLocale());

  els.amenitySheet.category.textContent = categoryLabel || t("amenity.generic");
  els.amenitySheet.title.textContent = name;
  els.amenitySheet.status.textContent = openStatus.label;
  els.amenitySheet.status.className = `amenity-sheet-status ${openStatus.className}`;
  els.amenitySheet.hours.textContent = openingHoursText || t("amenity.unknownHours");
  openModal("amenityDetail");
}

function resolveLiveApiBaseUrl() {
  const configuredValue = typeof window.WOLADEN_LIVE_API_BASE_URL === "string"
    ? window.WOLADEN_LIVE_API_BASE_URL.trim()
    : "";
  const resolved = computeLiveApiBaseUrl({
    configuredValue,
    locationHref: window.location.href,
    locationHostname: window.location.hostname,
  });
  if (!resolved && configuredValue) {
    console.warn("Ignoring invalid live API base URL", configuredValue);
  }
  return resolved;
}

function resolveGermanLiveApiBaseUrl() {
  const configuredValue = typeof window.WOLADEN_DE_LIVE_API_BASE_URL === "string"
    ? window.WOLADEN_DE_LIVE_API_BASE_URL.trim()
    : "";
  const resolved = computeGermanLiveApiBaseUrl({
    configuredValue,
    locationHref: window.location.href,
    locationHostname: window.location.hostname,
  });
  if (!resolved && configuredValue) {
    console.warn("Ignoring invalid German live API base URL", configuredValue);
  }
  return resolved;
}

function resolveGeocoderApiBaseUrl() {
  const configuredValue = typeof window.WOLADEN_GEOCODER_API_BASE_URL === "string"
    ? window.WOLADEN_GEOCODER_API_BASE_URL.trim()
    : "";
  const resolved = computeGeocoderApiBaseUrl({
    configuredValue,
    locationHref: window.location.href,
    locationHostname: window.location.hostname,
  });
  if (!resolved && configuredValue) {
    console.warn("Ignoring invalid geocoder API base URL", configuredValue);
  }
  return resolved;
}

const LIVE_API_BASE_URL = resolveLiveApiBaseUrl();
const LIVE_DE_API_BASE_URL = resolveGermanLiveApiBaseUrl();
const GEOCODER_API_BASE_URL = resolveGeocoderApiBaseUrl();

function normalizeAvailabilityStatus(value) {
  const raw = String(value || "").trim();
  if (raw === "free" || raw === "occupied" || raw === "out_of_order") {
    return raw;
  }
  return "unknown";
}

function hasLiveStationSummary(props) {
  const total = Number(props.live_total_evses || 0);
  const fetchedAt = String(
    props.live_source_observed_at || props.live_fetched_at || props.live_ingested_at || "",
  ).trim();
  return Boolean(fetchedAt) || (Number.isFinite(total) && total > 0);
}

function hasAggregateOccupancySummary(props) {
  const counts = getAvailabilityCounts(props);
  return Number.isFinite(counts.total) && counts.total > 0;
}

function getAvailabilityCounts(props) {
  if (hasLiveStationSummary(props)) {
    return {
      total: Number(props.live_total_evses || 0),
      available: Number(props.live_available_evses || 0),
      occupied: Number(props.live_occupied_evses || 0),
      outOfOrder: Number(props.live_out_of_order_evses || 0),
      unknown: Number(props.live_unknown_evses || 0),
    };
  }
  return {
    total: Number(props.occupancy_total_evses || 0),
    available: Number(props.occupancy_available_evses || 0),
    occupied: Number(props.occupancy_occupied_evses || 0),
    outOfOrder: Number(props.occupancy_out_of_order_evses || 0),
    unknown: Number(props.occupancy_unknown_evses || 0),
  };
}

function getAvailabilityStatus(props) {
  const counts = getAvailabilityCounts(props);
  if (hasLiveStationSummary(props)) {
    return normalizeAvailabilityStatus(props.live_availability_status);
  }
  if (counts.available > 0) {
    return "free";
  }
  if (counts.occupied > 0) {
    return "occupied";
  }
  if (counts.total > 0 && counts.outOfOrder >= counts.total) {
    return "out_of_order";
  }
  return "unknown";
}

function formatAvailabilityLabel(status) {
  if (status === "free") {
    return t("availability.free");
  }
  if (status === "occupied") {
    return t("availability.occupied");
  }
  if (status === "out_of_order") {
    return t("availability.out_of_order");
  }
  return t("availability.unknown");
}

function getAvailabilityToneClass(status) {
  return `status-tone-${normalizeAvailabilityStatus(status)}`;
}

function setAvailabilityTone(element, status) {
  if (!element) return;
  element.classList.remove(
    "status-tone-free",
    "status-tone-occupied",
    "status-tone-out_of_order",
    "status-tone-unknown",
  );
  element.classList.add(getAvailabilityToneClass(status));
}

function formatOccupancySummary(props) {
  const counts = getAvailabilityCounts(props);
  const total = counts.total;
  const available = counts.available;
  const occupied = counts.occupied;
  const outOfOrder = counts.outOfOrder;
  const unknown = counts.unknown;

  if (!Number.isFinite(total) || total <= 0) {
    return "";
  }
  const parts = [];
  if (available > 0) {
    parts.push(t("availability.available", { count: Math.round(available) }));
  }
  if (occupied > 0) {
    parts.push(t("availability.occupiedCount", { count: Math.round(occupied) }));
  }
  if (outOfOrder > 0) {
    parts.push(t("availability.outOfOrderCount", { count: Math.round(outOfOrder) }));
  }
  if (unknown > 0) {
    parts.push(t("availability.unknownCount", { count: Math.round(unknown) }));
  }
  return parts.length ? parts.join(", ") : t("availability.summaryUnknown");
}

function getOccupancyObservedAt(props) {
  if (hasLiveStationSummary(props)) {
    return props.live_source_observed_at || props.live_fetched_at || props.live_ingested_at || "";
  }
  return props.occupancy_last_updated || "";
}

function buildAggregateLiveEvses(props) {
  const counts = getAvailabilityCounts(props);
  const liveTotal = Math.max(0, Math.round(Number(counts.total || 0)));
  if (!Number.isFinite(liveTotal) || liveTotal <= 0) {
    return [];
  }

  const available = Math.max(0, Math.round(Number(counts.available || 0)));
  const occupied = Math.max(0, Math.round(Number(counts.occupied || 0)));
  const outOfOrder = Math.max(0, Math.round(Number(counts.outOfOrder || 0)));
  const explicitUnknown = Math.max(0, Math.round(Number(counts.unknown || 0)));
  const knownWithoutUnknown = available + occupied + outOfOrder;
  const observedUnknown = Math.max(explicitUnknown, liveTotal - knownWithoutUnknown);
  const observedTotal = knownWithoutUnknown + observedUnknown;
  const staticTotal = Math.max(0, Math.round(Number(props.charging_points_count || 0)));
  const displayTotal = Math.max(liveTotal, observedTotal, staticTotal);
  const staticMissing = Math.max(0, displayTotal - observedTotal);
  const observedAt = getOccupancyObservedAt(props);

    return [
    ["free", available, ""],
    ["occupied", occupied, ""],
    ["out_of_order", outOfOrder, ""],
    ["unknown", observedUnknown, ""],
    ["unknown", staticMissing, t("station.notInLiveFeed")],
  ].flatMap(([availabilityStatus, count, statusNote]) =>
    Array.from({ length: Math.max(0, Number(count || 0)) }, () => ({
      availability_status: availabilityStatus,
      source_observed_at: statusNote ? "" : observedAt,
      status_note: statusNote,
    })),
  ).slice(0, displayTotal);
}

function formatProviderLabel(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const normalized = raw
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  if (
    normalized === "woladen_bnetza" ||
    normalized === "de_woladen_bnetza" ||
    normalized === "mobilithek_de_woladen_bnetza" ||
    normalized.includes("_woladen_bnetza") ||
    normalized.includes("_bnetza")
  ) {
    return "Mobilithek";
  }
  return raw
    .replace(/^mobilithek_/, "")
    .replace(/_static$/, "")
    .replace(/-json$/i, "")
    .replaceAll("_", " ");
}

function getLiveSourceLabel(props) {
  const sourceName = String(props.detail_source_name || "").trim();
  if (sourceName) {
    return formatProviderLabel(sourceName);
  }
  const sourceUid = String(props.detail_source_uid || "").trim();
  if (sourceUid) {
    return formatProviderLabel(sourceUid);
  }
  return "";
}

function parseLiveTimestamp(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  const date = /^\d{11,}$/.test(raw) ? new Date(Number(raw)) : new Date(raw);
  const time = date.getTime();
  if (!Number.isFinite(time)) {
    return null;
  }
  return { raw, time };
}

function latestLiveTimestamp(values) {
  let latest = null;
  let fallback = "";
  values.forEach((value) => {
    const raw = String(value || "").trim();
    if (!raw) {
      return;
    }
    if (!fallback) {
      fallback = raw;
    }
    const parsed = parseLiveTimestamp(raw);
    if (parsed && (!latest || parsed.time > latest.time)) {
      latest = parsed;
    }
  });
  return latest?.raw || fallback;
}

function getLatestLiveUpdateValue(props, liveDetail = null) {
  const station = liveDetail?.station && typeof liveDetail.station === "object"
    ? liveDetail.station
    : {};
  const evses = Array.isArray(liveDetail?.evses) ? liveDetail.evses : [];
  return latestLiveTimestamp([
    ...evses.flatMap((evse) => [
      evse?.source_observed_at,
      evse?.fetched_at,
      evse?.ingested_at,
    ]),
    station.source_observed_at,
    station.fetched_at,
    station.ingested_at,
    props.live_source_observed_at,
    props.live_fetched_at,
    props.live_ingested_at,
    props.occupancy_last_updated,
  ]);
}

function formatLiveSourceLine(source, timestamp) {
  if (source && timestamp) {
    return t("station.liveViaUpdated", { source, date: timestamp });
  }
  if (source) {
    return t("station.liveVia", { source });
  }
  if (timestamp) {
    return t("station.updated", { date: timestamp });
  }
  return "";
}

function formatOccupancySource(props, liveDetail = null) {
  const hasLiveDetailRows = Array.isArray(liveDetail?.evses) && liveDetail.evses.length > 0;
  if (hasLiveStationSummary(props) || hasLiveDetailRows) {
    const provider = getLiveSourceLabel(props);
    const timestamp = formatDetailTimestamp(
      getLatestLiveUpdateValue(props, liveDetail),
    );
    return formatLiveSourceLine(provider, timestamp) || t("station.liveDataAvailable");
  }

  const counts = getAvailabilityCounts(props);
  if (!Number.isFinite(counts.total) || counts.total <= 0) {
    return "";
  }
  const sourceUid = String(props.occupancy_source_uid || "").trim();
  const sourceName = String(props.occupancy_source_name || "").trim();
  const timestamp = formatDetailTimestamp(getLatestLiveUpdateValue(props, liveDetail) || getOccupancyObservedAt(props));
  const formattedSourceName = formatProviderLabel(sourceName);
  const formattedSourceUid = formatProviderLabel(sourceUid);
  if (formattedSourceName === "Mobilithek" || formattedSourceUid === "Mobilithek") {
    return formatLiveSourceLine("Mobilithek", timestamp);
  }
  if (sourceName.startsWith("Mobilithek")) {
    return formatLiveSourceLine(sourceName, timestamp);
  }
  if (sourceUid.startsWith("mobilithek_")) {
    const source = formattedSourceName ? `Mobilithek (${formattedSourceName})` : "Mobilithek";
    return formatLiveSourceLine(source, timestamp);
  }
  if (sourceName) {
    return formatLiveSourceLine(`MobiData BW (${formattedSourceName})`, timestamp);
  }
  return formatLiveSourceLine("MobiData BW", timestamp);
}

function formatDetailTimestamp(value) {
  return formatDateTime(value);
}

function formatStaticDetailSource(props) {
  const sourceName = String(props.detail_source_name || "").trim();
  const timestamp = formatDetailTimestamp(props.detail_last_updated);
  if (!sourceName && !timestamp) {
    return "";
  }
  if (sourceName && timestamp) {
    return t("station.detailsSource", { source: sourceName, date: timestamp });
  }
  if (sourceName) {
    return t("station.detailsSourceOnly", { source: sourceName });
  }
  return t("station.updated", { date: timestamp });
}

function formatTelephoneHref(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const normalized = raw.replace(/[^+\d]/g, "");
  return normalized ? `tel:${normalized}` : "";
}

function buildStaticDetailRows(props) {
  const rows = [];
  const pushRow = (label, value) => {
    const text = String(value || "").trim();
    if (!text) return;
    rows.push({ label, value: text });
  };

  pushRow(t("staticDetails.payment"), props.payment_methods_display);
  pushRow(t("staticDetails.access"), props.auth_methods_display);
  pushRow(t("staticDetails.connectors"), props.connector_types_display);
  pushRow(t("staticDetails.currentType"), props.current_types_display);
  const connectorCount = Number(props.connector_count || 0);
  if (Number.isFinite(connectorCount) && connectorCount > 0) {
    pushRow(t("staticDetails.connectors"), t("staticDetails.sockets", { count: Math.round(connectorCount) }));
  }
  pushRow(t("staticDetails.service"), props.service_types_display);

  if (props.green_energy === true) {
    pushRow(t("staticDetails.energy"), t("staticDetails.renewable"));
  } else if (props.green_energy === false) {
    pushRow(t("staticDetails.energy"), t("staticDetails.notRenewable"));
  }

  return rows;
}

function getLiveDetailPrice(liveDetail = null) {
  const stationPrice = String(liveDetail?.station?.price_display || "").trim();
  if (stationPrice) {
    return stationPrice;
  }

  const evses = Array.isArray(liveDetail?.evses) ? liveDetail.evses : [];
  const uniquePrices = Array.from(new Set(
    evses
      .map((evse) => String(evse?.price_display || "").trim())
      .filter(Boolean),
  ));
  if (uniquePrices.length > 0) {
    return uniquePrices[0];
  }
  return "";
}

function getDisplayPrice(props, liveDetail = null) {
  const livePrice = String(props.live_price_display || "").trim();
  if (livePrice) {
    return livePrice;
  }
  const liveDetailPrice = getLiveDetailPrice(liveDetail);
  if (liveDetailPrice) {
    return liveDetailPrice;
  }
  return String(props.price_display || "").trim();
}

function getStationIdFromProps(props) {
  return String(props?.station_id || "").trim();
}

function applyLiveStationSummaryToProps(props, summary) {
  if (!props || !summary) return;
  LIVE_STATION_FIELDS.forEach((key) => {
    props[`live_${key}`] = summary[key];
  });
}

const CATALOG_LIVE_SIGNAL_FIELDS = [
  "availability_status",
  "available_evses",
  "occupied_evses",
  "out_of_order_evses",
  "unknown_evses",
  "total_evses",
  "source_observed_at",
  "fetched_at",
  "ingested_at",
];

function hasCatalogLiveStationSummary(station) {
  if (!station || typeof station !== "object") {
    return false;
  }
  return CATALOG_LIVE_SIGNAL_FIELDS.some((key) => {
    const value = station[key];
    return value !== undefined && value !== null && String(value).trim() !== "";
  });
}

function catalogLiveStationSummary(station, stationId = "") {
  if (!hasCatalogLiveStationSummary(station)) {
    return null;
  }
  const summary = {
    station_id: normalizeStationId(stationId || station?.station_id || ""),
  };
  LIVE_STATION_FIELDS.forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(station, key)) {
      summary[key] = station[key];
    }
  });
  summary.availability_status = normalizeAvailabilityStatus(summary.availability_status);
  return summary;
}

function upsertCatalogLiveStationSummary(stationId, summary) {
  const normalizedStationId = normalizeStationId(stationId || summary?.station_id || "");
  if (!normalizedStationId || !summary) {
    return null;
  }
  const normalizedSummary = {
    ...summary,
    station_id: normalizedStationId,
  };
  state.live.summaryByStationId.set(normalizedStationId, normalizedSummary);
  state.live.summaryFetchedAtByStationId.set(normalizedStationId, Date.now());
  return normalizedSummary;
}

function markCatalogLiveStationSummaryMissing(stationId) {
  const normalizedStationId = normalizeStationId(stationId || "");
  if (!normalizedStationId) {
    return;
  }
  state.live.summaryByStationId.delete(normalizedStationId);
  state.live.summaryFetchedAtByStationId.set(normalizedStationId, Date.now());
}

function applyCachedLiveStationSummaryToFeature(feature) {
  const props = feature?.properties;
  const stationId = getStationIdFromProps(props);
  if (!stationId) {
    return false;
  }
  const summary = state.live.summaryByStationId.get(stationId) ||
    state.live.summaryByStationId.get(toLiveApiStationId(stationId));
  if (!summary) {
    return false;
  }
  applyLiveStationSummaryToProps(props, summary);
  return true;
}

function clearLiveStationSummaryFromProps(props) {
  if (!props) return;
  LIVE_STATION_FIELDS.forEach((key) => {
    delete props[`live_${key}`];
  });
}

function formatEvseCode(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  if (raw.length <= 20) {
    return raw;
  }
  return `${raw.slice(0, 10)}…${raw.slice(-6)}`;
}

function parseLiveJsonCollection(value) {
  if (Array.isArray(value)) {
    return value;
  }
  if (value && typeof value === "object") {
    return [value];
  }
  const raw = String(value || "").trim();
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed;
    }
    if (parsed && typeof parsed === "object") {
      return [parsed];
    }
    return parsed === null || parsed === "" ? [] : [parsed];
  } catch {
    return [raw];
  }
}

function humanizeLiveCodeText(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const spaced = raw
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .trim();
  if (!spaced) {
    return "";
  }
  return `${spaced.charAt(0).toUpperCase()}${spaced.slice(1)}`;
}

function formatLiveDetailScalar(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "boolean") {
    return value ? t("common.yes") : t("common.no");
  }
  if (typeof value === "number") {
    return String(value);
  }
  const raw = String(value).trim();
  if (!raw) {
    return "";
  }
  const timestamp = formatDetailTimestamp(raw);
  if (timestamp && timestamp !== raw) {
    return timestamp;
  }
  return humanizeLiveCodeText(raw);
}

function formatLiveDetailCollection(value) {
  const items = parseLiveJsonCollection(value);
  return items
    .map((item) => {
      if (Array.isArray(item)) {
        return formatLiveDetailCollection(item);
      }
      if (item && typeof item === "object") {
        return formatLiveDetailObject(item);
      }
      return formatLiveDetailScalar(item);
    })
    .filter(Boolean)
    .join(" • ");
}

function formatLiveDetailObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return formatLiveDetailScalar(value);
  }
  const entries = Object.entries(value).filter(([, entryValue]) => {
    if (entryValue === null || entryValue === undefined) {
      return false;
    }
    if (typeof entryValue === "string") {
      return Boolean(entryValue.trim());
    }
    if (Array.isArray(entryValue)) {
      return entryValue.length > 0;
    }
    if (typeof entryValue === "object") {
      return Object.keys(entryValue).length > 0;
    }
    return true;
  });
  if (entries.length === 0) {
    return "";
  }
  if (entries.length === 1 && entries[0][0] === "value") {
    return formatLiveDetailScalar(entries[0][1]);
  }
  return entries
    .map(([key, entryValue]) => {
      const formatted = Array.isArray(entryValue) || (entryValue && typeof entryValue === "object")
        ? formatLiveDetailCollection(entryValue)
        : formatLiveDetailScalar(entryValue);
      if (!formatted) {
        return "";
      }
      const label = LIVE_DYNAMIC_KEY_LABELS[key] ?? humanizeLiveCodeText(key);
      return label ? `${label}: ${formatted}` : formatted;
    })
    .filter(Boolean)
    .join(", ");
}

function buildLiveDynamicNotes(evse) {
  const notes = [];
  const nextSlotText = formatLiveDetailCollection(evse.next_available_charging_slots);
  if (nextSlotText) {
    notes.push({ label: t("station.nextSlot"), value: nextSlotText });
  }
  const supplementalText = formatLiveDetailCollection(evse.supplemental_facility_status);
  if (supplementalText) {
    notes.push({ label: t("station.supplementalStatus"), value: supplementalText });
  }
  return notes;
}

/* --- STATE --- */
const state = {
  features: [], // All charger features
  staticFeatures: [], // Static fast-charger fallback features
  filtered: [], // Currently filtered features
  favoriteMetadata: createEmptyFavoriteMetadata(),
  favorites: new Set(), // Set of station_ids
  favoriteCategoryFilter: FAVORITE_FILTER_ALL,
  ratings: new Map(), // station_id -> 1-5 rating stored locally
  notes: new Map(), // station_id -> personal note stored locally
  favoriteSort: FAVORITE_SORT_DISTANCE,
  ratingClientId: "",
  ratingSummariesByStationId: new Map(),
  ratingSummaryFetchedAtByStationId: new Map(),
  pendingRatingSummaryStationIds: new Set(),
  pendingRatingSubmissions: new Set(),
  ratingSubmissionErrors: new Map(),
  userPos: null, // { lat, lon }
  startupLocationRequested: false,
  location: {
    permissionState: LOCATION_PERMISSION_UNKNOWN,
    requestState: LOCATION_REQUEST_IDLE,
    errorCode: "",
  },
  filters: {
    operator: DEFAULT_FILTER_SETTINGS.operator,
    minPower: DEFAULT_MIN_POWER_KW,
    minAmenityCount: DEFAULT_FILTER_SETTINGS.minAmenityCount,
    amenities: new Set(DEFAULT_FILTER_SETTINGS.amenities),
    amenityNameQuery: DEFAULT_FILTER_SETTINGS.amenityNameQuery,
    availableOnly: DEFAULT_FILTER_SETTINGS.availableOnly,
    currentlyOpenOnly: DEFAULT_FILTER_SETTINGS.currentlyOpenOnly,
  },
  live: {
    baseUrl: LIVE_API_BASE_URL,
    deBaseUrl: LIVE_DE_API_BASE_URL,
    summaryByStationId: new Map(),
    summaryFetchedAtByStationId: new Map(),
    pendingSummaryStationIds: new Set(),
    detailByStationId: new Map(),
    reachable: false,
  },
  catalog: {
    loading: false,
    error: null,
    lastQueryKey: "",
    lastResultCount: null,
    center: null, // { lat, lon, source }
    detailByStationId: new Map(),
    pendingDetailStationIds: new Set(),
    missingDetailStationIds: new Set(),
  },
  search: {
    activeResult: null,
    loading: false,
    requestSeq: 0,
    results: [],
    suggestionTimer: 0,
  },
  route: {
    endpoints: {
      origin: null,
      destination: null,
    },
    suggestions: {
      origin: [],
      destination: [],
    },
    loading: false,
    requestSeq: 0,
    suggestionSeq: 0,
    suggestionTimers: {
      origin: 0,
      destination: 0,
    },
    result: null,
    features: [],
    error: null,
    calculatedFilters: null,
  },
  keyboard: {
    selectedStationId: "",
  },
  mapFocus: {
    stationId: "",
  },
  mapInteraction: {
    hasUserInteracted: false,
  },
  analytics: {
    oftenBrokenStationIds: new Set(),
    oftenOccupiedStationIds: new Set(),
  },
  easterEgg: {
    active: false,
    overlay: null,
    canvas: null,
    ctx: null,
    core: null,
    animationFrame: 0,
    frame: 0,
    width: 0,
    height: 0,
    audioContext: null,
    audioSource: null,
    audioGain: null,
    audioBuffer: null,
    machine: null,
  },
  modal: {
    lastFocusedByName: new Map(),
  },
  occupancyHistory: {
    byStationId: new Map(),
    availableStationIds: null,
    manifestPromise: null,
    pendingStationIds: new Set(),
    missingStationIds: new Set(),
  },
  data: {
    geoData: null,
    summaryData: null,
    openStaticSummaryData: null,
    managementSnapshotData: null,
  },
  views: {
    map: null, // Leaflet map instance
    detailMap: null, // Mini map in detail view
    layers: {
      chargers: null,
      route: null,
      user: null,
      detailAmenities: null,
    },
    markersByStationId: new Map(),
  },
};

/* --- DOM ELEMENTS --- */
const els = {
  app: document.getElementById("app"),
  views: {
    map: document.getElementById("view-map"),
    list: document.getElementById("view-list"),
    route: document.getElementById("view-route"),
    favorites: document.getElementById("view-favorites"),
    info: document.getElementById("view-info"),
  },
  navItems: document.querySelectorAll(".nav-item"),
  modals: {
    filter: document.getElementById("modal-filter"),
    detail: document.getElementById("modal-detail"),
    amenityDetail: document.getElementById("modal-amenity-detail"),
  },
  lists: {
    chargers: document.getElementById("charger-list"),
    favorites: document.getElementById("favorites-list"),
  },
  favorites: {
    sort: document.getElementById("favorites-sort"),
    categoryFilters: document.getElementById("favorites-category-filters"),
  },
  filter: {
    trigger: document.getElementById("filter-trigger"),
    label: document.getElementById("filter-label"),
    count: document.getElementById("filter-count"),
    activeLabel: document.getElementById("filter-active-label"),
    operator: document.getElementById("filter-operator"),
    amenityName: document.getElementById("filter-amenity-name"),
    availableOnly: document.getElementById("filter-available-only"),
    currentlyOpen: document.getElementById("filter-currently-open"),
    power: document.getElementById("filter-power"),
    powerLabel: document.getElementById("filter-power-label"),
    powerVal: document.getElementById("filter-power-val"),
    amenityCount: document.getElementById("filter-amenity-count"),
    amenityCountLabel: document.getElementById("filter-amenity-count-label"),
    amenityCountVal: document.getElementById("filter-amenity-count-val"),
    amenities: document.getElementById("filter-amenities"),
    applyBtn: document.getElementById("btn-apply-filter"),
    listFilterBtn: document.getElementById("btn-list-filter"),
    routeFilterBtn: document.getElementById("btn-route-filter"),
    activeSummary: document.getElementById("active-filter-summary"),
  },
  route: {
    form: document.getElementById("route-form"),
    originInput: document.getElementById("route-origin-input"),
    originResults: document.getElementById("route-origin-results"),
    originCurrent: document.getElementById("route-origin-current"),
    destinationInput: document.getElementById("route-destination-input"),
    destinationResults: document.getElementById("route-destination-results"),
    destinationCurrent: document.getElementById("route-destination-current"),
    swapBtn: document.getElementById("route-swap"),
    submitBtn: document.getElementById("route-submit"),
    favoriteAllBtn: document.getElementById("route-favorite-all"),
    status: document.getElementById("route-status"),
    summary: document.getElementById("route-summary"),
    results: document.getElementById("route-results"),
  },
  routeMap: {
    lock: document.getElementById("route-map-lock"),
    clearBtn: document.getElementById("route-map-clear"),
  },
  search: {
    form: document.getElementById("location-search-form"),
    input: document.getElementById("location-search-input"),
    results: document.getElementById("location-search-results"),
  },
  detail: {
    title: document.getElementById("detail-title"),
    address: document.getElementById("detail-address"),
    powerChip: document.getElementById("detail-power-chip"),
    power: document.getElementById("detail-power"),
    occupancy: document.getElementById("detail-occupancy"),
    occupancyPill: document.getElementById("detail-occupancy-pill"),
    occupancySource: document.getElementById("detail-occupancy-source"),
    highlights: document.getElementById("detail-highlights"),
    priceChip: document.getElementById("detail-price-chip"),
    price: document.getElementById("detail-price"),
    hoursChip: document.getElementById("detail-hours-chip"),
    hours: document.getElementById("detail-hours"),
    ratingBadge: document.getElementById("detail-rating-badge"),
    ratingStatus: document.getElementById("detail-rating-status"),
    ratingStars: document.getElementById("detail-rating-stars"),
    noteInput: document.getElementById("detail-note-input"),
    noteStatus: document.getElementById("detail-note-status"),
    categoryEditor: document.getElementById("detail-favorite-categories"),
    categoryChips: document.getElementById("detail-category-chips"),
    categoryInput: document.getElementById("detail-category-input"),
    categoryAddBtn: document.getElementById("detail-category-add"),
    categorySuggestions: document.getElementById("detail-category-suggestions"),
    categoryStatus: document.getElementById("detail-category-status"),
    amenityTitle: document.getElementById("detail-amenities-title"),
    amenityList: document.getElementById("detail-amenities-list"),
    detailsSection: document.getElementById("detail-details-section"),
    detailsList: document.getElementById("detail-details-list"),
    detailsSource: document.getElementById("detail-details-source"),
    liveSection: document.getElementById("detail-live-section"),
    liveTitle: document.getElementById("detail-live-title"),
    liveUpdated: document.getElementById("detail-live-updated"),
    liveList: document.getElementById("detail-live-list"),
    occupancyHistorySection: document.getElementById("detail-occupancy-history-section"),
    occupancyHistoryRange: document.getElementById("detail-occupancy-history-range"),
    occupancyHistoryChart: document.getElementById("detail-occupancy-history-chart"),
    favBtn: document.getElementById("btn-toggle-fav"),
    googleBtn: document.getElementById("btn-nav-google"),
    appleBtn: document.getElementById("btn-nav-apple"),
    helpdeskPhoneBtn: document.getElementById("btn-helpdesk-phone"),
    mapContainer: document.getElementById("detail-map"),
  },
  amenitySheet: {
    category: document.getElementById("amenity-sheet-category"),
    title: document.getElementById("amenity-sheet-title"),
    status: document.getElementById("amenity-sheet-status"),
    hours: document.getElementById("amenity-sheet-hours"),
  },
  buttons: {
    locate: document.getElementById("btn-locate"),
    closeFilter: document.querySelector('[data-close="modal-filter"]'),
    closeDetail: document.querySelector('[data-close="modal-detail"]'),
    closeAmenityDetail: document.querySelector('[data-close="modal-amenity-detail"]'),
  },
  meta: document.getElementById("app-meta"),
  languageSelect: document.getElementById("language-select"),
  info: {
    stationCount: document.getElementById("bundle-station-count"),
    chargerCount: document.getElementById("bundle-charger-count"),
    mappedCountries: document.getElementById("mapped-country-list"),
    dataSources: document.getElementById("data-source-list"),
  },
};

const VIEW_ORDER = ["view-list", "view-map", "view-route", "view-favorites", "view-info"];
const VIEW_IDS = new Set(VIEW_ORDER);
const VIEW_HASH_ALIASES = new Map([
  ["list", "view-list"],
  ["liste", "view-list"],
  ["map", "view-map"],
  ["karte", "view-map"],
  ["route", "view-route"],
  ["routes", "view-route"],
  ["routing", "view-route"],
  ["route-planer", "view-route"],
  ["route-planner", "view-route"],
  ["favorites", "view-favorites"],
  ["favoriten", "view-favorites"],
  ["info", "view-info"],
]);
const INITIAL_REQUESTED_VIEW_ID = normalizeRequestedViewId(window.location.hash);
let hasAppliedInitialRequestedView = false;

/* --- INITIALIZATION --- */
async function init() {
  initLanguageControls();
  loadFavorites();
  loadRatings();
  loadNotes();
  loadFilters();
  initMap();
  initNavigation();
  syncViewWithRequestedHash();
  initFilters();
  initLocationSearch();
  initRoutePlanner();
  window.addEventListener("popstate", syncDetailModalWithUrl);
  window.addEventListener("hashchange", syncViewWithRequestedHash);

  // Event Listeners
  els.buttons.locate.addEventListener("click", requestUserLocation);
  els.filter.trigger.addEventListener("click", () => openModal("filter"));
  els.filter.listFilterBtn.addEventListener("click", () => openModal("filter"));
  els.filter.routeFilterBtn?.addEventListener("click", () => openModal("filter"));
  els.filter.applyBtn.addEventListener("click", () => closeModal("filter"));
  els.route.favoriteAllBtn?.addEventListener("click", addRouteResultsToFavorites);
  els.routeMap.clearBtn?.addEventListener("click", clearRoute);

  els.buttons.closeFilter.addEventListener("click", () => closeModal("filter"));
  els.buttons.closeDetail.addEventListener("click", () => closeModal("detail"));
  els.buttons.closeAmenityDetail.addEventListener("click", () => closeModal("amenityDetail"));

  // Close modals on backdrop click
  Object.entries(els.modals).forEach(([name, modal]) => {
    modal.addEventListener("click", (e) => {
      if (e.target === modal) closeModal(name);
    });
  });

  els.detail.favBtn.addEventListener("click", toggleDetailFavorite);
  els.detail.ratingStars.addEventListener("click", handleRatingClick);
  els.detail.noteInput.addEventListener("input", handleDetailNoteInput);
  els.detail.categoryInput?.addEventListener("input", renderDetailCategorySuggestions);
  els.detail.categoryInput?.addEventListener("keydown", handleDetailCategoryKeydown);
  els.detail.categoryAddBtn?.addEventListener("click", () => addDetailFavoriteCategory());
  els.detail.categorySuggestions?.addEventListener("click", handleDetailCategorySuggestionClick);
  els.detail.categoryChips?.addEventListener("click", handleDetailCategoryChipClick);
  els.favorites.sort.addEventListener("change", handleFavoriteSortChange);
  document.addEventListener("keydown", handleGlobalKeydown);
  document.addEventListener("keyup", handleGlobalKeyup);
  window.addEventListener("blur", stopEasterEgg);
  document.addEventListener("visibilitychange", () => {
    if (document.hidden) {
      stopEasterEgg();
      stopMapGPSRefresh();
    } else if (isMapViewActive()) {
      startMapGPSRefresh();
    }
  });

  // Load Data
  await loadData();
}

function initLanguageControls() {
  applyDocumentTranslations();
  populateLanguageSelect(els.languageSelect);
  if (els.languageSelect) {
    els.languageSelect.addEventListener("change", (event) => {
      void setLanguage(event.target.value);
    });
  }
  onLanguageChange(refreshLanguageSensitiveViews);
}

function refreshLanguageSensitiveViews() {
  updatePowerFilterLabel();
  populateOperators();
  renderAmenityFilters();
  setActiveNavItem(getActiveViewId());
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
  if (state.route.result) {
    if (els.views.route.classList.contains("active")) {
      renderRouteResults();
    } else {
      renderRouteLayer();
    }
  }
  renderRouteMapLock();
  if (currentDetailFeature && !els.modals.detail.classList.contains("hidden")) {
    const stationId = getStationIdFromProps(currentDetailFeature.properties);
    populateDetailContent(currentDetailFeature, state.live.detailByStationId.get(stationId) || null);
  }
  if (state.data.summaryData) {
    setAppMeta(
      state.data.geoData,
      state.data.summaryData,
      state.data.openStaticSummaryData,
    );
  }
}

/* --- DATA LOADING --- */
let catalogSearchSequence = 0;
let catalogMapMoveTimer = 0;
let mapGPSRefreshTimer = 0;

async function fetchOptionalJson(path) {
  try {
    const response = await fetch(path);
    if (!response.ok) {
      return null;
    }
    return await response.json();
  } catch (error) {
    return null;
  }
}

function latestManagementSnapshotPath(indexData) {
  const snapshotPaths = indexData?.snapshot_paths;
  if (!snapshotPaths || typeof snapshotPaths !== "object") {
    return "";
  }
  const latestDate = String(indexData?.latest_date || "").trim() ||
    [...(Array.isArray(indexData?.available_dates) ? indexData.available_dates : [])].pop();
  const relativePath = latestDate ? snapshotPaths[latestDate] : "";
  return String(relativePath || "").trim();
}

async function loadLatestManagementSnapshot(indexData) {
  const relativePath = latestManagementSnapshotPath(indexData);
  if (!relativePath || relativePath.includes("..")) {
    return null;
  }
  return fetchOptionalJson(`./data/management/${relativePath}`);
}

function stationIdsFromAnalyticsRows(rows) {
  if (!Array.isArray(rows)) {
    return new Set();
  }
  return new Set(
    rows
      .map((row) => normalizeStationId(row?.station_id || row?.stationId || ""))
      .filter(Boolean),
  );
}

function setAnalyticsStationStates(snapshotData) {
  state.analytics.oftenBrokenStationIds = stationIdsFromAnalyticsRows(snapshotData?.broken_stations);
  state.analytics.oftenOccupiedStationIds = stationIdsFromAnalyticsRows(snapshotData?.busiest_stations);
}

async function loadData() {
  try {
    const [summaryRes, openStaticSummaryData, staticGeoData, managementIndexData] = await Promise.all([
      fetch("./data/summary.json"),
      fetchOptionalJson("./data/open_static_summary.json"),
      fetchOptionalJson("./data/chargers_fast.geojson"),
      fetchOptionalJson("./data/management/index.json"),
    ]);
    if (!summaryRes.ok) throw new Error("Network response was not ok");
    const summaryData = await summaryRes.json();
    const managementSnapshotData = await loadLatestManagementSnapshot(managementIndexData);
    state.data.geoData = staticGeoData;
    state.data.summaryData = summaryData;
    state.data.openStaticSummaryData = openStaticSummaryData;
    state.data.managementSnapshotData = managementSnapshotData;
    setAnalyticsStationStates(managementSnapshotData);
    state.staticFeatures = normalizeStaticFallbackFeatures(staticGeoData);

    populateOperators();
    setAppMeta(staticGeoData, summaryData, openStaticSummaryData);
    renderAmenityFilters(); // Render dynamic amenity filters
    await loadStaticRatingSummaries(summaryData);
    await syncLocationPermissionState();

    applyFilters(); // Initial location gate render
    updateFilterLabel();
    syncDetailModalWithUrl();

    // Request location once after data is ready, but only when the page is visible.
    // This is more reliable on restores/background loads than a single immediate call.
    queueStartupLocationRequest();
  } catch (err) {
    console.error("Failed to load data", err);
    els.lists.chargers.innerHTML = `<div class="empty-state">${escapeHtml(t("errors.dataLoad"))}<br>${escapeHtml(err.message)}</div>`;
  }
}

function normalizeStaticFallbackFeatures(geoData) {
  const features = Array.isArray(geoData?.features) ? geoData.features : [];
  return features
    .map((feature) => prepareChargerFeature(feature, "fast"))
    .filter((feature) => {
      const stationId = getStationIdFromProps(feature.properties);
      const [lon, lat] = feature.geometry?.coordinates || [];
      return Boolean(stationId) && Number.isFinite(Number(lat)) && Number.isFinite(Number(lon));
    });
}

async function loadStaticRatingSummaries(summaryData) {
  const hasStaticRatings = Boolean(
    summaryData?.ratings?.available ||
    Number(summaryData?.records?.station_ratings_total || 0) > 0
  );
  if (!hasStaticRatings) {
    return;
  }

  try {
    const response = await fetch("./data/station_ratings.json");
    if (!response.ok) {
      return;
    }
    const payload = await response.json();
    const ratings = Array.isArray(payload?.ratings) ? payload.ratings : [];
    upsertRatingSummaries(ratings);
  } catch (err) {
    console.warn("Failed to load static station ratings", err);
  }
}

function geocoderApiUrl(path, params = {}) {
  return buildGeocoderApiUrl(GEOCODER_API_BASE_URL, path, params);
}

function initLocationSearch() {
  if (!els.search.form || !els.search.input || !els.search.results) {
    return;
  }
  els.search.form.addEventListener("submit", (event) => {
    event.preventDefault();
    submitLocationSearch({ returnToMap: els.views.map.classList.contains("active") });
  });
  els.search.input.addEventListener("input", queueLocationSuggestions);
  els.search.input.addEventListener("focus", () => {
    if (state.search.results.length > 0) {
      renderLocationSearchResults(state.search.results);
    }
  });
  els.search.input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      submitLocationSearch({ returnToMap: els.views.map.classList.contains("active") });
      return;
    }
    if (event.key === "Escape") {
      clearLocationSearchResults();
      els.search.input.blur();
    }
  });
  document.addEventListener("click", (event) => {
    if (!els.search.form.contains(event.target)) {
      clearLocationSearchResults();
    }
  });
}

function submitLocationSearch(options = {}) {
  const { returnToMap = false } = options;
  cancelQueuedLocationSuggestions();
  void runLocationAutocomplete(els.search.input?.value, { selectFirst: true });
  if (returnToMap) {
    focusMapKeyboardNavigation();
  }
}

function focusLocationSearchInput() {
  if (!els.search.input) {
    return;
  }
  els.search.input.focus({ preventScroll: true });
  els.search.input.select();
}

function focusMapKeyboardNavigation() {
  if (!els.views.map.classList.contains("active")) {
    return;
  }
  els.search.input?.blur();
  const map = state.views.map;
  const mapContainer = map && typeof map.getContainer === "function"
    ? map.getContainer()
    : null;
  if (mapContainer && typeof mapContainer.focus === "function") {
    mapContainer.focus({ preventScroll: true });
  }
}

function getLocationSearchFocus() {
  if (hasResolvedUserLocation()) {
    return state.userPos;
  }
  if (!state.views.map) {
    return null;
  }
  const center = state.views.map.getCenter();
  return { lat: center.lat, lon: center.lng };
}

function queueLocationSuggestions() {
  cancelQueuedLocationSuggestions();
  const query = String(els.search.input?.value || "").trim();
  if (query.length < 3) {
    clearLocationSearchResults();
    return;
  }
  state.search.suggestionTimer = window.setTimeout(() => {
    state.search.suggestionTimer = 0;
    void runLocationAutocomplete(query);
  }, GEOCODER_SUGGESTION_DEBOUNCE_MS);
}

function cancelQueuedLocationSuggestions() {
  if (!state.search.suggestionTimer) {
    return;
  }
  window.clearTimeout(state.search.suggestionTimer);
  state.search.suggestionTimer = 0;
}

function clearLocationSearchResults() {
  if (!els.search.results) {
    return;
  }
  cancelQueuedLocationSuggestions();
  state.search.requestSeq += 1;
  state.search.loading = false;
  state.search.results = [];
  els.search.results.hidden = true;
  els.search.results.replaceChildren();
  els.search.input?.setAttribute("aria-expanded", "false");
}

function renderLocationSearchMessage(message, tone = "muted") {
  if (!els.search.results) {
    return;
  }
  const item = document.createElement("div");
  item.className = `location-search-message location-search-message-${tone}`;
  item.textContent = message;
  els.search.results.replaceChildren(item);
  els.search.results.hidden = false;
  els.search.input?.setAttribute("aria-expanded", "true");
}

function renderLocationSearchResults(results) {
  if (!els.search.results) {
    return;
  }
  els.search.results.replaceChildren();
  if (!results.length) {
    renderLocationSearchMessage(t("search.noResults"));
    return;
  }
  const list = document.createElement("div");
  list.className = "location-search-result-list";
  list.setAttribute("role", "listbox");
  results.forEach((result, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "location-search-result";
    button.setAttribute("role", "option");
    button.dataset.index = String(index);
    const title = document.createElement("span");
    title.className = "location-search-result-title";
    title.textContent = result.name || result.label;
    const meta = document.createElement("span");
    meta.className = "location-search-result-meta";
    meta.textContent = [
      result.locality && result.locality !== result.name ? result.locality : "",
      result.region,
      result.country,
    ].filter(Boolean).join(" · ") || result.label;
    button.append(title, meta);
    button.addEventListener("click", () => selectLocationSearchResult(result));
    list.appendChild(button);
  });
  els.search.results.appendChild(list);
  els.search.results.hidden = false;
  els.search.input?.setAttribute("aria-expanded", "true");
}

async function runLocationAutocomplete(rawQuery, options = {}) {
  const { selectFirst = false } = options;
  const query = String(rawQuery || "").trim();
  if (query.length < 2) {
    renderLocationSearchMessage(t("search.minChars"));
    return;
  }

  const requestId = ++state.search.requestSeq;
  state.search.loading = true;
  if (selectFirst) {
    renderLocationSearchMessage(t("search.searching"));
  }
  const focus = getLocationSearchFocus();
  const url = geocoderApiUrl("autocomplete", {
    q: query,
    lat: focus?.lat,
    lon: focus?.lon,
    limit: 5,
  });
  if (!url) {
    state.search.loading = false;
    renderLocationSearchMessage(t("search.notConfigured"), "error");
    return;
  }

  try {
    const payload = normalizeGeocodePayload(
      await fetchJsonWithTimeout(url, {}, GEOCODER_API_TIMEOUT_MS),
    );
    if (requestId !== state.search.requestSeq) {
      return;
    }
    state.search.results = payload.results;
    state.search.loading = false;
    if (!payload.ok) {
      renderLocationSearchMessage(t("search.unavailable"), "error");
      return;
    }
    if (selectFirst && payload.results.length > 0) {
      selectLocationSearchResult(payload.results[0]);
      return;
    }
    renderLocationSearchResults(payload.results);
  } catch (error) {
    if (requestId !== state.search.requestSeq) {
      return;
    }
    state.search.loading = false;
    renderLocationSearchMessage(t("search.unavailable"), "error");
  }
}

function selectLocationSearchResult(result) {
  if (!result || !Number.isFinite(result.lat) || !Number.isFinite(result.lon)) {
    return;
  }
  state.search.activeResult = result;
  if (els.search.input) {
    els.search.input.value = result.label;
  }
  clearLocationSearchResults();
  if (!hasPinnedRouteMap()) {
    setCatalogSearchCenter({ lat: result.lat, lon: result.lon }, "search");
    void loadCatalogStationsForCurrentCenter({ force: true, reset: true });
  }
  if (!state.views.map) {
    return;
  }
  const zoom = Math.max(state.views.map.getZoom(), 12);
  state.views.map.flyTo([result.lat, result.lon], zoom, {
    animate: true,
    duration: 0.5,
  });
}

const ROUTE_FIELDS = ["origin", "destination"];

function routeFieldElements(field) {
  if (field === "origin") {
    return {
      input: els.route.originInput,
      results: els.route.originResults,
      current: els.route.originCurrent,
    };
  }
  return {
    input: els.route.destinationInput,
    results: els.route.destinationResults,
    current: els.route.destinationCurrent,
  };
}

function initRoutePlanner() {
  if (!els.route.form) {
    return;
  }

  ROUTE_FIELDS.forEach((field) => {
    const controls = routeFieldElements(field);
    controls.input?.addEventListener("input", () => {
      state.route.endpoints[field] = null;
      queueRouteSuggestions(field);
      renderRouteStatus("");
    });
    controls.input?.addEventListener("focus", () => {
      if (state.route.suggestions[field].length > 0) {
        renderRouteSearchResults(field, state.route.suggestions[field]);
      }
    });
    controls.input?.addEventListener("keydown", (event) => handleRouteInputKeydown(field, event));
    controls.current?.addEventListener("click", () => {
      void setRouteEndpointFromCurrentLocation(field);
    });
  });

  els.route.form.addEventListener("submit", (event) => {
    event.preventDefault();
    void submitRouteSearch();
  });
  els.route.swapBtn?.addEventListener("click", swapRouteEndpoints);
  document.addEventListener("click", (event) => {
    if (!els.route.form.contains(event.target)) {
      ROUTE_FIELDS.forEach(clearRouteSuggestions);
    }
  });
  renderRouteResults();
}

function handleRouteInputKeydown(field, event) {
  if (event.key === "Enter") {
    event.preventDefault();
    void resolveRouteEndpointForSubmit(field).then((resolved) => {
      if (resolved) {
        const otherField = field === "origin" ? "destination" : "origin";
        routeFieldElements(otherField).input?.focus();
      }
    });
    return;
  }
  if (event.key === "Escape") {
    clearRouteSuggestions(field);
    routeFieldElements(field).input?.blur();
  }
}

function getRouteSearchFocus() {
  return getLocationSearchFocus();
}

function queueRouteSuggestions(field) {
  cancelQueuedRouteSuggestions(field);
  const query = String(routeFieldElements(field).input?.value || "").trim();
  if (query.length < 3) {
    clearRouteSuggestions(field);
    return;
  }
  state.route.suggestionTimers[field] = window.setTimeout(() => {
    state.route.suggestionTimers[field] = 0;
    void runRouteAutocomplete(field, query);
  }, ROUTE_SUGGESTION_DEBOUNCE_MS);
}

function cancelQueuedRouteSuggestions(field) {
  const timer = state.route.suggestionTimers[field];
  if (!timer) {
    return;
  }
  window.clearTimeout(timer);
  state.route.suggestionTimers[field] = 0;
}

function clearRouteSuggestions(field) {
  cancelQueuedRouteSuggestions(field);
  state.route.suggestionSeq += 1;
  state.route.suggestions[field] = [];
  const controls = routeFieldElements(field);
  if (!controls.results) {
    return;
  }
  controls.results.hidden = true;
  controls.results.replaceChildren();
  controls.input?.setAttribute("aria-expanded", "false");
}

function renderRouteSearchMessage(field, message, tone = "muted") {
  const controls = routeFieldElements(field);
  if (!controls.results) {
    return;
  }
  const item = document.createElement("div");
  item.className = `location-search-message location-search-message-${tone}`;
  item.textContent = message;
  controls.results.replaceChildren(item);
  controls.results.hidden = false;
  controls.input?.setAttribute("aria-expanded", "true");
}

function renderRouteSearchResults(field, results) {
  const controls = routeFieldElements(field);
  if (!controls.results) {
    return;
  }
  controls.results.replaceChildren();
  if (!results.length) {
    renderRouteSearchMessage(field, t("search.noResults"));
    return;
  }
  const list = document.createElement("div");
  list.className = "location-search-result-list";
  list.setAttribute("role", "listbox");
  results.forEach((result, index) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "location-search-result";
    button.setAttribute("role", "option");
    button.dataset.index = String(index);
    const title = document.createElement("span");
    title.className = "location-search-result-title";
    title.textContent = result.name || result.label;
    const meta = document.createElement("span");
    meta.className = "location-search-result-meta";
    meta.textContent = [
      result.locality && result.locality !== result.name ? result.locality : "",
      result.region,
      result.country,
    ].filter(Boolean).join(" · ") || result.label;
    button.append(title, meta);
    button.addEventListener("click", () => setRouteEndpoint(field, result));
    list.appendChild(button);
  });
  controls.results.appendChild(list);
  controls.results.hidden = false;
  controls.input?.setAttribute("aria-expanded", "true");
}

async function runRouteAutocomplete(field, rawQuery, options = {}) {
  const { selectFirst = false } = options;
  const query = String(rawQuery || "").trim();
  if (query.length < 2) {
    renderRouteSearchMessage(field, t("search.minChars"));
    return;
  }

  const requestId = ++state.route.suggestionSeq;
  if (selectFirst) {
    renderRouteSearchMessage(field, t("search.searching"));
  }
  const focus = getRouteSearchFocus();
  const url = geocoderApiUrl("autocomplete", {
    q: query,
    lat: focus?.lat,
    lon: focus?.lon,
    limit: 5,
  });
  if (!url) {
    renderRouteSearchMessage(field, t("search.notConfigured"), "error");
    return;
  }

  try {
    const payload = normalizeGeocodePayload(
      await fetchJsonWithTimeout(url, {}, GEOCODER_API_TIMEOUT_MS),
    );
    if (requestId !== state.route.suggestionSeq) {
      return;
    }
    state.route.suggestions[field] = payload.results;
    if (!payload.ok) {
      renderRouteSearchMessage(field, t("search.unavailable"), "error");
      return;
    }
    if (selectFirst && payload.results.length > 0) {
      setRouteEndpoint(field, payload.results[0]);
      return;
    }
    renderRouteSearchResults(field, payload.results);
  } catch (error) {
    if (requestId !== state.route.suggestionSeq) {
      return;
    }
    renderRouteSearchMessage(field, t("search.unavailable"), "error");
  }
}

function formatRouteCoordinateLabel(endpoint) {
  const normalized = normalizeRouteEndpoint(endpoint);
  if (!normalized) {
    return "";
  }
  return `${normalized.lat.toFixed(5)}, ${normalized.lon.toFixed(5)}`;
}

function setRouteEndpoint(field, value) {
  const endpoint = normalizeRouteEndpoint(value);
  if (!endpoint) {
    return false;
  }
  const nextEndpoint = {
    ...endpoint,
    label: endpoint.label || formatRouteCoordinateLabel(endpoint),
  };
  state.route.endpoints[field] = nextEndpoint;
  const controls = routeFieldElements(field);
  if (controls.input) {
    controls.input.value = nextEndpoint.label;
  }
  clearRouteSuggestions(field);
  renderRouteStatus("");
  return true;
}

async function setRouteEndpointFromCurrentLocation(field) {
  try {
    const position = await ensureRouteUserPosition();
    setRouteEndpoint(field, {
      ...position,
      label: t("route.currentLocation"),
    });
  } catch (error) {
    renderRouteStatus(t("route.locationUnavailable"), "error");
  }
}

async function ensureRouteUserPosition() {
  if (hasResolvedUserLocation()) {
    return state.userPos;
  }
  if (!navigator.geolocation) {
    throw new Error("geolocation_unavailable");
  }
  updateLocationState({
    requestState: LOCATION_REQUEST_PENDING,
    errorCode: "",
  });
  const position = await requestBrowserLocation(navigator.geolocation, {
    enableHighAccuracy: false,
    timeout: 5000,
    maximumAge: 300000,
  });
  state.userPos = {
    lat: position.lat,
    lon: position.lon,
  };
  updateLocationState({
    permissionState: LOCATION_PERMISSION_GRANTED,
    requestState: LOCATION_REQUEST_READY,
    errorCode: "",
  });
  updateUserMarker();
  return state.userPos;
}

function swapRouteEndpoints() {
  const origin = state.route.endpoints.origin;
  const destination = state.route.endpoints.destination;
  const originValue = els.route.originInput?.value || "";
  const destinationValue = els.route.destinationInput?.value || "";
  state.route.endpoints.origin = destination;
  state.route.endpoints.destination = origin;
  if (els.route.originInput) {
    els.route.originInput.value = destinationValue;
  }
  if (els.route.destinationInput) {
    els.route.destinationInput.value = originValue;
  }
  ROUTE_FIELDS.forEach(clearRouteSuggestions);
}

async function resolveRouteEndpointForSubmit(field) {
  if (state.route.endpoints[field]) {
    return true;
  }
  const value = String(routeFieldElements(field).input?.value || "").trim();
  if (value.length < 2) {
    return false;
  }
  await runRouteAutocomplete(field, value, { selectFirst: true });
  return Boolean(state.route.endpoints[field]);
}

function routeEndpointsAreSame(origin, destination) {
  return distanceBetweenCoordinatesMeters(origin, destination) < 25;
}

function routeErrorMessage(error) {
  const detail = apiErrorDetailText(error?.detail);
  if (
    detail === "route_provider_quota_exhausted" ||
    detail === "route_provider_rate_limited" ||
    (detail === "route_provider_auth_failed" && Number(error?.status) === 503)
  ) {
    return t("route.capacityExhausted");
  }
  return t("route.searchError");
}

async function submitRouteSearch() {
  ROUTE_FIELDS.forEach(cancelQueuedRouteSuggestions);
  renderRouteStatus(t("route.resolving"));
  const hasOrigin = await resolveRouteEndpointForSubmit("origin");
  const hasDestination = await resolveRouteEndpointForSubmit("destination");
  const origin = normalizeRouteEndpoint(state.route.endpoints.origin);
  const destination = normalizeRouteEndpoint(state.route.endpoints.destination);

  if (!hasOrigin || !hasDestination || !origin || !destination) {
    renderRouteStatus(t("route.missingEndpoints"), "error");
    return;
  }
  if (routeEndpointsAreSame(origin, destination)) {
    renderRouteStatus(t("route.sameEndpoint"), "error");
    return;
  }
  if (!state.live.baseUrl) {
    renderRouteStatus(t("route.notConfigured"), "error");
    return;
  }

  const requestId = ++state.route.requestSeq;
  const filters = routeFiltersPayload(routeEffectiveFilters());
  state.route.loading = true;
  state.route.error = null;
  renderRouteResults();

  try {
    const payload = await fetchJsonWithTimeout(
      buildLiveApiUrl("/v1/routes/chargers"),
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          origin,
          destination,
          filters,
          filter_mode: ROUTE_FILTER_MODE,
        }),
      },
      ROUTE_API_TIMEOUT_MS,
    );
    if (requestId !== state.route.requestSeq) {
      return;
    }
    const result = normalizeRouteChargerResponse(payload);
    state.route.result = result;
    state.route.features = result.stations
      .map(routeStationToFeature)
      .filter(Boolean);
    state.route.calculatedFilters = filters;
    state.route.error = null;
    state.live.reachable = true;
    renderRouteStatus("");
    renderRouteMapLock();
  } catch (error) {
    if (requestId !== state.route.requestSeq) {
      return;
    }
    console.error("Failed to load route chargers", error);
    state.route.result = null;
    state.route.features = [];
    state.route.calculatedFilters = null;
    state.route.error = error;
    renderRouteStatus(routeErrorMessage(error), "error");
    renderRouteMapLock();
  } finally {
    if (requestId === state.route.requestSeq) {
      state.route.loading = false;
      renderRouteResults();
      renderRouteLayer();
      renderMapMarkers();
    }
  }
}

function routeStationToFeature(item) {
  if (!item?.station) {
    return null;
  }
  const feature = catalogStationToFeature(item.station);
  const routeInfo = item.route || {};
  feature.properties = {
    ...(feature.properties || {}),
    route_drive_distance_to_route_m: routeInfo.drive_distance_to_route_m,
    route_detour_m: routeInfo.route_detour_m,
    route_straight_line_distance_to_route_m: routeInfo.straight_line_distance_to_route_m,
    route_position_m: routeInfo.route_position_m,
    route_nearest_point_lat: routeInfo.nearest_route_point?.lat,
    route_nearest_point_lon: routeInfo.nearest_route_point?.lon,
  };
  applyCachedLiveStationSummaryToFeature(feature);
  return feature;
}

function renderRouteStatus(message, tone = "muted") {
  if (!els.route.status) {
    return;
  }
  els.route.status.className = `route-status route-status-${tone}`;
  els.route.status.textContent = message || "";
  els.route.status.hidden = !message;
}

function compactRouteFavoriteEndpointLabel(value) {
  const text = String(value || "").replace(/\s+/g, " ").trim();
  return text.split(",").map((part) => part.trim()).find(Boolean) || text;
}

function routeEndpointFavoriteLabel(field) {
  const controls = routeFieldElements(field);
  const endpoint = normalizeRouteEndpoint(state.route.endpoints[field]);
  return compactRouteFavoriteEndpointLabel(endpoint?.label || controls.input?.value) || t(`route.${field}`);
}

function routeFavoriteCategoryLabel() {
  return normalizeFavoriteCategoryLabel(t("route.favoriteCategory", {
    origin: routeEndpointFavoriteLabel("origin"),
    destination: routeEndpointFavoriteLabel("destination"),
  }));
}

function routeFavoriteCandidateFeatures() {
  if (!state.route.result || state.route.loading) {
    return [];
  }
  return getRouteDisplayFeatures().filter((feature) => getStationIdFromProps(feature.properties));
}

function renderRouteActionState() {
  if (!els.route.favoriteAllBtn) {
    return;
  }
  const candidateCount = routeFavoriteCandidateFeatures().length;
  els.route.favoriteAllBtn.disabled = candidateCount === 0;
  els.route.favoriteAllBtn.setAttribute(
    "aria-label",
    candidateCount > 0
      ? t("route.addAllFavoritesWithCount", { count: candidateCount })
      : t("route.addAllFavorites"),
  );
}

function addRouteResultsToFavorites() {
  const features = routeFavoriteCandidateFeatures();
  const category = routeFavoriteCategoryLabel();
  if (features.length === 0 || !category) {
    return;
  }
  const stationIds = [];
  features.forEach((feature) => {
    const stationId = getStationIdFromProps(feature.properties);
    if (!stationId) {
      return;
    }
    ensureFavoriteItem(state.favoriteMetadata, stationId, { source: FAVORITE_SOURCE_ROUTE });
    addFavoriteCategory(state.favoriteMetadata, stationId, category, { source: FAVORITE_SOURCE_ROUTE });
    stationIds.push(stationId);
  });
  syncFavoriteStationIdsFromMetadata();
  state.favoriteCategoryFilter = getFavoriteCategoryKey(category);
  saveFavorites();
  refreshFavoriteDependentViews(stationIds);
  renderRouteResults();
  renderRouteStatus(t("route.favoritesAdded", { count: stationIds.length, category }), "success");
}

function routeEffectiveFilters() {
  return {
    ...state.filters,
    availableOnly: false,
  };
}

function getRouteDisplayFeatures() {
  const now = new Date();
  const filters = routeEffectiveFilters();
  return state.route.features
    .filter((feature) => matchesFeatureFilters(feature, filters, { getDisplayedMaxPowerKw, now }))
    .sort(compareRouteFeatures);
}

function compareRouteFeatures(a, b) {
  const positionDiff = routePosition(a) - routePosition(b);
  if (positionDiff !== 0) {
    return positionDiff;
  }
  const accessDiff = routeAccessDistance(a) - routeAccessDistance(b);
  if (accessDiff !== 0) {
    return accessDiff;
  }
  const amenitiesDiff = Number(b.properties?.amenities_total || 0) - Number(a.properties?.amenities_total || 0);
  if (amenitiesDiff !== 0) {
    return amenitiesDiff;
  }
  const powerDiff = getDisplayedMaxPowerKw(b.properties || {}) - getDisplayedMaxPowerKw(a.properties || {});
  if (powerDiff !== 0) {
    return powerDiff;
  }
  return 0;
}

function routeAccessDistance(feature) {
  const distance = Number(feature?.properties?.route_drive_distance_to_route_m);
  return Number.isFinite(distance) ? distance : Number.POSITIVE_INFINITY;
}

function routePosition(feature) {
  const distance = Number(feature?.properties?.route_position_m);
  return Number.isFinite(distance) ? distance : Number.POSITIVE_INFINITY;
}

function routeFiltersRequireRecalculation() {
  if (!state.route.calculatedFilters) {
    return false;
  }
  const current = routeFiltersPayload(routeEffectiveFilters());
  const baseline = state.route.calculatedFilters;
  if (baseline.operator && current.operator !== baseline.operator) {
    return true;
  }
  if (!baseline.operator && current.operator) {
    return false;
  }
  if (current.min_power_kw < baseline.min_power_kw) {
    return true;
  }
  if (current.min_amenities_total < baseline.min_amenities_total) {
    return true;
  }
  const currentAmenities = new Set(current.selected_amenities);
  if (baseline.selected_amenities.some((key) => !currentAmenities.has(key))) {
    return true;
  }
  if (baseline.amenity_name_query) {
    const baselineName = baseline.amenity_name_query.toLowerCase();
    const currentName = current.amenity_name_query.toLowerCase();
    if (!currentName || !currentName.includes(baselineName)) {
      return true;
    }
  }
  if (baseline.available_only && !current.available_only) {
    return true;
  }
  if (baseline.currently_open_only && !current.currently_open_only) {
    return true;
  }
  return false;
}

function renderRouteResults() {
  if (!els.route.results) {
    return;
  }
  renderRouteActionState();
  els.route.results.replaceChildren();

  if (state.route.loading) {
    hideRouteSummary();
    const loading = document.createElement("div");
    loading.className = "loading-state route-loading-state";
    loading.setAttribute("data-nosnippet", "");
    loading.setAttribute("role", "status");
    loading.setAttribute("aria-live", "polite");
    const spinner = document.createElement("span");
    spinner.className = "route-loading-spinner";
    spinner.setAttribute("aria-hidden", "true");
    const text = document.createElement("span");
    text.className = "route-loading-text";
    text.textContent = t("route.loading");
    loading.append(spinner, text);
    els.route.results.appendChild(loading);
    return;
  }

  if (!state.route.result) {
    hideRouteSummary();
    if (state.route.error) {
      return;
    }
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.setAttribute("data-nosnippet", "");
    empty.textContent = t("route.empty");
    els.route.results.appendChild(empty);
    return;
  }

  const displayFeatures = getRouteDisplayFeatures();
  renderRouteSummary(displayFeatures.length);
  if (routeFiltersRequireRecalculation()) {
    els.route.results.appendChild(createRouteRecalculateNotice());
  }
  if (displayFeatures.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.setAttribute("data-nosnippet", "");
    empty.textContent = t("route.noFilteredResults");
    els.route.results.appendChild(empty);
    renderRouteLayer();
    return;
  }
  displayFeatures.forEach((feature) => {
    els.route.results.appendChild(createStationCard(feature, { route: true }));
  });
  requestLiveSummariesForFeatures(displayFeatures);
  requestRatingSummariesForFeatures(displayFeatures);
  renderRouteLayer();
}

function hideRouteSummary() {
  if (!els.route.summary) {
    return;
  }
  els.route.summary.hidden = true;
  els.route.summary.replaceChildren();
}

function createRouteSummaryStat(label, value) {
  const item = document.createElement("div");
  item.className = "route-summary-stat";
  const labelEl = document.createElement("span");
  labelEl.textContent = label;
  const valueEl = document.createElement("strong");
  valueEl.textContent = value;
  item.append(labelEl, valueEl);
  return item;
}

function renderRouteSummary(resultCount) {
  if (!els.route.summary || !state.route.result) {
    return;
  }
  const route = state.route.result.route;
  els.route.summary.replaceChildren(
    createRouteSummaryStat(t("route.summaryDistance"), formatRouteDistanceMeters(route.distance_m)),
    createRouteSummaryStat(t("route.summaryDuration"), formatRouteDuration(route.duration_s)),
    createRouteSummaryStat(t("route.summaryStations"), t("route.resultsCount", { count: resultCount })),
  );
  const mapButton = document.createElement("button");
  mapButton.type = "button";
  mapButton.className = "text-btn route-map-btn";
  mapButton.textContent = t("route.showMap");
  mapButton.addEventListener("click", () => {
    switchView("view-map");
    window.requestAnimationFrame(focusMapOnRoute);
  });
  els.route.summary.appendChild(mapButton);
  els.route.summary.hidden = false;
}

function createRouteRecalculateNotice() {
  const notice = document.createElement("div");
  notice.className = "route-filter-notice";
  const message = document.createElement("span");
  message.textContent = t("route.filterChanged");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "text-btn";
  button.textContent = t("route.recalculate");
  button.addEventListener("click", () => {
    void submitRouteSearch();
  });
  notice.append(message, button);
  return notice;
}

function routeGeometryLatLngs() {
  const coordinates = state.route.result?.route?.geometry?.coordinates;
  if (!Array.isArray(coordinates)) {
    return [];
  }
  return coordinates
    .map((point) => {
      const lon = Number(point?.[0]);
      const lat = Number(point?.[1]);
      return Number.isFinite(lat) && Number.isFinite(lon) ? [lat, lon] : null;
    })
    .filter(Boolean);
}

function renderRouteLayer() {
  if (!state.views.layers.route) {
    return;
  }
  state.views.layers.route.clearLayers();
  if (!state.route.result) {
    return;
  }

  const latLngs = routeGeometryLatLngs();
  if (latLngs.length > 1) {
    L.polyline(latLngs, {
      color: "#0f766e",
      weight: 5,
      opacity: 0.78,
      lineCap: "round",
      lineJoin: "round",
    }).addTo(state.views.layers.route);
  }

  if (!hasPinnedRouteMap()) {
    getRouteDisplayFeatures().forEach((feature) => {
      applyCachedLiveStationSummaryToFeature(feature);
      bindStationMarker(createStationMarker(feature), feature)
        .addTo(state.views.layers.route);
    });
  }
}

function hasPinnedRouteMap() {
  return Boolean(state.route.result && isMapViewActive());
}

function renderRouteMapLock() {
  if (!els.routeMap.lock) {
    return;
  }
  const pinned = hasPinnedRouteMap();
  els.routeMap.lock.hidden = !pinned;
  if (els.buttons.locate) {
    if (pinned) {
      els.buttons.locate.setAttribute("aria-describedby", "route-map-lock-message");
    } else {
      els.buttons.locate.removeAttribute("aria-describedby");
    }
  }
  if (pinned) {
    stopMapGPSRefresh();
  }
}

function clearRoute() {
  state.route.requestSeq += 1;
  state.route.loading = false;
  state.route.result = null;
  state.route.features = [];
  state.route.error = null;
  state.route.calculatedFilters = null;
  renderRouteStatus("");
  renderRouteResults();
  renderRouteLayer();
  renderRouteMapLock();
  applyFilters();
  if (isMapViewActive()) {
    startMapGPSRefresh();
    loadCatalogStationsFromMapCenter({
      force: true,
      requireUserInteraction: false,
      reset: true,
    });
  }
}

function focusMapOnRoute() {
  if (!state.views.map || !state.route.result) {
    return;
  }
  const bounds = L.latLngBounds([]);
  routeGeometryLatLngs().forEach((latLng) => bounds.extend(latLng));
  getRouteDisplayFeatures().forEach((feature) => {
    const coords = getFeatureLatLon(feature);
    if (coords) {
      bounds.extend([coords.lat, coords.lon]);
    }
  });
  if (!bounds.isValid()) {
    return;
  }
  state.views.map.invalidateSize({ pan: false });
  state.views.map.fitBounds(bounds.pad(0.08), {
    animate: true,
    maxZoom: 13,
  });
}

function formatRouteDistanceMeters(value) {
  const meters = Number(value);
  if (!Number.isFinite(meters)) {
    return "";
  }
  if (meters >= 10000) {
    return `${formatRouteInteger(Math.round(meters / 1000))} km`;
  }
  if (meters >= 1000) {
    return `${(meters / 1000).toFixed(1)} km`;
  }
  return `${formatRouteInteger(Math.round(meters))} m`;
}

function formatRouteInteger(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return "";
  }
  return new Intl.NumberFormat(getLocale()).format(Math.round(numeric));
}

function formatRouteDuration(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "";
  }
  const minutes = Math.max(1, Math.round(seconds / 60));
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  if (hours > 0 && remainder > 0) {
    return t("route.durationHoursMinutes", { hours, minutes: remainder });
  }
  if (hours > 0) {
    return t("route.durationHours", { hours });
  }
  return t("route.durationMinutes", { minutes });
}

function prepareChargerFeature(feature, powerClass) {
  const prepared = feature || {};
  const stationId = normalizeStationId(prepared.properties?.station_id || "");
  prepared.properties = {
    ...(prepared.properties || {}),
    ...(stationId ? { station_id: stationId } : {}),
    charger_power_class: powerClass,
  };
  return prepared;
}

function firstText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) {
      return text;
    }
  }
  return "";
}

function finiteNumber(value, fallback = 0) {
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function catalogAmenityFilterKey(category) {
  const raw = String(category || "").trim();
  if (!raw) {
    return "";
  }
  if (AMENITY_MAPPING[raw]) {
    return raw;
  }
  const prefixed = `amenity_${raw}`;
  return AMENITY_MAPPING[prefixed] ? prefixed : "";
}

function normalizeCatalogAmenityExamples(examples) {
  if (!Array.isArray(examples)) {
    return [];
  }
  return examples
    .map((item) => {
      if (!item || typeof item !== "object") {
        return null;
      }
      const category = firstText(item.category, item.kind, item.amenity_kind);
      return {
        category,
        name: firstText(item.name, item.title),
        distance_m: finiteNumber(item.distance_m, Number.NaN),
        lat: finiteNumber(item.lat ?? item.latitude, Number.NaN),
        lon: finiteNumber(item.lon ?? item.longitude, Number.NaN),
        opening_hours: firstText(item.opening_hours, item.hours),
      };
    })
    .filter(Boolean);
}

function catalogAmenityExamplesFromStation(station) {
  const explicitExamples = normalizeCatalogAmenityExamples(station?.amenity_examples);
  if (explicitExamples.length > 0) {
    return explicitExamples;
  }
  const nearestKind = firstText(station?.nearest_amenity_kind);
  const nearestName = firstText(station?.nearest_amenity_name);
  if (!nearestKind && !nearestName) {
    return [];
  }
  return [
    {
      category: nearestKind,
      name: nearestName,
      distance_m: finiteNumber(station?.nearest_amenity_distance_m, Number.NaN),
    },
  ];
}

function applyCatalogAmenityCounts(props, categoryCounts) {
  if (!props || !categoryCounts || typeof categoryCounts !== "object") {
    return;
  }
  Object.entries(categoryCounts).forEach(([category, count]) => {
    const filterKey = catalogAmenityFilterKey(category);
    if (!filterKey) {
      return;
    }
    props[filterKey] = finiteNumber(count, 0);
  });
}

function catalogStationToFeature(station) {
  const lat = finiteNumber(station?.latitude, Number.NaN);
  const lon = finiteNumber(station?.longitude, Number.NaN);
  const maxPowerKw = finiteNumber(station?.max_power_kw, 0);
  const categoryCounts = station?.amenity_category_counts && typeof station.amenity_category_counts === "object"
    ? station.amenity_category_counts
    : {};
  const props = {
    station_id: normalizeStationId(station?.station_id || ""),
    operator: firstText(station?.operator_name, station?.operator, station?.station_name, t("station.unknownOperator")),
    station_name: firstText(station?.station_name),
    address: firstText(station?.address),
    postcode: firstText(station?.postal_code, station?.postcode),
    city: firstText(station?.city),
    country_code: firstText(station?.country_code),
    charging_points_count: Math.max(1, Math.round(finiteNumber(
      station?.charger_count ?? station?.charging_points_count ?? station?.total_evses,
      1,
    ))),
    connector_count: Math.max(0, Math.round(finiteNumber(station?.connector_count, 0))),
    max_power_kw: maxPowerKw,
    max_individual_power_kw: maxPowerKw,
    connector_types_display: firstText(station?.connector_types),
    current_types_display: firstText(station?.current_types),
    payment_methods_display: firstText(station?.payment_methods),
    auth_methods_display: firstText(station?.auth_methods),
    service_types_display: firstText(station?.service_types),
    opening_hours: firstText(station?.opening_hours),
    opening_hours_display: firstText(station?.opening_hours),
    green_energy: station?.green_energy,
    helpdesk_phone: firstText(station?.helpdesk_phone),
    price_display: firstText(station?.price_display),
    price_currency: firstText(station?.price_currency),
    detail_source_uid: firstText(station?.source_uid, station?.provider_uid),
    detail_source_name: firstText(station?.provider_uid, station?.source_uid),
    detail_last_updated: firstText(station?.detail_last_updated),
    source_station_id: firstText(station?.source_station_id),
    source_url: firstText(station?.source_url),
    public_bundle_status: firstText(station?.public_bundle_status),
    amenities_total: Math.max(0, Math.round(finiteNumber(station?.amenities_total, 0))),
    nearest_amenity_kind: firstText(station?.nearest_amenity_kind),
    nearest_amenity_name: firstText(station?.nearest_amenity_name),
    nearest_amenity_distance_m: finiteNumber(station?.nearest_amenity_distance_m, Number.NaN),
    amenity_category_counts: categoryCounts,
    amenity_examples: catalogAmenityExamplesFromStation(station),
    distance_m: finiteNumber(station?.distance_m, Number.NaN),
  };
  applyCatalogAmenityCounts(props, categoryCounts);
  const liveSummary = catalogLiveStationSummary(station, props.station_id);
  if (liveSummary) {
    applyLiveStationSummaryToProps(props, liveSummary);
  }
  return prepareChargerFeature(
    {
      type: "Feature",
      geometry: {
        type: "Point",
        coordinates: [lon, lat],
      },
      properties: props,
    },
    maxPowerKw >= DEFAULT_MIN_POWER_KW ? "fast" : "normal",
  );
}

function mergeCatalogFeature(existing, incoming) {
  if (!existing) {
    applyCachedLiveStationSummaryToFeature(incoming);
    return incoming;
  }

  const existingProps = existing.properties || {};
  const incomingProps = incoming.properties || {};
  const existingAmenities = Array.isArray(existingProps.amenity_examples)
    ? existingProps.amenity_examples.length
    : 0;
  const incomingAmenities = Array.isArray(incomingProps.amenity_examples)
    ? incomingProps.amenity_examples.length
    : 0;
  const merged = {
    ...incoming,
    properties: {
      ...existingProps,
      ...incomingProps,
    },
  };

  if (existingAmenities > incomingAmenities) {
    merged.properties.amenity_examples = existingProps.amenity_examples;
    merged.properties.amenity_category_counts =
      existingProps.amenity_category_counts || incomingProps.amenity_category_counts;
    merged.properties.amenities_total = Math.max(
      finiteNumber(existingProps.amenities_total, 0),
      finiteNumber(incomingProps.amenities_total, 0),
    );
    if (merged.properties.amenity_category_counts) {
      applyCatalogAmenityCounts(merged.properties, merged.properties.amenity_category_counts);
    }
  }

  applyCachedLiveStationSummaryToFeature(merged);
  return merged;
}

function upsertCatalogFeatures(features, { reset = false } = {}) {
  const byStationId = new Map();
  if (!reset) {
    state.features.forEach((feature) => {
      const stationId = normalizeStationId(getStationIdFromProps(feature.properties));
      if (stationId && !byStationId.has(stationId)) {
        byStationId.set(stationId, feature);
      }
    });
  }

  features.forEach((feature) => {
    const stationId = normalizeStationId(getStationIdFromProps(feature.properties));
    if (!stationId) {
      return;
    }
    byStationId.set(stationId, mergeCatalogFeature(byStationId.get(stationId), feature));
  });

  while (byStationId.size > CATALOG_ACCUMULATED_FEATURE_LIMIT) {
    const oldestStationId = byStationId.keys().next().value;
    if (!oldestStationId) {
      break;
    }
    byStationId.delete(oldestStationId);
  }

  state.features = Array.from(byStationId.values());
}

function catalogSearchMode() {
  return Number(state.filters.minPower ?? DEFAULT_MIN_POWER_KW) < DEFAULT_MIN_POWER_KW
    ? "local"
    : "travel";
}

function normalizeCatalogCenter(center) {
  const lat = Number(center?.lat);
  const lon = Number(center?.lon ?? center?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  return {
    lat,
    lon,
    source: String(center?.source || "map"),
  };
}

function hasCatalogSearchCenter() {
  return Boolean(normalizeCatalogCenter(state.catalog.center));
}

function hasCatalogSearchContext() {
  return Boolean(state.catalog.lastQueryKey || hasCatalogSearchCenter());
}

function hasCatalogListContext() {
  return Boolean(hasCatalogSearchContext() || state.features.length > 0);
}

function hasEmptyCatalogResult() {
  return Boolean(
    state.catalog.lastQueryKey &&
    state.catalog.lastResultCount === 0 &&
    !state.catalog.loading &&
    !state.catalog.error,
  );
}

function getCatalogStaticFallbackFeatures() {
  const center = getCatalogSearchCenter();
  if (!center) {
    return state.staticFeatures;
  }
  return state.staticFeatures.filter((feature) => {
    const coords = getFeatureLatLon(feature);
    return coords && distanceBetweenCoordinatesMeters(center, coords) <= CATALOG_SEARCH_RADIUS_M;
  });
}

function getFilterSourceFeatures() {
  if (
    (state.catalog.loading || state.catalog.error) &&
    state.features.length === 0 &&
    state.staticFeatures.length > 0
  ) {
    return getCatalogStaticFallbackFeatures();
  }
  if (hasEmptyCatalogResult() && state.staticFeatures.length > 0) {
    return getCatalogStaticFallbackFeatures();
  }
  if (hasCatalogSearchContext() || state.catalog.loading) {
    return state.features;
  }
  return state.staticFeatures;
}

function setCatalogSearchCenter(center, source = "map") {
  const normalized = normalizeCatalogCenter({ ...center, source });
  if (!normalized) {
    return false;
  }
  state.catalog.center = normalized;
  return true;
}

function getCatalogSearchCenter() {
  return normalizeCatalogCenter(state.catalog.center);
}

function getDistanceReferencePosition() {
  return getCatalogSearchCenter() || (hasResolvedUserLocation() ? state.userPos : null);
}

function catalogSearchQueryKey() {
  const center = getCatalogSearchCenter();
  if (!center) {
    return "";
  }
  const minPower = Number(state.filters.minPower ?? DEFAULT_MIN_POWER_KW);
  return JSON.stringify({
    lat: center.lat.toFixed(5),
    lon: center.lon.toFixed(5),
    radius_m: CATALOG_SEARCH_RADIUS_M,
    limit: CATALOG_SEARCH_LIMIT,
    mode: catalogSearchMode(),
    min_power_kw: Number.isFinite(minPower) ? minPower : DEFAULT_MIN_POWER_KW,
  });
}

async function loadCatalogStationsForCurrentCenter({ force = false, reset = false } = {}) {
  const center = getCatalogSearchCenter();
  if (!state.live.baseUrl || !center || hasPinnedRouteMap()) {
    return;
  }

  const queryKey = catalogSearchQueryKey();
  if (!force && queryKey && queryKey === state.catalog.lastQueryKey) {
    return;
  }

  const searchSequence = ++catalogSearchSequence;
  const minPowerKw = Number(state.filters.minPower ?? DEFAULT_MIN_POWER_KW);
  state.catalog.loading = true;
  state.catalog.error = null;
  state.catalog.lastResultCount = null;
  state.catalog.lastQueryKey = queryKey;
  populateOperators();
  renderAmenityFilters();
  applyFilters();

  try {
    const payload = await fetchJsonWithTimeout(
      buildLiveApiUrl("/v1/catalog/search", {
        lat: center.lat,
        lon: center.lon,
        radius_m: CATALOG_SEARCH_RADIUS_M,
        limit: CATALOG_SEARCH_LIMIT,
        mode: catalogSearchMode(),
        min_power_kw: Number.isFinite(minPowerKw) ? minPowerKw : DEFAULT_MIN_POWER_KW,
      }),
      {},
      LIVE_API_TIMEOUT_MS,
    );
    if (searchSequence !== catalogSearchSequence) {
      return;
    }
    if (!payload || typeof payload !== "object" || !Array.isArray(payload.stations)) {
      throw new Error("Unexpected catalog search payload");
    }
    const incomingFeatures = [];
    payload.stations.forEach((station) => {
      const feature = catalogStationToFeature(station);
      const stationId = getStationIdFromProps(feature.properties);
      const [lon, lat] = feature.geometry.coordinates || [];
      if (!stationId || !Number.isFinite(Number(lat)) || !Number.isFinite(Number(lon))) {
        return;
      }
      const liveSummary = catalogLiveStationSummary(station, stationId);
      if (liveSummary) {
        const cachedSummary = upsertCatalogLiveStationSummary(stationId, liveSummary);
        applyLiveStationSummaryToProps(feature.properties, cachedSummary);
      } else {
        markCatalogLiveStationSummaryMissing(stationId);
      }
      incomingFeatures.push(feature);
    });
    upsertCatalogFeatures(incomingFeatures, { reset });
    state.catalog.lastResultCount = state.features.length;
    state.live.reachable = true;
  } catch (err) {
    if (searchSequence !== catalogSearchSequence) {
      return;
    }
    console.error("Failed to load live catalog station search", err);
    state.catalog.lastResultCount = state.features.length || null;
    state.catalog.error = err;
  } finally {
    if (searchSequence === catalogSearchSequence) {
      state.catalog.loading = false;
      populateOperators();
      renderAmenityFilters();
      applyFilters();
      updateFilterLabel();
      syncDetailModalWithUrl();
    }
  }
}

function buildApiUrl(baseUrl, path, params = {}) {
  const url = new URL(path, baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url.toString();
}

function buildLiveApiUrl(path, params = {}) {
  return buildApiUrl(state.live.baseUrl, path, params);
}

function buildStationLiveApiUrl(stationId, path, params = {}) {
  return buildApiUrl(liveApiBaseUrlForStationId(stationId), path, params);
}

function isAbortError(error) {
  return error?.name === "AbortError";
}

function apiErrorDetailText(value) {
  return String(value || "").trim();
}

class HttpApiError extends Error {
  constructor(status, detail = "", payload = null) {
    super(detail ? `HTTP ${status}: ${detail}` : `HTTP ${status}`);
    this.name = "HttpApiError";
    this.status = status;
    this.detail = detail;
    this.payload = payload;
  }
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = LIVE_API_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const requestHeaders = {
      Accept: "application/json",
      ...(options.headers || {}),
    };
    const response = await fetch(url, {
      ...options,
      signal: controller.signal,
      headers: requestHeaders,
    });
    if (!response.ok) {
      let payload = null;
      try {
        payload = await response.json();
      } catch {
        payload = null;
      }
      const detail =
        payload && typeof payload === "object"
          ? apiErrorDetailText(payload.detail || payload.error || payload.message)
          : "";
      throw new HttpApiError(response.status, detail, payload);
    }
    return await response.json();
  } finally {
    window.clearTimeout(timer);
  }
}

function upsertLiveStationSummaries(stations, missingStationIds = []) {
  const affectedStationIds = new Set();

  stations.forEach((summary) => {
    const stationId = getStationIdFromProps(summary);
    if (!stationId) {
      return;
    }
    affectedStationIds.add(stationId);
    state.live.summaryByStationId.set(stationId, summary);
    state.live.summaryFetchedAtByStationId.set(stationId, Date.now());
    const feature = findFeatureByStationId(stationId);
    if (feature) {
      applyLiveStationSummaryToProps(feature.properties, summary);
    }
    const routeFeature = findRouteFeatureByStationId(stationId);
    if (routeFeature && routeFeature !== feature) {
      applyLiveStationSummaryToProps(routeFeature.properties, summary);
    }
  });

  missingStationIds.forEach((stationId) => {
    const id = String(stationId || "").trim();
    if (!id) {
      return;
    }
    affectedStationIds.add(id);
    state.live.summaryByStationId.delete(id);
    state.live.summaryFetchedAtByStationId.set(id, Date.now());
    const feature = findFeatureByStationId(id);
    if (feature) {
      clearLiveStationSummaryFromProps(feature.properties);
    }
    const routeFeature = findRouteFeatureByStationId(id);
    if (routeFeature && routeFeature !== feature) {
      clearLiveStationSummaryFromProps(routeFeature.properties);
    }
  });

  return Array.from(affectedStationIds);
}

function requestLiveSummariesForFeatures(features) {
  if (!state.live.baseUrl && !state.live.deBaseUrl) {
    return;
  }

  const stationIds = Array.from(new Set(
    features
      .filter((feature) => shouldRequestLiveDataForProps(feature?.properties))
      .map((feature) => getStationIdFromProps(feature.properties))
      .filter(Boolean),
  ));
  if (stationIds.length === 0) {
    return;
  }
  const apiIdByStationId = new Map(stationIds.map((stationId) => [
    stationId,
    toLiveApiStationId(stationId),
  ]));
  const stationIdByApiId = new Map(
    Array.from(apiIdByStationId.entries()).map(([stationId, apiId]) => [apiId, stationId]),
  );

  const now = Date.now();
  const pendingIds = stationIds.filter((stationId) => {
    if (state.live.pendingSummaryStationIds.has(stationId)) {
      return false;
    }
    const fetchedAt = state.live.summaryFetchedAtByStationId.get(stationId) || 0;
    return !fetchedAt || now - fetchedAt >= LIVE_SUMMARY_REFRESH_MS;
  });

  if (pendingIds.length === 0) {
    return;
  }

  pendingIds.forEach((stationId) => {
    state.live.pendingSummaryStationIds.add(stationId);
  });

  void (async () => {
    const affectedStationIds = [];
    try {
      const groupedIds = groupStationIdsByLiveApiBaseUrl(pendingIds);
      await Promise.all(Array.from(groupedIds.entries()).flatMap(([baseUrl, groupedStationIds]) =>
        chunkArray(groupedStationIds, LIVE_STATION_LOOKUP_BATCH_SIZE).map(async (batchStationIds) => {
          const groupedApiIds = batchStationIds.map((stationId) =>
            apiIdByStationId.get(stationId) || stationId,
          );
          try {
            const payload = await fetchJsonWithTimeout(
              buildApiUrl(baseUrl, "/v1/stations/lookup"),
              {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                },
                body: JSON.stringify({ station_ids: groupedApiIds }),
              },
              LIVE_API_TIMEOUT_MS,
            );
            if (!payload || typeof payload !== "object" || !Array.isArray(payload.stations)) {
              throw new Error("Unexpected live station lookup payload");
            }
            state.live.reachable = true;
            const stations = payload.stations.map((station) => {
              const apiId = String(station?.station_id || "").trim();
              const normalizedApiId = normalizeStationId(apiId);
              const appStationId = stationIdByApiId.get(apiId) ||
                stationIdByApiId.get(normalizedApiId) ||
                normalizedApiId;
              return appStationId ? { ...station, station_id: appStationId } : station;
            });
            const missingStationIds = (payload.missing_station_ids || []).map((apiId) => {
              const rawApiId = String(apiId || "").trim();
              const normalizedApiId = normalizeStationId(rawApiId);
              return stationIdByApiId.get(rawApiId) || stationIdByApiId.get(normalizedApiId) || normalizedApiId;
            });
            affectedStationIds.push(...upsertLiveStationSummaries(
              stations,
              missingStationIds,
            ));
          } catch (err) {
            console.error(`Failed to load live station summaries from ${baseUrl}`, err);
          }
        }),
      ));
    } catch (err) {
      console.error("Failed to load live station summaries", err);
    } finally {
      pendingIds.forEach((stationId) => {
        state.live.pendingSummaryStationIds.delete(stationId);
      });
      if (affectedStationIds.length > 0) {
        refreshRenderedViews({ markerStationIds: Array.from(new Set(affectedStationIds)) });
      }
    }
  })();
}

function upsertRatingSummaries(summaries, missingStationIds = []) {
  summaries.forEach((summary) => {
    const normalized = normalizeRatingSummary(summary);
    if (!normalized) {
      return;
    }
    const stationId = normalizeStationId(normalized.station_id);
    state.ratingSummariesByStationId.set(stationId, {
      ...normalized,
      station_id: stationId,
    });
    state.ratingSummaryFetchedAtByStationId.set(stationId, Date.now());
  });

  missingStationIds.forEach((stationId) => {
    const id = normalizeStationId(stationId);
    if (!id) {
      return;
    }
    state.ratingSummariesByStationId.delete(id);
    state.ratingSummaryFetchedAtByStationId.set(id, Date.now());
  });
}

function requestRatingSummariesForFeatures(features) {
  if (!SHARED_RATINGS_ENABLED || !state.live.baseUrl) {
    return;
  }

  const stationIds = Array.from(new Set(
    features
      .map((feature) => getStationIdFromProps(feature?.properties))
      .filter(Boolean),
  ));
  if (stationIds.length === 0) {
    return;
  }

  const now = Date.now();
  const pendingIds = stationIds.filter((stationId) => {
    if (state.pendingRatingSummaryStationIds.has(stationId)) {
      return false;
    }
    const fetchedAt = state.ratingSummaryFetchedAtByStationId.get(stationId) || 0;
    return !fetchedAt || now - fetchedAt >= RATING_SUMMARY_REFRESH_MS;
  });

  if (pendingIds.length === 0) {
    return;
  }

  pendingIds.forEach((stationId) => {
    state.pendingRatingSummaryStationIds.add(stationId);
  });

  void (async () => {
    try {
      let changed = false;
      await Promise.all(chunkArray(pendingIds, RATING_LOOKUP_BATCH_SIZE).map(async (batchIds) => {
        const payload = await fetchJsonWithTimeout(
          buildLiveApiUrl("/v1/ratings/lookup"),
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({ station_ids: batchIds }),
          },
          RATING_API_TIMEOUT_MS,
        );
        if (!payload || typeof payload !== "object" || !Array.isArray(payload.ratings)) {
          throw new Error("Unexpected rating lookup payload");
        }
        state.live.reachable = true;
        upsertRatingSummaries(payload.ratings, payload.missing_station_ids || []);
        changed = true;
      }));
      if (changed) {
        refreshRenderedViews();
      }
    } catch (err) {
      console.error("Failed to load station ratings", err);
    } finally {
      pendingIds.forEach((stationId) => {
        state.pendingRatingSummaryStationIds.delete(stationId);
      });
    }
  })();
}

function refreshRenderedViews({ markerStationIds = [] } = {}) {
  updateMapMarkersForStationIds(markerStationIds);
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
  if (currentDetailFeature && !els.modals.detail.classList.contains("hidden")) {
    const stationId = getStationIdFromProps(currentDetailFeature.properties);
    populateDetailContent(currentDetailFeature, state.live.detailByStationId.get(stationId) || null);
  }
}

function hasResolvedUserLocation() {
  return Boolean(
    state.userPos &&
    Number.isFinite(Number(state.userPos.lat)) &&
    Number.isFinite(Number(state.userPos.lon))
  );
}

function shouldAttemptStartupLocation() {
  if (
    state.startupLocationRequested ||
    hasResolvedUserLocation() ||
    getRequestedStationId() ||
    !navigator.geolocation
  ) {
    return false;
  }
  const permissionState = normalizeLocationPermissionState(state.location.permissionState);
  return ![
    LOCATION_PERMISSION_DENIED,
    LOCATION_PERMISSION_UNSUPPORTED,
  ].includes(permissionState);
}

function updateLocationState(patch = {}) {
  Object.assign(state.location, patch);
  if (hasResolvedUserLocation()) {
    state.location.requestState = LOCATION_REQUEST_READY;
    state.location.errorCode = "";
  }
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
}

function getLocationListViewModel() {
  const viewModel = getLocationLookupViewModel({
    hasLocation: hasResolvedUserLocation(),
    isRequesting: state.location.requestState === LOCATION_REQUEST_PENDING,
    permissionState: state.location.permissionState,
    errorCode: state.location.errorCode,
    geolocationSupported: Boolean(navigator.geolocation),
  });
  return localizeLocationViewModel(viewModel);
}

function localizeLocationViewModel(viewModel) {
  if (!viewModel || viewModel.kind === LOCATION_REQUEST_READY) {
    return viewModel;
  }
  if (viewModel.kind === LOCATION_REQUEST_PENDING) {
    return {
      ...viewModel,
      title: t("location.pendingTitle"),
      message: t("location.pendingMessage"),
      actionLabel: "",
    };
  }
  if (viewModel.kind === LOCATION_REQUEST_IDLE) {
    return {
      ...viewModel,
      title: t("location.idleTitle"),
      message: t("location.idleMessage"),
      actionLabel: t("location.idleAction"),
    };
  }
  if (state.location.errorCode === LOCATION_ERROR_PERMISSION_DENIED || state.location.permissionState === LOCATION_PERMISSION_DENIED) {
    return {
      ...viewModel,
      title: t("location.deniedTitle"),
      message: t("location.deniedMessage"),
      actionLabel: t("location.retry"),
    };
  }
  if (state.location.errorCode === "timeout") {
    return {
      ...viewModel,
      title: t("location.timeoutTitle"),
      message: t("location.timeoutMessage"),
      actionLabel: t("location.retry"),
    };
  }
  if (state.location.errorCode === "position_unavailable") {
    return {
      ...viewModel,
      title: t("location.positionTitle"),
      message: t("location.positionMessage"),
      actionLabel: t("location.retry"),
    };
  }
  return {
    ...viewModel,
    title: t("location.unavailableTitle"),
    message: viewModel.actionLabel ? t("location.unknownMessage") : t("location.unavailableMessage"),
    actionLabel: viewModel.actionLabel ? t("location.retry") : "",
  };
}

function renderLocationGate(container, viewModel) {
  container.innerHTML = "";
  container.appendChild(createLocationPanel(viewModel));
}

function createCatalogErrorPanel() {
  const panel = document.createElement("section");
  panel.className = "location-gate location-gate-error";
  panel.setAttribute("data-nosnippet", "");
  panel.innerHTML = `
    <h3 class="location-gate-title">${escapeHtml(t("errors.catalogTitle"))}</h3>
    <p class="location-gate-copy">${escapeHtml(t("errors.catalogMessage"))}</p>
  `;
  const actions = document.createElement("div");
  actions.className = "location-gate-actions";
  const button = document.createElement("button");
  button.type = "button";
  button.className = "primary-btn";
  button.textContent = t("errors.reload");
  button.addEventListener("click", () => {
    void loadCatalogStationsForCurrentCenter({ force: true, reset: true });
  });
  actions.appendChild(button);
  panel.appendChild(actions);
  return panel;
}

function renderCatalogError(container) {
  container.innerHTML = "";
  const panel = createCatalogErrorPanel();
  container.appendChild(panel);
}

function createLocationPanel(viewModel) {
  const panel = document.createElement("section");
  panel.className = `location-gate location-gate-${viewModel.kind}`;
  if (!viewModel.blocksStationList) {
    panel.classList.add("location-gate-inline");
  }
  panel.setAttribute("data-nosnippet", "");

  const title = document.createElement("h3");
  title.className = "location-gate-title";
  title.textContent = viewModel.title;
  panel.appendChild(title);

  const copy = document.createElement("p");
  copy.className = "location-gate-copy";
  copy.textContent = viewModel.message;
  panel.appendChild(copy);

  if (viewModel.actionLabel || viewModel.blocksStationList) {
    const actions = document.createElement("div");
    actions.className = "location-gate-actions";
    if (viewModel.actionLabel) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "primary-btn";
      button.textContent = viewModel.actionLabel;
      button.addEventListener("click", requestUserLocation);
      actions.appendChild(button);
    }
    if (viewModel.blocksStationList) {
      const searchButton = document.createElement("button");
      searchButton.type = "button";
      searchButton.className = "text-btn location-gate-secondary";
      searchButton.textContent = t("location.searchMap");
      searchButton.addEventListener("click", () => {
        switchView("view-map");
        window.setTimeout(focusLocationSearchInput, 0);
      });
      actions.appendChild(searchButton);
    }
    panel.appendChild(actions);
  }

  return panel;
}

async function loadLiveStationDetail(stationId) {
  if (!stationId || !liveApiBaseUrlForStationId(stationId)) {
    return null;
  }
  const feature = findFeatureByStationId(stationId);
  if (feature && !shouldRequestLiveDataForProps(feature.properties)) {
    return null;
  }
  if (state.live.detailByStationId.has(stationId)) {
    return state.live.detailByStationId.get(stationId);
  }

  try {
    const apiStationId = toLiveApiStationId(stationId);
    const payload = await fetchJsonWithTimeout(
      buildStationLiveApiUrl(stationId, `/v1/stations/${encodeURIComponent(apiStationId)}`, {
        history_limit: 20,
      }),
      {},
      LIVE_DETAIL_TIMEOUT_MS,
    );
    if (!payload || typeof payload !== "object") {
      throw new Error("Unexpected station detail payload");
    }
    const normalizedPayload = {
      ...payload,
      station: payload.station ? { ...payload.station, station_id: stationId } : payload.station,
      evses: Array.isArray(payload.evses)
        ? payload.evses.map((evse) => ({ ...evse, station_id: stationId }))
        : payload.evses,
    };
    state.live.reachable = true;
    state.live.detailByStationId.set(stationId, normalizedPayload);
    if (feature && normalizedPayload.station) {
      applyLiveStationSummaryToProps(feature.properties, normalizedPayload.station);
      state.live.summaryByStationId.set(stationId, normalizedPayload.station);
      state.live.summaryFetchedAtByStationId.set(stationId, Date.now());
    }
    refreshRenderedViews({ markerStationIds: [stationId] });
    return normalizedPayload;
  } catch (err) {
    if (!isAbortError(err)) {
      console.error(`Failed to load live detail for station ${stationId}`, err);
    }
    return null;
  }
}

function catalogDetailLookupIds(stationId) {
  const normalized = normalizeStationId(stationId);
  const ids = [normalized];
  const namespacedMatch = normalized.match(NAMESPACED_STATION_ID_RE);
  if (namespacedMatch) {
    ids.push(namespacedMatch[1].toLowerCase());
  }
  return Array.from(new Set(ids.filter(Boolean)));
}

async function fetchCatalogStationDetailPayload(stationId) {
  let lastError = null;
  for (const lookupId of catalogDetailLookupIds(stationId)) {
    try {
      return await fetchJsonWithTimeout(
        buildLiveApiUrl(`/v1/catalog/stations/${encodeURIComponent(lookupId)}`),
        {},
        CATALOG_DETAIL_TIMEOUT_MS,
      );
    } catch (err) {
      lastError = err;
    }
  }
  throw lastError || new Error("catalog_station_detail_failed");
}

function applyCatalogStationDetailToFeature(feature, payload) {
  if (!feature || !payload || typeof payload !== "object") {
    return;
  }

  if (payload.station) {
    const currentDistance = feature.properties?.distance_m;
    const enriched = catalogStationToFeature(payload.station);
    feature.geometry = enriched.geometry;
    feature.properties = {
      ...feature.properties,
      ...enriched.properties,
      distance_m: Number.isFinite(Number(enriched.properties.distance_m))
        ? enriched.properties.distance_m
        : currentDistance,
    };
  }

  const props = feature.properties;
  const amenities = payload.amenities && typeof payload.amenities === "object"
    ? payload.amenities
    : null;
  if (amenities) {
    props.amenities_total = Math.max(0, Math.round(finiteNumber(
      amenities.amenities_total,
      props.amenities_total || 0,
    )));
    props.nearest_amenity_kind = firstText(
      amenities.nearest_amenity_kind,
      props.nearest_amenity_kind,
    );
    props.nearest_amenity_name = firstText(
      amenities.nearest_amenity_name,
      props.nearest_amenity_name,
    );
    props.nearest_amenity_distance_m = finiteNumber(
      amenities.nearest_amenity_distance_m,
      props.nearest_amenity_distance_m,
    );
    if (amenities.amenity_category_counts && typeof amenities.amenity_category_counts === "object") {
      props.amenity_category_counts = amenities.amenity_category_counts;
      applyCatalogAmenityCounts(props, amenities.amenity_category_counts);
    }
    const examples = normalizeCatalogAmenityExamples(amenities.amenity_examples);
    if (examples.length > 0) {
      props.amenity_examples = examples;
    }
  }

  const chargers = Array.isArray(payload.chargers) ? payload.chargers : [];
  if (chargers.length > 0) {
    props.connector_count = chargers.length;
    props.charging_points_count = Math.max(getChargingPointCount(props), chargers.length);
    const connectorTypes = Array.from(new Set(
      chargers.map((charger) => firstText(charger.connector_type)).filter(Boolean),
    ));
    const currentTypes = Array.from(new Set(
      chargers.map((charger) => firstText(charger.current_type)).filter(Boolean),
    ));
    if (connectorTypes.length > 0) {
      props.connector_types_display = connectorTypes.join(", ");
    }
    if (currentTypes.length > 0) {
      props.current_types_display = currentTypes.join(", ");
    }
  }
}

async function loadCatalogStationDetail(stationId) {
  const normalizedStationId = normalizeStationId(stationId);
  if (!state.live.baseUrl || !normalizedStationId) {
    return null;
  }
  if (state.catalog.detailByStationId.has(normalizedStationId)) {
    return state.catalog.detailByStationId.get(normalizedStationId);
  }
  if (state.catalog.pendingDetailStationIds.has(normalizedStationId)) {
    return null;
  }

  state.catalog.pendingDetailStationIds.add(normalizedStationId);
  try {
    const payload = await fetchCatalogStationDetailPayload(normalizedStationId);
    if (!payload || typeof payload !== "object" || !payload.station) {
      throw new Error("Unexpected catalog station detail payload");
    }

    const payloadStationId = normalizeStationId(payload.station.station_id || normalizedStationId);
    let feature = findFeatureByStationId(payloadStationId) || findFeatureByStationId(normalizedStationId);
    if (!feature) {
      const detailFeature = catalogStationToFeature(payload.station);
      upsertCatalogFeatures([detailFeature]);
      feature = findFeatureByStationId(payloadStationId) || detailFeature;
    }
    applyCatalogStationDetailToFeature(feature, payload);
    const featureStationId = getStationIdFromProps(feature.properties) || payloadStationId || normalizedStationId;
    state.catalog.detailByStationId.set(featureStationId, payload);
    state.catalog.missingDetailStationIds.delete(featureStationId);
    if (featureStationId && normalizeStationId(getRequestedStationId()) === normalizeStationId(featureStationId)) {
      await loadLiveStationDetail(featureStationId);
      applyCachedLiveStationSummaryToFeature(feature);
    }
    populateOperators();
    renderAmenityFilters();
    applyFilters();
    if (
      currentDetailFeature &&
      getStationIdFromProps(currentDetailFeature.properties) === featureStationId &&
      !els.modals.detail.classList.contains("hidden")
    ) {
      populateDetailContent(feature, state.live.detailByStationId.get(featureStationId) || null);
    }
    return payload;
  } catch (err) {
    console.warn(`Failed to load catalog detail for station ${normalizedStationId}`, err);
    state.catalog.missingDetailStationIds.add(normalizedStationId);
    return null;
  } finally {
    state.catalog.pendingDetailStationIds.delete(normalizedStationId);
  }
}

function setAppMeta(geoData, summaryData, openStaticSummaryData = null) {
  const generatedAt =
    openStaticSummaryData?.generated_at ||
    summaryData?.run?.finished_at ||
    geoData?.generated_at ||
    null;

  if (els.meta && generatedAt) {
    const date = formatDateTime(generatedAt);
    const stationTotal = Number(openStaticSummaryData?.bundle?.station_count || 0);
    const chargerTotal = Number(openStaticSummaryData?.bundle?.charger_count || 0);
    const countSuffix = stationTotal && chargerTotal
      ? t("info.countSuffix", {
          stations: formatInteger(stationTotal),
          chargers: formatInteger(chargerTotal),
        })
      : "";
    els.meta.textContent = t("info.dataUpdated", { date, counts: countSuffix });
  }
  renderBundleCounts(openStaticSummaryData, summaryData);
  renderMappedCountries(openStaticSummaryData);
  renderDataSources(openStaticSummaryData);
}

function formatInteger(value) {
  return formatLocalizedInteger(value);
}

function formatCountryName(countryCode, fallback = "") {
  const code = String(countryCode || "").trim().toUpperCase();
  if (!code || typeof Intl.DisplayNames !== "function") {
    return fallback || code;
  }
  try {
    return new Intl.DisplayNames([getLocale(), "en"], { type: "region" }).of(code) || fallback || code;
  } catch {
    return fallback || code;
  }
}

function countrySourcesByCode(openStaticSummaryData) {
  const sourcesByCode = new Map();
  normalizeBundleSources(openStaticSummaryData, getLocale()).forEach((source) => {
    const code = String(source.countryCode || "").trim().toUpperCase();
    if (!code) {
      return;
    }
    const sources = sourcesByCode.get(code) || [];
    sources.push(source);
    sourcesByCode.set(code, sources);
  });
  return sourcesByCode;
}

function compactCountrySourceLabel(source) {
  const code = String(source?.countryCode || "").trim().toUpperCase();
  const label = String(source?.displayName || formatBundleSourceTitle(source) || "").trim();
  if (!label) {
    return t("info.sourceUnknown");
  }
  return code
    ? label.replace(new RegExp(`^${code}\\s*:?\\s*`, "i"), "").trim() || label
    : label;
}

function countrySourceLinks(countryCode, sourcesByCode) {
  const code = String(countryCode || "").trim().toUpperCase();
  if (code === "DE") {
    return [DE_MOBILITHEK_SOURCE];
  }
  const seen = new Set();
  return (sourcesByCode.get(code) || [])
    .map((source) => ({
      label: compactCountrySourceLabel(source),
      url: String(source.sourceUrl || "").trim(),
    }))
    .filter((source) => {
      const key = `${source.label}\u0001${source.url}`;
      if (seen.has(key)) {
        return false;
      }
      seen.add(key);
      return Boolean(source.label);
    });
}

function appendCountrySourceLinks(cell, countryCode, sourcesByCode) {
  const links = countrySourceLinks(countryCode, sourcesByCode);
  if (!links.length) {
    cell.textContent = t("info.sourceUnknown");
    return;
  }
  const list = document.createElement("div");
  list.className = "country-source-list";
  links.forEach((source) => {
    const item = document.createElement(source.url ? "a" : "span");
    item.textContent = source.label;
    if (source.url) {
      item.href = source.url;
      item.target = "_blank";
      item.rel = "noopener noreferrer";
    }
    list.appendChild(item);
  });
  cell.appendChild(list);
}

function renderBundleCounts(openStaticSummaryData, summaryData) {
  const countryTotals = normalizeMappedCountries(openStaticSummaryData, getLocale()).reduce(
    (totals, country) => ({
      stations: totals.stations + (Number(country.stationCount) || 0),
      chargers: totals.chargers + (Number(country.chargerCount) || 0),
    }),
    { stations: 0, chargers: 0 },
  );
  const stationCount =
    Number(openStaticSummaryData?.bundle?.station_count || 0) ||
    countryTotals.stations ||
    Number(summaryData?.records?.full_registry_active_stations_total || 0);
  const chargerCount =
    Number(openStaticSummaryData?.bundle?.charger_count || 0) ||
    countryTotals.chargers ||
    Number(summaryData?.records?.raw_rows || 0);
  if (els.info.stationCount) {
    els.info.stationCount.textContent = formatInteger(stationCount) || "...";
  }
  if (els.info.chargerCount) {
    els.info.chargerCount.textContent = formatInteger(chargerCount) || "...";
  }
}

function renderMappedCountries(openStaticSummaryData) {
  const container = els.info.mappedCountries;
  if (!container) {
    return;
  }
  const displayCountries = normalizeMappedCountries(openStaticSummaryData, getLocale());
  const sourcesByCode = countrySourcesByCode(openStaticSummaryData);
  container.replaceChildren();
  if (!displayCountries.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = t("info.countryLoadError");
    row.appendChild(cell);
    container.appendChild(row);
    return;
  }
  displayCountries.forEach((country) => {
    const row = document.createElement("tr");
    const name = document.createElement("td");
    const code = document.createElement("td");
    const count = document.createElement("td");
    const source = document.createElement("td");
    name.textContent = formatCountryName(country.code, country.name);
    code.textContent = country.code ? `(${country.code})` : "";
    code.className = "country-code";
    count.className = "station-count";
    count.textContent = formatInteger(country.stationCount) || "...";
    source.className = "country-source";
    appendCountrySourceLinks(source, country.code, sourcesByCode);
    row.append(name, code, count, source);
    container.appendChild(row);
  });
}

function renderDataSources(openStaticSummaryData) {
  const container = els.info.dataSources;
  if (!container) {
    return;
  }
  const displaySources = normalizeBundleSources(openStaticSummaryData, getLocale());
  container.replaceChildren();
  let renderedSourceCount = 0;

  displaySources.forEach((source) => {
    const sourceTitle = formatBundleSourceTitle(source);
    if (!sourceTitle) {
      return;
    }

    const item = document.createElement("li");
    const title = document.createElement("div");
    title.className = "source-title";
    if (source.sourceUrl) {
      const link = document.createElement("a");
      link.href = source.sourceUrl;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent = sourceTitle;
      title.appendChild(link);
    } else {
      title.textContent = sourceTitle;
    }
    item.appendChild(title);
    container.appendChild(item);
    renderedSourceCount += 1;
  });

  if (!renderedSourceCount) {
    const item = document.createElement("li");
    item.textContent = t("info.sourceLoadError");
    container.appendChild(item);
  }

  const geocoderItem = document.createElement("li");
  const geocoderTitle = document.createElement("div");
  geocoderTitle.className = "source-title";
  const geocoderLink = document.createElement("a");
  geocoderLink.href = "https://openrouteservice.org/dev/#/api-docs/geocode/autocomplete/get";
  geocoderLink.target = "_blank";
  geocoderLink.rel = "noopener noreferrer";
  geocoderLink.textContent = t("sources.geocoder");
  geocoderTitle.appendChild(geocoderLink);
  geocoderItem.appendChild(geocoderTitle);
  container.appendChild(geocoderItem);

  const easterEggItem = document.createElement("li");
  const easterEggTitle = document.createElement("div");
  easterEggTitle.className = "source-title";
  const easterEggLink = document.createElement("a");
  easterEggLink.href = "https://hellmood.111mb.de//wake_up_16b_writeup.html";
  easterEggLink.target = "_blank";
  easterEggLink.rel = "noopener noreferrer";
  easterEggLink.textContent = t("sources.easterEgg");
  easterEggTitle.appendChild(easterEggLink);
  easterEggItem.appendChild(easterEggTitle);
  container.appendChild(easterEggItem);
}

function populateOperators() {
  const selectedOperator = state.filters.operator;
  const operatorCounts = new Map();
  getFilterSourceFeatures().forEach((feature) => {
    const name = String(feature?.properties?.operator || "").trim();
    if (!name) return;
    operatorCounts.set(name, (operatorCounts.get(name) || 0) + 1);
  });

  const operators = Array.from(operatorCounts.entries())
    .filter(([, stations]) => stations >= 1)
    .map(([name]) => name)
    .sort();

  els.filter.operator.querySelectorAll("option:not([value=''])").forEach((option) => {
    option.remove();
  });
  operators.forEach((op) => {
    const opt = document.createElement("option");
    opt.value = op;
    opt.textContent = op;
    els.filter.operator.appendChild(opt);
  });
  els.filter.operator.value = selectedOperator;
}

/* --- MAP LOGIC --- */
function initMap() {
  state.views.map = L.map("map", { zoomControl: false }).setView(
    [51.1657, 10.4515],
    6,
  );

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "© OpenStreetMap",
  }).addTo(state.views.map);

  state.views.layers.route = L.layerGroup().addTo(state.views.map);
  state.views.layers.chargers = L.layerGroup().addTo(state.views.map);

  state.views.layers.user = L.layerGroup().addTo(state.views.map);
  ["dragstart", "zoomstart"].forEach((eventName) => {
    state.views.map.on(eventName, () => {
      state.mapInteraction.hasUserInteracted = true;
    });
  });
  state.views.map.on("moveend", queueCatalogSearchFromMapMove);
  state.views.map.on("zoomend", () => {
    renderMapMarkers();
  });

  // Detail Mini Map
  state.views.detailMap = L.map("detail-map", {
    zoomControl: false,
    dragging: false,
    touchZoom: false,
    boxZoom: false,
    scrollWheelZoom: false,
    doubleClickZoom: false,
    attributionControl: false,
  });
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
  }).addTo(state.views.detailMap);
  state.views.detailMap.setView([51.1657, 10.4515], 6, { animate: false });
  state.views.layers.detailAmenities = L.layerGroup().addTo(state.views.detailMap);
}

function loadCatalogStationsFromMapCenter({
  force = false,
  requireUserInteraction = false,
  reset = false,
} = {}) {
  if (!state.views.map) {
    return;
  }
  if (hasPinnedRouteMap()) {
    return;
  }
  if (
    requireUserInteraction &&
    !state.mapInteraction.hasUserInteracted &&
    !hasCatalogSearchCenter()
  ) {
    return;
  }
  const mapCenter = state.views.map.getCenter();
  const nextCenter = normalizeCatalogCenter({
    lat: mapCenter.lat,
    lon: mapCenter.lng,
    source: "map",
  });
  if (!nextCenter) {
    return;
  }
  const previousCenter = getCatalogSearchCenter();
  if (
    !force &&
    previousCenter &&
    distanceBetweenCoordinatesMeters(previousCenter, nextCenter) < CATALOG_MIN_RELOAD_DISTANCE_M
  ) {
    return;
  }
  setCatalogSearchCenter(nextCenter, "map");
  void loadCatalogStationsForCurrentCenter({ force, reset });
}

function queueCatalogSearchFromMapMove() {
  if (!state.views.map) {
    return;
  }
  if (hasPinnedRouteMap()) {
    window.clearTimeout(catalogMapMoveTimer);
    return;
  }
  window.clearTimeout(catalogMapMoveTimer);
  catalogMapMoveTimer = window.setTimeout(() => {
    loadCatalogStationsFromMapCenter({ requireUserInteraction: true });
  }, CATALOG_MAP_MOVE_DEBOUNCE_MS);
}

function isMapViewActive() {
  return Boolean(els.views.map?.classList.contains("active"));
}

function startMapGPSRefresh() {
  if (!navigator.geolocation || mapGPSRefreshTimer || document.visibilityState === "hidden" || hasPinnedRouteMap()) {
    return;
  }
  mapGPSRefreshTimer = window.setInterval(() => {
    if (!isMapViewActive() || document.visibilityState === "hidden" || hasPinnedRouteMap()) {
      stopMapGPSRefresh();
      return;
    }
    void refreshCatalogFromGPSPosition({
      reset: false,
      recenter: false,
      showPending: false,
      showError: false,
      maximumAge: MAP_GPS_REFRESH_MAX_LOCATION_AGE_MS,
    });
  }, MAP_GPS_REFRESH_INTERVAL_MS);
}

function stopMapGPSRefresh() {
  if (!mapGPSRefreshTimer) {
    return;
  }
  window.clearInterval(mapGPSRefreshTimer);
  mapGPSRefreshTimer = 0;
}

function getMarkerColor(props) {
  const total = props.amenities_total || 0;
  if (total > 10) return "#f59e0b"; // Gold
  if (total > 5) return "#94a3b8"; // Silver
  if (total > 0) return "#b45309"; // Bronze
  return "#64748b"; // Grey
}

function hasAvailabilitySummary(props) {
  return hasLiveStationSummary(props) || hasAggregateOccupancySummary(props);
}

function isStationOutOfOrder(props) {
  return hasAvailabilitySummary(props) && getAvailabilityStatus(props) === "out_of_order";
}

function isStationFullyOccupied(props) {
  return hasAvailabilitySummary(props) && getAvailabilityStatus(props) === "occupied";
}

function isStationOneFreeLeft(props) {
  if (!hasAvailabilitySummary(props)) {
    return false;
  }
  const counts = getAvailabilityCounts(props);
  return counts.total > 1 && counts.available === 1;
}

function stationHasAnalyticsState(props, stateKey) {
  const stationId = normalizeStationId(getStationIdFromProps(props));
  return Boolean(stationId && state.analytics[stateKey]?.has(stationId));
}

function isStationOftenBroken(props) {
  return stationHasAnalyticsState(props, "oftenBrokenStationIds");
}

function isStationOftenOccupied(props) {
  return stationHasAnalyticsState(props, "oftenOccupiedStationIds");
}

function getStationCardStateClass(props) {
  if (hasAvailabilitySummary(props)) {
    const counts = getAvailabilityCounts(props);
    if ((counts.total > 0 && counts.outOfOrder >= counts.total) || isStationOutOfOrder(props)) {
      return "station-card-out-of-order";
    }
    if ((counts.total > 0 && counts.occupied >= counts.total) || isStationFullyOccupied(props)) {
      return "station-card-occupied";
    }
    if (isStationOneFreeLeft(props)) {
      return "station-card-one-free-left";
    }
  }
  if (isStationOftenBroken(props)) {
    return "station-card-often-broken";
  }
  if (isStationOftenOccupied(props)) {
    return "station-card-often-occupied";
  }
  return "";
}

function formatStationMarkerLabel(feature) {
  const props = feature?.properties || {};
  const name = firstText(props.operator, props.station_name, t("station.chargingStation"));
  const city = firstText(props.city);
  const power = Math.round(getDisplayedMaxPowerKw(props));
  const powerText = power > 0 ? `${power} kW` : "";
  const amenityText = formatAmenityCount(props.amenities_total);
  const occupancyText = formatOccupancySummary(props);
  const parts = [name, city, powerText, amenityText, occupancyText]
    .map((part) => String(part || "").trim())
    .filter(Boolean);
  return `${t("aria.detailsOpen")}: ${parts.join(", ")}`;
}

function enhanceStationMarkerElement(marker, feature) {
  const element = typeof marker.getElement === "function" ? marker.getElement() : null;
  if (!element) {
    return;
  }
  const stationId = getStationIdFromProps(feature?.properties || {});
  const label = formatStationMarkerLabel(feature);
  if (stationId) {
    element.setAttribute("data-station-id", stationId);
  }
  element.setAttribute("role", "button");
  element.setAttribute("aria-label", label);
  element.setAttribute("title", label);
  element.setAttribute("tabindex", "0");
  if (element.getAttribute("data-keyboard-bound") !== "true") {
    element.setAttribute("data-keyboard-bound", "true");
    element.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      openDetail(feature);
    });
  }
}

function getLiveStatusMarkerIcon(statusKey) {
  if (liveStatusMarkerIcons.has(statusKey)) {
    return liveStatusMarkerIcons.get(statusKey);
  }

  const config = LIVE_STATUS_MARKER_CONFIGS[statusKey];
  const icon = L.icon({
    iconUrl: `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(config.svg)}`,
    iconRetinaUrl: `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(config.svg)}`,
    iconSize: [config.size, config.size],
    iconAnchor: [config.size / 2, config.size / 2],
    className: "station-map-marker-icon",
  });
  liveStatusMarkerIcons.set(statusKey, icon);
  return icon;
}

function createLiveStatusMarker(lat, lon, statusKey, feature) {
  const marker = L.marker([lat, lon], {
    icon: getLiveStatusMarkerIcon(statusKey),
    keyboard: true,
    title: formatStationMarkerLabel(feature),
  });
  return marker;
}

function getFavoriteStationMarkerIcon() {
  if (favoriteStationMarkerIcon) {
    return favoriteStationMarkerIcon;
  }

  const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="${FAVORITE_MARKER_SIZE}" height="${FAVORITE_MARKER_SIZE}" viewBox="0 0 ${FAVORITE_MARKER_SIZE} ${FAVORITE_MARKER_SIZE}">
    <path d="M14 2.5l3.48 7.05 7.78 1.13-5.63 5.49 1.33 7.75L14 20.26l-6.96 3.66 1.33-7.75-5.63-5.49 7.78-1.13L14 2.5z" fill="#f59e0b" stroke="#ffffff" stroke-width="2.2" stroke-linejoin="round"/>
    <path d="M14 5.6l2.47 5 5.52.8-3.99 3.89.94 5.49L14 18.19l-4.94 2.59.94-5.49-3.99-3.89 5.52-.8L14 5.6z" fill="#fbbf24"/>
  </svg>`;
  favoriteStationMarkerIcon = L.icon({
    iconUrl: `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`,
    iconRetinaUrl: `data:image/svg+xml;charset=UTF-8,${encodeURIComponent(svg)}`,
    iconSize: [FAVORITE_MARKER_SIZE, FAVORITE_MARKER_SIZE],
    iconAnchor: [FAVORITE_MARKER_SIZE / 2, FAVORITE_MARKER_SIZE / 2],
    className: "station-map-marker-icon station-map-marker-favorite-icon",
  });
  return favoriteStationMarkerIcon;
}

function createFavoriteStationMarker(lat, lon, feature) {
  return L.marker([lat, lon], {
    icon: getFavoriteStationMarkerIcon(),
    keyboard: true,
    title: formatStationMarkerLabel(feature),
  });
}

function createStationMarker(feature) {
  const [lon, lat] = feature.geometry.coordinates;
  const props = feature.properties;
  const stationId = getStationIdFromProps(props);

  if (stationId && state.favorites.has(stationId)) {
    return createFavoriteStationMarker(lat, lon, feature);
  }

  if (isStationOutOfOrder(props)) {
    return createLiveStatusMarker(lat, lon, "outOfOrder", feature);
  }

  if (isStationFullyOccupied(props)) {
    return createLiveStatusMarker(lat, lon, "fullyOccupied", feature);
  }

  const color = getMarkerColor(props);
  return L.circleMarker([lat, lon], {
    color: "#ffffff",
    weight: 1,
    fillColor: color,
    fillOpacity: 1,
    radius: 8,
  });
}

function bindStationMarker(marker, feature) {
  marker.bindTooltip(formatStationMarkerLabel(feature), { direction: "top", offset: [0, -8] });
  marker.on("add", () => enhanceStationMarkerElement(marker, feature));
  marker.on("click", () => openDetail(feature));
  return marker;
}

function renderDetailStationMarker(feature) {
  if (!state.views.detailMap || !feature) {
    return;
  }
  if (state.views.detailMap.stationMarker) {
    state.views.detailMap.removeLayer(state.views.detailMap.stationMarker);
  }
  const marker = createStationMarker(feature).addTo(state.views.detailMap);
  marker.bindTooltip(formatStationMarkerLabel(feature), { direction: "top", offset: [0, -8] });
  state.views.detailMap.stationMarker = marker;
}

function getMapMarkerFeatures() {
  if (hasPinnedRouteMap()) {
    return getRouteDisplayFeatures();
  }
  const zoom = Number(
    state.views.map && typeof state.views.map.getZoom === "function"
      ? state.views.map.getZoom()
      : NaN,
  );
  if (Number.isFinite(zoom) && zoom >= MAP_UNCLUSTERED_FULL_RENDER_ZOOM) {
    return state.filtered;
  }
  return state.filtered.slice(0, MAP_UNCLUSTERED_MARKER_LIMIT);
}

function getFeatureLatLon(feature) {
  const [lon, lat] = feature?.geometry?.coordinates || [];
  if (!Number.isFinite(Number(lat)) || !Number.isFinite(Number(lon))) {
    return null;
  }
  return { lat: Number(lat), lon: Number(lon) };
}

function centerMapOnFeature(feature, options = {}) {
  if (!state.views.map) {
    return;
  }
  const { minZoom = 13 } = options;
  const coords = getFeatureLatLon(feature);
  if (!coords) {
    return;
  }
  const zoom = Math.max(state.views.map.getZoom(), minZoom);
  state.views.map.setView([coords.lat, coords.lon], zoom, { animate: false });
}

function focusMapOnPendingStation() {
  const stationId = normalizeStationId(state.mapFocus.stationId || "");
  if (!stationId || !state.views.map) {
    return false;
  }
  const feature = findFeatureByStationId(stationId) ||
    (
      currentDetailFeature &&
      normalizeStationId(getStationIdFromProps(currentDetailFeature.properties)) === stationId
        ? currentDetailFeature
        : null
    );
  if (!feature) {
    return false;
  }
  centerMapOnFeature(feature);
  state.mapFocus.stationId = "";
  return true;
}

function renderMapMarkers() {
  state.views.layers.chargers.clearLayers();
  state.views.markersByStationId.clear();

  getMapMarkerFeatures().forEach((feature) => {
    applyCachedLiveStationSummaryToFeature(feature);
    const marker = bindStationMarker(createStationMarker(feature), feature);
    const stationId = getStationIdFromProps(feature.properties);
    if (stationId) {
      state.views.markersByStationId.set(stationId, marker);
    }

    marker.addTo(state.views.layers.chargers);
  });
}

function findMapMarkerFeatureByStationId(stationId) {
  if (hasPinnedRouteMap()) {
    return findRouteFeatureByStationId(stationId);
  }
  return findFeatureByStationId(stationId);
}

function updateMapMarkersForStationIds(stationIds) {
  if (!state.views.layers.chargers || !Array.isArray(stationIds) || stationIds.length === 0) {
    return;
  }

  const displayedFeatures = new Set(getMapMarkerFeatures());
  Array.from(new Set(stationIds)).forEach((stationId) => {
    const feature = findMapMarkerFeatureByStationId(stationId);
    const existingMarker = state.views.markersByStationId.get(stationId);
    const isFiltered = hasPinnedRouteMap() || (feature ? state.filtered.includes(feature) : false);
    const isDisplayedOnMap = feature ? displayedFeatures.has(feature) : false;

    if (existingMarker) {
      state.views.layers.chargers.removeLayer(existingMarker);
      state.views.markersByStationId.delete(stationId);
    }

    if (!feature || !isFiltered || !isDisplayedOnMap) {
      return;
    }

    const nextMarker = bindStationMarker(createStationMarker(feature), feature);
    state.views.markersByStationId.set(stationId, nextMarker);
    nextMarker.addTo(state.views.layers.chargers);
  });
}

function updateUserMarker() {
  if (!state.userPos || !state.views.layers.user) return;
  state.views.layers.user.clearLayers();

  L.circleMarker([state.userPos.lat, state.userPos.lon], {
    color: "#ffffff",
    weight: 2,
    fillColor: "#3b82f6", // Blue
    fillOpacity: 1,
    radius: 10,
  }).addTo(state.views.layers.user);
}

/* --- NAVIGATION & VIEWS --- */
function initNavigation() {
  els.navItems.forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetId = btn.dataset.target;
      switchView(targetId);
    });
  });
}

function getActiveViewId() {
  return VIEW_ORDER.find((viewId) => document.getElementById(viewId)?.classList.contains("active")) ||
    "view-list";
}

function focusNavItem(viewId) {
  const navItem = Array.from(els.navItems).find((btn) => btn.dataset.target === viewId);
  navItem?.focus({ preventScroll: true });
}

function setActiveNavItem(viewId) {
  els.navItems.forEach((btn) => {
    const isActive = btn.dataset.target === viewId;
    btn.classList.toggle("active", isActive);
    if (isActive) {
      btn.setAttribute("aria-current", "page");
    } else {
      btn.removeAttribute("aria-current");
    }
  });
}

function handleViewNavigationKeydown(event) {
  if (isAnyModalOpen() || event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) {
    return false;
  }
  if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
    return false;
  }
  const currentIndex = VIEW_ORDER.indexOf(getActiveViewId());
  if (currentIndex < 0) {
    return false;
  }
  const delta = event.key === "ArrowRight" ? 1 : -1;
  const nextIndex = (currentIndex + delta + VIEW_ORDER.length) % VIEW_ORDER.length;
  const nextViewId = VIEW_ORDER[nextIndex];
  event.preventDefault();
  switchView(nextViewId);
  focusNavItem(nextViewId);
  return true;
}

function normalizeRequestedViewId(value) {
  const rawHash = String(value || "")
    .replace(/^#/, "")
    .replace(/^\/+/, "")
    .trim()
    .toLowerCase();
  if (!rawHash) {
    return "";
  }
  const canonicalHash = VIEW_HASH_ALIASES.get(rawHash) || rawHash;
  return VIEW_IDS.has(canonicalHash) ? canonicalHash : "";
}

function getRequestedViewIdFromHash() {
  const requestedViewId = normalizeRequestedViewId(window.location.hash);
  if (requestedViewId) {
    return requestedViewId;
  }
  if (!hasAppliedInitialRequestedView && INITIAL_REQUESTED_VIEW_ID) {
    return INITIAL_REQUESTED_VIEW_ID;
  }
  return "view-list";
}

function updateRequestedViewHash(viewId) {
  const url = new URL(window.location.href);
  url.hash = viewId && viewId !== "view-list" ? viewId : "";
  const next = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState(window.history.state, "", next);
}

function syncViewWithRequestedHash() {
  const viewId = getRequestedViewIdFromHash();
  hasAppliedInitialRequestedView = true;
  switchView(viewId, { syncHash: false });
}

function switchView(viewId, options = {}) {
  const { syncHash = true } = options;
  if (!VIEW_IDS.has(viewId)) {
    return;
  }
  // Hide all views
  Object.values(els.views).forEach((el) => {
    el.classList.remove("active");
    el.classList.add("hidden");
    // Small delay to allow display:none to apply before opacity transition if needed
    // But CSS transitions handle opacity/visibility
  });

  // Show target
  const target = document.getElementById(viewId);
  if (target) {
    target.classList.remove("hidden");
    // Force reflow
    void target.offsetWidth;
    target.classList.add("active");
  }
  setActiveNavItem(viewId);
  if (syncHash) {
    updateRequestedViewHash(viewId);
  }

  // Refresh lists if needed
  if (viewId === "view-list") renderList();
  if (viewId === "view-route") renderRouteResults();
  if (viewId === "view-favorites") renderFavorites();

  // Map resize fix
  if (viewId === "view-map" && state.views.map) {
    renderRouteLayer();
    renderRouteMapLock();
    refreshMapMarkersFromCurrentFeatures();
    if (!hasPinnedRouteMap()) {
      startMapGPSRefresh();
    }
    requestAnimationFrame(() => {
      state.views.map.invalidateSize({ pan: false });
      focusMapOnPendingStation();
      state.views.map.invalidateSize({ pan: false });
      refreshMapMarkersFromCurrentFeatures();
    });
    setTimeout(() => {
      state.views.map.invalidateSize({ pan: false });
      refreshMapMarkersFromCurrentFeatures();
    }, 150);
  } else {
    stopMapGPSRefresh();
    renderRouteMapLock();
  }
}

function refreshMapMarkersFromCurrentFeatures() {
  if (!state.views.layers.chargers) {
    return;
  }
  renderMapMarkers();
}

/* --- FILTER LOGIC --- */
function syncFilterControlsFromState() {
  els.filter.operator.value = state.filters.operator || "";
  els.filter.amenityName.value = state.filters.amenityNameQuery || "";
  els.filter.availableOnly.checked = Boolean(state.filters.availableOnly);
  els.filter.currentlyOpen.checked = Boolean(state.filters.currentlyOpenOnly);
  els.filter.power.value = String(Math.round(Number(state.filters.minPower || DEFAULT_MIN_POWER_KW)));
  els.filter.amenityCount.value = String(Math.round(Number(state.filters.minAmenityCount || 0)));
  updatePowerFilterLabel();
  updateAmenityCountFilterLabel();
}

function initFilters() {
  syncFilterControlsFromState();

  // Operator
  els.filter.operator.addEventListener("change", (e) => {
    state.filters.operator = e.target.value;
    updateFilters();
  });

  // Amenity name
  els.filter.amenityName.addEventListener("input", (e) => {
    state.filters.amenityNameQuery = e.target.value;
    updateFilters();
  });

  // Available charging points
  els.filter.availableOnly.addEventListener("change", (e) => {
    state.filters.availableOnly = e.target.checked;
    updateFilters();
  });

  // Currently open offers
  els.filter.currentlyOpen.addEventListener("change", (e) => {
    state.filters.currentlyOpenOnly = e.target.checked;
    updateFilters();
  });

  // Power
  els.filter.power.addEventListener("input", (e) => {
    state.filters.minPower = Number(e.target.value);
    els.filter.powerVal.textContent = state.filters.minPower;
    updatePowerFilterLabel();
    updateFilters({ reloadCatalog: true });
  });

  // Amenity count
  els.filter.amenityCount.addEventListener("input", (e) => {
    state.filters.minAmenityCount = Number(e.target.value);
    els.filter.amenityCountVal.textContent = state.filters.minAmenityCount;
    updateAmenityCountFilterLabel();
    updateFilters();
  });
}

function updatePowerFilterLabel() {
  if (els.filter.powerLabel) {
    els.filter.powerLabel.textContent = t("filters.minPower", {
      value: Math.round(Number(state.filters.minPower || DEFAULT_MIN_POWER_KW)),
    });
  }
  if (els.filter.powerVal) {
    els.filter.powerVal.textContent = String(Math.round(Number(state.filters.minPower || DEFAULT_MIN_POWER_KW)));
  }
}

function updateAmenityCountFilterLabel() {
  const value = Math.round(Number(state.filters.minAmenityCount || 0));
  if (els.filter.amenityCountLabel) {
    els.filter.amenityCountLabel.textContent = t("filters.minAmenities", { value });
  }
  if (els.filter.amenityCountVal) {
    els.filter.amenityCountVal.textContent = String(value);
  }
}

function renderAmenityFilters() {
  els.filter.amenities.innerHTML = "";
  
  // Find all available amenities in data
  const availableAmenities = new Set();
  const amenityKeys = Object.keys(AMENITY_MAPPING);
  
  getFilterSourceFeatures().forEach(f => {
    const p = f.properties;
    amenityKeys.forEach(key => {
      if (p[key] > 0) availableAmenities.add(key);
    });
  });

  // Sort by name for better UX
  const sortedKeys = Array.from(availableAmenities).sort((a, b) => {
    const labelA = getAmenityLabel(a);
    const labelB = getAmenityLabel(b);
    return labelA.localeCompare(labelB, getLocale());
  });

  sortedKeys.forEach((key) => {
    const config = AMENITY_MAPPING[key];
    const label = getAmenityLabel(key);
    const path = getAmenityIconPath(key);

    const button = document.createElement("button");
    button.type = "button";
    button.className = "amenity-toggle";
    button.dataset.key = key;
    const isActive = state.filters.amenities.has(key);
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
    button.setAttribute(
      "aria-label",
      isActive
        ? t("filters.activeAmenity", { label })
        : t("filters.filterAmenity", { label }),
    );

    if (path) {
      button.innerHTML = `<img src="${path}" alt="" loading="lazy"><span class="amenity-name">${escapeHtml(label)}</span>`;
    } else {
      button.innerHTML = `<span class="amenity-icon-fallback" aria-hidden="true">?</span><span class="amenity-name">${escapeHtml(label)}</span>`;
    }

    button.addEventListener("click", () => {
      const nextActive = !state.filters.amenities.has(key);
      button.classList.toggle("active", nextActive);
      button.setAttribute("aria-pressed", nextActive ? "true" : "false");
      button.setAttribute(
        "aria-label",
        nextActive ? `${config.label} Filter aktiv` : `${config.label} filtern`,
      );
      if (nextActive) {
        state.filters.amenities.add(key);
      } else {
        state.filters.amenities.delete(key);
      }
      updateFilters();
    });

    els.filter.amenities.appendChild(button);
  });
}

function updateFilters(options = {}) {
  const { reloadCatalog = false } = options;
  saveFilters();
  if (reloadCatalog && hasCatalogSearchCenter() && !hasPinnedRouteMap()) {
    void loadCatalogStationsForCurrentCenter({ force: true, reset: true }).then(updateFilterLabel);
    updateFilterLabel();
    return;
  }

  applyFilters();
  updateFilterLabel();
}

function updateFilterLabel() {
  const filterCount = countActiveFilters(state.filters);
  const labels = getActiveFilterLabels();
  const labelSummary = labels.join(", ");
  const routeFilters = routeEffectiveFilters();
  const routeFilterCount = countActiveFilters(routeFilters);
  const routeLabels = getActiveFilterLabels(routeFilters);
  const routeLabelSummary = routeLabels.join(", ");

  if (els.filter.label) {
    els.filter.label.textContent =
      filterCount > 0 ? `${t("filters.title")} (${filterCount})` : t("filters.all");
  }
  if (els.filter.activeLabel) {
    els.filter.activeLabel.hidden = labels.length === 0;
    els.filter.activeLabel.textContent = labels.join(" · ");
  }
  if (els.filter.trigger) {
    els.filter.trigger.setAttribute(
      "aria-label",
      filterCount > 0
        ? t("filters.openWithCount", { count: filterCount, labels: labelSummary })
        : t("aria.filterOpen"),
    );
  }
  if (els.filter.count) {
    els.filter.count.hidden = filterCount <= 0;
    els.filter.count.textContent = String(filterCount);
  }
  if (els.filter.listFilterBtn) {
    els.filter.listFilterBtn.textContent = filterCount > 0 ? `${t("filters.title")} (${filterCount})` : t("filters.title");
    els.filter.listFilterBtn.setAttribute(
      "aria-label",
      filterCount > 0
        ? t("filters.openWithCount", { count: filterCount, labels: labelSummary })
        : t("aria.filterOpen"),
    );
    els.filter.listFilterBtn.classList.toggle("active", filterCount > 0);
  }
  if (els.filter.routeFilterBtn) {
    els.filter.routeFilterBtn.textContent = routeFilterCount > 0
      ? `${t("filters.title")} (${routeFilterCount})`
      : t("filters.title");
    els.filter.routeFilterBtn.setAttribute(
      "aria-label",
      routeFilterCount > 0
        ? t("filters.openWithCount", { count: routeFilterCount, labels: routeLabelSummary })
        : t("aria.filterOpen"),
    );
    els.filter.routeFilterBtn.classList.toggle("active", routeFilterCount > 0);
  }
  renderActiveFilterSummary(filterCount);
}

function renderActiveFilterSummary(filterCount) {
  const container = els.filter.activeSummary;
  if (!container) {
    return;
  }
  const labels = getActiveFilterLabels();
  container.hidden = filterCount <= 0 || labels.length === 0;
  container.replaceChildren();
  if (container.hidden) {
    container.removeAttribute("aria-label");
    return;
  }
  const summaryText = t("filters.selectedOnly", { labels: labels.join(" · ") });
  container.setAttribute("aria-label", summaryText);

  const summary = document.createElement("span");
  summary.className = "active-filter-summary-text";
  summary.textContent = summaryText;
  container.appendChild(summary);

  if (hasClearableFilters()) {
    const clearButton = document.createElement("button");
    clearButton.type = "button";
    clearButton.className = "active-filter-clear";
    clearButton.textContent = "X";
    clearButton.setAttribute("aria-label", t("filters.reset"));
    clearButton.title = t("filters.reset");
    clearButton.addEventListener("click", clearFilters);
    container.appendChild(clearButton);
  }
}

function getActiveFilterLabels(filters = state.filters) {
  const labels = [];
  if (filters.operator) {
    labels.push(filters.operator);
  }
  const amenityNameQuery = String(filters.amenityNameQuery || "").trim();
  if (amenityNameQuery) {
    labels.push(t("filters.namePrefix", { value: amenityNameQuery }));
  }
  if (filters.availableOnly) {
    labels.push(t("filters.availableOnly"));
  }
  if (filters.currentlyOpenOnly) {
    labels.push(t("filters.currentlyOpen"));
  }
  const minPower = Number(filters.minPower);
  if (Number.isFinite(minPower) && minPower > 0) {
    labels.push(t("filters.minPowerLabel", { value: Math.round(minPower) }));
  }
  const minAmenityCount = Number(filters.minAmenityCount);
  if (Number.isFinite(minAmenityCount) && minAmenityCount > 0) {
    labels.push(t("filters.minAmenitiesLabel", { value: Math.round(minAmenityCount) }));
  }
  Array.from(filters.amenities || [])
    .map((key) => getAmenityLabel(key))
    .sort((a, b) => a.localeCompare(b, getLocale()))
    .forEach((label) => labels.push(label));
  return labels;
}

function hasClearableFilters() {
  const selectedAmenities =
    state.filters.amenities instanceof Set ? state.filters.amenities.size : 0;
  const minPower = Number(state.filters.minPower);
  const minAmenityCount = Number(state.filters.minAmenityCount);
  return Boolean(
    state.filters.operator ||
      String(state.filters.amenityNameQuery || "").trim() ||
      state.filters.availableOnly ||
      state.filters.currentlyOpenOnly ||
      selectedAmenities > 0 ||
      (Number.isFinite(minAmenityCount) && minAmenityCount !== DEFAULT_FILTER_SETTINGS.minAmenityCount) ||
      (Number.isFinite(minPower) && minPower !== DEFAULT_MIN_POWER_KW),
  );
}

function clearFilters() {
  state.filters.operator = "";
  state.filters.minPower = DEFAULT_MIN_POWER_KW;
  state.filters.minAmenityCount = DEFAULT_FILTER_SETTINGS.minAmenityCount;
  state.filters.amenities.clear();
  state.filters.amenityNameQuery = "";
  state.filters.availableOnly = false;
  state.filters.currentlyOpenOnly = false;
  els.filter.operator.value = "";
  els.filter.amenityName.value = "";
  els.filter.availableOnly.checked = false;
  els.filter.currentlyOpen.checked = false;
  els.filter.power.value = String(DEFAULT_MIN_POWER_KW);
  els.filter.powerVal.textContent = String(DEFAULT_MIN_POWER_KW);
  els.filter.amenityCount.value = String(DEFAULT_FILTER_SETTINGS.minAmenityCount);
  els.filter.amenityCountVal.textContent = String(DEFAULT_FILTER_SETTINGS.minAmenityCount);
  updatePowerFilterLabel();
  updateAmenityCountFilterLabel();
  renderAmenityFilters();
  updateFilters({ reloadCatalog: true });
}

function getRatingForProps(props) {
  return getUserRating(state.ratings, getStationIdFromProps(props));
}

function getNoteForProps(props) {
  return getUserNote(state.notes, getStationIdFromProps(props));
}

function getRatingSummaryForProps(props) {
  const stationId = getStationIdFromProps(props);
  if (!stationId) {
    return null;
  }
  return state.ratingSummariesByStationId.get(stationId) || null;
}

function getRatingDisplayForProps(props) {
  const summary = getRatingSummaryForProps(props);
  if (summary) {
    return {
      value: summary.average_rating,
      title: t("rating.average", { count: formatRatingCountLabel(summary.rating_count) }),
      count: summary.rating_count,
      localOnly: false,
    };
  }

  const userRating = getRatingForProps(props);
  if (userRating > 0) {
    return {
      value: userRating,
      title: t("rating.localTitle"),
      count: 0,
      localOnly: true,
    };
  }
  return null;
}

function formatRatingCountLabel(count) {
  const numeric = Math.round(Number(count || 0));
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }
  return t(numeric === 1 ? "rating.one" : "rating.many", {
    count: formatInteger(numeric),
  });
}

function formatRatingDisplayValue(value) {
  return formatRatingValue(value, getLocale());
}

function renderRatingBadge(displayRating) {
  if (!displayRating) {
    return "";
  }
  const value = formatRatingDisplayValue(displayRating.value);
  if (!value) {
    return "";
  }
  return `<span class="rating-badge" title="${escapeHtml(displayRating.title)}"><span aria-hidden="true">★</span>${escapeHtml(value)}</span>`;
}

function updateRatingDependentViews() {
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
  if (currentDetailFeature && !els.modals.detail.classList.contains("hidden")) {
    updateDetailRating(currentDetailFeature.properties);
  }
}

function applyFilters() {
  const now = new Date();
  state.filtered = getFilterSourceFeatures().filter((feature) =>
    matchesFeatureFilters(feature, state.filters, { getDisplayedMaxPowerKw, now }),
  );

  if (getDistanceReferencePosition()) {
    state.filtered.sort((a, b) => getDistance(a) - getDistance(b));
  }

  // Update Views
  renderMapMarkers();

  // If list is active, re-render it
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
  if (els.views.route.classList.contains("active")) {
    renderRouteResults();
  } else {
    renderRouteLayer();
  }
  renderRouteMapLock();
}

/* --- LIST RENDERING --- */
function renderList() {
  const container = els.lists.chargers;
  container.innerHTML = "";

  if (state.catalog.loading && state.features.length === 0 && state.staticFeatures.length === 0) {
    container.innerHTML = `<div class="loading-state" data-nosnippet>${escapeHtml(t("list.loadingRadius"))}</div>`;
    return;
  }
  if (state.catalog.error && state.staticFeatures.length === 0) {
    renderCatalogError(container);
    return;
  }
  if (state.catalog.error) {
    const panel = createCatalogErrorPanel();
    panel.classList.add("location-gate-inline");
    container.appendChild(panel);
  } else if (state.catalog.loading) {
    const loading = document.createElement("div");
    loading.className = "loading-state";
    loading.setAttribute("data-nosnippet", "");
    loading.textContent = t("list.loadingLiveFallback");
    container.appendChild(loading);
  }

  const locationViewModel = getLocationListViewModel();
  if (locationViewModel.blocksStationList && !hasCatalogListContext()) {
    const panel = createLocationPanel(locationViewModel);
    if (state.staticFeatures.length === 0) {
      container.appendChild(panel);
      return;
    }
    panel.classList.add("location-gate-inline");
    container.appendChild(panel);
  }

  // Keep the web list aligned with the native apps.
  const displayItems = getListDisplayItems();

  if (displayItems.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = t("list.empty");
    container.appendChild(empty);
    return;
  }

  displayItems.forEach((feature) => {
    const card = createStationCard(feature);
    container.appendChild(card);
  });
  requestLiveSummariesForFeatures(displayItems);
  requestRatingSummariesForFeatures(displayItems);

  const displayLimit = getListDisplayLimit();
  if (state.filtered.length > displayLimit) {
    const more = document.createElement("div");
    more.style.textAlign = "center";
    more.style.padding = "1rem";
    more.style.color = "#888";
    more.textContent = t("list.more", { count: state.filtered.length - displayLimit });
    container.appendChild(more);
  }
}

function syncFavoriteStationIdsFromMetadata() {
  state.favorites = getFavoriteStationIds(state.favoriteMetadata);
}

function getFavoriteCategoryKey(category) {
  return normalizeFavoriteCategoryLabel(category).toLocaleLowerCase();
}

function getFavoriteItemForStationId(stationId) {
  return state.favoriteMetadata.items.get(normalizeStationId(stationId));
}

function getFavoriteCategoriesForStationId(stationId) {
  return getFavoriteItemForStationId(stationId)?.categories || [];
}

function getSortedFavoriteCategories() {
  return getFavoriteCategories(state.favoriteMetadata)
    .sort((left, right) => left.localeCompare(right, getLocale()));
}

function hasUncategorizedFavorites() {
  return Array.from(state.favorites).some(
    (stationId) => getFavoriteCategoriesForStationId(stationId).length === 0,
  );
}

function favoriteMatchesCategoryFilter(stationId, filter = state.favoriteCategoryFilter) {
  if (filter === FAVORITE_FILTER_ALL) {
    return true;
  }
  const categories = getFavoriteCategoriesForStationId(stationId);
  if (filter === FAVORITE_CATEGORY_UNCATEGORIZED) {
    return categories.length === 0;
  }
  return categories.some((category) => getFavoriteCategoryKey(category) === filter);
}

function favoriteCategoryFilterExists(filter) {
  if (filter === FAVORITE_FILTER_ALL) {
    return true;
  }
  if (filter === FAVORITE_CATEGORY_UNCATEGORIZED) {
    return hasUncategorizedFavorites();
  }
  return getSortedFavoriteCategories().some(
    (category) => getFavoriteCategoryKey(category) === filter,
  );
}

function normalizeFavoriteCategoryFilter() {
  if (!favoriteCategoryFilterExists(state.favoriteCategoryFilter)) {
    state.favoriteCategoryFilter = FAVORITE_FILTER_ALL;
  }
}

function getFavoriteCategoryLabelForFilter(filter) {
  if (filter === FAVORITE_FILTER_ALL) {
    return t("favorites.all");
  }
  if (filter === FAVORITE_CATEGORY_UNCATEGORIZED) {
    return t("favorites.uncategorized");
  }
  return getSortedFavoriteCategories().find(
    (category) => getFavoriteCategoryKey(category) === filter,
  ) || filter;
}

function countFavoritesForFilter(filter) {
  return Array.from(state.favorites)
    .filter((stationId) => favoriteMatchesCategoryFilter(stationId, filter))
    .length;
}

function renderFavoriteCategoryFilters() {
  const container = els.favorites.categoryFilters;
  if (!container) {
    return;
  }
  container.replaceChildren();
  container.hidden = state.favorites.size === 0;
  if (state.favorites.size === 0) {
    return;
  }

  normalizeFavoriteCategoryFilter();
  const filterItems = [
    {
      key: FAVORITE_FILTER_ALL,
      label: t("favorites.all"),
      count: state.favorites.size,
    },
    ...getSortedFavoriteCategories().map((category) => ({
      key: getFavoriteCategoryKey(category),
      label: category,
      count: countFavoritesForFilter(getFavoriteCategoryKey(category)),
    })),
  ];
  if (hasUncategorizedFavorites()) {
    filterItems.push({
      key: FAVORITE_CATEGORY_UNCATEGORIZED,
      label: t("favorites.uncategorized"),
      count: countFavoritesForFilter(FAVORITE_CATEGORY_UNCATEGORIZED),
    });
  }

  filterItems.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "favorite-filter-chip";
    button.dataset.favoriteCategoryFilter = item.key;
    button.setAttribute("role", "listitem");
    button.classList.toggle("active", item.key === state.favoriteCategoryFilter);
    button.setAttribute("aria-pressed", item.key === state.favoriteCategoryFilter ? "true" : "false");
    button.setAttribute(
      "aria-label",
      t("favorites.categoryFilterAria", { category: item.label, count: item.count }),
    );
    button.innerHTML = `
      <span class="favorite-filter-label">${escapeHtml(item.label)}</span>
      <span class="favorite-filter-count">${escapeHtml(formatLocalizedInteger(item.count) || item.count)}</span>
    `;
    button.addEventListener("click", () => {
      state.favoriteCategoryFilter = item.key;
      renderFavorites();
    });
    container.appendChild(button);
  });
}

function favoriteIdsForCurrentFilter() {
  normalizeFavoriteCategoryFilter();
  return Array.from(state.favorites)
    .filter((stationId) => favoriteMatchesCategoryFilter(stationId));
}

function buildFavoriteFeatureGroups(favoriteFeatures) {
  const filter = state.favoriteCategoryFilter;
  if (filter !== FAVORITE_FILTER_ALL) {
    return [{
      key: filter,
      label: getFavoriteCategoryLabelForFilter(filter),
      features: favoriteFeatures,
    }];
  }

  const groups = getSortedFavoriteCategories()
    .map((category) => {
      const categoryKey = getFavoriteCategoryKey(category);
      return {
        key: categoryKey,
        label: category,
        features: favoriteFeatures.filter((feature) => {
          const stationId = getStationIdFromProps(feature.properties);
          return favoriteMatchesCategoryFilter(stationId, categoryKey);
        }),
      };
    })
    .filter((group) => group.features.length > 0);

  const uncategorized = favoriteFeatures.filter((feature) => {
    const stationId = getStationIdFromProps(feature.properties);
    return favoriteMatchesCategoryFilter(stationId, FAVORITE_CATEGORY_UNCATEGORIZED);
  });
  if (uncategorized.length > 0) {
    groups.push({
      key: FAVORITE_CATEGORY_UNCATEGORIZED,
      label: t("favorites.uncategorized"),
      features: uncategorized,
    });
  }
  return groups;
}

function uniqueFeaturesByStationId(features) {
  const byStationId = new Map();
  features.forEach((feature) => {
    const stationId = getStationIdFromProps(feature.properties);
    if (stationId && !byStationId.has(stationId)) {
      byStationId.set(stationId, feature);
    }
  });
  return Array.from(byStationId.values());
}

function renderFavoriteGroup(container, group) {
  const section = document.createElement("section");
  section.className = "favorite-group";
  section.dataset.favoriteGroup = group.key;

  const heading = document.createElement("div");
  heading.className = "favorite-group-heading";
  const countText = t(
    group.features.length === 1 ? "favorites.groupCountOne" : "favorites.groupCountMany",
    { count: formatLocalizedInteger(group.features.length) || group.features.length },
  );
  heading.innerHTML = `
    <h3>${escapeHtml(group.label)}</h3>
    <span>${escapeHtml(countText)}</span>
  `;
  section.appendChild(heading);

  const list = document.createElement("div");
  list.className = "favorite-group-list";
  group.features.forEach((feature) => {
    list.appendChild(createStationCard(feature, { showNote: true }));
  });
  section.appendChild(list);
  container.appendChild(section);
}

function renderFavorites() {
  const container = els.lists.favorites;
  container.innerHTML = "";
  renderFavoriteCategoryFilters();

  if (state.favorites.size === 0) {
    container.innerHTML = `<div class="empty-state" style="text-align:center; padding:2rem; color:#888;">
      ${escapeHtml(t("favorites.empty"))}<br>
      ${escapeHtml(t("favorites.emptyHelp"))}
    </div>`;
    return;
  }

  if (state.catalog.loading && state.features.length === 0 && state.staticFeatures.length === 0) {
    container.innerHTML = `<div class="loading-state" data-nosnippet>${escapeHtml(t("favorites.loading"))}</div>`;
    return;
  }
  if (state.catalog.error && state.staticFeatures.length === 0) {
    renderCatalogError(container);
    return;
  }
  if (state.catalog.error) {
    const panel = createCatalogErrorPanel();
    panel.classList.add("location-gate-inline");
    container.appendChild(panel);
  } else if (state.catalog.loading) {
    const loading = document.createElement("div");
    loading.className = "loading-state";
    loading.setAttribute("data-nosnippet", "");
    loading.textContent = t("favorites.loadingFallback");
    container.appendChild(loading);
  }

  const locationViewModel = getLocationListViewModel();
  if (locationViewModel.blocksStationList && !hasCatalogListContext() && state.staticFeatures.length === 0) {
    renderLocationGate(container, locationViewModel);
    return;
  }

  const matchingFavoriteIds = favoriteIdsForCurrentFilter();
  const hasMissingFavorite = matchingFavoriteIds.some((stationId) =>
    !findFeatureByStationId(stationId),
  );

  const favFeatures = matchingFavoriteIds
    .map((stationId) => findFeatureByStationId(stationId))
    .filter(Boolean);

  favFeatures.sort(compareFavoriteFeatures);

  if (favFeatures.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.style.textAlign = "center";
    empty.style.padding = "2rem";
    empty.style.color = "#888";
    empty.textContent = t("favorites.outsideArea");
    container.appendChild(empty);
    return;
  }

  const groups = buildFavoriteFeatureGroups(favFeatures);
  groups.forEach((group) => renderFavoriteGroup(container, group));

  const uniqueFeatures = uniqueFeaturesByStationId(favFeatures);
  requestLiveSummariesForFeatures(uniqueFeatures);
  requestRatingSummariesForFeatures(uniqueFeatures);

  if (hasMissingFavorite) {
    const note = document.createElement("div");
    note.className = "empty-state";
    note.style.textAlign = "center";
    note.style.padding = "1rem";
    note.style.color = "#888";
    note.textContent = t("favorites.someOutsideArea");
    container.appendChild(note);
  }
}

function formatRouteCardLine(props) {
  const accessDistance = Number(props?.route_drive_distance_to_route_m);
  const routePosition = Number(props?.route_position_m);
  const parts = [];
  if (Number.isFinite(accessDistance)) {
    parts.push(t("route.cardAccess", { distance: formatRouteDistanceMeters(accessDistance) }));
  }
  if (Number.isFinite(routePosition)) {
    parts.push(t("route.cardPosition", { distance: formatRouteDistanceMeters(routePosition) }));
  }
  return parts.join(" · ");
}

function createStationCard(feature, options = {}) {
  const p = feature.properties;
  const stationId = getStationIdFromProps(p);
  const div = document.createElement("div");
  div.className = "station-card";
  const stateClass = getStationCardStateClass(p);
  if (stateClass) {
    div.classList.add(stateClass);
  }
  div.tabIndex = 0;
  div.setAttribute("role", "button");
  div.dataset.stationId = stationId;
  div.setAttribute(
    "aria-label",
    `${t("aria.detailsOpen")}: ${p.operator || t("station.chargingStation")} ${p.city || ""}`.trim(),
  );
  if (stationId && state.keyboard.selectedStationId === stationId) {
    div.classList.add("keyboard-selected");
    div.setAttribute("aria-selected", "true");
  } else {
    div.setAttribute("aria-selected", "false");
  }

  const distance = getDistanceFormatted(feature);
  const occupancySummary = formatOccupancySummary(p);
  const priceDisplay = getDisplayPrice(p);
  const availabilityStatus = getAvailabilityStatus(p);

  // Top Amenities (max 3 badges)
  const amenityBadges = Object.keys(AMENITY_MAPPING)
    .filter((k) => p[k] > 0)
    .sort((a, b) => p[b] - p[a]) // Most frequent first
    .slice(0, 3)
    .map((k) => {
      const label = getAmenityLabel(k);
      const iconPath = getAmenityIconPath(k);
      const icon = iconPath
        ? `<img src="${iconPath}" alt="" loading="lazy">`
        : `<span class="badge-amenity-fallback" aria-hidden="true"></span>`;
      return `<span class="badge badge-amenity" title="${escapeHtml(label)}">${icon}<span>${escapeHtml(label)}</span></span>`;
    })
    .join("");
  const liveBadge = occupancySummary
    ? `<span class="badge badge-live ${escapeHtml(getAvailabilityToneClass(availabilityStatus))}">${escapeHtml(occupancySummary)}</span>`
    : "";
  const priceBadge = priceDisplay
    ? `<span class="badge badge-price">${escapeHtml(priceDisplay)}</span>`
    : "";
  const dynamicBadges = `${liveBadge}${priceBadge}`;
  const dynamicLine = dynamicBadges
    ? `<div class="card-badge-line card-badge-line-dynamic">${dynamicBadges}</div>`
    : "";
  const amenityLine = amenityBadges
    ? `<div class="card-badge-line card-badge-line-amenities">${amenityBadges}</div>`
    : "";
  const ratingBadge = renderRatingBadge(getRatingDisplayForProps(p));
  const metrics = `${ratingBadge}${distance ? `<span class="card-distance">${distance}</span>` : ""}`;
  const metricsMarkup = metrics
    ? `<div class="card-metrics">${metrics}</div>`
    : "";
  const note = options.showNote ? getNoteForProps(p) : "";
  const noteMarkup = note
    ? `<div class="card-note"><span class="card-note-label">${escapeHtml(t("station.note"))}</span><p>${escapeHtml(note)}</p></div>`
    : "";
  const routeLine = options.route ? formatRouteCardLine(p) : "";
  const routeMarkup = routeLine
    ? `<div class="card-route-meta">${escapeHtml(routeLine)}</div>`
    : "";

  const markerColor = getMarkerColor(p);
  const isFavoriteStation = Boolean(stationId && state.favorites.has(stationId));
  const cardMarker = isFavoriteStation
    ? `<span class="favorite-station-star card-favorite-star" aria-hidden="true">★</span>`
    : `<span class="amenity-dot" style="background-color: ${markerColor}"></span>`;
  
  div.innerHTML = `
    <div class="card-header">
      <div class="card-title-row">
        ${cardMarker}
        <h3 class="card-title">${escapeHtml(p.operator || t("station.unknownOperator"))}</h3>
      </div>
      ${metricsMarkup}
    </div>
    <div class="card-meta">
      ${escapeHtml(p.city || "")}<br>
      ${Math.round(getDisplayedMaxPowerKw(p))} kW max • ${formatChargingPointCount(p)} • ${formatAmenityCount(p.amenities_total)}
    </div>
    ${routeMarkup}
    <div class="card-badges">
      ${dynamicLine}${amenityLine}
    </div>
    ${noteMarkup}
  `;

  div.addEventListener("click", () => {
    state.keyboard.selectedStationId = stationId;
    openDetail(feature);
  });
  div.addEventListener("focus", () => {
    if (stationId) {
      state.keyboard.selectedStationId = stationId;
      updateListKeyboardSelection();
    }
  });
  return div;
}

function getListDisplayItems() {
  return state.filtered.slice(0, getListDisplayLimit());
}

function getListDisplayLimit() {
  return hasCatalogSearchContext() ? CATALOG_LIST_MAX_STATIONS : STATIC_FALLBACK_LIST_LIMIT;
}

function isEditableKeyTarget(target) {
  if (!target || !(target instanceof HTMLElement)) {
    return false;
  }
  const tagName = target.tagName.toLowerCase();
  return target.isContentEditable || ["input", "select", "textarea"].includes(tagName);
}

function isModalOpen(name) {
  return Boolean(els.modals[name] && !els.modals[name].classList.contains("hidden"));
}

function isAnyModalOpen() {
  return Object.values(els.modals).some((modal) => modal && !modal.classList.contains("hidden"));
}

function handleModalKeydown(event) {
  const modalName = getTopOpenModalName();
  if (!modalName) {
    return false;
  }
  if (event.key === "Escape") {
    event.preventDefault();
    closeModal(modalName);
    return true;
  }
  if (event.key === "Tab") {
    trapModalTab(event, modalName);
    return true;
  }
  return false;
}

function isEasterEggKey(event) {
  return String(event?.key || "").toLowerCase() === "e" &&
    !event.altKey &&
    !event.ctrlKey &&
    !event.metaKey;
}

function handleEasterEggKeydown(event) {
  if (!isEasterEggKey(event)) {
    return false;
  }
  event.preventDefault();
  startEasterEgg();
  return true;
}

function handleGlobalKeyup(event) {
  if (isEasterEggKey(event)) {
    stopEasterEgg();
  }
}

function ensureEasterEggOverlay() {
  if (state.easterEgg.overlay) {
    return;
  }

  const overlay = document.createElement("div");
  overlay.className = "easter-egg-overlay";
  overlay.setAttribute("aria-hidden", "true");

  const canvas = document.createElement("canvas");
  canvas.className = "easter-egg-canvas";

  const core = document.createElement("div");
  core.className = "easter-egg-core";
  core.textContent = "E";

  overlay.append(canvas, core);
  document.body.appendChild(overlay);

  state.easterEgg.overlay = overlay;
  state.easterEgg.canvas = canvas;
  state.easterEgg.ctx = canvas.getContext("2d");
  state.easterEgg.core = core;
}

function startEasterEgg() {
  if (state.easterEgg.active) {
    return;
  }
  ensureEasterEggOverlay();
  state.easterEgg.machine = createEasterEggMachine();
  state.easterEgg.active = true;
  state.easterEgg.overlay?.classList.add("is-active");
  resizeEasterEggCanvas();
  drawEasterEggFrame();
  startEasterEggAudio();
}

function stopEasterEgg() {
  if (!state.easterEgg.active) {
    return;
  }
  state.easterEgg.active = false;
  state.easterEgg.overlay?.classList.remove("is-active");
  if (state.easterEgg.animationFrame) {
    window.cancelAnimationFrame(state.easterEgg.animationFrame);
    state.easterEgg.animationFrame = 0;
  }
  stopEasterEggAudio();
}

function resizeEasterEggCanvas() {
  const canvas = state.easterEgg.canvas;
  const ctx = state.easterEgg.ctx;
  if (!canvas || !ctx) {
    return;
  }
  const width = window.innerWidth;
  const height = window.innerHeight;
  const dpr = Math.min(window.devicePixelRatio || 1, 2);
  if (state.easterEgg.width === width && state.easterEgg.height === height) {
    return;
  }
  state.easterEgg.width = width;
  state.easterEgg.height = height;
  canvas.width = Math.max(1, Math.floor(width * dpr));
  canvas.height = Math.max(1, Math.floor(height * dpr));
  canvas.style.width = `${width}px`;
  canvas.style.height = `${height}px`;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
}

function prefersReducedMotion() {
  return typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

function createEasterEggMachine() {
  const memory = new Uint8Array(EASTER_EGG_MEMORY_SIZE);
  for (let offset = 0; offset < EASTER_EGG_MEMORY_SIZE; offset += 1) {
    if (offset < EASTER_EGG_VISIBLE_BYTES) {
      memory[offset] = offset % 2 === 0 ? 0x20 : 0x07;
    } else {
      memory[offset] = ((offset * 73) ^ (offset >> 3) ^ (offset >> 9) ^ 0xb8) & 0xff;
    }
  }
  return {
    memory,
    si: 0,
    al: 0,
  };
}

function stepEasterEggMachine(machine, iterations = 1) {
  if (!machine?.memory) {
    return;
  }
  for (let index = 0; index < iterations; index += 1) {
    // Port of wake_up 16b's loop: lodsb; sub si,57; xor [si],al; out 61h,al.
    machine.al = machine.memory[machine.si];
    machine.si = (machine.si + 1) & 0xffff;
    machine.si = (machine.si - EASTER_EGG_SUBTRACT_STEP) & 0xffff;
    machine.memory[machine.si] ^= machine.al;
  }
}

function drawEasterEggFrame() {
  if (!state.easterEgg.active || !state.easterEgg.ctx) {
    return;
  }

  resizeEasterEggCanvas();
  if (!state.easterEgg.machine) {
    state.easterEgg.machine = createEasterEggMachine();
  }
  stepEasterEggMachine(state.easterEgg.machine, EASTER_EGG_ITERATIONS_PER_FRAME);

  const ctx = state.easterEgg.ctx;
  const width = state.easterEgg.width;
  const height = state.easterEgg.height;
  const reducedMotion = prefersReducedMotion();
  state.easterEgg.frame += reducedMotion ? 0.35 : 1;

  ctx.fillStyle = reducedMotion ? "rgba(0, 6, 4, 0.48)" : "rgba(0, 6, 4, 0.30)";
  ctx.fillRect(0, 0, width, height);
  const cellWidth = width / EASTER_EGG_TEXT_COLUMNS;
  const cellHeight = height / EASTER_EGG_TEXT_ROWS;
  const fontSize = Math.max(11, Math.min(cellHeight * 0.78, cellWidth * 1.35));
  ctx.font = `${fontSize}px "SFMono-Regular", Consolas, "Liberation Mono", monospace`;
  ctx.textBaseline = "top";

  const memory = state.easterEgg.machine.memory;
  for (let row = 0; row < EASTER_EGG_TEXT_ROWS; row += 1) {
    for (let column = 0; column < EASTER_EGG_TEXT_COLUMNS; column += 1) {
      const offset = (row * EASTER_EGG_TEXT_COLUMNS + column) * 2;
      const charByte = memory[offset];
      const attrByte = memory[offset + 1];
      const toggled = charByte !== 0x20 || attrByte !== 0x07;
      const sierpinski = (charByte & 0x02) !== 0;
      const char = toggled
        ? EASTER_EGG_CHARS[(charByte ^ attrByte ^ column ^ row) % EASTER_EGG_CHARS.length]
        : ".";
      const brightness = Math.min(1, 0.18 + ((attrByte & 0x0f) / 15) * 0.38 + (sierpinski ? 0.42 : 0));
      const x = column * cellWidth + cellWidth * 0.18;
      const y = row * cellHeight + cellHeight * 0.12;

      ctx.globalAlpha = toggled ? brightness : 0.08;
      ctx.fillStyle = sierpinski ? "#d8ffe5" : toggled ? "#41ff87" : "#0aa64a";
      ctx.fillText(char, x, y);

      if (!reducedMotion && toggled) {
        ctx.globalAlpha = 0.08 + brightness * 0.12;
        ctx.fillRect(column * cellWidth, row * cellHeight + cellHeight * 0.86, cellWidth * 0.7, 1);
      }
    }
  }
  ctx.globalAlpha = 1;

  state.easterEgg.animationFrame = window.requestAnimationFrame(drawEasterEggFrame);
}

function startEasterEggAudio() {
  const AudioContextConstructor = window.AudioContext || window.webkitAudioContext;
  if (!AudioContextConstructor || state.easterEgg.audioSource) {
    return;
  }
  const audioContext = state.easterEgg.audioContext || new AudioContextConstructor();
  state.easterEgg.audioContext = audioContext;

  const resumePromise = typeof audioContext.resume === "function"
    ? audioContext.resume()
    : null;
  if (resumePromise && typeof resumePromise.catch === "function") {
    resumePromise.catch(() => {});
  }

  if (!state.easterEgg.audioBuffer || state.easterEgg.audioBuffer.sampleRate !== audioContext.sampleRate) {
    state.easterEgg.audioBuffer = createEasterEggAudioBuffer(audioContext);
  }

  const source = audioContext.createBufferSource();
  const gain = audioContext.createGain();
  const filter = audioContext.createBiquadFilter();
  const now = audioContext.currentTime;

  source.buffer = state.easterEgg.audioBuffer;
  source.loop = true;
  filter.type = "lowpass";
  filter.frequency.setValueAtTime(2400, now);
  filter.Q.setValueAtTime(0.45, now);
  gain.gain.setValueAtTime(0.0001, now);
  gain.gain.exponentialRampToValueAtTime(0.045, now + 0.08);

  source.connect(filter);
  filter.connect(gain);
  gain.connect(audioContext.destination);
  source.start(now);
  source.onended = () => {
    if (state.easterEgg.audioSource === source) {
      state.easterEgg.audioSource = null;
      state.easterEgg.audioGain = null;
    }
  };

  state.easterEgg.audioSource = source;
  state.easterEgg.audioGain = gain;
}

function stopEasterEggAudio() {
  const audioContext = state.easterEgg.audioContext;
  const source = state.easterEgg.audioSource;
  const gain = state.easterEgg.audioGain;
  if (!audioContext || !source || !gain) {
    return;
  }
  const now = audioContext.currentTime;
  gain.gain.cancelScheduledValues(now);
  gain.gain.setTargetAtTime(0.0001, now, 0.035);
  try {
    source.stop(now + 0.18);
  } catch (error) {
    // The source may already be stopping after a rapid key repeat/release.
  }
  state.easterEgg.audioSource = null;
  state.easterEgg.audioGain = null;
}

function createEasterEggAudioBuffer(audioContext) {
  const length = Math.floor(audioContext.sampleRate * EASTER_EGG_AUDIO_SECONDS);
  const buffer = audioContext.createBuffer(1, length, audioContext.sampleRate);
  const data = buffer.getChannelData(0);
  const machine = createEasterEggMachine();
  let machineStep = -1;

  for (let i = 0; i < length; i += 1) {
    const targetStep = Math.floor((i * EASTER_EGG_AUDIO_RATE) / audioContext.sampleRate);
    while (machineStep < targetStep) {
      stepEasterEggMachine(machine, 1);
      machineStep += 1;
    }
    const speakerBit = (machine.al & 0x02) ? 1 : -1;
    const gritBit = (machine.al & 0x01) ? 1 : -1;
    const highBit = (machine.al & 0x20) ? 1 : -1;
    data[i] = speakerBit * 0.28 + gritBit * 0.06 + highBit * 0.035;
  }

  return buffer;
}

function handleGlobalKeydown(event) {
  if (event.defaultPrevented) {
    return;
  }
  if (handleModalKeydown(event)) {
    return;
  }
  if (isEditableKeyTarget(event.target)) {
    return;
  }
  if (handleEasterEggKeydown(event)) {
    return;
  }
  if (handleViewNavigationKeydown(event)) {
    return;
  }
  if (handleMapKeyboardEvent(event)) {
    return;
  }
  if (!els.views.list.classList.contains("active") || isAnyModalOpen()) {
    return;
  }
  if (event.key === "ArrowDown" || event.key === "ArrowUp") {
    event.preventDefault();
    moveListKeyboardSelection(event.key === "ArrowDown" ? 1 : -1);
    return;
  }
  if (event.key === "Enter") {
    const feature = selectedListFeature();
    if (feature) {
      event.preventDefault();
      openDetail(feature);
    }
  }
}

function handleMapKeyboardEvent(event) {
  if (!els.views.map.classList.contains("active") || isAnyModalOpen() || !state.views.map) {
    return false;
  }
  const action = getMapKeyboardAction(event.key);
  if (!action) {
    return false;
  }
  event.preventDefault();
  return performMapKeyboardAction(action, state.views.map, {
    focusSearchInput: focusLocationSearchInput,
  });
}

function selectedListFeature() {
  const selectedStationId = state.keyboard.selectedStationId;
  if (!selectedStationId) {
    return null;
  }
  return getListDisplayItems().find(
    (feature) => getStationIdFromProps(feature.properties) === selectedStationId,
  ) || null;
}

function moveListKeyboardSelection(delta) {
  const displayItems = getListDisplayItems();
  if (displayItems.length === 0) {
    state.keyboard.selectedStationId = "";
    return;
  }
  const selectedIndex = displayItems.findIndex(
    (feature) => getStationIdFromProps(feature.properties) === state.keyboard.selectedStationId,
  );
  const fallbackIndex = delta > 0 ? -1 : displayItems.length;
  const nextIndex = Math.max(
    0,
    Math.min(displayItems.length - 1, (selectedIndex >= 0 ? selectedIndex : fallbackIndex) + delta),
  );
  state.keyboard.selectedStationId = getStationIdFromProps(displayItems[nextIndex].properties);
  updateListKeyboardSelection();
  focusSelectedListCard();
}

function updateListKeyboardSelection() {
  els.lists.chargers.querySelectorAll(".station-card").forEach((card) => {
    const isSelected = card.dataset.stationId === state.keyboard.selectedStationId;
    card.classList.toggle("keyboard-selected", isSelected);
    card.setAttribute("aria-selected", isSelected ? "true" : "false");
  });
}

function focusSelectedListCard() {
  if (!state.keyboard.selectedStationId || !els.views.list.classList.contains("active")) {
    return;
  }
  const card = Array.from(els.lists.chargers.querySelectorAll(".station-card"))
    .find((candidate) => candidate.dataset.stationId === state.keyboard.selectedStationId);
  if (!card) {
    return;
  }
  card.focus({ preventScroll: true });
  card.scrollIntoView({ block: "nearest" });
}

function compareFavoriteFeatures(a, b) {
  if (state.favoriteSort === FAVORITE_SORT_RATING) {
    return compareFavoriteFeaturesByRating(a, b);
  }

  return compareFavoriteFeaturesByDistance(a, b);
}

function compareFavoriteFeaturesByRating(a, b) {
  const ratingDiff = getFavoriteSortRating(b.properties) - getFavoriteSortRating(a.properties);
  if (ratingDiff !== 0) {
    return ratingDiff;
  }
  return compareFavoriteFeaturesByName(a, b);
}

function compareFavoriteFeaturesByDistance(a, b) {
  const leftDistance = getDistance(a);
  const rightDistance = getDistance(b);
  const leftFinite = Number.isFinite(leftDistance);
  const rightFinite = Number.isFinite(rightDistance);
  if (leftFinite && rightFinite && leftDistance !== rightDistance) {
    return leftDistance - rightDistance;
  }
  if (leftFinite !== rightFinite) {
    return leftFinite ? -1 : 1;
  }
  return compareFavoriteFeaturesByName(a, b);
}

function compareFavoriteFeaturesByName(a, b) {
  const leftName = String(a.properties?.operator || "");
  const rightName = String(b.properties?.operator || "");
  const nameDiff = leftName.localeCompare(rightName, "de");
  if (nameDiff !== 0) {
    return nameDiff;
  }
  const leftId = getStationIdFromProps(a.properties);
  const rightId = getStationIdFromProps(b.properties);
  return leftId.localeCompare(rightId);
}

function getFavoriteSortRating(props) {
  const userRating = getRatingForProps(props);
  if (userRating > 0) {
    return userRating;
  }
  const displayRating = getRatingDisplayForProps(props);
  const rating = Number(displayRating?.value || 0);
  return Number.isFinite(rating) ? rating : 0;
}

function getDisplayedMaxPowerKw(props) {
  const maxIndividual = sanitizeDisplayedPowerKw(props.max_individual_power_kw);
  if (maxIndividual > 0) {
    return maxIndividual;
  }
  return sanitizeDisplayedPowerKw(props.max_power_kw);
}

function shouldRequestLiveDataForProps(props) {
  return getDisplayedMaxPowerKw(props) >= DEFAULT_MIN_POWER_KW;
}

function getChargingPointCount(props) {
  const count = Number(props.charging_points_count || 0);
  if (Number.isFinite(count) && count > 0) {
    return Math.round(count);
  }
  return 1;
}

function formatChargingPointCount(props) {
  const count = getChargingPointCount(props);
  return t(count === 1 ? "station.chargingPointOne" : "station.chargingPointMany", {
    count,
  });
}

/* --- DETAIL MODAL --- */
let currentDetailFeature = null;

function renderDetailLiveState(feature, liveDetail = null) {
  const props = feature.properties;
  const liveEvses = Array.isArray(liveDetail?.evses) ? liveDetail.evses : [];
  const evses = liveEvses.length > 0 ? liveEvses : buildAggregateLiveEvses(props);
  const hasLiveData = hasLiveStationSummary(props) || hasAggregateOccupancySummary(props) || evses.length > 0;
    if (!hasLiveData) {
    els.detail.liveSection.hidden = true;
    els.detail.liveTitle.textContent = t("station.live");
    els.detail.liveUpdated.hidden = true;
    els.detail.liveUpdated.textContent = "";
    els.detail.liveList.innerHTML = "";
    return;
  }

  els.detail.liveTitle.textContent = t("station.live");
  els.detail.liveUpdated.textContent = "";
  els.detail.liveUpdated.hidden = true;
  els.detail.liveList.innerHTML = "";

  if (evses.length === 0) {
    const summaryRow = document.createElement("div");
    summaryRow.className = "live-evse-row live-evse-row-summary";
    const priceDisplay = getDisplayPrice(props, liveDetail);
    summaryRow.innerHTML = `
      <div class="live-evse-row-head">
        <strong class="live-evse-title">${escapeHtml(t("station.stationStatus"))}</strong>
        <span class="live-status-pill ${escapeHtml(getAvailabilityToneClass(getAvailabilityStatus(props)))}">${escapeHtml(formatAvailabilityLabel(getAvailabilityStatus(props)))}</span>
      </div>
      <div class="live-evse-row-meta">
        <span>${escapeHtml(formatOccupancySummary(props) || t("station.liveDataAvailable"))}</span>
        ${priceDisplay ? `<span class="live-evse-price">${escapeHtml(priceDisplay)}</span>` : ""}
      </div>
    `;
    els.detail.liveList.appendChild(summaryRow);
    els.detail.liveSection.hidden = false;
    return;
  }

  evses.forEach((evse, index) => {
    const row = document.createElement("div");
    const status = normalizeAvailabilityStatus(evse.availability_status);
    const metaParts = [];
    const evseCode = formatEvseCode(evse.provider_evse_id);
    if (evseCode) {
      metaParts.push(evseCode);
    }
    const statusNote = String(evse.status_note || "").trim();
    if (statusNote) {
      metaParts.push(statusNote);
    }
    const priceDisplay = String(evse.price_display || "").trim();
    const metaMarkup = metaParts.length || priceDisplay
      ? `
      <div class="live-evse-row-meta">
        ${metaParts.length ? `<span>${escapeHtml(metaParts.join(" • "))}</span>` : ""}
        ${priceDisplay ? `<span class="live-evse-price">${escapeHtml(priceDisplay)}</span>` : ""}
      </div>
    `
      : "";
    const dynamicNotes = buildLiveDynamicNotes(evse);
    const notesMarkup = dynamicNotes.length
      ? `
      <div class="live-evse-row-details">
        ${dynamicNotes.map((note) => `
          <div class="live-evse-row-detail">
            <strong>${escapeHtml(note.label)}</strong>
            <span>${escapeHtml(note.value)}</span>
          </div>
        `).join("")}
      </div>
    `
      : "";
    row.className = "live-evse-row";
    row.innerHTML = `
      <div class="live-evse-row-head">
        <strong class="live-evse-title">${escapeHtml(t("station.evse", { index: index + 1 }))}</strong>
        <span class="live-status-pill ${escapeHtml(getAvailabilityToneClass(status))}">${escapeHtml(formatAvailabilityLabel(status))}</span>
      </div>
      ${metaMarkup}
      ${notesMarkup}
    `;
    els.detail.liveList.appendChild(row);
  });

  els.detail.liveSection.hidden = false;
}

function formatHistoryDate(value) {
  return formatDate(value);
}

function formatOccupancyHistoryRange(history) {
  const start = formatHistoryDate(history?.start_date);
  const end = formatHistoryDate(history?.end_date);
  const days = Number(history?.included_days || 0);
  const dayLabel = days > 0 ? t("common.days", { count: days }) : "";
  if (start && end && start !== end) {
    return dayLabel ? `${start} - ${end} · ${dayLabel}` : `${start} - ${end}`;
  }
  return end || start || dayLabel;
}

function normalizeOccupancyHistory(history) {
  const values = history?.hourly_average_occupied;
  if (!values || typeof values !== "object") return null;
  const hourly = Array.from({ length: 24 }, (_, hour) => {
    const key = `${String(hour).padStart(2, "0")}:00`;
    const value = Number(values[key]);
    return {
      hour,
      key,
      value: Number.isFinite(value) && value > 0 ? value : 0,
    };
  });
  return { ...history, hourly };
}

function formatOccupancyHistoryValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "0";
  }
  return new Intl.NumberFormat(getLocale(), {
    minimumFractionDigits: Number.isInteger(numeric) ? 0 : 1,
    maximumFractionDigits: 1,
  }).format(numeric);
}

function safeOccupancyHistoryStationId(stationId) {
  const rawStationId = String(stationId || "").trim();
  const namespacedMatch = rawStationId.match(NAMESPACED_STATION_ID_RE);
  const fileStationId = namespacedMatch
    ? namespacedMatch[1].toLowerCase()
    : (LEGACY_STATION_ID_RE.test(rawStationId) ? rawStationId.toLowerCase() : rawStationId);
  const safeStationId = fileStationId
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/^[._-]+|[._-]+$/g, "");
  return safeStationId || "station";
}

function occupancyHistoryPathForStationId(stationId) {
  const safeStationId = safeOccupancyHistoryStationId(stationId);
  const shardKey = safeStationId.replace(/[^A-Za-z0-9]+/g, "").toLowerCase();
  const shardLength = Math.min(shardKey.length, 6);
  const shards = [];
  for (let index = 0; index < shardLength; index += 2) {
    const shard = shardKey.slice(index, index + 2);
    if (shard) shards.push(shard);
  }
  return [...shards, `${safeStationId}.json`].join("/");
}

function occupancyHistoryUrlForStationId(stationId) {
  const historyPath = occupancyHistoryPathForStationId(stationId);
  return new URL(`./data/station-occupancy/${historyPath}`, import.meta.url);
}

async function loadOccupancyHistoryManifest() {
  if (state.occupancyHistory.availableStationIds) {
    return state.occupancyHistory.availableStationIds;
  }
  if (!state.occupancyHistory.manifestPromise) {
    state.occupancyHistory.manifestPromise = fetchOptionalJson(
      "./data/station-occupancy/index.json",
    ).then((payload) => {
      const stationIds = Array.isArray(payload?.station_ids)
        ? payload.station_ids
        : [];
      const availableStationIds = new Set(
        stationIds
          .map((stationId) => safeOccupancyHistoryStationId(stationId))
          .filter(Boolean),
      );
      state.occupancyHistory.availableStationIds = availableStationIds;
      return availableStationIds;
    });
  }
  return state.occupancyHistory.manifestPromise;
}

function renderOccupancyHistoryChart(history, feature) {
  const normalized = normalizeOccupancyHistory(history);
  if (!normalized) return false;
  const maxObserved = Math.max(...normalized.hourly.map((item) => item.value), 0);
  const scale = Math.max(maxObserved, getChargingPointCount(feature.properties), 1);
  const bars = normalized.hourly.map((item) => {
    const percent = Math.max(0, Math.min(100, (item.value / scale) * 100));
    const visiblePercent = item.value > 0 ? Math.max(percent, 3) : 0;
    const hourLabel = `${String(item.hour).padStart(2, "0")}:00`;
    const shortHourLabel = String(item.hour).padStart(2, "0");
    const valueLabel = formatOccupancyHistoryValue(item.value);
    const occupiedLabel = t("availability.occupied").toLocaleLowerCase(getLocale());
    return `
      <div class="occupancy-history-hour" style="--occupancy-percent: ${visiblePercent.toFixed(1)}%" title="${escapeHtml(hourLabel)}: ${escapeHtml(valueLabel)} ${escapeHtml(occupiedLabel)}">
        <span class="occupancy-history-label">${shortHourLabel}</span>
        <div class="occupancy-history-track" aria-hidden="true">
          <div class="occupancy-history-bar"></div>
        </div>
      </div>
    `;
  }).join("");

  els.detail.occupancyHistoryRange.textContent = formatOccupancyHistoryRange(normalized);
  els.detail.occupancyHistoryChart.innerHTML = `
    <div class="occupancy-history-bars" role="img" aria-label="${escapeHtml(t("station.typicalOccupancy"))}">
      ${bars}
    </div>
  `;
  els.detail.occupancyHistorySection.hidden = false;
  return true;
}

function loadDetailOccupancyHistoryFile(stationId, feature) {
  const cached = state.occupancyHistory.byStationId.get(stationId);
  if (cached) {
    renderOccupancyHistoryChart(cached, feature);
    return;
  }

  if (state.occupancyHistory.pendingStationIds.has(stationId) || state.occupancyHistory.missingStationIds.has(stationId)) {
    return;
  }

  state.occupancyHistory.pendingStationIds.add(stationId);
  loadOccupancyHistoryManifest()
    .then((availableStationIds) => {
      const safeStationId = safeOccupancyHistoryStationId(stationId);
      if (!availableStationIds.has(safeStationId)) {
        state.occupancyHistory.missingStationIds.add(stationId);
        return null;
      }
      return fetch(occupancyHistoryUrlForStationId(stationId));
    })
    .then((response) => {
      if (!response) {
        return null;
      }
      if (response.status === 404) {
        state.occupancyHistory.missingStationIds.add(stationId);
        return null;
      }
      if (!response.ok) {
        throw new Error(`Unexpected occupancy history response ${response.status}`);
      }
      return response.json();
    })
    .then((history) => {
      if (!history) return;
      state.occupancyHistory.byStationId.set(stationId, history);
      const currentStationId = currentDetailFeature
        ? getStationIdFromProps(currentDetailFeature.properties)
        : "";
      if (currentStationId === stationId) {
        renderOccupancyHistoryChart(history, currentDetailFeature);
      }
    })
    .catch((err) => {
      console.warn(`Failed to load occupancy history for station ${stationId}`, err);
    })
    .finally(() => {
      state.occupancyHistory.pendingStationIds.delete(stationId);
    });
}

function renderDetailOccupancyHistory(feature) {
  const stationId = getStationIdFromProps(feature.properties);
  els.detail.occupancyHistorySection.hidden = true;
  els.detail.occupancyHistoryRange.textContent = "";
  els.detail.occupancyHistoryChart.innerHTML = "";

  const cached = state.occupancyHistory.byStationId.get(stationId);
  if (cached) {
    renderOccupancyHistoryChart(cached, feature);
    return;
  }

  loadDetailOccupancyHistoryFile(stationId, feature);
}

function updateDetailRating(props) {
  const stationId = getStationIdFromProps(props);
  const rating = getRatingForProps(props);
  const summary = getRatingSummaryForProps(props);
  const displayRating = getRatingDisplayForProps(props);
  const ratingValue = formatRatingDisplayValue(displayRating?.value);

  if (ratingValue) {
    els.detail.ratingBadge.innerHTML = `<span aria-hidden="true">★</span>${escapeHtml(ratingValue)}`;
    els.detail.ratingBadge.hidden = false;
  } else {
    els.detail.ratingBadge.textContent = "";
    els.detail.ratingBadge.hidden = true;
  }

  const summaryText = summary
    ? t("rating.summary", {
        value: formatRatingDisplayValue(summary.average_rating),
        count: formatRatingCountLabel(summary.rating_count),
      })
    : "";
  const userText = rating > 0 ? t("rating.yourRating", { rating }) : "";
  const isSubmitting = stationId && state.pendingRatingSubmissions.has(stationId);
  const submissionError = stationId ? state.ratingSubmissionErrors.get(stationId) : "";

  if (isSubmitting) {
    els.detail.ratingStatus.textContent = t("rating.save");
  } else if (submissionError) {
    els.detail.ratingStatus.textContent = t("rating.serverError");
  } else if (userText && summaryText) {
    els.detail.ratingStatus.textContent = `${userText} · ${summaryText}`;
  } else if (userText) {
    els.detail.ratingStatus.textContent = SHARED_RATINGS_ENABLED && state.live.baseUrl
      ? userText
      : `${userText} · ${t("rating.localOnly")}`;
  } else if (summaryText) {
    els.detail.ratingStatus.textContent = summaryText;
  } else {
    els.detail.ratingStatus.textContent = t("rating.unrated");
  }

  els.detail.ratingStars.querySelectorAll(".rating-star-btn").forEach((button) => {
    const buttonRating = normalizeRating(button.dataset.rating);
    const isActive = rating > 0 && buttonRating <= rating;
    button.classList.toggle("active", isActive);
    button.setAttribute(
      "aria-label",
      t(buttonRating === 1 ? "rating.starOne" : "rating.starMany", { count: buttonRating }),
    );
    button.setAttribute("aria-checked", buttonRating === rating ? "true" : "false");
    button.disabled = Boolean(isSubmitting);
  });
}

function updateDetailNote(props) {
  const stationId = getStationIdFromProps(props);
  const note = getNoteForProps(props);
  els.detail.noteInput.value = note;
  els.detail.noteInput.disabled = !stationId;
  els.detail.noteStatus.textContent = note
    ? t("detail.noteSaved")
    : t("detail.noteDeviceOnly");
}

function currentDetailStationId() {
  return currentDetailFeature
    ? getStationIdFromProps(currentDetailFeature.properties)
    : "";
}

function renderDetailCategorySuggestions() {
  const suggestionsEl = els.detail.categorySuggestions;
  const input = els.detail.categoryInput;
  if (!suggestionsEl || !input || !currentDetailFeature) {
    return;
  }
  const stationId = currentDetailStationId();
  const categories = getFavoriteCategoriesForStationId(stationId);
  const suggestions = favoriteCategorySuggestions(
    state.favoriteMetadata,
    input.value,
    {
      exclude: categories,
      locale: getLocale(),
    },
  );
  suggestionsEl.replaceChildren();
  input.setAttribute("aria-expanded", suggestions.length > 0 ? "true" : "false");
  suggestionsEl.hidden = suggestions.length === 0;
  suggestions.forEach((category) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "category-suggestion";
    button.dataset.category = category;
    button.textContent = category;
    suggestionsEl.appendChild(button);
  });
}

function updateDetailFavoriteCategories() {
  const editor = els.detail.categoryEditor;
  if (!editor || !currentDetailFeature) {
    return;
  }
  const stationId = currentDetailStationId();
  const isFavorite = Boolean(stationId && state.favorites.has(stationId));
  editor.hidden = !isFavorite;
  if (!isFavorite) {
    els.detail.categoryChips?.replaceChildren();
    if (els.detail.categoryInput) {
      els.detail.categoryInput.value = "";
      els.detail.categoryInput.setAttribute("aria-expanded", "false");
    }
    if (els.detail.categorySuggestions) {
      els.detail.categorySuggestions.replaceChildren();
      els.detail.categorySuggestions.hidden = true;
    }
    return;
  }

  const categories = getFavoriteCategoriesForStationId(stationId);
  els.detail.categoryChips.replaceChildren();
  if (categories.length === 0) {
    const empty = document.createElement("span");
    empty.className = "category-empty";
    empty.textContent = t("favorites.uncategorized");
    els.detail.categoryChips.appendChild(empty);
  } else {
    categories.forEach((category) => {
      const chip = document.createElement("span");
      chip.className = "category-chip";
      chip.innerHTML = `
        <span>${escapeHtml(category)}</span>
        <button type="button" data-remove-category="${escapeHtml(category)}" aria-label="${escapeHtml(t("detail.removeCategory", { category }))}">×</button>
      `;
      els.detail.categoryChips.appendChild(chip);
    });
  }
  if (els.detail.categoryStatus) {
    els.detail.categoryStatus.textContent = t("detail.categoryDeviceOnly");
  }
  renderDetailCategorySuggestions();
}

function refreshFavoriteDependentViews(stationIds = []) {
  updateMapMarkersForStationIds(stationIds);
  if (currentDetailFeature) {
    renderDetailStationMarker(currentDetailFeature);
  }
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
}

function addDetailFavoriteCategory(value = els.detail.categoryInput?.value || "") {
  const stationId = currentDetailStationId();
  const category = normalizeFavoriteCategoryLabel(value);
  if (!stationId || !category) {
    return;
  }
  addFavoriteCategory(state.favoriteMetadata, stationId, category, {
    source: FAVORITE_SOURCE_MANUAL,
  });
  syncFavoriteStationIdsFromMetadata();
  saveFavorites();
  if (els.detail.categoryInput) {
    els.detail.categoryInput.value = "";
  }
  updateFavBtnState();
  updateDetailFavoriteCategories();
  refreshFavoriteDependentViews([stationId]);
}

function handleDetailCategoryKeydown(event) {
  if (event.key === "Enter" || event.key === ",") {
    event.preventDefault();
    addDetailFavoriteCategory();
  } else if (event.key === "Escape" && els.detail.categorySuggestions) {
    els.detail.categorySuggestions.hidden = true;
    event.currentTarget.setAttribute("aria-expanded", "false");
  }
}

function handleDetailCategorySuggestionClick(event) {
  const button = event.target.closest("[data-category]");
  if (!button) {
    return;
  }
  addDetailFavoriteCategory(button.dataset.category || "");
}

function handleDetailCategoryChipClick(event) {
  const button = event.target.closest("[data-remove-category]");
  if (!button) {
    return;
  }
  const stationId = currentDetailStationId();
  if (!stationId) {
    return;
  }
  removeFavoriteCategory(state.favoriteMetadata, stationId, button.dataset.removeCategory || "");
  syncFavoriteStationIdsFromMetadata();
  saveFavorites();
  updateDetailFavoriteCategories();
  refreshFavoriteDependentViews([stationId]);
}

function populateDetailContent(feature, liveDetail = null) {
  const p = feature.properties;
  const powerDisplay = t("station.maxPower", {
    power: Math.round(getDisplayedMaxPowerKw(p)),
    points: formatChargingPointCount(p),
  });

  els.detail.title.textContent = p.operator || t("station.unknownOperator");
  els.detail.address.textContent = `${p.address || ""}, ${p.postcode || ""} ${p.city || ""}`;
  els.detail.power.textContent = powerDisplay;
  els.detail.powerChip.hidden = !powerDisplay;

  const occupancySummary = formatOccupancySummary(p);
  const occupancySource = formatOccupancySource(p, liveDetail);
  const availabilityStatus = getAvailabilityStatus(p);
  if (occupancySummary) {
    els.detail.occupancy.textContent = occupancySummary;
    els.detail.occupancyPill.hidden = false;
    setAvailabilityTone(els.detail.occupancyPill, availabilityStatus);
  } else {
    els.detail.occupancy.textContent = "";
    els.detail.occupancyPill.hidden = true;
  }

  const priceDisplay = getDisplayPrice(p, liveDetail);
  const openingHoursDisplay = formatOpeningHoursForDisplay(p.opening_hours_display, getLocale());
  const showPower = Boolean(powerDisplay);
  const showOccupancy = Boolean(occupancySummary);
  const showPrice = Boolean(priceDisplay);
  const showHours = Boolean(openingHoursDisplay);
  els.detail.highlights.hidden = !showPower && !showOccupancy && !showPrice && !showHours;
  els.detail.priceChip.hidden = !showPrice;
  els.detail.hoursChip.hidden = !showHours;
  els.detail.price.textContent = priceDisplay;
  els.detail.hours.textContent = openingHoursDisplay;
  els.detail.amenityTitle.textContent = formatAmenityCount(p.amenities_total);

  updateDetailRating(p);
  updateDetailNote(p);
  updateDetailFavoriteCategories();
  renderDetailAmenities(p);
  renderDetailStaticInfo(p);
  renderDetailLiveState(feature, liveDetail);
  renderDetailOccupancyHistory(feature);
  if (!els.modals.detail.classList.contains("hidden")) {
    renderDetailStationMarker(feature);
  }

  if (occupancySource) {
    els.detail.occupancySource.textContent = occupancySource;
    els.detail.occupancySource.hidden = false;
  } else {
    els.detail.occupancySource.textContent = "";
    els.detail.occupancySource.hidden = true;
  }
}

function openDetail(feature, options = {}) {
  const syncUrl = options.syncUrl !== false;
  currentDetailFeature = feature;
  const p = feature.properties;
  const stationId = getStationIdFromProps(p);
  if (stationId) {
    state.mapFocus.stationId = stationId;
  }

  populateDetailContent(feature, state.live.detailByStationId.get(getStationIdFromProps(p)) || null);

  // Favorite Button State
  updateFavBtnState();

  // Navigation Links
  const [lon, lat] = feature.geometry.coordinates;
  els.detail.googleBtn.href = `https://www.google.com/maps/dir/?api=1&destination=${lat},${lon}`;
  els.detail.appleBtn.href = `http://maps.apple.com/?daddr=${lat},${lon}`;
  if (els.detail.helpdeskPhoneBtn) {
    const phoneHref = formatTelephoneHref(p.helpdesk_phone);
    els.detail.helpdeskPhoneBtn.hidden = !phoneHref;
    if (phoneHref) {
      els.detail.helpdeskPhoneBtn.href = phoneHref;
    els.detail.helpdeskPhoneBtn.title = t("detail.helpTitle", { phone: p.helpdesk_phone });
    } else {
      els.detail.helpdeskPhoneBtn.removeAttribute("href");
      els.detail.helpdeskPhoneBtn.removeAttribute("title");
    }
  }

  // Mini Map
  if (state.views.layers.detailAmenities) {
    state.views.layers.detailAmenities.clearLayers();
  }
  renderDetailStationMarker(feature);

  const amenityBounds = renderDetailAmenityMarkers(p.amenity_examples || []);

  openModal("detail");

  if (!state.views.detailMap) {
    return;
  }

  const applyDetailViewport = () => {
    if (amenityBounds.length > 0) {
      const bounds = L.latLngBounds([[lat, lon], [lat, lon]]);
      amenityBounds.forEach((pair) => bounds.extend(pair));
      state.views.detailMap.fitBounds(bounds.pad(0.25), { animate: false, maxZoom: 17 });
      return;
    }
    state.views.detailMap.setView([lat, lon], 16, { animate: false });
  };

  const ensureViewportWhenReady = (attempt = 0) => {
    if (!state.views.detailMap || els.modals.detail.classList.contains("hidden")) return;
    const mapEl = els.detail.mapContainer;
    state.views.detailMap.invalidateSize({ pan: false, animate: false });
    const hasSize = !!mapEl && mapEl.clientWidth > 0 && mapEl.clientHeight > 0;
    if (hasSize || attempt >= 12) {
      applyDetailViewport();
      return;
    }
    requestAnimationFrame(() => ensureViewportWhenReady(attempt + 1));
  };

  // Fit only when the modal layout is actually measurable.
  ensureViewportWhenReady();
  setTimeout(() => ensureViewportWhenReady(), 200);
  setTimeout(() => ensureViewportWhenReady(), 500);

  if (syncUrl) {
    updateRequestedStationId(p.station_id || "");
  }

  if (stationId) {
    void loadCatalogStationDetail(stationId);
    void loadLiveStationDetail(stationId);
    requestRatingSummariesForFeatures([feature]);
  }
}

function renderDetailAmenityMarkers(examples) {
  if (!state.views.layers.detailAmenities) {
    return [];
  }

  const bounds = [];
  examples.slice(0, 20).forEach((item) => {
    const lat = Number(item?.lat);
    const lon = Number(item?.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return;
    }

    const amenityKey = `amenity_${item.category || ""}`;
  const amenityLabel = AMENITY_MAPPING[amenityKey] ? getAmenityLabel(amenityKey) : (item.category || t("amenity.generic"));
    const amenityName = item.name ? `${item.name}` : amenityLabel;
    const iconPath = getAmenityIconPath(amenityKey);
    const markerIcon = iconPath
      ? L.divIcon({
          className: "mini-amenity-marker",
          html: `<img src="${iconPath}" alt="${escapeHtml(amenityLabel)}" loading="lazy">`,
          iconSize: [22, 22],
          iconAnchor: [11, 11],
        })
      : L.divIcon({
          className: "mini-amenity-marker fallback",
          html: "<span>•</span>",
          iconSize: [16, 16],
          iconAnchor: [8, 8],
        });

    const marker = L.marker([lat, lon], {
      icon: markerIcon,
      keyboard: false,
    }).addTo(state.views.layers.detailAmenities);
    marker.bindTooltip(escapeHtml(amenityName), { direction: "top", offset: [0, -8] });
    bounds.push([lat, lon]);
  });
  return bounds;
}

function renderDetailAmenities(props) {
  els.detail.amenityList.innerHTML = "";
  const examples = props.amenity_examples || [];

  if (examples.length === 0) {
    els.detail.amenityList.innerHTML = `<div style="color:#888">${escapeHtml(t("amenity.noDetails"))}</div>`;
    return;
  }

  const now = new Date();
  const groupedExamples = new Map(AMENITY_GROUPS.map((group) => [t(group.labelKey), []]));
  examples.slice(0, 15).forEach((item) => {
    const groupLabel = getAmenityGroupLabel(item?.category);
    groupedExamples.get(groupLabel).push(item);
  });
  AMENITY_GROUPS.forEach((group) => {
    const groupLabel = t(group.labelKey);
    const groupItems = groupedExamples.get(groupLabel).sort(compareAmenityExamples);
    if (groupItems.length === 0) return;
    const groupElement = document.createElement("div");
    groupElement.className = "amenity-group";
    const title = document.createElement("h4");
    title.className = "amenity-group-title";
    title.textContent = groupLabel;
    groupElement.appendChild(title);
    const itemsElement = document.createElement("div");
    itemsElement.className = "amenity-group-items";
    groupItems.forEach((item) => {
      const catConfig = AMENITY_MAPPING[`amenity_${item.category}`] || {
        label: item.category || t("amenity.generic"),
      };
      const iconPath = getAmenityIconPath(`amenity_${item.category}`);
      const categoryLabel = AMENITY_MAPPING[`amenity_${item.category}`]
        ? getAmenityLabel(`amenity_${item.category}`)
        : catConfig.label;
      const name = item.name || categoryLabel;
      const openStatus = formatAmenityOpenStatus(item, now);
      const distance = formatAmenityDistance(item);
      const button = document.createElement("button");
      button.type = "button";
      button.className = "amenity-item";
      button.addEventListener("click", () => openAmenityDetailSheet(item, categoryLabel, now));
      button.innerHTML = `
      ${iconPath
        ? `<img src="${iconPath}" alt="${escapeHtml(categoryLabel)}" loading="lazy">`
        : `<span class="amenity-item-icon-fallback" aria-hidden="true"></span>`}
      <div class="amenity-detail">
        <span class="amenity-detail-name">${escapeHtml(name)}</span>
        <span class="amenity-detail-meta ${openStatus.className}">${escapeHtml(openStatus.label)}</span>
      </div>
      <span class="amenity-item-spacer"></span>
      ${distance ? `<span class="amenity-distance">${escapeHtml(distance)}</span>` : ""}
      <span class="amenity-chevron" aria-hidden="true"></span>
    `;
      itemsElement.appendChild(button);
    });
    groupElement.appendChild(itemsElement);
    els.detail.amenityList.appendChild(groupElement);
  });
}

function renderDetailStaticInfo(props) {
  els.detail.detailsList.innerHTML = "";
  const rows = buildStaticDetailRows(props);
  const sourceText = formatStaticDetailSource(props);

  if (rows.length === 0 && !sourceText) {
    els.detail.detailsSection.hidden = true;
    els.detail.detailsSource.hidden = true;
    els.detail.detailsSource.textContent = "";
    return;
  }

  rows.forEach((item) => {
    const div = document.createElement("div");
    div.className = "detail-info-row";
    div.innerHTML = `
      <span class="detail-info-label">${escapeHtml(item.label)}</span>
      <span class="detail-info-value">${escapeHtml(item.value)}</span>
    `;
    els.detail.detailsList.appendChild(div);
  });

  if (sourceText) {
    els.detail.detailsSource.textContent = sourceText;
    els.detail.detailsSource.hidden = false;
  } else {
    els.detail.detailsSource.textContent = "";
    els.detail.detailsSource.hidden = true;
  }
  els.detail.detailsSection.hidden = false;
}

function toggleDetailFavorite() {
  if (!currentDetailFeature) return;
  const id = getStationIdFromProps(currentDetailFeature.properties);
  if (!id) return;

  if (state.favorites.has(id)) {
    removeFavoriteItem(state.favoriteMetadata, id);
  } else {
    ensureFavoriteItem(state.favoriteMetadata, id, { source: FAVORITE_SOURCE_MANUAL });
  }
  syncFavoriteStationIdsFromMetadata();

  updateFavBtnState();
  saveFavorites();
  refreshFavoriteDependentViews([id]);
}

function updateFavBtnState() {
  if (!currentDetailFeature) return;
  const id = getStationIdFromProps(currentDetailFeature.properties);
  if (!id) return;
  const isFav = state.favorites.has(id);

  if (isFav) {
    els.detail.favBtn.classList.add("active");
    els.detail.favBtn.setAttribute("aria-pressed", "true");
    els.detail.favBtn.setAttribute("aria-label", t("aria.removeFavorite"));
    els.detail.favBtn
      .querySelector("polygon")
      .setAttribute("fill", "currentColor");
  } else {
    els.detail.favBtn.classList.remove("active");
    els.detail.favBtn.setAttribute("aria-pressed", "false");
    els.detail.favBtn.setAttribute("aria-label", t("aria.saveFavorite"));
    els.detail.favBtn.querySelector("polygon").setAttribute("fill", "none");
  }
  updateDetailFavoriteCategories();
}

async function handleRatingClick(event) {
  const button = event.target.closest(".rating-star-btn");
  if (!button || !currentDetailFeature) {
    return;
  }
  const rating = normalizeRating(button.dataset.rating);
  const stationId = getStationIdFromProps(currentDetailFeature.properties);
  if (!stationId || rating <= 0) {
    return;
  }

  state.ratings.set(stationId, rating);
  state.ratingSubmissionErrors.delete(stationId);
  saveRatings();
  updateRatingDependentViews();

  if (!SHARED_RATINGS_ENABLED || !state.live.baseUrl) {
    return;
  }

  state.pendingRatingSubmissions.add(stationId);
  updateRatingDependentViews();

  try {
    const payload = await fetchJsonWithTimeout(
      buildLiveApiUrl("/v1/ratings"),
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          station_id: stationId,
          rating,
          client_id: getOrCreateRatingClientId(),
        }),
      },
      RATING_API_TIMEOUT_MS,
    );
    const summary = normalizeRatingSummary(payload?.rating);
    if (!summary) {
      throw new Error("Unexpected rating submission payload");
    }
    state.live.reachable = true;
    upsertRatingSummaries([summary]);
  } catch (err) {
    state.ratingSubmissionErrors.set(stationId, String(err?.message || err || "Fehler"));
    console.error(`Failed to submit rating for station ${stationId}`, err);
  } finally {
    state.pendingRatingSubmissions.delete(stationId);
    updateRatingDependentViews();
  }
}

function handleDetailNoteInput(event) {
  if (!currentDetailFeature) {
    return;
  }
  const stationId = getStationIdFromProps(currentDetailFeature.properties);
  if (!stationId) {
    return;
  }

  const note = normalizeNote(event.target.value);
  if (note) {
    state.notes.set(stationId, note);
    els.detail.noteStatus.textContent = t("detail.noteSaved");
  } else {
    state.notes.delete(stationId);
    els.detail.noteStatus.textContent = t("detail.noteDeviceOnly");
  }
  saveNotes();

  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
}

function handleFavoriteSortChange(event) {
  state.favoriteSort = event.target.value === FAVORITE_SORT_RATING
    ? FAVORITE_SORT_RATING
    : FAVORITE_SORT_DISTANCE;
  els.favorites.sort.value = state.favoriteSort;
  renderFavorites();
}

/* --- UTILS --- */
function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function sanitizeDisplayedPowerKw(value) {
  const numeric = Number(value || 0);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return 0;
  }
  return Math.min(numeric, MAX_DISPLAY_POWER_KW);
}

function getStationPagePath(props) {
  const stationId = normalizeStationId(props?.station_id || "");
  if (!stationId) {
    return "./";
  }
  return `./station/${encodeStationPageId(stationId)}.html`;
}

function encodeStationIdValue(value) {
  return encodeURIComponent(String(value || "").trim()).replace(/%3A/gi, ":");
}

function encodeStationPageId(value) {
  const stationId = normalizeStationId(value);
  const separatorIndex = stationId.indexOf(":");
  if (separatorIndex > 0) {
    const namespace = stationId.slice(0, separatorIndex);
    const localId = stationId.slice(separatorIndex + 1);
    return `${encodeURIComponent(namespace)}/${encodeURIComponent(localId)}`;
  }
  return encodeURIComponent(stationId);
}

function normalizeStationId(value) {
  const stationId = String(value || "").trim();
  if (!stationId) {
    return "";
  }
  if (LEGACY_STATION_ID_RE.test(stationId)) {
    return `${STATION_ID_NAMESPACE}${stationId.toLowerCase()}`;
  }
  const namespacedMatch = stationId.match(NAMESPACED_STATION_ID_RE);
  if (namespacedMatch) {
    return `${STATION_ID_NAMESPACE}${namespacedMatch[1].toLowerCase()}`;
  }
  const countryMatch = stationId.match(COUNTRY_STATION_ID_RE);
  if (countryMatch) {
    return `${countryMatch[1].toUpperCase()}:${countryMatch[2]}`;
  }
  return stationId;
}

function toLiveApiStationId(value) {
  return normalizeStationId(value);
}

function isGermanStationId(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return false;
  }
  return LEGACY_STATION_ID_RE.test(raw) || /^DE:/i.test(normalizeStationId(raw));
}

function liveApiBaseUrlForStationId(stationId) {
  if (isGermanStationId(stationId)) {
    return state.live.deBaseUrl || state.live.baseUrl;
  }
  return state.live.baseUrl;
}

function groupStationIdsByLiveApiBaseUrl(stationIds) {
  const groups = new Map();
  stationIds.forEach((stationId) => {
    const baseUrl = liveApiBaseUrlForStationId(stationId);
    if (!baseUrl) {
      return;
    }
    if (!groups.has(baseUrl)) {
      groups.set(baseUrl, []);
    }
    groups.get(baseUrl).push(stationId);
  });
  return groups;
}

function chunkArray(items, size) {
  const chunkSize = Math.max(1, Math.floor(Number(size) || 1));
  const chunks = [];
  for (let index = 0; index < items.length; index += chunkSize) {
    chunks.push(items.slice(index, index + chunkSize));
  }
  return chunks;
}

function getRequestedStationId() {
  const params = new URLSearchParams(window.location.search);
  return normalizeStationId(params.get("station") || "");
}

function updateRequestedStationId(stationId) {
  const url = new URL(window.location.href);
  const params = new URLSearchParams(url.search);
  params.delete("station");
  const searchParts = [];
  const normalizedStationId = normalizeStationId(stationId);
  if (normalizedStationId) {
    searchParts.push(`station=${encodeStationIdValue(normalizedStationId)}`);
  }
  const rest = params.toString();
  if (rest) {
    searchParts.push(rest);
  }
  url.search = searchParts.length ? `?${searchParts.join("&")}` : "";
  const next = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState(window.history.state, "", next);
}

function findFeatureByStationId(stationId) {
  const normalizedStationId = normalizeStationId(stationId);
  return state.features.find((feature) =>
    normalizeStationId(feature.properties?.station_id || "") === normalizedStationId,
  ) || findRouteFeatureByStationId(normalizedStationId) || state.staticFeatures.find((feature) =>
    normalizeStationId(feature.properties?.station_id || "") === normalizedStationId,
  ) || null;
}

function findRouteFeatureByStationId(stationId) {
  const normalizedStationId = normalizeStationId(stationId);
  return state.route.features.find((feature) =>
    normalizeStationId(feature.properties?.station_id || "") === normalizedStationId,
  ) || null;
}

function syncDetailModalWithUrl() {
  const rawStationId = new URLSearchParams(window.location.search).get("station") || "";
  const stationId = getRequestedStationId();
  if (rawStationId && stationId && rawStationId !== stationId) {
    updateRequestedStationId(stationId);
  }
  if (!stationId) {
    if (!els.modals.detail.classList.contains("hidden")) {
      closeModal("detail", { syncUrl: false });
    }
    return;
  }
  const feature = findFeatureByStationId(stationId);
  if (!feature) {
    if (
      state.live.baseUrl &&
      !state.catalog.pendingDetailStationIds.has(stationId) &&
      !state.catalog.missingDetailStationIds.has(stationId)
    ) {
      void loadCatalogStationDetail(stationId).then(syncDetailModalWithUrl);
      return;
    }
    console.warn("Unknown station requested", stationId);
    return;
  }

  if (currentDetailFeature?.properties?.station_id === stationId) {
    return;
  }

  centerMapOnFeature(feature);
  openDetail(feature, { syncUrl: false });
}

function distanceBetweenCoordinatesMeters(left, right) {
  const leftLat = Number(left?.lat);
  const leftLon = Number(left?.lon ?? left?.lng);
  const rightLat = Number(right?.lat);
  const rightLon = Number(right?.lon ?? right?.lng);
  if (
    !Number.isFinite(leftLat) ||
    !Number.isFinite(leftLon) ||
    !Number.isFinite(rightLat) ||
    !Number.isFinite(rightLon)
  ) {
    return Infinity;
  }
  // Haversine approx is enough for sorting
  const R = 6371e3; // meters
  const φ1 = (leftLat * Math.PI) / 180;
  const φ2 = (rightLat * Math.PI) / 180;
  const Δφ = ((rightLat - leftLat) * Math.PI) / 180;
  const Δλ = ((rightLon - leftLon) * Math.PI) / 180;

  const a =
    Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c; // meters
}

function getDistance(feature) {
  const backendDistance = Number(feature?.properties?.distance_m);
  if (Number.isFinite(backendDistance) && backendDistance >= 0) {
    return backendDistance;
  }
  const reference = getDistanceReferencePosition();
  if (!reference) return Infinity;
  const [lon, lat] = feature.geometry.coordinates;
  return distanceBetweenCoordinatesMeters(reference, { lat, lon });
}

function getDistanceFormatted(feature) {
  const d = getDistance(feature);
  if (d === Infinity) return "";
  if (d > 1000) return (d / 1000).toFixed(1) + " km";
  return Math.round(d) + " m";
}

async function queueStartupLocationRequest() {
  if (!shouldAttemptStartupLocation()) {
    return;
  }

  const detach = () => {
    document.removeEventListener("visibilitychange", onVisibilityChange);
    window.removeEventListener("focus", attemptWhenVisible);
    window.removeEventListener("pageshow", attemptWhenVisible);
  };

  const attemptWhenVisible = () => {
    if (state.startupLocationRequested || state.userPos) {
      detach();
      return;
    }
    if (document.visibilityState === "hidden") {
      return;
    }
    state.startupLocationRequested = true;
    detach();
    window.requestAnimationFrame(() => {
      window.setTimeout(requestUserLocation, 0);
    });
  };

  const onVisibilityChange = () => {
    if (document.visibilityState === "visible") {
      attemptWhenVisible();
    }
  };

  document.addEventListener("visibilitychange", onVisibilityChange);
  window.addEventListener("focus", attemptWhenVisible);
  window.addEventListener("pageshow", attemptWhenVisible);
  attemptWhenVisible();
}

async function syncLocationPermissionState() {
  if (!navigator.geolocation) {
    updateLocationState({
      permissionState: LOCATION_PERMISSION_UNSUPPORTED,
      requestState: LOCATION_REQUEST_ERROR,
      errorCode: "unsupported",
    });
    return;
  }

  const permissionsApi = navigator.permissions;
  if (!permissionsApi || typeof permissionsApi.query !== "function") {
    updateLocationState({
      permissionState: normalizeLocationPermissionState("unknown"),
      requestState: LOCATION_REQUEST_IDLE,
      errorCode: "",
    });
    return;
  }

  try {
    const permission = await permissionsApi.query({ name: "geolocation" });
    const permissionState = normalizeLocationPermissionState(permission.state);
    updateLocationState({
      permissionState,
      requestState: permissionState === LOCATION_PERMISSION_DENIED
        ? LOCATION_REQUEST_ERROR
        : LOCATION_REQUEST_IDLE,
      errorCode: permissionState === LOCATION_PERMISSION_DENIED
        ? LOCATION_ERROR_PERMISSION_DENIED
        : "",
    });
  } catch (err) {
    console.warn("Geolocation permission check failed", err);
    updateLocationState({
      permissionState: normalizeLocationPermissionState("unknown"),
      requestState: LOCATION_REQUEST_IDLE,
      errorCode: "",
    });
  }
}

async function refreshCatalogFromGPSPosition({
  recenter = false,
  reset = false,
  showPending = true,
  showError = true,
  maximumAge = 300000,
} = {}) {
  if (hasPinnedRouteMap()) {
    renderRouteMapLock();
    return;
  }
  if (!navigator.geolocation) {
    if (showError) {
      updateLocationState({
        permissionState: LOCATION_PERMISSION_UNSUPPORTED,
        requestState: LOCATION_REQUEST_ERROR,
        errorCode: "unsupported",
      });
    }
    return;
  }

  if (showPending) {
    updateLocationState({
      requestState: LOCATION_REQUEST_PENDING,
      errorCode: "",
    });
  }

  try {
    const position = await requestBrowserLocation(navigator.geolocation, {
      enableHighAccuracy: false,
      timeout: 5000,
      maximumAge,
    });

    state.userPos = {
      lat: position.lat,
      lon: position.lon,
    };
    updateLocationState({
      permissionState: LOCATION_PERMISSION_GRANTED,
      requestState: LOCATION_REQUEST_READY,
      errorCode: "",
    });
    setCatalogSearchCenter(state.userPos, "location");
    updateUserMarker();

    if (recenter && state.views.map) {
      state.views.map.flyTo([state.userPos.lat, state.userPos.lon], 13);
    }
    await loadCatalogStationsForCurrentCenter({ force: true, reset });
  } catch (err) {
    console.warn("Location error", err);
    if (showError) {
      updateLocationState({
        permissionState: err.code === LOCATION_ERROR_PERMISSION_DENIED
          ? LOCATION_PERMISSION_DENIED
          : state.location.permissionState,
        requestState: LOCATION_REQUEST_ERROR,
        errorCode: err.code || "unknown",
      });
    }
  }
}

async function requestUserLocation() {
  await refreshCatalogFromGPSPosition({
    recenter: true,
    reset: true,
    showPending: true,
    showError: true,
    maximumAge: 300000,
  });
}

/* --- LOCALSTORAGE --- */
function applyStoredFilterSettings(settings) {
  if (!settings) {
    return;
  }
  state.filters.operator = settings.operator;
  state.filters.minPower = settings.minPower;
  state.filters.minAmenityCount = settings.minAmenityCount;
  state.filters.amenities = new Set(settings.amenities);
  state.filters.amenityNameQuery = settings.amenityNameQuery;
  state.filters.availableOnly = settings.availableOnly;
  state.filters.currentlyOpenOnly = settings.currentlyOpenOnly;
}

function loadFilters() {
  try {
    applyStoredFilterSettings(
      parseStoredFilterSettings(localStorage.getItem(FILTERS_STORAGE_KEY), DEFAULT_FILTER_SETTINGS),
    );
  } catch (e) {
    console.error("Error loading filters", e);
  }
}

function loadFavorites() {
  try {
    const storedMetadata = parseStoredFavoriteMetadata(
      localStorage.getItem(FAVORITES_V2_STORAGE_KEY),
      { normalizeStationId },
    );
    if (storedMetadata) {
      state.favoriteMetadata = storedMetadata;
    } else {
      state.favoriteMetadata = migrateLegacyFavorites(
        localStorage.getItem(FAVORITES_LEGACY_STORAGE_KEY),
        { normalizeStationId },
      );
      syncFavoriteStationIdsFromMetadata();
      if (state.favoriteMetadata.items.size > 0) {
        saveFavorites();
      }
      return;
    }
    syncFavoriteStationIdsFromMetadata();
  } catch (e) {
    console.error("Error loading favorites", e);
    state.favoriteMetadata = createEmptyFavoriteMetadata();
    syncFavoriteStationIdsFromMetadata();
  }
}

function loadRatings() {
  try {
    state.ratings = new Map(
      Array.from(parseStoredRatings(localStorage.getItem(RATINGS_STORAGE_KEY)).entries())
        .map(([stationId, rating]) => [normalizeStationId(stationId), rating])
        .filter(([stationId]) => stationId),
    );
  } catch (e) {
    console.error("Error loading ratings", e);
    state.ratings = new Map();
  }
}

function loadNotes() {
  try {
    state.notes = new Map(
      Array.from(parseStoredNotes(localStorage.getItem(NOTES_STORAGE_KEY)).entries())
        .map(([stationId, note]) => [normalizeStationId(stationId), note])
        .filter(([stationId]) => stationId),
    );
  } catch (e) {
    console.error("Error loading notes", e);
    state.notes = new Map();
  }
}

function createRatingClientId() {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }
  const bytes = new Uint8Array(16);
  if (window.crypto?.getRandomValues) {
    window.crypto.getRandomValues(bytes);
    return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 18)}`;
}

function getOrCreateRatingClientId() {
  const current = String(state.ratingClientId || "").trim();
  if (current.length >= 16) {
    return current;
  }

  try {
    const stored = String(localStorage.getItem(RATING_CLIENT_STORAGE_KEY) || "").trim();
    if (stored.length >= 16) {
      state.ratingClientId = stored;
      return stored;
    }
  } catch (e) {
    console.error("Error loading rating client id", e);
  }

  const created = createRatingClientId();
  state.ratingClientId = created;
  try {
    localStorage.setItem(RATING_CLIENT_STORAGE_KEY, created);
  } catch (e) {
    console.error("Error saving rating client id", e);
  }
  return created;
}

function saveRatings() {
  try {
    localStorage.setItem(RATINGS_STORAGE_KEY, serializeStoredRatings(state.ratings));
  } catch (e) {
    console.error("Error saving ratings", e);
  }
}

function saveNotes() {
  try {
    localStorage.setItem(NOTES_STORAGE_KEY, serializeStoredNotes(state.notes));
  } catch (e) {
    console.error("Error saving notes", e);
  }
}

function saveFilters() {
  try {
    localStorage.setItem(
      FILTERS_STORAGE_KEY,
      serializeStoredFilterSettings(state.filters, DEFAULT_FILTER_SETTINGS),
    );
  } catch (e) {
    console.error("Error saving filters", e);
  }
}

function saveFavorites() {
  try {
    const arr = Array.from(state.favorites);
    localStorage.setItem(FAVORITES_V2_STORAGE_KEY, serializeFavoriteMetadata(state.favoriteMetadata));
    localStorage.setItem(FAVORITES_LEGACY_STORAGE_KEY, JSON.stringify(arr));
  } catch (e) {
    console.error("Error saving favorites", e);
  }
}

/* --- MODAL UTILS --- */
const MODAL_FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

function getModalContent(name) {
  return els.modals[name]?.querySelector(".modal-content") || null;
}

function getTopOpenModalName() {
  return Object.keys(els.modals)
    .reverse()
    .find((name) => isModalOpen(name)) || "";
}

function visibleElement(element) {
  return Boolean(element && (element.offsetWidth || element.offsetHeight || element.getClientRects().length));
}

function getModalFocusableElements(name) {
  const content = getModalContent(name);
  if (!content) {
    return [];
  }
  return Array.from(content.querySelectorAll(MODAL_FOCUSABLE_SELECTOR))
    .filter((element) => !element.disabled && visibleElement(element));
}

function focusModal(name) {
  const content = getModalContent(name);
  if (!content) {
    return;
  }
  const focusTarget = getModalFocusableElements(name)[0] || content;
  focusTarget.focus({ preventScroll: true });
}

function restoreModalFocus(name) {
  const previous = state.modal.lastFocusedByName.get(name);
  state.modal.lastFocusedByName.delete(name);
  if (previous && previous.isConnected && visibleElement(previous)) {
    previous.focus({ preventScroll: true });
    return true;
  }
  return false;
}

function trapModalTab(event, name) {
  const focusable = getModalFocusableElements(name);
  if (focusable.length === 0) {
    event.preventDefault();
    getModalContent(name)?.focus({ preventScroll: true });
    return;
  }
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  const active = document.activeElement;
  if (event.shiftKey && active === first) {
    event.preventDefault();
    last.focus({ preventScroll: true });
    return;
  }
  if (!event.shiftKey && active === last) {
    event.preventDefault();
    first.focus({ preventScroll: true });
  }
}

function openModal(name) {
  const m = els.modals[name];
  if (!m) {
    return;
  }
  const activeElement = document.activeElement;
  if (activeElement instanceof HTMLElement && !m.contains(activeElement)) {
    state.modal.lastFocusedByName.set(name, activeElement);
  }
  m.classList.remove("hidden");
  window.requestAnimationFrame(() => focusModal(name));
}

function closeModal(name, options = {}) {
  const syncUrl = options.syncUrl !== false;
  const m = els.modals[name];
  if (m) {
    m.classList.add("hidden");
  }
  if (name === "detail") {
    currentDetailFeature = null;
    if (syncUrl) {
      updateRequestedStationId("");
    }
  }
  const restored = restoreModalFocus(name);
  if (name === "detail" && !restored) {
    focusSelectedListCard();
  }
}

/* --- BOOTSTRAP --- */
initI18n()
  .then(init)
  .catch((error) => {
    console.error("Failed to initialize language bundle", error);
    void init();
  });
