import { countActiveFilters, matchesFeatureFilters } from "./filtering.mjs";
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
} from "./location.mjs";
import {
  normalizeLiveApiBaseUrl,
  resolveLiveApiBaseUrl as computeLiveApiBaseUrl,
} from "./live-api.mjs";

/**
 * woladen.de - Modern Frontend Logic
 */

/* --- CONFIGURATION & CONSTANTS --- */
const MAX_DISPLAY_POWER_KW = 400;
const LIVE_SUMMARY_REFRESH_MS = 15000;
const LIVE_API_TIMEOUT_MS = 3500;
const LIVE_DETAIL_TIMEOUT_MS = 4000;
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
  return `${rounded} ${rounded === 1 ? "Angebot vor Ort" : "Angebote vor Ort"}`;
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

const LIVE_API_BASE_URL = resolveLiveApiBaseUrl();

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
    return "Frei";
  }
  if (status === "occupied") {
    return "Belegt";
  }
  if (status === "out_of_order") {
    return "Defekt";
  }
  return "Unbekannt";
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
    parts.push(`${Math.round(available)} frei`);
  }
  if (occupied > 0) {
    parts.push(`${Math.round(occupied)} belegt`);
  }
  if (outOfOrder > 0) {
    parts.push(`${Math.round(outOfOrder)} defekt`);
  }
  if (unknown > 0) {
    parts.push(`${Math.round(unknown)} unbekannt`);
  }
  return parts.length ? parts.join(", ") : "Belegung unbekannt";
}

function formatProviderLabel(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
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

function formatOccupancySource(props) {
  if (hasLiveStationSummary(props)) {
    const provider = getLiveSourceLabel(props);
    const timestamp = formatDetailTimestamp(
      props.live_source_observed_at || props.live_fetched_at || props.live_ingested_at,
    );
    if (provider && timestamp) {
      return `Live via ${provider} • Seit ${timestamp}`;
    }
    if (provider) {
      return `Live via ${provider}`;
    }
    if (timestamp) {
      return `Live seit ${timestamp}`;
    }
    return "Live via lokaler API";
  }

  const counts = getAvailabilityCounts(props);
  if (!Number.isFinite(counts.total) || counts.total <= 0) {
    return "";
  }
  const sourceUid = String(props.occupancy_source_uid || "").trim();
  const sourceName = String(props.occupancy_source_name || "").trim();
  if (sourceName.startsWith("Mobilithek")) {
    return `Live via ${sourceName}`;
  }
  if (sourceUid.startsWith("mobilithek_")) {
    return sourceName ? `Live via Mobilithek (${sourceName})` : "Live via Mobilithek";
  }
  if (sourceName) {
    return `Live via MobiData BW (${sourceName})`;
  }
  return "Live via MobiData BW";
}

function formatDetailTimestamp(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return raw;
  }
  return new Intl.DateTimeFormat("de-DE", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatStaticDetailSource(props) {
  const sourceName = String(props.detail_source_name || "").trim();
  const timestamp = formatDetailTimestamp(props.detail_last_updated);
  if (!sourceName && !timestamp) {
    return "";
  }
  if (sourceName && timestamp) {
    return `Details via ${sourceName} • Stand ${timestamp}`;
  }
  if (sourceName) {
    return `Details via ${sourceName}`;
  }
  return `Stand ${timestamp}`;
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

  pushRow("Bezahlen", props.payment_methods_display);
  pushRow("Zugang", props.auth_methods_display);
  pushRow("Stecker", props.connector_types_display);
  pushRow("Stromart", props.current_types_display);
  const connectorCount = Number(props.connector_count || 0);
  if (Number.isFinite(connectorCount) && connectorCount > 0) {
    pushRow("Anschlüsse", `${Math.round(connectorCount)} Steckplätze`);
  }
  pushRow("Service", props.service_types_display);

  if (props.green_energy === true) {
    pushRow("Strom", "100 % erneuerbar");
  } else if (props.green_energy === false) {
    pushRow("Strom", "Nicht als erneuerbar markiert");
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
    return value ? "Ja" : "Nein";
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
    notes.push({ label: "Nächster Slot", value: nextSlotText });
  }
  const supplementalText = formatLiveDetailCollection(evse.supplemental_facility_status);
  if (supplementalText) {
    notes.push({ label: "Zusatzstatus", value: supplementalText });
  }
  return notes;
}

/* --- STATE --- */
const state = {
  features: [], // All charger features
  filtered: [], // Currently filtered features
  favorites: new Set(), // Set of station_ids
  userPos: null, // { lat, lon }
  startupLocationRequested: false,
  location: {
    permissionState: LOCATION_PERMISSION_UNKNOWN,
    requestState: LOCATION_REQUEST_IDLE,
    errorCode: "",
  },
  filters: {
    operator: "",
    minPower: 50,
    amenities: new Set(),
    amenityNameQuery: "",
  },
  live: {
    baseUrl: LIVE_API_BASE_URL,
    summaryByStationId: new Map(),
    summaryFetchedAtByStationId: new Map(),
    pendingSummaryStationIds: new Set(),
    detailByStationId: new Map(),
    reachable: false,
  },
  views: {
    map: null, // Leaflet map instance
    detailMap: null, // Mini map in detail view
    layers: {
      chargers: null,
      user: null,
      detailAmenities: null,
    },
  },
};

/* --- DOM ELEMENTS --- */
const els = {
  app: document.getElementById("app"),
  views: {
    map: document.getElementById("view-map"),
    list: document.getElementById("view-list"),
    favorites: document.getElementById("view-favorites"),
    info: document.getElementById("view-info"),
  },
  navItems: document.querySelectorAll(".nav-item"),
  modals: {
    filter: document.getElementById("modal-filter"),
    detail: document.getElementById("modal-detail"),
  },
  lists: {
    chargers: document.getElementById("charger-list"),
    favorites: document.getElementById("favorites-list"),
  },
  filter: {
    trigger: document.getElementById("filter-trigger"),
    label: document.getElementById("filter-label"),
    operator: document.getElementById("filter-operator"),
    amenityName: document.getElementById("filter-amenity-name"),
    power: document.getElementById("filter-power"),
    powerVal: document.getElementById("filter-power-val"),
    amenities: document.getElementById("filter-amenities"),
    applyBtn: document.getElementById("btn-apply-filter"),
    listFilterBtn: document.getElementById("btn-list-filter"),
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
    amenityTitle: document.getElementById("detail-amenities-title"),
    amenityList: document.getElementById("detail-amenities-list"),
    detailsSection: document.getElementById("detail-details-section"),
    detailsList: document.getElementById("detail-details-list"),
    detailsSource: document.getElementById("detail-details-source"),
    liveSection: document.getElementById("detail-live-section"),
    liveTitle: document.getElementById("detail-live-title"),
    liveUpdated: document.getElementById("detail-live-updated"),
    liveList: document.getElementById("detail-live-list"),
    favBtn: document.getElementById("btn-toggle-fav"),
    googleBtn: document.getElementById("btn-nav-google"),
    appleBtn: document.getElementById("btn-nav-apple"),
    helpdeskPhoneBtn: document.getElementById("btn-helpdesk-phone"),
    mapContainer: document.getElementById("detail-map"),
  },
  buttons: {
    locate: document.getElementById("btn-locate"),
    closeFilter: document.querySelector('[data-close="modal-filter"]'),
    closeDetail: document.querySelector('[data-close="modal-detail"]'),
  },
  meta: document.getElementById("app-meta"),
};

/* --- INITIALIZATION --- */
async function init() {
  loadFavorites();
  initMap();
  initNavigation();
  initFilters();
  window.addEventListener("popstate", syncDetailModalWithUrl);

  // Event Listeners
  els.buttons.locate.addEventListener("click", () => requestUserLocation(false));
  els.filter.trigger.addEventListener("click", () => openModal("filter"));
  els.filter.listFilterBtn.addEventListener("click", () => openModal("filter"));
  els.filter.applyBtn.addEventListener("click", () => closeModal("filter"));

  els.buttons.closeFilter.addEventListener("click", () => closeModal("filter"));
  els.buttons.closeDetail.addEventListener("click", () => closeModal("detail"));

  // Close modals on backdrop click
  Object.values(els.modals).forEach((modal) => {
    modal.addEventListener("click", (e) => {
      if (e.target === modal)
        closeModal(modal.id === "modal-filter" ? "filter" : "detail");
    });
  });

  els.detail.favBtn.addEventListener("click", toggleDetailFavorite);

  // Load Data
  await loadData();
}

/* --- DATA LOADING --- */
async function loadData() {
  try {
    const [geoRes, opRes, summaryRes] = await Promise.all([
      fetch("./data/chargers_fast.geojson"),
      fetch("./data/operators.json"),
      fetch("./data/summary.json"),
    ]);

    if (!geoRes.ok || !opRes.ok || !summaryRes.ok) throw new Error("Network response was not ok");

    const geoData = await geoRes.json();
    const opData = await opRes.json();
    const summaryData = await summaryRes.json();

    state.features = geoData.features || [];

    // Sort features initially just to have a defined order, strictly standard
    // Real sorting happens when we have location

    populateOperators(opData);
    setAppMeta(geoData, summaryData);
    renderAmenityFilters(); // Render dynamic amenity filters
    await syncLocationPermissionState();

    applyFilters(); // Initial render
    syncDetailModalWithUrl();

    // Request location once after data is ready, but only when the page is visible.
    // This is more reliable on restores/background loads than a single immediate call.
    queueStartupLocationRequest();
  } catch (err) {
    console.error("Failed to load data", err);
    els.lists.chargers.innerHTML = `<div class="empty-state">Fehler beim Laden der Daten.<br>${err.message}</div>`;
  }
}

function buildLiveApiUrl(path, params = {}) {
  const url = new URL(path, state.live.baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") {
      return;
    }
    url.searchParams.set(key, String(value));
  });
  return url.toString();
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
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } finally {
    window.clearTimeout(timer);
  }
}

function upsertLiveStationSummaries(stations, missingStationIds = []) {
  stations.forEach((summary) => {
    const stationId = getStationIdFromProps(summary);
    if (!stationId) {
      return;
    }
    state.live.summaryByStationId.set(stationId, summary);
    state.live.summaryFetchedAtByStationId.set(stationId, Date.now());
    const feature = findFeatureByStationId(stationId);
    if (feature) {
      applyLiveStationSummaryToProps(feature.properties, summary);
    }
  });

  missingStationIds.forEach((stationId) => {
    const id = String(stationId || "").trim();
    if (!id) {
      return;
    }
    state.live.summaryByStationId.delete(id);
    state.live.summaryFetchedAtByStationId.set(id, Date.now());
    const feature = findFeatureByStationId(id);
    if (feature) {
      clearLiveStationSummaryFromProps(feature.properties);
    }
  });
}

function requestLiveSummariesForFeatures(features) {
  if (!state.live.baseUrl) {
    return;
  }

  const stationIds = Array.from(new Set(
    features.map((feature) => getStationIdFromProps(feature.properties)).filter(Boolean),
  ));
  if (stationIds.length === 0) {
    return;
  }

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
    try {
      const payload = await fetchJsonWithTimeout(
        buildLiveApiUrl("/v1/stations/lookup"),
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ station_ids: pendingIds }),
        },
        LIVE_API_TIMEOUT_MS,
      );
      if (!payload || typeof payload !== "object" || !Array.isArray(payload.stations)) {
        throw new Error("Unexpected live station lookup payload");
      }
      state.live.reachable = true;
      upsertLiveStationSummaries(payload.stations, payload.missing_station_ids || []);
      refreshRenderedViews();
    } catch (err) {
      console.error("Failed to load live station summaries", err);
    } finally {
      pendingIds.forEach((stationId) => {
        state.live.pendingSummaryStationIds.delete(stationId);
      });
    }
  })();
}

function refreshRenderedViews() {
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
  return getLocationLookupViewModel({
    hasLocation: hasResolvedUserLocation(),
    isRequesting: state.location.requestState === LOCATION_REQUEST_PENDING,
    permissionState: state.location.permissionState,
    errorCode: state.location.errorCode,
    geolocationSupported: Boolean(navigator.geolocation),
  });
}

function renderLocationGate(container, viewModel) {
  container.innerHTML = "";

  const panel = document.createElement("section");
  panel.className = `location-gate location-gate-${viewModel.kind}`;
  panel.setAttribute("data-nosnippet", "");

  const title = document.createElement("h3");
  title.className = "location-gate-title";
  title.textContent = viewModel.title;
  panel.appendChild(title);

  const copy = document.createElement("p");
  copy.className = "location-gate-copy";
  copy.textContent = viewModel.message;
  panel.appendChild(copy);

  if (viewModel.actionLabel) {
    const actions = document.createElement("div");
    actions.className = "location-gate-actions";
    const button = document.createElement("button");
    button.type = "button";
    button.className = "primary-btn";
    button.textContent = viewModel.actionLabel;
    button.addEventListener("click", () => requestUserLocation(false));
    actions.appendChild(button);
    panel.appendChild(actions);
  }

  container.appendChild(panel);
}

async function loadLiveStationDetail(stationId) {
  if (!state.live.baseUrl || !stationId) {
    return null;
  }
  if (state.live.detailByStationId.has(stationId)) {
    return state.live.detailByStationId.get(stationId);
  }

  try {
    const payload = await fetchJsonWithTimeout(
      buildLiveApiUrl(`/v1/stations/${encodeURIComponent(stationId)}`, {
        history_limit: 20,
      }),
      {},
      LIVE_DETAIL_TIMEOUT_MS,
    );
    if (!payload || typeof payload !== "object") {
      throw new Error("Unexpected station detail payload");
    }
    state.live.reachable = true;
    state.live.detailByStationId.set(stationId, payload);
    const feature = findFeatureByStationId(stationId);
    if (feature && payload.station) {
      applyLiveStationSummaryToProps(feature.properties, payload.station);
      state.live.summaryByStationId.set(stationId, payload.station);
      state.live.summaryFetchedAtByStationId.set(stationId, Date.now());
    }
    refreshRenderedViews();
    return payload;
  } catch (err) {
    console.error(`Failed to load live detail for station ${stationId}`, err);
    return null;
  }
}

function setAppMeta(geoData, summaryData) {
  if (!els.meta) return;

  const generatedAt =
    summaryData?.run?.finished_at ||
    geoData?.generated_at ||
    null;

  if (generatedAt) {
    const parsed = new Date(generatedAt);
    const date = Number.isNaN(parsed.getTime()) ? generatedAt : parsed.toLocaleString("de-DE", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
    els.meta.textContent = `Datenstand: ${date}`;
  }
}

function populateOperators(opData) {
  const operators = opData.operators
    .filter((o) => o.stations >= 100) // Only major ones
    .map((o) => o.name)
    .sort();

  operators.forEach((op) => {
    const opt = document.createElement("option");
    opt.value = op;
    opt.textContent = op;
    els.filter.operator.appendChild(opt);
  });
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

  state.views.layers.chargers = L.markerClusterGroup
    ? L.markerClusterGroup()
    : L.layerGroup();
  state.views.layers.chargers.addTo(state.views.map);

  state.views.layers.user = L.layerGroup().addTo(state.views.map);

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

function getMarkerColor(props) {
  const total = props.amenities_total || 0;
  if (total > 10) return "#f59e0b"; // Gold
  if (total > 5) return "#94a3b8"; // Silver
  if (total > 0) return "#b45309"; // Bronze
  return "#64748b"; // Grey
}

function renderMapMarkers() {
  state.views.layers.chargers.clearLayers();

  const markers = state.filtered.map((feature) => {
    const [lon, lat] = feature.geometry.coordinates;
    const color = getMarkerColor(feature.properties);

    // Simple Circle Marker for performance and clean look
    const marker = L.circleMarker([lat, lon], {
      color: "#ffffff",
      weight: 1,
      fillColor: color,
      fillOpacity: 1,
      radius: 8,
    });

    marker.on("click", () => openDetail(feature));
    return marker;
  });

  markers.forEach((m) => m.addTo(state.views.layers.chargers));
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
      // Switch active state
      els.navItems.forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");

      const targetId = btn.dataset.target;
      switchView(targetId);
    });
  });
}

function switchView(viewId) {
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

  // Refresh lists if needed
  if (viewId === "view-list") renderList();
  if (viewId === "view-favorites") renderFavorites();

  // Map resize fix
  if (viewId === "view-map" && state.views.map) {
    setTimeout(() => state.views.map.invalidateSize(), 100);
  }
}

/* --- FILTER LOGIC --- */
function initFilters() {
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

  // Power
  els.filter.power.addEventListener("input", (e) => {
    state.filters.minPower = Number(e.target.value);
    els.filter.powerVal.textContent = state.filters.minPower;
    updateFilters();
  });
}

function renderAmenityFilters() {
  els.filter.amenities.innerHTML = "";
  
  // Find all available amenities in data
  const availableAmenities = new Set();
  const amenityKeys = Object.keys(AMENITY_MAPPING);
  
  state.features.forEach(f => {
    const p = f.properties;
    amenityKeys.forEach(key => {
      if (p[key] > 0) availableAmenities.add(key);
    });
  });

  // Sort by name for better UX
  const sortedKeys = Array.from(availableAmenities).sort((a, b) => {
    const labelA = AMENITY_MAPPING[a].label;
    const labelB = AMENITY_MAPPING[b].label;
    return labelA.localeCompare(labelB);
  });

  sortedKeys.forEach((key) => {
    const config = AMENITY_MAPPING[key];
    const path = getAmenityIconPath(key);

    const div = document.createElement("div");
    div.className = "amenity-toggle";
    div.dataset.key = key;

    if (path) {
      div.innerHTML = `<img src="${path}" alt="${config.label}" loading="lazy"><span class="amenity-name">${config.label}</span>`;
    } else {
      div.innerHTML = `<div style="width:32px;height:32px;background:#eee;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px">?</div><span class="amenity-name">${config.label}</span>`;
    }

    div.addEventListener("click", () => {
      div.classList.toggle("active");
      if (div.classList.contains("active")) {
        state.filters.amenities.add(key);
      } else {
        state.filters.amenities.delete(key);
      }
      updateFilters();
    });

    els.filter.amenities.appendChild(div);
  });
}

function updateFilters() {
  applyFilters();

  const filterCount = countActiveFilters(state.filters);

  els.filter.label.textContent =
    filterCount > 0 ? `Filter (${filterCount})` : "Alle Filter";
}

function applyFilters() {
  state.filtered = state.features.filter((feature) =>
    matchesFeatureFilters(feature, state.filters, { getDisplayedMaxPowerKw }),
  );

  // Re-sort if we have location
  if (state.userPos) {
    state.filtered.sort((a, b) => getDistance(a) - getDistance(b));
  }

  // Update Views
  renderMapMarkers();

  // If list is active, re-render it
  if (els.views.list.classList.contains("active")) {
    renderList();
  }
}

/* --- LIST RENDERING --- */
function renderList() {
  const container = els.lists.chargers;
  container.innerHTML = "";

  const locationViewModel = getLocationListViewModel();
  if (locationViewModel.blocksStationList) {
    renderLocationGate(container, locationViewModel);
    return;
  }

  // Limit to first 50 items for performance
  const displayItems = state.filtered.slice(0, 50);

  if (displayItems.length === 0) {
    container.innerHTML = `<div class="empty-state">Keine Ladestationen gefunden.</div>`;
    return;
  }

  displayItems.forEach((feature) => {
    const card = createStationCard(feature);
    container.appendChild(card);
  });
  requestLiveSummariesForFeatures(displayItems);

  if (state.filtered.length > 50) {
    const more = document.createElement("div");
    more.style.textAlign = "center";
    more.style.padding = "1rem";
    more.style.color = "#888";
    more.textContent = `...und ${state.filtered.length - 50} weitere`;
    container.appendChild(more);
  }
}

function renderFavorites() {
  const container = els.lists.favorites;
  container.innerHTML = "";

  if (state.favorites.size === 0) {
    container.innerHTML = `<div class="empty-state" style="text-align:center; padding:2rem; color:#888;">
      Noch keine Favoriten gespeichert.<br>
      Klicke auf den <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="vertical-align:middle"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"></polygon></svg> Stern in der Detailansicht um Stationen zu merken.
    </div>`;
    return;
  }

  // Find feature objects for favorites
  const favFeatures = state.features.filter((f) =>
    state.favorites.has(f.properties.station_id),
  );

  if (state.userPos) {
    favFeatures.sort((a, b) => getDistance(a) - getDistance(b));
  }

  favFeatures.forEach((feature) => {
    const card = createStationCard(feature);
    container.appendChild(card);
  });
  requestLiveSummariesForFeatures(favFeatures);
}

function createStationCard(feature) {
  const p = feature.properties;
  const div = document.createElement("div");
  div.className = "station-card";

  const distance = getDistanceFormatted(feature);
  const occupancySummary = formatOccupancySummary(p);
  const priceDisplay = getDisplayPrice(p);
  const availabilityStatus = getAvailabilityStatus(p);

  // Top Amenities (max 3 badges)
  const amenityBadges = Object.keys(AMENITY_MAPPING)
    .filter((k) => p[k] > 0)
    .sort((a, b) => p[b] - p[a]) // Most frequent first
    .slice(0, 3)
    .map((k) => `<span class="badge">${AMENITY_MAPPING[k].label}</span>`)
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

  const markerColor = getMarkerColor(p);
  
  div.innerHTML = `
    <div class="card-header">
      <div class="card-title-row">
        <span class="amenity-dot" style="background-color: ${markerColor}"></span>
        <h3 class="card-title">${escapeHtml(p.operator || "Unbekannt")}</h3>
      </div>
      ${distance ? `<span class="card-distance">${distance}</span>` : ""}
    </div>
    <div class="card-meta">
      ${escapeHtml(p.city || "")}<br>
      ${Math.round(getDisplayedMaxPowerKw(p))} kW max • ${getChargingPointCount(p)} Ladepunkte • ${formatAmenityCount(p.amenities_total)}
    </div>
    <div class="card-badges">
      ${dynamicLine}${amenityLine}
    </div>
  `;

  div.addEventListener("click", () => openDetail(feature));
  return div;
}

function getDisplayedMaxPowerKw(props) {
  const maxIndividual = sanitizeDisplayedPowerKw(props.max_individual_power_kw);
  if (maxIndividual > 0) {
    return maxIndividual;
  }
  return sanitizeDisplayedPowerKw(props.max_power_kw);
}

function getChargingPointCount(props) {
  const count = Number(props.charging_points_count || 0);
  if (Number.isFinite(count) && count > 0) {
    return Math.round(count);
  }
  return 1;
}

/* --- DETAIL MODAL --- */
let currentDetailFeature = null;

function renderDetailLiveState(feature, liveDetail = null) {
  const props = feature.properties;
  const evses = Array.isArray(liveDetail?.evses) ? liveDetail.evses : [];
  const hasLiveData = hasLiveStationSummary(props) || evses.length > 0;
  if (!hasLiveData) {
    els.detail.liveSection.hidden = true;
    els.detail.liveTitle.textContent = "Live";
    els.detail.liveUpdated.hidden = true;
    els.detail.liveUpdated.textContent = "";
    els.detail.liveList.innerHTML = "";
    return;
  }

  els.detail.liveTitle.textContent = "Live";
  els.detail.liveUpdated.textContent = "";
  els.detail.liveUpdated.hidden = true;
  els.detail.liveList.innerHTML = "";

  if (evses.length === 0) {
    const summaryRow = document.createElement("div");
    summaryRow.className = "live-evse-row live-evse-row-summary";
    const priceDisplay = getDisplayPrice(props, liveDetail);
    summaryRow.innerHTML = `
      <div class="live-evse-row-head">
        <strong class="live-evse-title">Stationsstatus</strong>
        <span class="live-status-pill ${escapeHtml(getAvailabilityToneClass(getAvailabilityStatus(props)))}">${escapeHtml(formatAvailabilityLabel(getAvailabilityStatus(props)))}</span>
      </div>
      <div class="live-evse-row-meta">
        <span>${escapeHtml(formatOccupancySummary(props) || "Live-Daten verfügbar")}</span>
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
    const observedText = formatDetailTimestamp(
      evse.source_observed_at || evse.fetched_at || evse.ingested_at,
    );
    const metaParts = [];
    const evseCode = formatEvseCode(evse.provider_evse_id);
    if (evseCode) {
      metaParts.push(evseCode);
    }
    if (observedText) {
      metaParts.push(`Stand ${observedText}`);
    }
    const priceDisplay = String(evse.price_display || "").trim();
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
        <strong class="live-evse-title">Ladepunkt ${index + 1}</strong>
        <span class="live-status-pill ${escapeHtml(getAvailabilityToneClass(status))}">${escapeHtml(formatAvailabilityLabel(status))}</span>
      </div>
      <div class="live-evse-row-meta">
        <span>${escapeHtml(metaParts.join(" • ") || "Live-Daten verfügbar")}</span>
        ${priceDisplay ? `<span class="live-evse-price">${escapeHtml(priceDisplay)}</span>` : ""}
      </div>
      ${notesMarkup}
    `;
    els.detail.liveList.appendChild(row);
  });

  els.detail.liveSection.hidden = false;
}

function populateDetailContent(feature, liveDetail = null) {
  const p = feature.properties;
  const powerDisplay = `${Math.round(getDisplayedMaxPowerKw(p))} kW max / ${getChargingPointCount(p)} Ladepunkte`;

  els.detail.title.textContent = p.operator || "Unbekannt";
  els.detail.address.textContent = `${p.address || ""}, ${p.postcode || ""} ${p.city || ""}`;
  els.detail.power.textContent = powerDisplay;
  els.detail.powerChip.hidden = !powerDisplay;

  const occupancySummary = formatOccupancySummary(p);
  const occupancySource = formatOccupancySource(p);
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
  const openingHoursDisplay = String(p.opening_hours_display || "").trim();
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

  renderDetailAmenities(p);
  renderDetailStaticInfo(p);
  renderDetailLiveState(feature, liveDetail);

  if (occupancySource) {
    els.detail.occupancySource.textContent = occupancySource;
    els.detail.occupancySource.hidden = !els.detail.liveSection.hidden;
  } else {
    els.detail.occupancySource.textContent = "";
    els.detail.occupancySource.hidden = true;
  }
}

function openDetail(feature, options = {}) {
  const syncUrl = options.syncUrl !== false;
  currentDetailFeature = feature;
  const p = feature.properties;

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
      els.detail.helpdeskPhoneBtn.title = `Hilfe ${p.helpdesk_phone}`;
    } else {
      els.detail.helpdeskPhoneBtn.removeAttribute("href");
      els.detail.helpdeskPhoneBtn.removeAttribute("title");
    }
  }

  // Mini Map
  // Clear old markers from detail map? Not strictly needed if we just pan,
  // but better to add a marker for the station
  if (state.views.detailMap.stationMarker)
    state.views.detailMap.removeLayer(state.views.detailMap.stationMarker);
  if (state.views.layers.detailAmenities) {
    state.views.layers.detailAmenities.clearLayers();
  }

  state.views.detailMap.stationMarker = L.circleMarker([lat, lon], {
    color: "#fff",
    fillColor: "#0f766e",
    fillOpacity: 1,
    radius: 8,
  }).addTo(state.views.detailMap);

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

  const stationId = getStationIdFromProps(p);
  if (stationId) {
    void loadLiveStationDetail(stationId);
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
    const amenityLabel = AMENITY_MAPPING[amenityKey]?.label || item.category || "Angebot vor Ort";
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
    els.detail.amenityList.innerHTML = `<div style="color:#888">Keine Details verfügbar.</div>`;
    return;
  }

  examples.slice(0, 15).forEach((item) => {
    // item: { category, name, opening_hours, distance_m, lat, lon }
    const catConfig = AMENITY_MAPPING[`amenity_${item.category}`] || {
      label: item.category || "Angebot vor Ort",
    };
    const iconPath = getAmenityIconPath(`amenity_${item.category}`);

    // Helper to format text
    const name = item.name || catConfig.label;
    const meta = [
      item.distance_m ? `~${Math.round(item.distance_m)}m` : null,
      item.opening_hours,
    ]
      .filter(Boolean)
      .join(" • ");

    const div = document.createElement("div");
    div.className = "amenity-item";

    let iconHtml = iconPath
      ? `<img src="${iconPath}" alt="${catConfig.label}">`
      : `<div style="width:24px;height:24px;background:#eee;border-radius:4px"></div>`;

    div.innerHTML = `
      ${iconHtml}
      <div class="amenity-detail">
        <span class="amenity-detail-name">${escapeHtml(name)}</span>
        <span class="amenity-detail-meta">${escapeHtml(meta)}</span>
      </div>
    `;
    els.detail.amenityList.appendChild(div);
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
  const id = currentDetailFeature.properties.station_id;

  if (state.favorites.has(id)) {
    state.favorites.delete(id);
  } else {
    state.favorites.add(id);
  }

  updateFavBtnState();
  saveFavorites();

  // If we are in favorites view, refresh
  if (els.views.favorites.classList.contains("active")) {
    renderFavorites();
  }
}

function updateFavBtnState() {
  if (!currentDetailFeature) return;
  const id = currentDetailFeature.properties.station_id;
  const isFav = state.favorites.has(id);

  if (isFav) {
    els.detail.favBtn.classList.add("active");
    els.detail.favBtn
      .querySelector("polygon")
      .setAttribute("fill", "currentColor");
  } else {
    els.detail.favBtn.classList.remove("active");
    els.detail.favBtn.querySelector("polygon").setAttribute("fill", "none");
  }
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
  const stationId = String(props?.station_id || "").trim();
  if (!stationId) {
    return "./";
  }
  return `./station/${encodeURIComponent(stationId)}.html`;
}

function getRequestedStationId() {
  const params = new URLSearchParams(window.location.search);
  return (params.get("station") || "").trim();
}

function updateRequestedStationId(stationId) {
  const url = new URL(window.location.href);
  if (stationId) {
    url.searchParams.set("station", stationId);
  } else {
    url.searchParams.delete("station");
  }
  const next = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState(window.history.state, "", next);
}

function findFeatureByStationId(stationId) {
  return state.features.find((feature) => feature.properties.station_id === stationId) || null;
}

function syncDetailModalWithUrl() {
  const stationId = getRequestedStationId();
  if (!stationId) {
    if (!els.modals.detail.classList.contains("hidden")) {
      closeModal("detail", { syncUrl: false });
    }
    return;
  }
  if (!state.features.length) {
    return;
  }

  const feature = findFeatureByStationId(stationId);
  if (!feature) {
    console.warn("Unknown station requested", stationId);
    return;
  }

  if (currentDetailFeature?.properties?.station_id === stationId) {
    return;
  }

  openDetail(feature, { syncUrl: false });
}

function getDistance(feature) {
  if (!state.userPos) return Infinity;
  const [lon, lat] = feature.geometry.coordinates;
  // Haversine approx is enough for sorting
  const R = 6371e3; // meters
  const φ1 = (state.userPos.lat * Math.PI) / 180;
  const φ2 = (lat * Math.PI) / 180;
  const Δφ = ((lat - state.userPos.lat) * Math.PI) / 180;
  const Δλ = ((lon - state.userPos.lon) * Math.PI) / 180;

  const a =
    Math.sin(Δφ / 2) * Math.sin(Δφ / 2) +
    Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c; // meters
}

function getDistanceFormatted(feature) {
  const d = getDistance(feature);
  if (d === Infinity) return "";
  if (d > 1000) return (d / 1000).toFixed(1) + " km";
  return Math.round(d) + " m";
}

async function queueStartupLocationRequest() {
  if (state.startupLocationRequested || state.userPos || !navigator.geolocation) {
    return;
  }
  if (state.location.permissionState !== LOCATION_PERMISSION_GRANTED) {
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
      window.setTimeout(() => requestUserLocation(true), 0);
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

async function requestUserLocation(silent = false) {
  const silentMode = silent === true;
  if (!navigator.geolocation) {
    updateLocationState({
      permissionState: LOCATION_PERMISSION_UNSUPPORTED,
      requestState: LOCATION_REQUEST_ERROR,
      errorCode: "unsupported",
    });
    if (!silentMode && els.views.map.classList.contains("active")) {
      switchView("view-list");
    }
    return;
  }

  updateLocationState({
    requestState: LOCATION_REQUEST_PENDING,
    errorCode: "",
  });

  try {
    const position = await requestBrowserLocation(navigator.geolocation, {
      enableHighAccuracy: true,
      timeout: 5000,
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
    applyFilters();

    if (!silentMode && state.views.map) {
      state.views.map.flyTo([state.userPos.lat, state.userPos.lon], 13);
    }
  } catch (err) {
    console.warn("Location error", err);
    updateLocationState({
      permissionState: err.code === LOCATION_ERROR_PERMISSION_DENIED
        ? LOCATION_PERMISSION_DENIED
        : state.location.permissionState,
      requestState: LOCATION_REQUEST_ERROR,
      errorCode: err.code || "unknown",
    });
    if (!silentMode && els.views.map.classList.contains("active")) {
      switchView("view-list");
    }
  }
}

/* --- LOCALSTORAGE --- */
function loadFavorites() {
  try {
    const raw = localStorage.getItem("woladen_favs");
    if (raw) {
      const arr = JSON.parse(raw);
      state.favorites = new Set(arr);
    }
  } catch (e) {
    console.error("Error loading favorites", e);
  }
}

function saveFavorites() {
  try {
    const arr = Array.from(state.favorites);
    localStorage.setItem("woladen_favs", JSON.stringify(arr));
  } catch (e) {
    console.error("Error saving favorites", e);
  }
}

/* --- MODAL UTILS --- */
function openModal(name) {
  const m = els.modals[name];
  if (m) m.classList.remove("hidden");
}

function closeModal(name, options = {}) {
  const syncUrl = options.syncUrl !== false;
  const m = els.modals[name];
  if (m) m.classList.add("hidden");
  if (name === "detail") {
    currentDetailFeature = null;
    if (syncUrl) {
      updateRequestedStationId("");
    }
  }
}

/* --- BOOTSTRAP --- */
init();
