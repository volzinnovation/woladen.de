import {
  buildLiveStationDetailPath,
  normalizeLiveApiBaseUrl,
  normalizeLiveStationId,
  resolveLiveApiBaseUrl,
} from "./live-api.mjs?v=20260806-station-query1";

const AMENITY_LABELS = {
  bakery: "Bakery",
  cafe: "Café",
  convenience: "Convenience store",
  fast_food: "Fast food",
  hotel: "Hotel",
  ice_cream: "Ice cream",
  museum: "Museum",
  park: "Park",
  pharmacy: "Pharmacy",
  playground: "Playground",
  restaurant: "Restaurant",
  supermarket: "Supermarket",
  toilets: "Toilets",
};

const DEFAULT_API_BASE_URL = "https://live-eu.woladen.de";

function firstText(...values) {
  for (const value of values) {
    const text = String(value ?? "").trim();
    if (text) return text;
  }
  return "";
}

function finiteNumber(value, fallback = Number.NaN) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function integer(value, fallback = 0) {
  const number = finiteNumber(value, fallback);
  return Math.max(0, Math.round(number));
}

function element(id) {
  return document.getElementById(id);
}

function setText(id, value) {
  const target = element(id);
  if (target) target.textContent = String(value || "");
  return target;
}

function stationPageApiBaseUrl() {
  const configured = typeof window.WOLADEN_LIVE_API_BASE_URL === "string"
    ? window.WOLADEN_LIVE_API_BASE_URL
    : "";
  const resolved = resolveLiveApiBaseUrl({
    configuredValue: configured,
    locationHref: window.location.href,
    locationHostname: window.location.hostname,
  });
  return normalizeLiveApiBaseUrl(resolved) || DEFAULT_API_BASE_URL;
}

export function catalogStationDetailUrl(baseUrl, stationId) {
  const base = normalizeLiveApiBaseUrl(baseUrl) || DEFAULT_API_BASE_URL;
  const normalized = normalizeLiveStationId(stationId);
  return normalized
    ? `${base}/v1/catalog/stations/${encodeURIComponent(normalized)}`
    : "";
}

export function liveStationDetailUrl(baseUrl, stationId) {
  const base = normalizeLiveApiBaseUrl(baseUrl) || DEFAULT_API_BASE_URL;
  const path = buildLiveStationDetailPath(stationId);
  return path ? `${base}${path}` : "";
}

function catalogLookupIds(stationId) {
  const normalized = normalizeLiveStationId(stationId);
  const ids = [normalized];
  const namespacedMatch = normalized.match(/^DE:([0-9a-f]{16})$/i);
  if (namespacedMatch) ids.push(namespacedMatch[1].toLowerCase());
  return Array.from(new Set(ids.filter(Boolean)));
}

async function fetchCatalogStation(stationId) {
  let lastError = null;
  for (const lookupId of catalogLookupIds(stationId)) {
    try {
      const response = await fetch(
        catalogStationDetailUrl(stationPageApiBaseUrl(), lookupId),
        {
          headers: { Accept: "application/json" },
          cache: "no-store",
        },
      );
      if (!response.ok) {
        throw new Error(`station_api_${response.status}`);
      }
      const payload = await response.json();
      if (!payload || typeof payload !== "object" || !payload.station) {
        throw new Error("station_api_invalid_payload");
      }
      return payload;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError || new Error("station_api_failed");
}

async function fetchLiveStation(stationId) {
  const response = await fetch(
    liveStationDetailUrl(stationPageApiBaseUrl(), stationId),
    {
      headers: { Accept: "application/json" },
      cache: "no-store",
    },
  );
  if (!response.ok) {
    throw new Error(`live_station_api_${response.status}`);
  }
  const payload = await response.json();
  if (!payload || typeof payload !== "object" || !payload.station) {
    throw new Error("live_station_api_invalid_payload");
  }
  return payload;
}

export function combineStationDetails(catalogPayload, livePayload) {
  const catalog = catalogPayload && typeof catalogPayload === "object"
    ? catalogPayload
    : {};
  const live = livePayload && typeof livePayload === "object" ? livePayload : null;
  const liveChargers = Array.isArray(live?.evses) ? live.evses : [];

  return {
    ...catalog,
    // The catalogue identifies and describes the station. The live endpoint is
    // authoritative for availability, operational status and current tariffs.
    // Do not infer a point-by-point match where the catalogue has no EVSE ID.
    chargers: liveChargers.length > 0 ? liveChargers : catalog.chargers,
    live_station: live?.station || null,
  };
}

async function fetchStation(stationId) {
  const catalogPayload = await fetchCatalogStation(stationId);
  try {
    return combineStationDetails(catalogPayload, await fetchLiveStation(stationId));
  } catch (error) {
    console.warn("Failed to load live station details", error);
    return combineStationDetails(catalogPayload, null);
  }
}

function formatPower(value) {
  const number = finiteNumber(value, 0);
  return `${new Intl.NumberFormat("en", { maximumFractionDigits: 1 }).format(number)} kW`;
}

function formatDistance(value) {
  const number = finiteNumber(value);
  if (!Number.isFinite(number)) return "";
  return `${Math.round(number)} m away`;
}

function addressText(station) {
  const locality = [
    firstText(station.postal_code, station.postcode),
    firstText(station.city),
  ].filter(Boolean).join(" ");
  return [firstText(station.address, station.station_name), locality, firstText(station.country_code)]
    .filter(Boolean)
    .join(" · ");
}

function createChip(icon, value) {
  const chip = document.createElement("span");
  chip.className = "detail-highlight-chip";
  const iconNode = document.createElement("span");
  iconNode.className = "icon";
  iconNode.setAttribute("aria-hidden", "true");
  iconNode.textContent = icon;
  chip.append(iconNode, document.createTextNode(value));
  return chip;
}

function appendDetailRow(container, label, value) {
  const text = firstText(value);
  if (!text) return;
  const row = document.createElement("div");
  row.className = "detail-info-row";
  const labelNode = document.createElement("span");
  labelNode.className = "detail-info-label";
  labelNode.textContent = label;
  const valueNode = document.createElement("span");
  valueNode.className = "detail-info-value";
  valueNode.textContent = text;
  row.append(labelNode, valueNode);
  container.append(row);
}

function amenityLabel(category) {
  const key = firstText(category).toLowerCase();
  return AMENITY_LABELS[key] || key.replaceAll("_", " ") || "Nearby place";
}

function renderAmenities(payload, station) {
  const container = element("station-amenities");
  container.replaceChildren();
  const examples = Array.isArray(payload.amenities?.amenity_examples)
    ? payload.amenities.amenity_examples
    : Array.isArray(station.amenity_examples)
      ? station.amenity_examples
      : [];
  if (examples.length === 0) {
    const empty = document.createElement("p");
    empty.className = "detail-subnote";
    empty.textContent = "No nearby places are listed for this station yet.";
    container.append(empty);
    return;
  }
  for (const example of examples.slice(0, 12)) {
    if (!example || typeof example !== "object") continue;
    const item = document.createElement("div");
    item.className = "amenity-item";
    const copy = document.createElement("div");
    copy.className = "amenity-copy";
    const name = document.createElement("strong");
    name.textContent = firstText(example.name, amenityLabel(example.category));
    const meta = document.createElement("span");
    meta.className = "amenity-detail-meta unknown";
    meta.textContent = [amenityLabel(example.category), formatDistance(example.distance_m)]
      .filter(Boolean)
      .join(" · ");
    copy.append(name, meta);
    item.append(copy);
    container.append(item);
  }
}

function renderLivePoints(payload) {
  const section = element("station-live-section");
  const container = element("station-live");
  const chargers = Array.isArray(payload.chargers) ? payload.chargers : [];
  container.replaceChildren();
  if (chargers.length === 0) {
    section.hidden = true;
    return;
  }
  section.hidden = false;
  setText("station-live-count", `${chargers.length} listed`);
  for (const [index, charger] of chargers.slice(0, 40).entries()) {
    if (!charger || typeof charger !== "object") continue;
    const row = document.createElement("div");
    row.className = "live-evse-row";
    const head = document.createElement("div");
    head.className = "live-evse-row-head";
    const title = document.createElement("strong");
    title.className = "live-evse-title";
    title.textContent = firstText(charger.evse_id, charger.provider_evse_id, `Point ${index + 1}`);
    const status = document.createElement("span");
    status.className = "live-status-pill";
    status.textContent = firstText(charger.availability_status, charger.operational_status, "Status unknown");
    head.append(title, status);
    const meta = document.createElement("div");
    meta.className = "live-evse-row-meta";
    meta.textContent = [
      firstText(charger.connector_type),
      formatPower(charger.max_power_kw || charger.power_kw),
      firstText(charger.price_display),
    ].filter(Boolean).join(" · ");
    row.append(head, meta);
    container.append(row);
  }
}

function updateMetadata(station, stationId) {
  const operator = firstText(station.station_name, station.operator_name, station.operator, "Charging station");
  const city = firstText(station.city, station.country_code);
  const title = `${operator}${city ? ` in ${city}` : ""} | Charging station | woladen.de`;
  const description = `${operator}${city ? ` in ${city}` : ""}. ${formatPower(station.max_power_kw)} charging station details, nearby places and navigation.`;
  document.title = title;
  const canonical = document.querySelector('link[rel="canonical"]');
  if (canonical) canonical.href = `${window.location.origin}${window.location.pathname}?station=${encodeURIComponent(stationId)}`;
  for (const selector of ['meta[name="description"]', 'meta[property="og:description"]']) {
    const meta = document.querySelector(selector);
    if (meta) meta.content = description;
  }
  const ogTitle = document.querySelector('meta[property="og:title"]');
  if (ogTitle) ogTitle.content = title;
  const ogUrl = document.querySelector('meta[property="og:url"]');
  if (ogUrl) ogUrl.content = window.location.href;
  const schema = element("station-page-schema");
  if (schema) {
    schema.textContent = JSON.stringify({
      "@context": "https://schema.org",
      "@type": "ElectricVehicleChargingStation",
      name: operator,
      url: window.location.href,
      address: addressText(station),
      latitude: finiteNumber(station.latitude),
      longitude: finiteNumber(station.longitude),
    });
  }
}

function renderStation(payload, stationId) {
  const station = payload.station;
  const operator = firstText(station.station_name, station.operator_name, station.operator, "Charging station");
  const stationName = firstText(station.station_name);
  const latitude = finiteNumber(station.latitude);
  const longitude = finiteNumber(station.longitude);
  const coordinates = Number.isFinite(latitude) && Number.isFinite(longitude)
    ? `${latitude.toFixed(5)}, ${longitude.toFixed(5)}`
    : "Location available in the web app";
  const appUrl = `/?station=${encodeURIComponent(stationId)}`;
  const googleUrl = Number.isFinite(latitude) && Number.isFinite(longitude)
    ? `https://www.google.com/maps/dir/?api=1&destination=${latitude},${longitude}`
    : "#";
  const appleUrl = Number.isFinite(latitude) && Number.isFinite(longitude)
    ? `https://maps.apple.com/?ll=${latitude},${longitude}`
    : "#";

  setText("station-kicker", `${firstText(station.country_code, "EU")} charging station`);
  setText("station-title", operator);
  setText("station-name", stationName && stationName !== operator ? stationName : "");
  setText("station-address", addressText(station));
  setText("station-map-caption", coordinates);
  const highlights = element("station-highlights");
  highlights.replaceChildren();
  highlights.append(createChip("⚡", `${formatPower(station.max_power_kw)} max`));
  highlights.append(createChip("🔌", `${integer(station.charger_count || station.charging_points_count, 1)} charging points`));
  if (firstText(station.price_display)) highlights.append(createChip("€", station.price_display));
  if (firstText(station.opening_hours)) highlights.append(createChip("🕒", station.opening_hours));
  if ((station.green_energy ?? station.green_energy_display) === true) {
    highlights.append(createChip("♻", "Green energy"));
  }

  const appLink = element("station-app-link");
  appLink.href = appUrl;
  const googleLink = element("station-google-link");
  googleLink.href = googleUrl;
  googleLink.hidden = googleUrl === "#";
  const appleLink = element("station-apple-link");
  appleLink.href = appleUrl;
  appleLink.hidden = appleUrl === "#";

  const details = element("station-details");
  details.replaceChildren();
  appendDetailRow(details, "Station", stationName);
  appendDetailRow(details, "Operator", operator);
  appendDetailRow(details, "Connectors", station.connector_types);
  appendDetailRow(details, "Sockets", station.connector_count || station.charger_count);
  appendDetailRow(details, "Payment", station.payment_methods);
  appendDetailRow(details, "Access", station.auth_methods);
  appendDetailRow(details, "Opening hours", station.opening_hours);
  appendDetailRow(details, "Helpdesk", station.helpdesk_phone);

  const source = firstText(station.source_uid, station.provider_uid);
  const updated = firstText(station.detail_last_updated);
  const liveUpdated = firstText(payload.live_station?.source_observed_at);
  const sourceNode = element("station-source");
  sourceNode.textContent = [
    source && `Details via ${source}`,
    updated && `updated ${updated}`,
    liveUpdated && `Live status: ${liveUpdated}`,
  ]
    .filter(Boolean)
    .join(" · ");
  sourceNode.hidden = !sourceNode.textContent;

  renderAmenities(payload, station);
  renderLivePoints(payload);
  updateMetadata(station, stationId);
  element("station-loading").hidden = true;
  element("station-error").hidden = true;
  element("station-content").hidden = false;
}

function showError(message) {
  element("station-loading").hidden = true;
  element("station-content").hidden = true;
  element("station-error").hidden = false;
  setText("station-error-copy", message);
}

async function start() {
  const stationId = new URL(window.location.href).searchParams.get("station")?.trim() || "";
  if (!stationId) {
    showError("This page needs a station query parameter, for example ?station=DE%3A47d719c1b62c750.");
    return;
  }
  try {
    const payload = await fetchStation(stationId);
    renderStation(payload, normalizeLiveStationId(stationId) || stationId);
  } catch (error) {
    console.warn("Failed to load station details", error);
    showError("The station details could not be loaded right now. Please open the web app and try again.");
  }
}

if (typeof document !== "undefined") {
  void start();
}
