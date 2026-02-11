const AMENITY_LABELS = {
  amenity_restaurant: "Restaurant",
  amenity_cafe: "Cafe",
  amenity_fast_food: "Fast Food",
  amenity_toilets: "Toiletten",
  amenity_supermarket: "Supermarkt",
  amenity_bakery: "Baeckerei",
  amenity_convenience: "Kiosk",
  amenity_pharmacy: "Apotheke",
  amenity_hotel: "Hotel",
  amenity_museum: "Museum",
  amenity_playground: "Spielplatz",
  amenity_park: "Park",
  amenity_ice_cream: "Eis",
};

const map = L.map("map", { zoomControl: true }).setView([51.2, 10.4], 6);

L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
}).addTo(map);

const layers = {
  chargers: L.layerGroup().addTo(map),
  route: L.layerGroup().addTo(map),
  user: L.layerGroup().addTo(map),
};

const state = {
  features: [],
  filtered: [],
  selectedFeature: null,
  selectedOperator: "",
  selectedAmenities: new Set(),
  minPowerKw: 50,
  userPosition: null,
};

const operatorSelect = document.getElementById("operatorSelect");
const powerRange = document.getElementById("powerRange");
const powerRangeValue = document.getElementById("powerRangeValue");
const amenityOptions = document.getElementById("amenityOptions");
const stats = document.getElementById("stats");
const buildMeta = document.getElementById("buildMeta");
const locateBtn = document.getElementById("locateBtn");
const routeBtn = document.getElementById("routeBtn");

function markerColor(feature) {
  const total = feature.properties.amenities_total || 0;
  if (total >= 6) return "#0b9d7a";
  if (total >= 3) return "#0f7dbe";
  if (total >= 1) return "#d86f22";
  return "#7f8b87";
}

function createMarker(feature) {
  const [lon, lat] = feature.geometry.coordinates;
  const marker = L.circleMarker([lat, lon], {
    radius: 7,
    fillOpacity: 0.86,
    color: "#1e2d28",
    weight: 1,
    fillColor: markerColor(feature),
  });

  const p = feature.properties;
  const amenitySummary = Object.keys(AMENITY_LABELS)
    .filter((key) => (p[key] || 0) > 0)
    .map((key) => `${AMENITY_LABELS[key]}: ${p[key]}`)
    .slice(0, 6)
    .join("<br>");

  const mapsUrl = `https://www.google.com/maps/dir/?api=1&destination=${lat},${lon}`;
  const popupHtml = `
    <strong>${escapeHtml(p.operator || "Unbekannt")}</strong><br>
    ${escapeHtml(p.address || "")}, ${escapeHtml(p.postcode || "")} ${escapeHtml(p.city || "")}<br>
    Leistung: ${Math.round(Number(p.max_power_kw || 0))} kW<br>
    Amenities gesamt: ${p.amenities_total || 0}<br>
    ${amenitySummary || "Keine Details"}<br>
    <a class="popup-link" href="${mapsUrl}" target="_blank" rel="noreferrer">Externe Navigation</a>
  `;

  marker.bindPopup(popupHtml);
  marker.on("click", () => {
    state.selectedFeature = feature;
    routeBtn.disabled = !state.userPosition;
  });

  return marker;
}

function renderOperatorOptions(operators) {
  operatorSelect.innerHTML = '<option value="">Alle Betreiber</option>';
  operators.forEach((operator) => {
    const option = document.createElement("option");
    option.value = operator;
    option.textContent = operator;
    operatorSelect.appendChild(option);
  });
}

function parseOperatorNames(payload) {
  if (!payload || !Array.isArray(payload.operators)) {
    return [];
  }

  const minStations = Number(payload.min_stations || 10);
  const names = payload.operators
    .filter((item) => item && typeof item.name === "string")
    .filter((item) => Number(item.stations || 0) >= minStations)
    .map((item) => item.name.trim())
    .filter((name) => name.length > 0);

  return Array.from(new Set(names)).sort((a, b) => a.localeCompare(b));
}

function renderAmenityFilters() {
  amenityOptions.innerHTML = "";
  Object.entries(AMENITY_LABELS).forEach(([key, label]) => {
    const wrapper = document.createElement("label");
    const input = document.createElement("input");
    input.type = "checkbox";
    input.value = key;
    input.addEventListener("change", () => {
      if (input.checked) {
        state.selectedAmenities.add(key);
      } else {
        state.selectedAmenities.delete(key);
      }
      applyFilters();
    });

    const span = document.createElement("span");
    span.textContent = label;
    wrapper.append(input, span);
    amenityOptions.appendChild(wrapper);
  });
}

function applyFilters() {
  state.filtered = state.features.filter((feature) => {
    const p = feature.properties;
    if (state.selectedOperator && p.operator !== state.selectedOperator) {
      return false;
    }
    if (Number(p.max_power_kw || 0) < state.minPowerKw) {
      return false;
    }

    if (state.selectedAmenities.size > 0) {
      for (const key of state.selectedAmenities) {
        if (Number(p[key] || 0) <= 0) {
          return false;
        }
      }
    }

    return true;
  });

  layers.chargers.clearLayers();
  state.filtered.forEach((feature) => {
    createMarker(feature).addTo(layers.chargers);
  });

  const withAmenities = state.filtered.filter((f) => (f.properties.amenities_total || 0) > 0).length;
  stats.textContent = `${state.filtered.length} Ladepunkte im Filter, davon ${withAmenities} mit >=1 Amenity.`;

  if (state.filtered.length > 0) {
    const group = L.featureGroup(layers.chargers.getLayers());
    map.fitBounds(group.getBounds().pad(0.15), { animate: false, maxZoom: 12 });
  }

  if (
    state.selectedFeature &&
    !state.filtered.some((feature) => feature.properties.station_id === state.selectedFeature.properties.station_id)
  ) {
    state.selectedFeature = null;
    routeBtn.disabled = true;
  }
}

function setBuildMeta(geojson) {
  const generated = geojson.generated_at
    ? new Date(geojson.generated_at).toISOString().replace("T", " ").replace(".000Z", " UTC")
    : "unbekannt";

  const source = geojson.source?.source_url || "unbekannt";
  buildMeta.textContent = `Stand: ${generated} | Quelle: ${source}`;
}

async function loadData() {
  try {
    const [geoResponse, operatorsResponse] = await Promise.all([
      fetch("./data/chargers_fast.geojson", { cache: "no-store" }),
      fetch("./data/operators.json", { cache: "no-store" }),
    ]);

    if (!geoResponse.ok) {
      throw new Error(`chargers_fast.geojson HTTP ${geoResponse.status}`);
    }
    if (!operatorsResponse.ok) {
      throw new Error(`operators.json HTTP ${operatorsResponse.status}`);
    }

    const [geojson, operatorsPayload] = await Promise.all([
      geoResponse.json(),
      operatorsResponse.json(),
    ]);

    state.features = geojson.features || [];
    const operators = parseOperatorNames(operatorsPayload);

    if (state.features.length === 0) {
      stats.textContent = "Keine Daten gefunden. Fuehre die Pipeline aus.";
      return;
    }

    renderOperatorOptions(operators);
    renderAmenityFilters();
    setBuildMeta(geojson);
    applyFilters();
  } catch (error) {
    stats.textContent =
      "Daten konnten nicht geladen werden. Erwartet werden ./data/chargers_fast.geojson und ./data/operators.json";
    buildMeta.textContent = String(error);
  }
}

function setUserPosition(lat, lon) {
  state.userPosition = { lat, lon };
  layers.user.clearLayers();

  L.circleMarker([lat, lon], {
    radius: 8,
    color: "#1a2856",
    fillColor: "#2563eb",
    fillOpacity: 0.9,
    weight: 1,
  }).addTo(layers.user).bindPopup("Dein Standort");

  map.flyTo([lat, lon], 11, { duration: 0.9 });
  routeBtn.disabled = !state.selectedFeature;
}

function requestGeolocation() {
  if (!navigator.geolocation) {
    stats.textContent = "Geolocation wird in diesem Browser nicht unterstuetzt.";
    return;
  }

  locateBtn.disabled = true;
  navigator.geolocation.getCurrentPosition(
    (position) => {
      locateBtn.disabled = false;
      setUserPosition(position.coords.latitude, position.coords.longitude);
    },
    (err) => {
      locateBtn.disabled = false;
      stats.textContent = `Standort konnte nicht gelesen werden: ${err.message}`;
    },
    { enableHighAccuracy: true, timeout: 10000, maximumAge: 120000 }
  );
}

async function routeToSelected() {
  if (!state.userPosition || !state.selectedFeature) {
    return;
  }

  const [destLon, destLat] = state.selectedFeature.geometry.coordinates;
  const start = `${state.userPosition.lon},${state.userPosition.lat}`;
  const end = `${destLon},${destLat}`;

  const url = `https://router.project-osrm.org/route/v1/driving/${start};${end}?overview=full&geometries=geojson`;

  routeBtn.disabled = true;
  try {
    const response = await fetch(url);
    const payload = await response.json();
    if (!payload.routes || payload.routes.length === 0) {
      throw new Error("Keine Route gefunden");
    }

    const route = payload.routes[0].geometry.coordinates.map(([lon, lat]) => [lat, lon]);
    layers.route.clearLayers();
    L.polyline(route, { color: "#d86f22", weight: 4, opacity: 0.85 }).addTo(layers.route);

    const routeBounds = L.latLngBounds(route);
    map.fitBounds(routeBounds.pad(0.2), { animate: false });
  } catch (error) {
    stats.textContent = `Route konnte nicht berechnet werden: ${error}`;
  } finally {
    routeBtn.disabled = !state.selectedFeature;
  }
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

operatorSelect.addEventListener("change", () => {
  state.selectedOperator = operatorSelect.value;
  applyFilters();
});

powerRange.addEventListener("input", () => {
  state.minPowerKw = Number(powerRange.value);
  powerRangeValue.textContent = `>= ${state.minPowerKw}`;
  applyFilters();
});

locateBtn.addEventListener("click", requestGeolocation);
routeBtn.addEventListener("click", routeToSelected);

loadData();
