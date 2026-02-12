/**
 * woladen.de - Modern Frontend Logic
 */

/* --- CONFIGURATION & CONSTANTS --- */
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

/* --- STATE --- */
const state = {
  features: [], // All charger features
  filtered: [], // Currently filtered features
  favorites: new Set(), // Set of station_ids
  userPos: null, // { lat, lon }
  filters: {
    operator: "",
    minPower: 50,
    amenities: new Set(),
  },
  views: {
    map: null, // Leaflet map instance
    detailMap: null, // Mini map in detail view
    layers: {
      chargers: null,
      user: null,
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
    power: document.getElementById("filter-power"),
    powerVal: document.getElementById("filter-power-val"),
    amenities: document.getElementById("filter-amenities"),
    applyBtn: document.getElementById("btn-apply-filter"),
    listFilterBtn: document.getElementById("btn-list-filter"),
  },
  detail: {
    title: document.getElementById("detail-title"),
    address: document.getElementById("detail-address"),
    power: document.getElementById("detail-power"),
    amenityCount: document.getElementById("detail-amenity-count"),
    amenityList: document.getElementById("detail-amenities-list"),
    favBtn: document.getElementById("btn-toggle-fav"),
    googleBtn: document.getElementById("btn-nav-google"),
    appleBtn: document.getElementById("btn-nav-apple"),
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

  // Event Listeners
  els.buttons.locate.addEventListener("click", requestUserLocation);
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
    const [geoRes, opRes] = await Promise.all([
      fetch("./data/chargers_fast.geojson"),
      fetch("./data/operators.json"),
    ]);

    if (!geoRes.ok || !opRes.ok) throw new Error("Network response was not ok");

    const geoData = await geoRes.json();
    const opData = await opRes.json();

    state.features = geoData.features || [];

    // Sort features initially just to have a defined order, strictly standard
    // Real sorting happens when we have location

    populateOperators(opData);
    setAppMeta(geoData);

    applyFilters(); // Initial render

    // Try getting location silently
    requestUserLocation(true);
  } catch (err) {
    console.error("Failed to load data", err);
    els.lists.chargers.innerHTML = `<div class="empty-state">Fehler beim Laden der Daten.<br>${err.message}</div>`;
  }
}

function setAppMeta(geoData) {
  if (els.meta && geoData.generated_at) {
    const date = new Date(geoData.generated_at).toLocaleDateString("de-DE", {
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

  // Power
  els.filter.power.addEventListener("input", (e) => {
    state.filters.minPower = Number(e.target.value);
    els.filter.powerVal.textContent = state.filters.minPower;
    updateFilters();
  });

  // Amenities
  const amenityKeys = Object.keys(AMENITY_MAPPING);
  amenityKeys.forEach((key) => {
    const config = AMENITY_MAPPING[key];
    const path = getAmenityIconPath(key);

    // Only show if we have an icon? Or show all with text?
    // Let's show all, using text fallback if no icon

    const div = document.createElement("div");
    div.className = "amenity-toggle";
    div.dataset.key = key;

    if (path) {
      div.innerHTML = `<img src="${path}" alt="${config.label}" loading="lazy"><span class="amenity-name">${config.label}</span>`;
    } else {
      // Placeholder icon or just text
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

  const total = state.filtered.length;
  // Update UI hint if needed
  const filterCount =
    (state.filters.operator ? 1 : 0) +
    (state.filters.minPower > 50 ? 1 : 0) +
    state.filters.amenities.size;

  els.filter.label.textContent =
    filterCount > 0 ? `Filter (${filterCount})` : "Alle Filter";
}

function applyFilters() {
  state.filtered = state.features.filter((f) => {
    const p = f.properties;

    // Operator
    if (state.filters.operator && p.operator !== state.filters.operator)
      return false;

    // Power
    if ((p.max_power_kw || 0) < state.filters.minPower) return false;

    // Amenities
    if (state.filters.amenities.size > 0) {
      for (const key of state.filters.amenities) {
        if (!p[key] || p[key] <= 0) return false;
      }
    }

    return true;
  });

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
}

function createStationCard(feature) {
  const p = feature.properties;
  const div = document.createElement("div");
  div.className = "station-card";

  const distance = getDistanceFormatted(feature);

  // Top Amenities (max 3 badges)
  const badges = Object.keys(AMENITY_MAPPING)
    .filter((k) => p[k] > 0)
    .sort((a, b) => p[b] - p[a]) // Most frequent first
    .slice(0, 3)
    .map((k) => `<span class="badge">${AMENITY_MAPPING[k].label}</span>`)
    .join("");

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
      ${escapeHtml(p.address || "")}, ${escapeHtml(p.city || "")}<br>
      ${Math.round(p.max_power_kw)} kW • ${p.amenities_total} Annehmlichkeit(en)
    </div>
    <div class="card-badges">
      ${badges}
    </div>
  `;

  div.addEventListener("click", () => openDetail(feature));
  return div;
}

/* --- DETAIL MODAL --- */
let currentDetailFeature = null;

function openDetail(feature) {
  currentDetailFeature = feature;
  const p = feature.properties;

  els.detail.title.textContent = p.operator || "Unbekannt";
  els.detail.address.textContent = `${p.address || ""}, ${p.postcode || ""} ${p.city || ""}`;
  els.detail.power.textContent = `${Math.round(p.max_power_kw || 0)} kW`;
  els.detail.amenityCount.textContent = `${p.amenities_total || 0} Amenities`;

  // Favorite Button State
  updateFavBtnState();

  // Navigation Links
  const [lon, lat] = feature.geometry.coordinates;
  els.detail.googleBtn.href = `https://www.google.com/maps/dir/?api=1&destination=${lat},${lon}`;
  els.detail.appleBtn.href = `http://maps.apple.com/?daddr=${lat},${lon}`;

  // Mini Map
  state.views.detailMap.setView([lat, lon], 16);
  // Clear old markers from detail map? Not strictly needed if we just pan,
  // but better to add a marker for the station
  if (state.views.detailMap.stationMarker)
    state.views.detailMap.removeLayer(state.views.detailMap.stationMarker);

  state.views.detailMap.stationMarker = L.circleMarker([lat, lon], {
    color: "#fff",
    fillColor: "#0f766e",
    fillOpacity: 1,
    radius: 8,
  }).addTo(state.views.detailMap);

  // Force map resize after modal transition
  setTimeout(() => state.views.detailMap.invalidateSize(), 300);

  // Amenity List
  renderDetailAmenities(p);

  openModal("detail");
}

function renderDetailAmenities(props) {
  els.detail.amenityList.innerHTML = "";
  const examples = props.amenity_examples || [];

  if (examples.length === 0) {
    els.detail.amenityList.innerHTML = `<div style="color:#888">Keine Details verfügbar.</div>`;
    return;
  }

  examples.slice(0, 15).forEach((item) => {
    // item: { category, name, opening_hours, distance_m }
    const catConfig = AMENITY_MAPPING[`amenity_${item.category}`] || {
      label: item.category,
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

function requestUserLocation(silent = false) {
  if (!navigator.geolocation) {
    if (!silent) alert("Geolocation nicht unterstützt.");
    return;
  }

  navigator.geolocation.getCurrentPosition(
    (pos) => {
      state.userPos = {
        lat: pos.coords.latitude,
        lon: pos.coords.longitude,
      };
      updateUserMarker();
      // Use Leaflet distanceTo for better accuracy if desired, but custom calc is fine
      // Sort list if needed
      applyFilters();

      // Fly to user on map if not silent
      if (!silent && state.views.map) {
        state.views.map.flyTo([state.userPos.lat, state.userPos.lon], 13);
      }
    },
    (err) => {
      console.warn("Location error", err);
      if (!silent) alert("Standort konnte nicht ermittelt werden.");
    },
    { enableHighAccuracy: true, timeout: 5000 },
  );
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

function closeModal(name) {
  const m = els.modals[name];
  if (m) m.classList.add("hidden");
}

/* --- BOOTSTRAP --- */
init();
