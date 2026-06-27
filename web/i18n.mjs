const DEFAULT_LANGUAGE = "en";
const STORAGE_KEY = "woladen_language_v1";

export const SUPPORTED_LANGUAGES = [
  "en",
  "de",
  "fr",
  "nl",
  "da",
  "fi",
  "sv",
  "el",
  "lv",
  "lt",
  "lb",
  "mt",
  "nb",
  "nn",
  "pl",
  "pt",
  "es",
  "cs",
  "hu",
  "sl",
  "it",
  "rm",
  "tr",
];

const LANGUAGE_ALIASES = new Map([
  ["no", "nb"],
  ["no-no", "nb"],
  ["nb-no", "nb"],
  ["nn-no", "nn"],
  ["el-cy", "el"],
  ["tr-cy", "tr"],
  ["en-mt", "en"],
]);

const OG_LOCALES = {
  en: "en_US",
  de: "de_DE",
  fr: "fr_FR",
  nl: "nl_NL",
  da: "da_DK",
  fi: "fi_FI",
  sv: "sv_SE",
  el: "el_GR",
  lv: "lv_LV",
  lt: "lt_LT",
  lb: "lb_LU",
  mt: "mt_MT",
  nb: "nb_NO",
  nn: "nn_NO",
  pl: "pl_PL",
  pt: "pt_PT",
  es: "es_ES",
  cs: "cs_CZ",
  hu: "hu_HU",
  sl: "sl_SI",
  it: "it_IT",
  rm: "rm_CH",
  tr: "tr_TR",
};

const FALLBACK_BUNDLE = {
  languageName: "English",
  meta: {
    title: "woladen - Smart EV Stops in Europe",
    description: "Find available chargers near great bakeries, restaurants, shops, playgrounds and cafés. Because charging time should be time well spent.",
    ogTitle: "woladen - Smart EV Stops in Europe",
    ogDescription: "Find available chargers near great bakeries, restaurants, shops, playgrounds and cafés. Because charging time should be time well spent.",
    socialAlt: "woladen preview with a Europe charger map and the slogan: The human side of charging.",
  },
  seo: {
    brandName: "woladen",
    seoName: "woladen - Smart EV Stops in Europe",
    primaryTagline: "Plugs for Cars. Perks for People.",
    humanHook: "The human side of charging.",
    timeLine: "Because charging time is your time.",
    productMessage: "Find available chargers near great bakeries, restaurants, shops, playgrounds and cafés. Because charging time should be time well spent.",
  },
  language: {
    label: "Language",
  },
  nav: {
    label: "Main navigation",
    list: "List",
    map: "Map",
    route: "Route",
    favorites: "Favorites",
    stats: "Stats",
    info: "Info",
  },
  search: {
    label: "Search place or address",
    placeholder: "Search place or address",
    noResults: "No suggestions found.",
    minChars: "Enter at least two characters.",
    searching: "Searching place...",
    unavailable: "Place search is currently unavailable.",
    notConfigured: "Place search is not configured.",
  },
  aria: {
    map: "Map with charging stations. Use the list for a more accessible results view.",
    locate: "Use my location",
    filterOpen: "Open filters",
    closeFilter: "Close filters",
    closeDetail: "Close details",
    closeAmenity: "Close amenity",
    saveFavorite: "Save favorite",
    removeFavorite: "Remove favorite",
    detailsOpen: "Open details",
    activeFilters: "Active filters: {labels}",
  },
  list: {
    title: "Charging stations nearby",
    loading: "Loading charging stations...",
    loadingRadius: "Loading charging stations within 20 km...",
    loadingLiveFallback: "Live search is loading. Saved fast chargers are shown until then.",
    empty: "No charging stations found.",
    more: "...and {count} more",
  },
  favorites: {
    title: "Favorites",
    sort: "Sort",
    distance: "Distance",
    rating: "Stars",
    categoryFilterLabel: "Favorite categories",
    categoryFilterAria: "Show {category} favorites, {count} stations",
    all: "All",
    uncategorized: "Uncategorized",
    groupCountOne: "{count} station",
    groupCountMany: "{count} stations",
    empty: "No favorites saved yet.",
    emptyHelp: "Tap the star in the detail view to save stations.",
    loading: "Loading favorites in the current search area...",
    loadingFallback: "Live search is loading. Favorites from saved fast chargers remain visible.",
    outsideArea: "Your favorites are not in the current search area.",
    someOutsideArea: "Some favorites are outside the current search area.",
  },
  filters: {
    title: "Filters",
    all: "All filters",
    operator: "Operator",
    allOperators: "All operators",
    amenityName: "Name of nearby place",
    amenityNamePlaceholder: "e.g. McDonald's",
    availableOnly: "Available now",
    availableOnlyNote: "Only stations with known availability and at least one free charging point",
    currentlyOpen: "Open now",
    currentlyOpenNote: "Only stations with at least one currently open nearby place",
    minPower: "Min. power: {value} kW",
    minAmenities: "Min. nearby places: {value}",
    amenities: "Nearby places",
    apply: "Apply",
    reset: "Remove filters",
    activeAmenity: "{label} filter active",
    filterAmenity: "Filter by {label}",
    openWithCount: "Open filters, {count} active: {labels}",
    selectedOnly: "Selected only: {labels}",
    namePrefix: "Name: {value}",
    minPowerLabel: "from {value} kW",
    minAmenitiesLabel: "{value}+ nearby places",
  },
  route: {
    title: "Route planner",
    origin: "From",
    destination: "To",
    originPlaceholder: "Start address",
    destinationPlaceholder: "Destination address",
    useCurrent: "My location",
    currentLocation: "My location",
    swap: "Swap route endpoints",
    submit: "Charging stations along route",
    empty: "Enter a start and destination to find charging stations along the route.",
    resolving: "Resolving route endpoints...",
    loading: "Calculating charging stations along the route...",
    missingEndpoints: "Choose a start and destination first.",
    sameEndpoint: "Start and destination are too close together.",
    notConfigured: "Route planning is not configured.",
    locationUnavailable: "Current location is not available.",
    searchError: "Charging stations along the route could not be loaded.",
    capacityExhausted: "Routing capacity is currently exhausted. Please try again later.",
    noFilteredResults: "No charging stations along the route match the current filters.",
    filterChanged: "Filters changed since this route was calculated. Broader filters need a new route search.",
    recalculate: "Recalculate",
    showMap: "Show on map",
    addAllFavorites: "Add all as favorites",
    addAllFavoritesWithCount: "Add all {count} as favorites",
    favoriteCategory: "Route {origin} / {destination}",
    favoritesAdded: "{count} stations saved as favorites in \"{category}\".",
    mapFixed: "Map fixed on stations along the route",
    removeRoute: "Remove route",
    summaryDistance: "Route",
    summaryDuration: "Drive time",
    summaryStations: "Charging stations",
    resultsCount: "{count} stations",
    cardAccess: "{distance} from route",
    cardPosition: "route km {distance}",
    durationMinutes: "{minutes} min",
    durationHours: "{hours} h",
    durationHoursMinutes: "{hours} h {minutes} min",
  },
  info: {
    title: "Info & Help",
    aboutTitle: "About woladen.de",
    countriesTitle: "Current country coverage",
    country: "Country",
    stations: "Stations",
    source: "Data source",
    loadingCountries: "Loading countries...",
    countryLoadError: "Country coverage could not be loaded.",
    legendTitle: "Legend",
    cardBackgroundTitle: "Station tile backgrounds",
    mapMarkerTitle: "Map markers",
    legendOneFreeLeft: "Light yellow: one charging point left",
    legendFullyOccupied: "Grey: fully occupied",
    legendOutOfOrder: "Light red: currently out of order",
    legendOftenBroken: "Lighter red: often broken in analytics",
    legendOftenOccupied: "Lighter grey: often occupied in analytics",
    legendGold: "More than 10 nearby places",
    legendSilver: "More than 5 nearby places",
    legendBronze: "At least 1 nearby place",
    legendGrey: "No nearby places",
    legendFavorite: "Favorite station",
    legendMarkerOutOfOrder: "Live marker: out of order",
    legendMarkerFullyOccupied: "Live marker: fully occupied",
    aboutIntro: "Find available chargers near great bakeries, restaurants, shops, playgrounds and cafés. woladen knows",
    aboutStationCountJoin: "stations with",
    aboutOutro: "charging points in many countries and shows them within 20 km of your location or selected map position. Because charging time should be time well spent. By default, woladen searches fast chargers from 50 kW; the power filter can include normal chargers below 50 kW.",
    sourceUnknown: "Data source",
    licensesTitle: "Map data & licenses",
    osmNote: "Map data and POI data © OpenStreetMap contributors, available under ODbL v1.0.",
    osmCopyright: "OpenStreetMap: copyright and license notes",
    odblLicense: "ODbL v1.0: full license text",
    contactTitle: "Contact & Code",
    github: "GitHub project",
    developedBy: "Developed by Prof. Dr. Raphael Volz, Hochschule Pforzheim",
    distributedBy: "Distributed by",
    contributorsTitle: "Contributors",
    studentsGroup: "Students of Hochschule Pforzheim, SS26",
    privacyTitle: "Privacy",
    privacyBody: "Location access is optional. When you use it, it focuses the map on your surroundings and sorts nearby stations. Favorites stay locally on your device.",
    privacyLink: "Privacy policy",
    imprintLink: "Imprint",
    dataSourcesTitle: "Data Sources & Licenses",
    loadingSources: "Loading data sources...",
    sourceLoadError: "Data sources could not be loaded.",
    dataUpdated: "Data updated: {date}{counts}",
    countSuffix: " · {stations} stations · {chargers} charging points",
  },
  amenity: {
    one: "{count} nearby place",
    many: "{count} nearby places",
    generic: "Nearby place",
    other: "Other",
    noDetails: "No details available.",
    open: "Open now",
    closed: "Closed",
    unknownHours: "Opening hours unknown",
    hours: "Opening hours",
    groups: {
      food: "Food & drink",
      shopping: "Shopping",
      leisure: "Leisure & nature",
      lodging: "Accommodation",
      other: "Other",
    },
    labels: {
      restaurant: "Restaurant",
      cafe: "Cafe",
      fast_food: "Fast food",
      toilets: "Toilets",
      supermarket: "Supermarket",
      bakery: "Bakery",
      convenience: "Convenience store",
      pharmacy: "Pharmacy",
      hotel: "Hotel",
      museum: "Museum",
      playground: "Playground",
      park: "Park",
      ice_cream: "Ice cream",
      bbq: "BBQ area",
      biergarten: "Beer garden",
      cinema: "Cinema",
      library: "Library",
      theatre: "Theatre",
      atm: "ATM",
      bank: "Bank",
      bench: "Bench",
      bicycle_rental: "Bike rental",
      car_sharing: "Car sharing",
      fuel: "Fuel station",
      hospital: "Hospital",
      police: "Police",
      post_box: "Post box",
      post_office: "Post office",
      pub: "Pub",
      school: "School",
      taxi: "Taxi",
      waste_basket: "Waste bin",
      swimming: "Swimming pool",
      gym: "Fitness",
      camp_site: "Camping",
      viewpoint: "Viewpoint",
      zoo: "Zoo",
      mall: "Shopping mall",
      doityourself: "DIY store",
      electronics: "Electronics",
    },
  },
  station: {
    chargingPointOne: "{count} charging point",
    chargingPointMany: "{count} charging points",
    unknownOperator: "Unknown operator",
    chargingStation: "Charging station",
    note: "Note",
    details: "Details",
    live: "Live",
    stationStatus: "Station status",
    liveDataAvailable: "Live data available",
    evse: "Charging point {index}",
    notInLiveFeed: "Not included in the live feed",
    typicalOccupancy: "Typical occupancy",
    typicalOccupancyNote: "Average occupied charging points by hour; unknown states do not count as occupied.",
    detailsSource: "Details via {source} · updated {date}",
    detailsSourceOnly: "Details via {source}",
    liveVia: "Live via {source}",
    liveViaUpdated: "Live via {source} · updated {date}",
    updated: "Updated {date}",
    maxPower: "{power} kW max / {points}",
    nextSlot: "Next slot",
    supplementalStatus: "Supplemental status",
  },
  common: {
    yes: "Yes",
    no: "No",
    days: "{count} days",
  },
  availability: {
    free: "Free",
    occupied: "Occupied",
    out_of_order: "Out of order",
    unknown: "Unknown",
    summaryUnknown: "Occupancy unknown",
    available: "{count} free",
    occupiedCount: "{count} occupied",
    outOfOrderCount: "{count} out of order",
    unknownCount: "{count} unknown",
  },
  rating: {
    label: "Rating",
    ariaLabel: "Rate charging station",
    starOne: "{count} star",
    starMany: "{count} stars",
    unrated: "Not rated yet",
    save: "Saving rating...",
    serverError: "Rating saved locally. Server currently unavailable.",
    yourRating: "Your rating: {rating} of 5",
    localOnly: "only on this device",
    average: "Average from {count}",
    summary: "Avg {value} from {count}",
    localTitle: "Your local rating",
    one: "{count} rating",
    many: "{count} ratings",
  },
  detail: {
    personalNote: "Personal note",
    notePlaceholder: "e.g. well lit, tight parking, bakery around the corner",
    noteSaved: "Note saved locally",
    noteDeviceOnly: "Stored only on this device",
    favoriteCategories: "Categories",
    categoryPlaceholder: "Add category",
    addCategory: "Add",
    categoryDeviceOnly: "Stored only on this device",
    removeCategory: "Remove {category}",
    help: "Help",
    helpTitle: "Help {phone}",
  },
  staticDetails: {
    payment: "Payment",
    access: "Access",
    connectors: "Connectors",
    currentType: "Current type",
    sockets: "{count} sockets",
    service: "Service",
    energy: "Energy",
    renewable: "100% renewable",
    notRenewable: "Not marked as renewable",
  },
  location: {
    idleTitle: "Find charging stations near you",
    idleMessage: "Share your location to load charging stations within 20 km.",
    idleAction: "Share location",
    pendingTitle: "Finding location",
    pendingMessage: "Charging stations within 20 km are loading.",
    deniedTitle: "Location permission needed",
    deniedMessage: "Enable location access to load charging stations within 20 km.",
    settingsMessage: "Enable location access for woladen in system settings to load charging stations near you.",
    openSettings: "Open Settings",
    usageDescription: "woladen uses your location to show charging points nearby.",
    unavailableTitle: "Location unavailable",
    unavailableMessage: "This search needs a browser with location access.",
    timeoutTitle: "Location search is taking too long",
    timeoutMessage: "Try again to load charging stations within 20 km.",
    positionTitle: "Location could not be determined",
    positionMessage: "Try again when your browser can provide a location.",
    unknownMessage: "Please try again to load charging stations within 20 km.",
    retry: "Try again",
    searchMap: "Search place on map",
  },
  errors: {
    dataLoad: "Error loading data.",
    catalogTitle: "Charging points could not be loaded",
    catalogMessage: "No network connection. Sorry, live search will not work until this device is online.",
    reload: "Reload",
  },
  sources: {
    geocoder: "GEO: openrouteservice Geocoding Autocomplete (Pelias)",
    easterEgg: "Easter Egg: wake up! 16b by HellMood (Port)",
  },
};

let fallbackBundle = FALLBACK_BUNDLE;
let activeBundle = FALLBACK_BUNDLE;
let activeLanguage = DEFAULT_LANGUAGE;
let activeLocale = DEFAULT_LANGUAGE;
const listeners = new Set();

function deepMerge(base, patch) {
  const result = { ...base };
  Object.entries(patch || {}).forEach(([key, value]) => {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      base[key] &&
      typeof base[key] === "object" &&
      !Array.isArray(base[key])
    ) {
      result[key] = deepMerge(base[key], value);
    } else {
      result[key] = value;
    }
  });
  return result;
}

function getNestedValue(source, key) {
  return String(key || "")
    .split(".")
    .reduce((value, part) => value?.[part], source);
}

function interpolate(value, params = {}) {
  return String(value || "").replace(/\{([A-Za-z0-9_]+)\}/g, (_, name) =>
    params[name] === undefined || params[name] === null ? "" : String(params[name]),
  );
}

export function t(key, params = {}) {
  const value = getNestedValue(activeBundle, key) ?? getNestedValue(fallbackBundle, key) ?? key;
  return interpolate(value, params);
}

export function getLanguage() {
  return activeLanguage;
}

export function getLocale() {
  return activeLocale;
}

export function ogLocaleForLanguage(language = activeLanguage) {
  return OG_LOCALES[language] || OG_LOCALES[DEFAULT_LANGUAGE];
}

export function normalizeLanguage(value) {
  const raw = String(value || "").trim().toLowerCase().replace("_", "-");
  if (!raw) {
    return "";
  }
  const alias = LANGUAGE_ALIASES.get(raw);
  if (alias) {
    return alias;
  }
  if (SUPPORTED_LANGUAGES.includes(raw)) {
    return raw;
  }
  const base = raw.split("-")[0];
  return LANGUAGE_ALIASES.get(base) || (SUPPORTED_LANGUAGES.includes(base) ? base : "");
}

function requestedLanguageFromUrl() {
  const params = new URLSearchParams(window.location.search);
  return normalizeLanguage(params.get("lang") || params.get("language") || "");
}

function storedLanguage() {
  try {
    return normalizeLanguage(window.localStorage?.getItem(STORAGE_KEY) || "");
  } catch {
    return "";
  }
}

function browserLanguage() {
  const languages = Array.isArray(navigator.languages) && navigator.languages.length
    ? navigator.languages
    : [navigator.language];
  for (const language of languages) {
    const normalized = normalizeLanguage(language);
    if (normalized) {
      return normalized;
    }
  }
  return "";
}

export function resolveInitialLanguage() {
  return requestedLanguageFromUrl() || storedLanguage() || browserLanguage() || DEFAULT_LANGUAGE;
}

async function fetchBundle(language) {
  if (language === DEFAULT_LANGUAGE) {
    return FALLBACK_BUNDLE;
  }
  try {
    const response = await fetch(new URL(`./i18n/${language}.json?v=20260627-route-actions1`, import.meta.url));
    if (!response.ok) {
      return {};
    }
    return await response.json();
  } catch {
    return {};
  }
}

function storeLanguage(language) {
  try {
    window.localStorage?.setItem(STORAGE_KEY, language);
  } catch {
    // Local storage may be disabled.
  }
}

function updateLanguageParam(language) {
  const url = new URL(window.location.href);
  if (language) {
    url.searchParams.set("lang", language);
  } else {
    url.searchParams.delete("lang");
  }
  const next = `${url.pathname}${url.search}${url.hash}`;
  window.history.replaceState(window.history.state, "", next);
}

function updateMeta() {
  document.documentElement.lang = activeLanguage;
  document.title = t("meta.title");
  const canonical = document.querySelector("link[rel='canonical']");
  if (canonical) {
    canonical.setAttribute(
      "href",
      activeLanguage === DEFAULT_LANGUAGE
        ? "https://woladen.de/"
        : `https://woladen.de/?lang=${encodeURIComponent(activeLanguage)}`,
    );
  }
  document.querySelector("meta[name='description']")?.setAttribute("content", t("meta.description"));
  document.querySelector("meta[property='og:title']")?.setAttribute("content", t("meta.ogTitle"));
  document.querySelector("meta[property='og:description']")?.setAttribute("content", t("meta.ogDescription"));
  document.querySelector("meta[property='og:locale']")?.setAttribute("content", ogLocaleForLanguage());
  document.querySelector("meta[property='og:image:alt']")?.setAttribute("content", t("meta.socialAlt"));
  document.querySelector("meta[name='twitter:title']")?.setAttribute("content", t("meta.ogTitle"));
  document.querySelector("meta[name='twitter:description']")?.setAttribute("content", t("meta.ogDescription"));
  document.querySelector("meta[name='twitter:image:alt']")?.setAttribute("content", t("meta.socialAlt"));
}

export function applyDocumentTranslations(root = document) {
  root.querySelectorAll("[data-i18n]").forEach((element) => {
    element.textContent = t(element.dataset.i18n);
  });
  root.querySelectorAll("[data-i18n-placeholder]").forEach((element) => {
    element.setAttribute("placeholder", t(element.dataset.i18nPlaceholder));
  });
  root.querySelectorAll("[data-i18n-aria-label]").forEach((element) => {
    element.setAttribute("aria-label", t(element.dataset.i18nAriaLabel));
  });
  root.querySelectorAll("[data-i18n-title]").forEach((element) => {
    element.setAttribute("title", t(element.dataset.i18nTitle));
  });
  updateMeta();
}

export function populateLanguageSelect(select) {
  if (!select) {
    return;
  }
  select.replaceChildren();
  const displayNames = typeof Intl.DisplayNames === "function"
    ? new Intl.DisplayNames([activeLocale, DEFAULT_LANGUAGE], { type: "language" })
    : null;
  SUPPORTED_LANGUAGES.forEach((language) => {
    const option = document.createElement("option");
    option.value = language;
    option.textContent = displayNames?.of(language) || language.toUpperCase();
    select.appendChild(option);
  });
  select.value = activeLanguage;
}

export async function setLanguage(language, options = {}) {
  const normalized = normalizeLanguage(language) || DEFAULT_LANGUAGE;
  const bundle = await fetchBundle(normalized);
  activeLanguage = normalized;
  activeLocale = normalized;
  activeBundle = deepMerge(fallbackBundle, bundle);
  if (options.persist !== false) {
    storeLanguage(normalized);
  }
  if (options.updateUrl !== false) {
    updateLanguageParam(normalized);
  }
  applyDocumentTranslations();
  document.querySelectorAll("[data-language-select]").forEach((select) => {
    populateLanguageSelect(select);
  });
  listeners.forEach((listener) => listener(normalized));
}

export async function initI18n() {
  fallbackBundle = FALLBACK_BUNDLE;
  await setLanguage(resolveInitialLanguage(), {
    persist: false,
    updateUrl: Boolean(requestedLanguageFromUrl()),
  });
}

export function onLanguageChange(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

export function formatInteger(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || numeric <= 0) {
    return "";
  }
  return new Intl.NumberFormat(getLocale()).format(numeric);
}

export function formatDateTime(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const date = /^\d{11,}$/.test(raw) ? new Date(Number(raw)) : new Date(raw);
  if (Number.isNaN(date.getTime())) {
    return raw;
  }
  return new Intl.DateTimeFormat(getLocale(), {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

export function formatDate(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return "";
  }
  const parsed = new Date(`${raw}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return raw;
  }
  return new Intl.DateTimeFormat(getLocale(), {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  }).format(parsed);
}
