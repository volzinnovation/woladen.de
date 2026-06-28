export const DEFAULT_OPENING_HOURS_MODULE_URL = "https://esm.sh/opening_hours@3.12.0?bundle";

const MINUTE_MS = 60 * 1000;
const DAY_MS = 24 * 60 * MINUTE_MS;

const WEEKDAY_LABELS = ["So", "Mo", "Di", "Mi", "Do", "Fr", "Sa"];

const ISO3_TO_ISO2 = {
  AUT: "AT",
  BEL: "BE",
  BGR: "BG",
  CHE: "CH",
  CYP: "CY",
  CZE: "CZ",
  DEU: "DE",
  DNK: "DK",
  ESP: "ES",
  EST: "EE",
  FIN: "FI",
  FRA: "FR",
  GRC: "GR",
  HRV: "HR",
  HUN: "HU",
  IRL: "IE",
  ITA: "IT",
  LTU: "LT",
  LUX: "LU",
  LVA: "LV",
  MLT: "MT",
  NLD: "NL",
  NOR: "NO",
  POL: "PL",
  PRT: "PT",
  ROU: "RO",
  SVK: "SK",
  SVN: "SI",
  SWE: "SE",
};

const COUNTRY_TIME_ZONES = {
  AT: "Europe/Vienna",
  BE: "Europe/Brussels",
  BG: "Europe/Sofia",
  CH: "Europe/Zurich",
  CY: "Asia/Nicosia",
  CZ: "Europe/Prague",
  DE: "Europe/Berlin",
  DK: "Europe/Copenhagen",
  EE: "Europe/Tallinn",
  ES: "Europe/Madrid",
  FI: "Europe/Helsinki",
  FR: "Europe/Paris",
  GR: "Europe/Athens",
  HR: "Europe/Zagreb",
  HU: "Europe/Budapest",
  IE: "Europe/Dublin",
  IT: "Europe/Rome",
  LT: "Europe/Vilnius",
  LU: "Europe/Luxembourg",
  LV: "Europe/Riga",
  MT: "Europe/Malta",
  NL: "Europe/Amsterdam",
  NO: "Europe/Oslo",
  PL: "Europe/Warsaw",
  PT: "Europe/Lisbon",
  RO: "Europe/Bucharest",
  SE: "Europe/Stockholm",
  SI: "Europe/Ljubljana",
  SK: "Europe/Bratislava",
};

let cachedOpeningHoursParser = null;
let parserLoadPromise = null;
let parserLoadFailed = false;

export function resolveOpeningHoursModuleUrl(value = "") {
  const candidate = String(value || "").trim();
  return candidate || DEFAULT_OPENING_HOURS_MODULE_URL;
}

export function getOpeningHoursParser() {
  return cachedOpeningHoursParser;
}

export function getOpeningHoursLoadStatus() {
  return {
    loaded: Boolean(cachedOpeningHoursParser),
    loading: Boolean(parserLoadPromise && !cachedOpeningHoursParser),
    failed: parserLoadFailed,
  };
}

export function loadOpeningHoursParser(moduleUrl = DEFAULT_OPENING_HOURS_MODULE_URL) {
  if (cachedOpeningHoursParser) {
    return Promise.resolve(cachedOpeningHoursParser);
  }
  if (parserLoadPromise) {
    return parserLoadPromise;
  }

  parserLoadFailed = false;
  parserLoadPromise = import(moduleUrl)
    .then((module) => {
      const Parser = module.default || module.opening_hours || module.OpeningHours;
      if (typeof Parser !== "function") {
        throw new Error("opening_hours.js module did not export a parser constructor");
      }
      cachedOpeningHoursParser = Parser;
      return Parser;
    })
    .catch((error) => {
      parserLoadFailed = true;
      console.warn("Failed to load opening_hours.js", error);
      return null;
    })
    .finally(() => {
      parserLoadPromise = null;
    });

  return parserLoadPromise;
}

export function normalizeCountryCode(value = "") {
  const normalized = String(value || "").trim().toUpperCase();
  if (normalized.length === 2) {
    return normalized;
  }
  if (normalized.length === 3 && ISO3_TO_ISO2[normalized]) {
    return ISO3_TO_ISO2[normalized];
  }
  return "";
}

export function countryTimeZone(countryCode = "") {
  return COUNTRY_TIME_ZONES[normalizeCountryCode(countryCode)] || "";
}

export function dateInTimeZone(date, timeZone) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime()) || !timeZone) {
    return date;
  }

  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone,
      hourCycle: "h23",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }).formatToParts(date);
    const values = Object.fromEntries(
      parts
        .filter((part) => part.type !== "literal")
        .map((part) => [part.type, Number(part.value)]),
    );
    return new Date(
      values.year,
      values.month - 1,
      values.day,
      values.hour,
      values.minute,
      values.second,
      date.getMilliseconds(),
    );
  } catch (error) {
    return date;
  }
}

function text(value) {
  return String(value || "").trim();
}

function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function openingHoursContext(item, countryCode) {
  const lat = finiteNumber(item?.lat);
  const lon = finiteNumber(item?.lon);
  const normalizedCountry = normalizeCountryCode(countryCode);
  const context = {};
  if (lat !== null && lon !== null) {
    context.lat = lat;
    context.lon = lon;
  }
  if (normalizedCountry) {
    context.address = { country_code: normalizedCountry.toLowerCase() };
  }
  return Object.keys(context).length > 0 ? context : null;
}

function isUsableDate(date) {
  return date instanceof Date && Number.isFinite(date.getTime());
}

function pad2(value) {
  return String(value).padStart(2, "0");
}

function formatClock(date) {
  return `${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
}

function calendarDayDifference(from, to) {
  const fromDay = new Date(from.getFullYear(), from.getMonth(), from.getDate());
  const toDay = new Date(to.getFullYear(), to.getMonth(), to.getDate());
  return Math.round((toDay.getTime() - fromDay.getTime()) / DAY_MS);
}

function formatChangePhrase(nextChange, now, verb) {
  const days = calendarDayDifference(now, nextChange);
  const time = formatClock(nextChange);
  if (days <= 0) {
    return `${verb} ${time}`;
  }
  if (days === 1) {
    return `${verb} morgen ${time}`;
  }
  if (days < 7) {
    return `${verb} ${WEEKDAY_LABELS[nextChange.getDay()]} ${time}`;
  }
  return `${verb} ${pad2(nextChange.getDate())}.${pad2(nextChange.getMonth() + 1)}. ${time}`;
}

export function formatAmenityOpeningStatus(item, options = {}) {
  const openingHours = text(item?.opening_hours);
  if (!openingHours) {
    return null;
  }

  const Parser = options.OpeningHours || cachedOpeningHoursParser;
  const loadStatus = options.loadStatus || getOpeningHoursLoadStatus();
  if (!Parser) {
    return {
      text: loadStatus.failed ? "Öffnungszeiten unklar" : "Prüfe Öffnungszeiten",
      tone: loadStatus.failed ? "unknown" : "pending",
    };
  }

  const now = options.now instanceof Date ? options.now : new Date();
  const countryCode = options.countryCode || item?.country_code || "";
  const timeZone = options.timeZone || countryTimeZone(countryCode);
  const localNow = dateInTimeZone(now, timeZone);

  try {
    const oh = new Parser(
      openingHours,
      openingHoursContext(item, countryCode),
      { tag_key: "opening_hours" },
    );
    const unknown = typeof oh.getUnknown === "function" ? Boolean(oh.getUnknown(localNow)) : false;
    if (unknown) {
      return { text: "Öffnung unklar", tone: "unknown" };
    }

    const isOpen = Boolean(oh.getState(localNow));
    const nextChange = typeof oh.getNextChange === "function" ? oh.getNextChange(localNow) : null;
    const hasNextChange = isUsableDate(nextChange);

    if (isOpen) {
      return {
        text: hasNextChange
          ? `Jetzt offen • ${formatChangePhrase(nextChange, localNow, "schließt")}`
          : "Jetzt offen • durchgehend",
        tone: "open",
      };
    }

    return {
      text: hasNextChange
        ? `Geschlossen • ${formatChangePhrase(nextChange, localNow, "öffnet")}`
        : "Geschlossen",
      tone: "closed",
    };
  } catch (error) {
    return { text: "Öffnungszeiten unklar", tone: "unknown" };
  }
}

export function resetOpeningHoursParserForTests() {
  cachedOpeningHoursParser = null;
  parserLoadPromise = null;
  parserLoadFailed = false;
}
