const BERLIN_TIME_ZONE = "Europe/Berlin";
const DAY_KEYS = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"];
const GERMAN_DAY_LABELS = {
  Mo: "Mo",
  Tu: "Di",
  We: "Mi",
  Th: "Do",
  Fr: "Fr",
  Sa: "Sa",
  Su: "So",
};
const WEEKDAY_TO_KEY = {
  Mon: "Mo",
  Tue: "Tu",
  Wed: "We",
  Thu: "Th",
  Fri: "Fr",
  Sat: "Sa",
  Sun: "Su",
};
const DAY_TOKEN = "(?:Mo|Tu|We|Th|Fr|Sa|Su|PH)";
const DAY_SELECTOR_RE = new RegExp(
  `^(${DAY_TOKEN}(?:\\s*-\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?(?:\\s*,\\s*${DAY_TOKEN}(?:\\s*-\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\\s+(.+)$`,
);
const TIME_RANGE_RE = /^(\d{1,2}):(\d{2})\s*(?:-\s*(\d{1,2}):(\d{2})(\+)?|\+)$/;

const berlinFormatter = new Intl.DateTimeFormat("en-GB", {
  timeZone: BERLIN_TIME_ZONE,
  weekday: "short",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  hourCycle: "h23",
});

function getPart(parts, type) {
  return parts.find((part) => part.type === type)?.value || "";
}

export function getGermanNowParts(date = new Date()) {
  const parts = berlinFormatter.formatToParts(date);
  const dayKey = WEEKDAY_TO_KEY[getPart(parts, "weekday")];
  const hour = Number(getPart(parts, "hour"));
  const minute = Number(getPart(parts, "minute"));
  const year = Number(getPart(parts, "year"));
  const month = Number(getPart(parts, "month"));
  const day = Number(getPart(parts, "day"));
  const dayIndex = DAY_KEYS.indexOf(dayKey);

  return {
    year,
    month,
    day,
    dayKey,
    dayIndex,
    previousDayKey: DAY_KEYS[(dayIndex + 6) % 7],
    minuteOfDay: hour * 60 + minute,
    isPublicHoliday: isGermanNationalPublicHoliday(year, month, day),
  };
}

export function formatOpeningHoursForGermanDisplay(openingHours) {
  const holidayStates = new Set();
  const dayClausesByBody = new Map();
  const fallbackClauses = [];

  for (const clause of splitOpeningHoursClauses(normalizeOpeningHours(openingHours))) {
    const trimmed = clause.trim();
    if (!trimmed) {
      continue;
    }

    const dayMatch = DAY_SELECTOR_RE.exec(trimmed);
    if (!dayMatch) {
      fallbackClauses.push(formatOpeningHoursClause(trimmed));
      continue;
    }

    const selector = parseDisplayDaySelector(dayMatch[1]);
    const body = dayMatch[2].trim();
    const isClosedClause = /^(?:off|closed)$/i.test(body);
    if (selector.matchesPublicHoliday) {
      if (!isClosedClause) {
        holidayStates.add("open");
      }
    }

    if (selector.selectedDays.size === 0 || isClosedClause) {
      continue;
    }

    const bodyDisplay = formatOpeningHoursClause(body);
    if (!dayClausesByBody.has(bodyDisplay)) {
      dayClausesByBody.set(bodyDisplay, {
        bodyDisplay,
        days: new Set(),
      });
    }
    selector.selectedDays.forEach((day) => dayClausesByBody.get(bodyDisplay).days.add(day));
  }

  const clauses = Array.from(dayClausesByBody.values())
    .sort((a, b) => firstDayIndex(a.days) - firstDayIndex(b.days))
    .map((item) => `${formatDisplayDays(item.days)} ${item.bodyDisplay}`);

  clauses.push(...fallbackClauses.filter(Boolean));

  if (holidayStates.has("open")) {
    clauses.push("an Feiertagen geöffnet");
  }

  return clauses.join("; ");
}

function parseDisplayDaySelector(selector) {
  const selectedDays = new Set();
  let matchesPublicHoliday = false;

  for (const part of selector.split(",")) {
    const token = part.trim();
    if (!token) {
      continue;
    }
    if (token === "PH") {
      matchesPublicHoliday = true;
      continue;
    }
    const range = /^([A-Z][a-z])\s*-\s*([A-Z][a-z])$/.exec(token);
    if (range) {
      expandDayRange(range[1], range[2]).forEach((day) => selectedDays.add(day));
    } else if (DAY_KEYS.includes(token)) {
      selectedDays.add(token);
    }
  }

  return { selectedDays, matchesPublicHoliday };
}

function firstDayIndex(days) {
  const indexes = DAY_KEYS
    .map((day, index) => (days.has(day) ? index : null))
    .filter((index) => index !== null);
  return indexes.length > 0 ? Math.min(...indexes) : DAY_KEYS.length;
}

function formatDisplayDays(days) {
  const ordered = DAY_KEYS.filter((day) => days.has(day));
  const ranges = [];
  for (let index = 0; index < ordered.length; index += 1) {
    const start = ordered[index];
    let end = start;
    while (
      index + 1 < ordered.length &&
      DAY_KEYS.indexOf(ordered[index + 1]) === DAY_KEYS.indexOf(end) + 1
    ) {
      index += 1;
      end = ordered[index];
    }
    ranges.push(
      start === end
        ? GERMAN_DAY_LABELS[start]
        : `${GERMAN_DAY_LABELS[start]}-${GERMAN_DAY_LABELS[end]}`,
    );
  }
  return ranges.join(", ");
}

function formatOpeningHoursClause(value) {
  return String(value || "")
    .trim()
    .replace(/\b(\d{1,2}:\d{2})\s*-\s*\d{1,2}:\d{2}\+/g, "ab $1")
    .replace(/\b(\d{1,2}:\d{2})\+/g, "ab $1")
    .replace(/\b(Mo|Tu|We|Th|Fr|Sa|Su)\b/g, (token) => GERMAN_DAY_LABELS[token])
    .replace(/\boff\b/gi, "geschlossen")
    .replace(/\bclosed\b/gi, "geschlossen")
    .replace(/\bopen\b/gi, "geöffnet")
    .replace(/,\s*/g, ", ");
}

function splitOpeningHoursClauses(openingHours) {
  const clauses = [];
  for (const semicolonPart of String(openingHours || "").split(";")) {
    let current = "";
    for (const commaPart of semicolonPart.split(",")) {
      const part = commaPart.trim();
      if (!part) {
        continue;
      }
      if (current && DAY_SELECTOR_RE.test(part) && /\d{1,2}:\d{2}|\+|off|closed|open/i.test(current)) {
        clauses.push(current.trim());
        current = part;
      } else {
        current = current ? `${current},${part}` : part;
      }
    }
    if (current.trim()) {
      clauses.push(current.trim());
    }
  }
  return clauses;
}

function getEasterSunday(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return Date.UTC(year, month - 1, day);
}

function isoDateFromUtcMs(utcMs) {
  return new Date(utcMs).toISOString().slice(5, 10);
}

export function isGermanNationalPublicHoliday(year, month, day) {
  const fixed = new Set(["01-01", "05-01", "10-03", "12-25", "12-26"]);
  const key = `${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  if (fixed.has(key)) {
    return true;
  }

  const easter = getEasterSunday(year);
  const dayMs = 24 * 60 * 60 * 1000;
  const movable = new Set([
    isoDateFromUtcMs(easter - 2 * dayMs),
    isoDateFromUtcMs(easter + dayMs),
    isoDateFromUtcMs(easter + 39 * dayMs),
    isoDateFromUtcMs(easter + 50 * dayMs),
  ]);
  return movable.has(key);
}

function normalizeOpeningHours(value) {
  return String(value || "")
    .trim()
    .replace(/\s+/g, " ");
}

function parseMinute(value) {
  const match = TIME_RANGE_RE.exec(value.trim());
  if (!match) {
    return null;
  }

  const startHour = Number(match[1]);
  const startMinute = Number(match[2]);
  const endHour = match[3] === undefined ? 24 : Number(match[3]);
  const endMinute = match[4] === undefined ? 0 : Number(match[4]);

  if (
    startHour > 24 ||
    endHour > 24 ||
    startMinute > 59 ||
    endMinute > 59 ||
    (startHour === 24 && startMinute !== 0) ||
    (endHour === 24 && endMinute !== 0)
  ) {
    return null;
  }

  return {
    start: startHour * 60 + startMinute,
    end: endHour * 60 + endMinute,
    openEnded: match[3] === undefined,
  };
}

function expandDayRange(startKey, endKey) {
  const start = DAY_KEYS.indexOf(startKey);
  const end = DAY_KEYS.indexOf(endKey);
  if (start < 0 || end < 0) {
    return [];
  }

  const days = [];
  for (let offset = 0; offset < DAY_KEYS.length; offset += 1) {
    const index = (start + offset) % DAY_KEYS.length;
    days.push(DAY_KEYS[index]);
    if (index === end) {
      break;
    }
  }
  return days;
}

function parseDaySelector(selector) {
  const selectedDays = new Set();
  let matchesPublicHoliday = false;

  for (const part of selector.split(",")) {
    const token = part.trim();
    if (!token) {
      continue;
    }
    if (token === "PH") {
      matchesPublicHoliday = true;
      continue;
    }
    const range = /^([A-Z][a-z])\s*-\s*([A-Z][a-z])$/.exec(token);
    if (range) {
      expandDayRange(range[1], range[2]).forEach((day) => selectedDays.add(day));
    } else if (DAY_KEYS.includes(token)) {
      selectedDays.add(token);
    }
  }

  return { selectedDays, matchesPublicHoliday };
}

function clauseApplies(daySelector, dayKey, isPublicHoliday) {
  if (!daySelector) {
    return true;
  }
  if (isPublicHoliday && daySelector.matchesPublicHoliday) {
    return true;
  }
  return daySelector.selectedDays.has(dayKey);
}

function parseClause(clause) {
  const trimmed = clause.trim();
  if (!trimmed) {
    return null;
  }

  const dayMatch = DAY_SELECTOR_RE.exec(trimmed);
  const daySelector = dayMatch ? parseDaySelector(dayMatch[1]) : null;
  const body = (dayMatch ? dayMatch[2] : trimmed).trim();

  if (/^(?:off|closed)$/i.test(body)) {
    return { daySelector, mode: "closed", ranges: [] };
  }
  if (/^open$/i.test(body)) {
    return { daySelector, mode: "open", ranges: [] };
  }

  const ranges = body.split(",").map(parseMinute);
  if (ranges.some((range) => range === null)) {
    return { daySelector, mode: "unknown", ranges: [] };
  }
  return { daySelector, mode: "times", ranges };
}

function isWithinRange(range, minuteOfDay, previousDay = false) {
  if (range.openEnded) {
    return previousDay ? minuteOfDay < 6 * 60 : minuteOfDay >= range.start;
  }
  if (range.start === range.end) {
    return true;
  }
  if (range.start < range.end) {
    return !previousDay && minuteOfDay >= range.start && minuteOfDay < range.end;
  }
  return previousDay ? minuteOfDay < range.end : minuteOfDay >= range.start;
}

function clauseStateForDay(clause, dayKey, isPublicHoliday, minuteOfDay, previousDay = false) {
  if (!clause || !clauseApplies(clause.daySelector, dayKey, isPublicHoliday)) {
    return null;
  }
  if (clause.mode === "closed") {
    return previousDay ? null : "closed";
  }
  if (clause.mode === "open") {
    return "open";
  }
  if (clause.mode === "unknown") {
    return previousDay ? null : "unknown";
  }
  if (clause.ranges.some((range) => isWithinRange(range, minuteOfDay, previousDay))) {
    return "open";
  }
  return previousDay ? null : "closed";
}

export function evaluateOpeningHours(openingHours, date = new Date()) {
  const normalized = normalizeOpeningHours(openingHours);
  if (!normalized) {
    return { state: "unknown" };
  }
  if (/^24\/7$/i.test(normalized)) {
    return { state: "open" };
  }
  if (/^(?:off|closed)$/i.test(normalized)) {
    return { state: "closed" };
  }

  const now = getGermanNowParts(date);
  const clauses = splitOpeningHoursClauses(normalized).map(parseClause).filter(Boolean);
  if (clauses.length === 0) {
    return { state: "unknown" };
  }

  let currentState = null;
  for (const clause of clauses) {
    const state = clauseStateForDay(
      clause,
      now.dayKey,
      now.isPublicHoliday,
      now.minuteOfDay,
      false,
    );
    if (state) {
      currentState = state;
    }
  }
  if (currentState === "open" || currentState === "unknown") {
    return { state: currentState };
  }

  for (const clause of clauses) {
    const state = clauseStateForDay(
      clause,
      now.previousDayKey,
      false,
      now.minuteOfDay,
      true,
    );
    if (state === "open") {
      return { state: "open" };
    }
  }

  return { state: currentState || "closed" };
}

export function getAmenityOpenStatus(amenity, date = new Date()) {
  return evaluateOpeningHours(amenity?.opening_hours, date);
}

export function hasOpenAmenity(properties, date = new Date()) {
  const examples = Array.isArray(properties?.amenity_examples)
    ? properties.amenity_examples
    : [];
  return examples.some((example) => getAmenityOpenStatus(example, date).state === "open");
}
