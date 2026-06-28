import test from "node:test";
import assert from "node:assert/strict";

import {
  countryTimeZone,
  dateInTimeZone,
  formatAmenityOpeningStatus,
  normalizeCountryCode,
} from "./opening-hours-status.mjs";

class OpenUntilEvening {
  getUnknown() {
    return false;
  }

  getState() {
    return true;
  }

  getNextChange(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate(), 18, 0, 0);
  }
}

class ClosedUntilTomorrow {
  getUnknown() {
    return false;
  }

  getState() {
    return false;
  }

  getNextChange(date) {
    return new Date(date.getFullYear(), date.getMonth(), date.getDate() + 1, 6, 0, 0);
  }
}

test("normalizes EU country codes and resolves opening-hours time zones", () => {
  assert.equal(normalizeCountryCode("DEU"), "DE");
  assert.equal(countryTimeZone("NLD"), "Europe/Amsterdam");
  assert.equal(countryTimeZone("CY"), "Asia/Nicosia");
});

test("builds a Date with local components from the target timezone", () => {
  const berlinDate = dateInTimeZone(
    new Date("2026-05-11T05:00:00.000Z"),
    "Europe/Berlin",
  );

  assert.equal(berlinDate.getFullYear(), 2026);
  assert.equal(berlinDate.getMonth(), 4);
  assert.equal(berlinDate.getDate(), 11);
  assert.equal(berlinDate.getHours(), 7);
});

test("formats an open amenity with its closing time", () => {
  const status = formatAmenityOpeningStatus(
    { opening_hours: "Mo-Fr 06:00-18:00", lat: 52.52, lon: 13.405 },
    {
      OpeningHours: OpenUntilEvening,
      countryCode: "DE",
      now: new Date("2026-05-11T05:00:00.000Z"),
    },
  );

  assert.deepEqual(status, {
    text: "Jetzt offen • schließt 18:00",
    tone: "open",
  });
});

test("formats a closed amenity with the next opening time", () => {
  const status = formatAmenityOpeningStatus(
    { opening_hours: "Mo-Fr 06:00-18:00", lat: 52.52, lon: 13.405 },
    {
      OpeningHours: ClosedUntilTomorrow,
      countryCode: "DE",
      now: new Date("2026-05-11T17:30:00.000Z"),
    },
  );

  assert.deepEqual(status, {
    text: "Geschlossen • öffnet morgen 06:00",
    tone: "closed",
  });
});

test("does not expose raw OSM opening-hours text when the parser is unavailable", () => {
  const status = formatAmenityOpeningStatus(
    { opening_hours: "Mo-Fr 06:00-18:00" },
    { loadStatus: { loaded: false, loading: true, failed: false } },
  );

  assert.deepEqual(status, {
    text: "Prüfe Öffnungszeiten",
    tone: "pending",
  });
});
