import test from "node:test";
import assert from "node:assert/strict";

import {
  evaluateOpeningHours,
  formatOpeningHoursForGermanDisplay,
  getGermanNowParts,
  hasOpenAmenity,
  isGermanNationalPublicHoliday,
} from "./opening-hours.mjs";

const berlinTime = (iso) => new Date(iso);

test("converts current time to Europe/Berlin parts", () => {
  const parts = getGermanNowParts(berlinTime("2026-01-10T10:30:00Z"));

  assert.equal(parts.dayKey, "Sa");
  assert.equal(parts.minuteOfDay, 11 * 60 + 30);
});

test("recognizes German nationwide public holidays", () => {
  assert.equal(isGermanNationalPublicHoliday(2026, 5, 1), true);
  assert.equal(isGermanNationalPublicHoliday(2026, 5, 2), false);
});

test("evaluates simple weekday time windows", () => {
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00", berlinTime("2026-01-10T10:00:00Z")).state,
    "open",
  );
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00", berlinTime("2026-01-10T20:30:00Z")).state,
    "closed",
  );
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00", berlinTime("2026-01-11T10:00:00Z")).state,
    "closed",
  );
});

test("supports public holiday override clauses", () => {
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00; PH off", berlinTime("2026-05-01T10:00:00Z")).state,
    "closed",
  );
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00", berlinTime("2026-05-01T10:00:00Z")).state,
    "open",
  );
});

test("evaluates combined closed Sunday and holiday clauses", () => {
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00; Su,PH off", berlinTime("2026-01-11T10:00:00Z")).state,
    "closed",
  );
  assert.equal(
    evaluateOpeningHours("Mo-Sa 08:00-20:00; Su,PH off", berlinTime("2026-05-01T10:00:00Z")).state,
    "closed",
  );
});

test("supports multiple day clauses and time windows", () => {
  assert.equal(
    evaluateOpeningHours(
      "Mo-Fr 11:30-14:30,17:30-22:00; Sa 17:30-22:00; Su off",
      berlinTime("2026-01-09T12:00:00Z"),
    ).state,
    "open",
  );
  assert.equal(
    evaluateOpeningHours(
      "Mo-Fr 11:30-14:30,17:30-22:00; Sa 17:30-22:00; Su off",
      berlinTime("2026-01-09T14:00:00Z"),
    ).state,
    "closed",
  );
});

test("supports overnight and open-ended ranges", () => {
  assert.equal(
    evaluateOpeningHours("Fr-Sa 22:00-02:00", berlinTime("2026-01-09T22:30:00Z")).state,
    "open",
  );
  assert.equal(
    evaluateOpeningHours("Fr-Sa 22:00-02:00", berlinTime("2026-01-10T00:30:00Z")).state,
    "open",
  );
  assert.equal(
    evaluateOpeningHours("Fr 18:00+", berlinTime("2026-01-09T20:00:00Z")).state,
    "open",
  );
  assert.equal(
    evaluateOpeningHours("Fr 17:00-18:00+", berlinTime("2026-01-09T16:30:00Z")).state,
    "open",
  );
});

test("returns unknown for missing or unsupported syntax", () => {
  assert.equal(evaluateOpeningHours("").state, "unknown");
  assert.equal(evaluateOpeningHours("by appointment").state, "unknown");
});

test("formats OSM opening hours with German labels for display", () => {
  assert.equal(
    formatOpeningHoursForGermanDisplay("Mo-Su 08:00-20:00; PH off"),
    "Mo-So 08:00-20:00",
  );
  assert.equal(
    formatOpeningHoursForGermanDisplay("Tu-Th 11:00-14:00,17:00-22:00; Su closed"),
    "Di-Do 11:00-14:00, 17:00-22:00",
  );
  assert.equal(
    formatOpeningHoursForGermanDisplay("PH,Mo-Su 11:00-23:00"),
    "Mo-So 11:00-23:00; an Feiertagen geöffnet",
  );
  assert.equal(
    formatOpeningHoursForGermanDisplay("Mo-Sa 08:00-20:00; Su,PH off"),
    "Mo-Sa 08:00-20:00",
  );
  assert.equal(
    formatOpeningHoursForGermanDisplay("Su 10:00-18:00; Sa 09:00-18:00; Mo-Fr 08:00-20:00"),
    "Mo-Fr 08:00-20:00; Sa 09:00-18:00; So 10:00-18:00",
  );
  assert.equal(
    formatOpeningHoursForGermanDisplay("Su 09:00-18:00; Sa 09:00-18:00; Mo-Fr 08:00-20:00"),
    "Mo-Fr 08:00-20:00; Sa-So 09:00-18:00",
  );
  assert.equal(
    formatOpeningHoursForGermanDisplay("Fr,Sa 18:00-02:00; PH,Su 18:00-23:00"),
    "Fr-Sa 18:00-02:00; So 18:00-23:00; an Feiertagen geöffnet",
  );
  assert.equal(formatOpeningHoursForGermanDisplay("Su,PH off"), "");
  assert.equal(
    formatOpeningHoursForGermanDisplay("Mo,Tu,Fr 17:00-18:00+; We off; Th 16:00-18:00+; Sa,Su 10:00-18:00+"),
    "Mo-Di, Fr ab 17:00; Do ab 16:00; Sa-So ab 10:00",
  );
});

test("station has an open amenity only when at least one amenity is confirmed open", () => {
  const properties = {
    amenity_examples: [
      { name: "Closed shop", opening_hours: "Mo-Fr 08:00-12:00" },
      { name: "Open cafe", opening_hours: "Mo-Su 08:00-20:00" },
      { name: "Unknown place" },
    ],
  };

  assert.equal(hasOpenAmenity(properties, berlinTime("2026-01-10T10:00:00Z")), true);
  assert.equal(hasOpenAmenity({ amenity_examples: [{ name: "Unknown place" }] }), false);
});
