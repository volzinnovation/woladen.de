import test from "node:test";
import assert from "node:assert/strict";

import {
  DEFAULT_FILTER_SETTINGS,
  normalizeStoredFilterSettings,
  parseStoredFilterSettings,
  serializeStoredFilterSettings,
} from "./filter-settings.mjs";

test("filter settings default first visitors to available fast chargers", () => {
  assert.deepEqual(normalizeStoredFilterSettings({}), DEFAULT_FILTER_SETTINGS);
  assert.equal(DEFAULT_FILTER_SETTINGS.availableOnly, true);
  assert.equal(DEFAULT_FILTER_SETTINGS.minPower, 50);
  assert.equal(DEFAULT_FILTER_SETTINGS.minAmenityCount, 0);
});

test("filter settings parse and serialize local storage payloads", () => {
  const settings = parseStoredFilterSettings(JSON.stringify({
    operator: " IONITY ",
    minPower: 153,
    minAmenityCount: 5.7,
    amenities: ["amenity_cafe", "bad-key", "amenity_cafe", "amenity_fast_food"],
    amenityNameQuery: " Bakery ",
    availableOnly: false,
    currentlyOpenOnly: true,
  }));

  assert.deepEqual(settings, {
    operator: "IONITY",
    minPower: 150,
    minAmenityCount: 6,
    amenities: ["amenity_cafe", "amenity_fast_food"],
    amenityNameQuery: "Bakery",
    availableOnly: false,
    currentlyOpenOnly: true,
  });

  assert.equal(
    serializeStoredFilterSettings({ ...settings, amenities: new Set(settings.amenities) }),
    JSON.stringify(settings),
  );
});

test("invalid filter settings fall back without disabling first-visit defaults", () => {
  assert.equal(parseStoredFilterSettings("not json"), null);
  assert.deepEqual(
    normalizeStoredFilterSettings({
      minPower: "999",
      min_amenities_total: "999",
      availableOnly: "false",
      currentlyOpenOnly: "true",
      amenities: ["amenity_supermarket", "../x"],
    }),
    {
      ...DEFAULT_FILTER_SETTINGS,
      minPower: 350,
      minAmenityCount: 25,
      amenities: ["amenity_supermarket"],
    },
  );
});
