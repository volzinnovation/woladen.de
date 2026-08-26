import test from "node:test";
import assert from "node:assert/strict";

import {
  compareOperatorNames,
  countActiveFilters,
  hasAvailableChargingPoint,
  matchesAmenityNameQuery,
  matchesFeatureFilters,
} from "./filtering.mjs";

test("operator names sort alphabetically regardless of letter case", () => {
  const operators = ["Zunder", "chargecloud", "Allego", "be.ENERGISED"];

  assert.deepEqual(
    operators.sort((left, right) => compareOperatorNames(left, right, "en")),
    ["Allego", "be.ENERGISED", "chargecloud", "Zunder"],
  );
});

test("matches amenity names ignoring case, punctuation, and diacritics", () => {
  const properties = {
    amenity_examples: [{ name: "McDonald's Café" }],
  };

  assert.equal(matchesAmenityNameQuery(properties, "mcdonalds"), true);
  assert.equal(matchesAmenityNameQuery(properties, "cafe"), true);
  assert.equal(matchesAmenityNameQuery(properties, "burger king"), false);
});

test("feature matcher combines provider, amenity type, power, and amenity-name query", () => {
  const feature = {
    properties: {
      operator: "EnBW",
      max_power_kw: 300,
      amenities_total: 8,
      amenity_fast_food: 2,
      amenity_examples: [{ name: "McDonald's" }],
    },
  };
  const filters = {
    operator: "EnBW",
    minPower: 150,
    minAmenityCount: 6,
    amenities: new Set(["amenity_fast_food"]),
    amenityNameQuery: "McDonald",
  };

  assert.equal(matchesFeatureFilters(feature, filters), true);
  assert.equal(
    matchesFeatureFilters(feature, { ...filters, amenityNameQuery: "Subway" }),
    false,
  );
  assert.equal(
    matchesFeatureFilters(feature, { ...filters, minAmenityCount: 9 }),
    false,
  );
});

test("active filter count includes amenity-name query", () => {
  const filters = {
    operator: "IONITY",
    minPower: 150,
    minAmenityCount: 6,
    amenities: new Set(["amenity_restaurant", "amenity_toilets"]),
    amenityNameQuery: "McDonald",
    currentlyOpenOnly: true,
  };

  assert.equal(countActiveFilters(filters), 7);
});

test("availability filter keeps stations with at least one free charging point", () => {
  const baseFilters = {
    minPower: 50,
    amenities: new Set(),
    amenityNameQuery: "",
    availableOnly: true,
  };
  const freeFeature = {
    properties: {
      max_power_kw: 150,
      occupancy_total_evses: 4,
      occupancy_available_evses: 1,
      occupancy_occupied_evses: 3,
    },
  };
  const occupiedFeature = {
    properties: {
      max_power_kw: 150,
      occupancy_total_evses: 4,
      occupancy_available_evses: 0,
      occupancy_occupied_evses: 4,
    },
  };
  const unknownFeature = {
    properties: {
      max_power_kw: 150,
      occupancy_total_evses: 0,
      occupancy_available_evses: 0,
    },
  };

  assert.equal(hasAvailableChargingPoint(freeFeature.properties), true);
  assert.equal(matchesFeatureFilters(freeFeature, baseFilters), true);
  assert.equal(matchesFeatureFilters(occupiedFeature, baseFilters), false);
  assert.equal(matchesFeatureFilters(unknownFeature, baseFilters), false);
  assert.equal(matchesFeatureFilters(unknownFeature, { ...baseFilters, availableOnly: false }), true);
});

test("availability filter supports live summary fields", () => {
  const feature = {
    properties: {
      max_power_kw: 150,
      live_total_evses: 2,
      live_available_evses: 1,
      live_occupied_evses: 1,
      live_source_observed_at: "2026-06-20T12:00:00Z",
    },
  };

  assert.equal(matchesFeatureFilters(feature, {
    minPower: 50,
    amenities: new Set(),
    availableOnly: true,
  }), true);
});

test("availability filter counts as an active default filter", () => {
  assert.equal(countActiveFilters({ minPower: 50, amenities: new Set(), availableOnly: true }), 2);
});

test("feature matcher filters for stations with a currently open amenity", () => {
  const feature = {
    properties: {
      max_power_kw: 150,
      amenity_examples: [
        { name: "Closed shop", opening_hours: "Mo-Fr 08:00-12:00" },
        { name: "Open cafe", opening_hours: "Mo-Su 08:00-20:00" },
      ],
    },
  };
  const filters = {
    minPower: 50,
    amenities: new Set(),
    amenityNameQuery: "",
    currentlyOpenOnly: true,
  };

  assert.equal(
    matchesFeatureFilters(feature, filters, { now: new Date("2026-01-10T10:00:00Z") }),
    true,
  );
  assert.equal(
    matchesFeatureFilters(feature, filters, { now: new Date("2026-01-10T20:00:00Z") }),
    false,
  );
});

test("active filter count includes the baseline fast-charger power constraint", () => {
  assert.equal(countActiveFilters({ minPower: 50, amenities: new Set() }), 1);
  assert.equal(countActiveFilters({ minPower: 150, amenities: new Set() }), 1);
  assert.equal(countActiveFilters({ minPower: 0, amenities: new Set() }), 0);
});
