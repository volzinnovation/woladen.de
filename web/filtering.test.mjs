import test from "node:test";
import assert from "node:assert/strict";

import {
  countActiveFilters,
  matchesAmenityNameQuery,
  matchesFeatureFilters,
} from "./filtering.mjs";

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
      amenity_fast_food: 2,
      amenity_examples: [{ name: "McDonald's" }],
    },
  };
  const filters = {
    operator: "EnBW",
    minPower: 150,
    amenities: new Set(["amenity_fast_food"]),
    amenityNameQuery: "McDonald",
  };

  assert.equal(matchesFeatureFilters(feature, filters), true);
  assert.equal(
    matchesFeatureFilters(feature, { ...filters, amenityNameQuery: "Subway" }),
    false,
  );
});

test("active filter count includes amenity-name query", () => {
  const filters = {
    operator: "IONITY",
    minPower: 150,
    amenities: new Set(["amenity_restaurant", "amenity_toilets"]),
    amenityNameQuery: "McDonald",
    currentlyOpenOnly: true,
  };

  assert.equal(countActiveFilters(filters), 6);
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

test("active filter count treats under-50 kW as an explicit filter change", () => {
  assert.equal(countActiveFilters({ minPower: 50, amenities: new Set() }), 0);
  assert.equal(countActiveFilters({ minPower: 0, amenities: new Set() }), 1);
});
