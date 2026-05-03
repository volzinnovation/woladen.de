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
  };

  assert.equal(countActiveFilters(filters), 5);
});

test("active filter count treats under-50 kW as an explicit filter change", () => {
  assert.equal(countActiveFilters({ minPower: 50, amenities: new Set() }), 0);
  assert.equal(countActiveFilters({ minPower: 0, amenities: new Set() }), 1);
});
