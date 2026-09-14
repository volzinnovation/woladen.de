import test from "node:test";
import assert from "node:assert/strict";

import {
  compareOperatorNames,
  countActiveFilters,
  hasAvailableChargingPoint,
  matchesAmenityNameQuery,
  matchesFeatureFilters,
  resolveOperatorGroupId,
} from "./filtering.mjs";
import { parseStoredFilterSettings, serializeStoredFilterSettings } from "./filter-settings.mjs";

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

test("feature matcher uses canonical operator group IDs when supplied", () => {
  const feature = {
    properties: {
      operator: "IONITY",
      operator_group_ids: ["ionity"],
      max_power_kw: 150,
    },
  };

  assert.equal(matchesFeatureFilters(feature, {
    operator: "ionity",
    minPower: 50,
    amenities: new Set(),
  }), true);
  assert.equal(matchesFeatureFilters(feature, {
    operator: "other-operator",
    minPower: 50,
    amenities: new Set(),
  }), false);
});

test("brand filters match canonical groups across company names, CPO codes and countries", () => {
  const stations = [
    ["ionity", "IONITY GmbH", "DE", "DE:ION"],
    ["ionity", "IONITY Holding GmbH & Co. KG", "NL", "NL:IOY"],
    ["enbw", "EnBW mobility+ AG & Co. KG", "DE", "DE:EBW"],
    ["enbw", "EnBW Energie Baden-Württemberg AG", "AT", "DE:EBW"],
    ["tesla", "Tesla Belgium BV", "BE", "BE:TSL"],
    ["tesla", "Tesla Motors Netherlands B.V.", "NL", "NL:TSL"],
  ];

  for (const [group, operator, country, key] of stations) {
    for (const selected of ["ionity", "enbw", "tesla"]) {
      const feature = {
        properties: {
          operator,
          country_code: country,
          operator_keys: [key],
          operator_group_ids: [group],
          max_power_kw: 150,
        },
      };
      assert.equal(matchesFeatureFilters(feature, { operator: selected }), selected === group,
        `${selected} / ${operator} / ${key}`);
    }
  }
});

test("singular operator groups work and canonical metadata takes precedence over display names", () => {
  const filters = { operator: "ionity" };
  const properties = { max_power_kw: 150, operator: "different display name", operator_group_id: " ionity " };
  assert.equal(matchesFeatureFilters({ properties }, filters), true);
  assert.equal(matchesFeatureFilters({ properties: { ...properties, operator_group_ids: [] } }, filters), true);
  assert.equal(matchesFeatureFilters({ properties: {
    ...properties,
    operator: "ionity",
    operator_group_ids: ["tesla"],
  } }, filters), false);
  assert.equal(matchesFeatureFilters({ properties: {
    ...properties,
    operator: "ionity",
    operator_group_id: "tesla",
  } }, filters), false);
});

test("saved company names and previous group IDs migrate through catalog aliases", () => {
  const operators = [
    { id: "ionity", name: "IONITY", aliases: ["IONITY GmbH"] },
    { id: "enbw", name: "EnBW", aliases: ["EnBW mobility+ AG & Co. KG", "enbw-mobility-ag-co-kg"] },
    { id: "tesla", name: "Tesla", aliases: ["Tesla Belgium BV", "tesla-belgium-bv"] },
  ];
  for (const entry of operators) {
    for (const selection of [entry.id, entry.name, ...entry.aliases]) {
      const operator = resolveOperatorGroupId(` ${selection.toUpperCase()} `, operators);
      const restored = parseStoredFilterSettings(serializeStoredFilterSettings({ operator }));
      assert.equal(restored.operator, entry.id);
    }
  }
  assert.equal(resolveOperatorGroupId("", operators), "");
  assert.equal(resolveOperatorGroupId("unknown-operator", operators), "");
  assert.equal(resolveOperatorGroupId("ionity", null), "ionity");
  assert.equal(resolveOperatorGroupId("unknown-operator", []), "unknown-operator");
});

test("canonical operator IDs take priority over another operator's alias", () => {
  assert.equal(resolveOperatorGroupId("IONITY", [
    { id: "other", name: "Other", aliases: ["IONITY"] },
    { id: "ionity", name: "IONITY", aliases: [] },
  ]), "ionity");
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
