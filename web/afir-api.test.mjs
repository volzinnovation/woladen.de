import assert from "node:assert/strict";
import test from "node:test";

import {
  afirChargingPointCount,
  afirCountryDisplayName,
  afirPointCoverage,
  afirStationDetailUrl,
  buildAfirCurrentUrl,
  createAfirDataSource,
  nextAfirLevel,
  normalizeAfirApiBaseUrl,
  previousAfirLevel,
  scopeFromAfirDimensions,
} from "./afir-api.mjs";

test("synthetic EU27 aggregate does not enter Intl region lookup", () => {
  const displayNames = {
    of() {
      throw new RangeError("invalid_argument");
    },
  };

  assert.equal(
    afirCountryDisplayName("EU27", displayNames),
    "EU27",
  );
  assert.equal(afirCountryDisplayName("", displayNames), "");
});

test("AFIR hierarchy follows country to individual point", () => {
  assert.equal(nextAfirLevel("country"), "provider");
  assert.equal(nextAfirLevel("provider"), "operator");
  assert.equal(nextAfirLevel("operator"), "location");
  assert.equal(nextAfirLevel("location"), "point");
  assert.equal(nextAfirLevel("point"), "");
  assert.equal(previousAfirLevel("point"), "location");
});

test("AFIR query keeps the selected hierarchy scope", () => {
  const url = new URL(
    buildAfirCurrentUrl(
      "https://live-eu.woladen.de/v1/afir-compliance/",
      {
        level: "point",
        scope: {
          country_code: "BE",
          provider_uid: "be:road",
          operator_id: "operator",
          location_id: "location",
        },
        limit: 50,
        offset: 100,
        sort: "charging_point_count",
        direction: "desc",
        search: "energy",
      },
    ),
  );

  assert.equal(url.pathname, "/v1/afir-compliance/current");
  assert.equal(url.searchParams.get("group_by"), "point");
  assert.equal(url.searchParams.get("country_code"), "BE");
  assert.equal(url.searchParams.get("provider_uid"), "be:road");
  assert.equal(url.searchParams.get("location_id"), "location");
  assert.equal(url.searchParams.get("limit"), "50");
  assert.equal(url.searchParams.get("offset"), "100");
  assert.equal(url.searchParams.get("sort"), "charging_point_count");
  assert.equal(url.searchParams.get("direction"), "desc");
  assert.equal(url.searchParams.get("search"), "energy");
});

test("AFIR short search terms are not sent as name queries", () => {
  const url = new URL(
    buildAfirCurrentUrl("https://example.test/v1/afir-compliance", {
      search: "de",
    }),
  );
  assert.equal(url.searchParams.has("search"), false);
});

test("AFIR data source uses public read-only endpoints", async () => {
  const calls = [];
  const source = createAfirDataSource({
    baseUrl: "https://example.test/v1/afir-compliance/",
    fetchImpl: async (url, options) => {
      calls.push({ url, options });
      return {
        ok: true,
        async json() {
          return { groups: [] };
        },
      };
    },
  });

  await source.loadMeta();
  await source.loadGroups({ level: "country" });

  assert.equal(normalizeAfirApiBaseUrl(source.baseUrl), source.baseUrl);
  assert.equal(calls[0].url, "https://example.test/v1/afir-compliance/meta");
  assert.match(calls[1].url, /group_by=country/);
  assert.equal(calls[0].options.cache, "no-store");
});

test("dimensions become filters without empty identifiers", () => {
  assert.deepEqual(
    scopeFromAfirDimensions({
      country_code: "BE",
      provider_uid: "be:road",
      operator_id: "",
      location_id: "site-1",
      detail_station_id: "catalog-station-for-display-only",
      source_uid: "not-a-hierarchy-filter",
    }),
    {
      country_code: "BE",
      provider_uid: "be:road",
      location_id: "site-1",
    },
  );
});

test("point results link only through the catalog detail station identity", () => {
  assert.equal(
    afirStationDetailUrl({
      detail_station_id: "be:be_energyvision_ocpi_locations:1f00b0f8-481a-6714-b53d-06f945fc8557",
    }),
    "./station.html?station=be%3Abe_energyvision_ocpi_locations%3A1f00b0f8-481a-6714-b53d-06f945fc8557",
  );
  assert.equal(
    afirStationDetailUrl({ detail_station_id: "BE:road/site 1" }),
    "./station.html?station=BE%3Aroad%2Fsite%201",
  );
  assert.equal(afirStationDetailUrl({ station_id: "raw-station" }), "");
  assert.equal(afirStationDetailUrl({ point_id: "point-only" }), "");
  assert.equal(
    afirStationDetailUrl({
      station_id: "raw-station",
      point_id: "raw-point",
    }),
    "",
  );
});

test("AFIR aggregate metrics use explicit deduplicated charging-point counts", () => {
  assert.equal(
    afirChargingPointCount({
      entity_counts: {
        charging_point_count: 27,
        point_count: 99,
      },
    }),
    27,
  );
  assert.equal(
    afirChargingPointCount({
      entity_counts: { point_count: 12 },
    }),
    12,
  );
  assert.deepEqual(
    afirPointCoverage({
      coverage_pct: 99,
      charging_point_coverage: {
        required_point_field_count: 54,
        present_point_field_count: 27,
        coverage_pct: 50,
        coverage_state: "partial",
      },
    }),
    {
      required_point_field_count: 54,
      present_point_field_count: 27,
      coverage_pct: 50,
      coverage_state: "partial",
    },
  );
});
