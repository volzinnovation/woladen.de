import assert from "node:assert/strict";
import test from "node:test";

import {
  afirStationDetailUrl,
  buildAfirCurrentUrl,
  createAfirDataSource,
  nextAfirLevel,
  normalizeAfirApiBaseUrl,
  previousAfirLevel,
  scopeFromAfirDimensions,
} from "./afir-api.mjs";

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
      source_uid: "not-a-hierarchy-filter",
    }),
    {
      country_code: "BE",
      provider_uid: "be:road",
      location_id: "site-1",
    },
  );
});

test("point results link to the existing station detail contract", () => {
  assert.equal(
    afirStationDetailUrl({ station_id: "BE:road/site 1" }),
    "./?station=BE%3Aroad%2Fsite%201",
  );
  assert.equal(afirStationDetailUrl({ point_id: "point-only" }), "");
});
