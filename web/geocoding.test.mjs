import test from "node:test";
import assert from "node:assert/strict";

import {
  buildGeocoderApiUrl,
  normalizeGeocodePayload,
  normalizeGeocoderApiBaseUrl,
  queryGeocoderApiBaseUrl,
  resolveGeocoderApiBaseUrl,
} from "./geocoding.mjs";

test("geocoder API base URL resolves to the live EU production endpoint", () => {
  assert.equal(
    resolveGeocoderApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/",
    }),
    "https://live-eu.woladen.de/v1/geocode",
  );
  assert.equal(
    resolveGeocoderApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/",
    }),
    "https://live-eu.woladen.de/v1/geocode",
  );
  assert.equal(
    resolveGeocoderApiBaseUrl({
      configuredValue: "",
      locationHref: "https://example.github.io/woladen.de/",
    }),
    "",
  );
});

test("geocoder API base URL can be configured and overridden", () => {
  assert.equal(
    normalizeGeocoderApiBaseUrl(" ./api/geocode/ ", "http://127.0.0.1:4177/"),
    "http://127.0.0.1:4177/api/geocode",
  );
  assert.equal(
    queryGeocoderApiBaseUrl(
      "http://127.0.0.1:4177/?geocoderApiBaseUrl=http://127.0.0.1:9000/geocode",
    ),
    "http://127.0.0.1:9000/geocode",
  );
  assert.equal(
    normalizeGeocoderApiBaseUrl("javascript:alert(1)", "https://woladen.de/"),
    "",
  );
});

test("buildGeocoderApiUrl writes autocomplete query parameters", () => {
  assert.equal(
    buildGeocoderApiUrl("https://live-eu.woladen.de/v1/geocode", "autocomplete", {
      q: "Berlin",
      lat: 52.5,
      lon: 13.4,
      limit: 5,
    }),
    "https://live-eu.woladen.de/v1/geocode/autocomplete?q=Berlin&lat=52.5&lon=13.4&limit=5",
  );
});

test("normalizeGeocodePayload keeps valid autocomplete results only", () => {
  const payload = normalizeGeocodePayload({
    ok: true,
    provider: "openrouteservice",
    attribution: "ORS attribution",
    source_url: "https://openrouteservice.org/dev/#/api-docs/geocode/autocomplete/get",
    results: [
      {
        id: "geonames:region:2950157",
        label: "Berlin, Germany",
        name: "Berlin",
        lat: 52.5,
        lon: 13.41667,
        country_code: "DEU",
        confidence: null,
      },
      { label: "Bad coordinate", lat: "x", lon: 13.4 },
    ],
  });

  assert.equal(payload.ok, true);
  assert.equal(payload.provider, "openrouteservice");
  assert.equal(payload.results.length, 1);
  assert.equal(payload.results[0].label, "Berlin, Germany");
  assert.equal(payload.results[0].countryCode, "DEU");
});
