import test from "node:test";
import assert from "node:assert/strict";

import {
  buildLiveStationDetailPath,
  normalizeLiveStationId,
  normalizeLiveApiBaseUrl,
  queryGermanLiveApiBaseUrl,
  queryLiveApiBaseUrl,
  readLiveDynamicFields,
  resolveGermanLiveApiBaseUrl,
  resolveLiveApiBaseUrl,
} from "./live-api.mjs";

test("preserves provider-prefixed station IDs for live lookup and F1-F3 rendering", () => {
  const stationId = "be:be_energyvision_ocpi_locations:1f00b0f8-481a-6714-b53d-06f945fc8557";
  const evse = {
    station_id: stationId,
    operational_status: "AVAILABLE",
    availability_status: "free",
    price_display: "0,50 €/kWh",
  };

  assert.equal(normalizeLiveStationId(stationId), stationId);
  assert.equal(
    normalizeLiveStationId("BE:be_energyvision_ocpi_locations:1f00b0f8-481a-6714-b53d-06f945fc8557"),
    stationId,
  );
  assert.equal(
    buildLiveStationDetailPath("BE:be_energyvision_ocpi_locations:1f00b0f8-481a-6714-b53d-06f945fc8557"),
    "/v1/stations/be%3Abe_energyvision_ocpi_locations%3A1f00b0f8-481a-6714-b53d-06f945fc8557",
  );
  assert.deepEqual(readLiveDynamicFields(evse), {
    operationalStatus: "AVAILABLE",
    availabilityStatus: "free",
    priceDisplay: "0,50 €/kWh",
  });
});

test("keeps German station identifiers on the DE live identity", () => {
  assert.equal(normalizeLiveStationId("64251DE84FB0E7B3"), "DE:64251de84fb0e7b3");
  assert.equal(normalizeLiveStationId("de:64251DE84FB0E7B3"), "DE:64251de84fb0e7b3");
  assert.equal(normalizeLiveStationId("de:provider-specific-id"), "DE:provider-specific-id");
});

test("normalizeLiveApiBaseUrl trims and strips trailing slashes", () => {
  assert.equal(normalizeLiveApiBaseUrl(" http://127.0.0.1:8001/ "), "http://127.0.0.1:8001");
  assert.equal(normalizeLiveApiBaseUrl("not-a-url"), "");
});

test("reads independent F1, F2, and F3 fields from latest-state station responses", () => {
  const lookupPayload = {
    stations: [{
      station_id: "DE:64251de84fb0e7b3",
      availability_status: "free",
      price_display: "ab 0,69 €/kWh",
    }],
  };
  const detailPayload = {
    station: lookupPayload.stations[0],
    evses: [{
      provider_evse_id: "DE*ABC*E123",
      operational_status: " INOPERATION ",
      availability_status: " occupied ",
      price_display: " ab 0,69 €/kWh ",
    }],
  };

  assert.deepEqual(readLiveDynamicFields(lookupPayload.stations[0]), {
    operationalStatus: "",
    availabilityStatus: "free",
    priceDisplay: "ab 0,69 €/kWh",
  });
  assert.deepEqual(readLiveDynamicFields(detailPayload.evses[0]), {
    operationalStatus: "INOPERATION",
    availabilityStatus: "occupied",
    priceDisplay: "ab 0,69 €/kWh",
  });
});

test("queryLiveApiBaseUrl reads the explicit local override", () => {
  assert.equal(
    queryLiveApiBaseUrl("http://127.0.0.1:4173/?station=abc&liveApiBaseUrl=http://127.0.0.1:8001"),
    "http://127.0.0.1:8001",
  );
});

test("queryGermanLiveApiBaseUrl reads the explicit German backend override", () => {
  assert.equal(
    queryGermanLiveApiBaseUrl("http://127.0.0.1:4173/?deLiveApiBaseUrl=http://127.0.0.1:8002"),
    "http://127.0.0.1:8002",
  );
});

test("query live API helpers ignore production-host overrides", () => {
  assert.equal(
    queryLiveApiBaseUrl("https://woladen.de/?liveApiBaseUrl=https://attacker.test"),
    "",
  );
  assert.equal(
    queryGermanLiveApiBaseUrl("https://www.woladen.de/?deLiveApiBaseUrl=https://attacker.test"),
    "",
  );
});

test("resolveLiveApiBaseUrl prefers query override over configured and host defaults", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "https://live-eu.woladen.de",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveLiveApiBaseUrl ignores query override on production hosts", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/?liveApiBaseUrl=https://attacker.test",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveLiveApiBaseUrl keeps configured values when production query override is ignored", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "https://configured.example.test/api/",
      locationHref: "https://woladen.de/?liveApiBaseUrl=https://attacker.test",
      locationHostname: "woladen.de",
    }),
    "https://configured.example.test/api",
  );
});

test("resolveLiveApiBaseUrl keeps the live-eu production default for localhost without override", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/",
      locationHostname: "127.0.0.1",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveLiveApiBaseUrl allows local query override without explicit hostname", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://localhost:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveLiveApiBaseUrl maps the public site to live-eu", () => {
  assert.equal(
    resolveLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl maps German live data to live-eu", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl ignores German-specific query override on production hosts", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://www.woladen.de/?deLiveApiBaseUrl=https://attacker.test",
      locationHostname: "www.woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl ignores primary query fallback on production hosts", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://woladen.de/?liveApiBaseUrl=https://attacker.test",
      locationHostname: "woladen.de",
    }),
    "https://live-eu.woladen.de",
  );
});

test("resolveGermanLiveApiBaseUrl falls back to the primary configured backend", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "https://example.test/",
      locationHostname: "example.test",
      primaryBaseUrl: "http://127.0.0.1:8010",
    }),
    "http://127.0.0.1:8010",
  );
});

test("resolveGermanLiveApiBaseUrl keeps local single-backend override behavior", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8001",
  );
});

test("resolveGermanLiveApiBaseUrl prefers the German-specific override", () => {
  assert.equal(
    resolveGermanLiveApiBaseUrl({
      configuredValue: "",
      locationHref: "http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001&deLiveApiBaseUrl=http://127.0.0.1:8002",
      locationHostname: "127.0.0.1",
    }),
    "http://127.0.0.1:8002",
  );
});
