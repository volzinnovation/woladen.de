import test from "node:test";
import assert from "node:assert/strict";

import {
  buildOccupancyApiUrl,
  classifyOutOfOrderProbability,
  normalizeOccupancyApiBaseUrl,
  normalizeOutOfOrderProbabilityStatus,
  normalizeOccupancyProfile,
  queryOccupancyApiBaseUrl,
  resolveOccupancyApiBaseUrl,
} from "./occupancy-api.mjs";

test("normalizeOccupancyApiBaseUrl resolves same-origin harness URLs", () => {
  assert.equal(
    normalizeOccupancyApiBaseUrl(" ./api/commercial/v1/mobile/ ", "http://127.0.0.1:4177/commercial_web/"),
    "http://127.0.0.1:4177/commercial_web/api/commercial/v1/mobile",
  );
  assert.equal(normalizeOccupancyApiBaseUrl("file:///tmp/data.sqlite"), "");
});

test("queryOccupancyApiBaseUrl reads explicit override", () => {
  assert.equal(
    queryOccupancyApiBaseUrl(
      "http://127.0.0.1:4177/commercial_web/?occupancyApiBaseUrl=http://127.0.0.1:8011/v1/mobile",
    ),
    "http://127.0.0.1:8011/v1/mobile",
  );
});

test("resolveOccupancyApiBaseUrl prefers query override", () => {
  assert.equal(
    resolveOccupancyApiBaseUrl({
      configuredValue: "./api/commercial/v1/mobile",
      locationHref: "http://127.0.0.1:4177/commercial_web/?occupancyApiBaseUrl=http://127.0.0.1:8011/v1/mobile",
    }),
    "http://127.0.0.1:8011/v1/mobile",
  );
});

test("buildOccupancyApiUrl preserves proxy base path", () => {
  assert.equal(
    buildOccupancyApiUrl(
      "http://127.0.0.1:4177/commercial_web/api/commercial/v1/mobile",
      "/stations/de%3Anear/occupancy-chart",
    ),
    "http://127.0.0.1:4177/commercial_web/api/commercial/v1/mobile/stations/de%3Anear/occupancy-chart",
  );
});

test("normalizeOccupancyProfile fills all hourly buckets", () => {
  const profile = normalizeOccupancyProfile({
    data_available: true,
    station_id: "de:near",
    hourly_average_occupied: {
      "00:00": 1.25,
      "12:00": "2.5",
    },
  });

  assert.equal(profile.station_id, "de:near");
  assert.equal(profile.hourly_average_occupied["00:00"], 1.25);
  assert.equal(profile.hourly_average_occupied["01:00"], 0);
  assert.equal(profile.hourly_average_occupied["12:00"], 2.5);
});

test("normalizeOccupancyProfile accepts sibling chart payloads without diagnostic fields", () => {
  const profile = normalizeOccupancyProfile({
    station_id: "at:econtrol:at-sma-eat1313905",
    start_date: "2026-05-10",
    end_date: "2026-05-16",
    included_days: 7,
    hourly_average_occupied: {
      "08:00": 1.2,
      "18:00": 3.4,
    },
  });

  assert.equal(profile.station_id, "at:econtrol:at-sma-eat1313905");
  assert.equal(profile.hourly_average_occupied["08:00"], 1.2);
  assert.equal(profile.hourly_average_occupied["09:00"], 0);
  assert.equal(profile.hourly_average_occupied["18:00"], 3.4);
  assert.equal(profile.confidence_label, undefined);
});

test("classifyOutOfOrderProbability uses strict requested thresholds", () => {
  assert.equal(classifyOutOfOrderProbability(0.5001), "mostly_broken");
  assert.equal(classifyOutOfOrderProbability(0.5), "often_broken");
  assert.equal(classifyOutOfOrderProbability(0.2501), "often_broken");
  assert.equal(classifyOutOfOrderProbability(0.25), "sometimes_broken");
  assert.equal(classifyOutOfOrderProbability(0.0101), "sometimes_broken");
  assert.equal(classifyOutOfOrderProbability(0.01), "");
});

test("normalizeOccupancyProfile derives outage probability status from legacy share", () => {
  const profile = normalizeOccupancyProfile({
    data_available: true,
    station_id: "at:econtrol:at-sma-eat1313905",
    out_of_order_share: "0.32",
    hourly_average_occupied: {
      "12:00": 1,
    },
  });

  assert.equal(profile.out_of_order_probability, 0.32);
  assert.equal(profile.out_of_order_probability_status, "often_broken");
});

test("normalizeOutOfOrderProbabilityStatus preserves API status when valid", () => {
  assert.equal(normalizeOutOfOrderProbabilityStatus("mostly_broken", 0.02), "mostly_broken");
  assert.equal(normalizeOutOfOrderProbabilityStatus("unexpected", 0.02), "sometimes_broken");
});
