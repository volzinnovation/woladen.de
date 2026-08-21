import test from "node:test";
import assert from "node:assert/strict";

import {
  LIVE_OPEN_STATIC_SUMMARY_URL,
  formatBundleSourceTitle,
  formatLicenseStatus,
  normalizeBundleSources,
  normalizeMappedCountries,
  openStaticSummaryPaths,
} from "./open-static-ui.mjs";

test("prefers the live bundle summary and retains the generated file as fallback", () => {
  assert.deepEqual(openStaticSummaryPaths(), [
    LIVE_OPEN_STATIC_SUMMARY_URL,
    "./data/open_static_summary.json",
  ]);
  assert.deepEqual(openStaticSummaryPaths("https://preview.example.test/summary.json"), [
    "https://preview.example.test/summary.json",
    LIVE_OPEN_STATIC_SUMMARY_URL,
    "./data/open_static_summary.json",
  ]);
});

test("normalizes mapped countries from generated open-static summary metadata", () => {
  const countries = normalizeMappedCountries({
    countries: [
      { code: "NL", name: "Niederlande", station_count: 61_244, charger_count: 157_380 },
      { country_code: "AT", country_name: "Österreich", stations: 14_661, chargers: 38_771 },
      { country_code: "DE", country_name: "Deutschland", stations: 72_155, chargers: 197_527 },
      { code: "ES", name: "Spanien", stationCount: 12_237, chargerCount: 36_432 },
    ],
  });

  assert.deepEqual(countries, [
    { code: "DE", name: "Deutschland", stationCount: 72_155, chargerCount: 197_527, fastStationCount: 0 },
    { code: "NL", name: "Niederlande", stationCount: 61_244, chargerCount: 157_380, fastStationCount: 0 },
    { code: "AT", name: "Österreich", stationCount: 14_661, chargerCount: 38_771, fastStationCount: 0 },
    { code: "ES", name: "Spanien", stationCount: 12_237, chargerCount: 36_432, fastStationCount: 0 },
  ]);
});

test("normalizes source rows and formats license review status", () => {
  const sources = normalizeBundleSources({
    sources: [
      {
        country_code: "OSM",
        source_uid: "OSM",
        display_name: "OpenStreetMap",
        source_url: "https://www.openstreetmap.org/",
        license: "ODbL-1.0",
      },
      {
        country_code: "CY",
        source_uid: "cy_traffic4cyprus_seed",
        display_name: "CY Traffic4Cyprus/FixCyprus",
        source_url: "https://fixcyprus.cy/gnosis/open/api/nap/datasets/electric_vehicle_chargers/",
        license: "source_terms_pending_human_publication_review_2026-05-03",
      },
    ],
  });

  assert.equal(sources[0].countryCode, "CY");
  assert.equal(
    formatBundleSourceTitle(sources[0]),
    "CY: Traffic4Cyprus/FixCyprus",
  );
  assert.equal(
    formatLicenseStatus(sources[0].license),
    "Quellbedingungen vor Veröffentlichung im Human Review (2026-05-03)",
  );
  assert.equal(formatLicenseStatus(sources[1].license), "ODbL 1.0");
});
