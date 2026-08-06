import assert from "node:assert/strict";
import test from "node:test";

import {
  parseEmbeddedStationDetailPayload,
  queryStationPagePath,
  shouldPreferStaticStationDetail,
  staticStationPagePath,
} from "./station-detail.mjs";

test("builds a query-driven public station page URL", () => {
  assert.equal(
    queryStationPagePath("AT:econtrol:at-cam-emaltacamp*001"),
    "./station.html?station=AT%3Aecontrol%3Aat-cam-emaltacamp*001",
  );
  assert.equal(queryStationPagePath(""), "./station.html");
});

test("builds a public URL for percent-encoded station page filenames", () => {
  assert.equal(
    staticStationPagePath("BE:be_road:station-1"),
    "./station/BE/be_road%253Astation-1.html",
  );
  assert.equal(
    staticStationPagePath("DE:47d719c1b62c750"),
    "./station/DE/47d719c1b62c750.html",
  );
  assert.doesNotMatch(staticStationPagePath("..:index"), /\.\.\//);
});

test("uses the catalog API before the query-driven station page", () => {
  assert.equal(
    shouldPreferStaticStationDetail(
      "be:be_energyvision_ocpi_locations:1f00b0f8-481a-6714-b53d-06f945fc8557",
    ),
    false,
  );
  assert.equal(
    shouldPreferStaticStationDetail("BE:be_road:station-1"),
    false,
  );
});

test("does not require a materialized station page for unbundled stations", () => {
  assert.equal(shouldPreferStaticStationDetail("DE:47d719c1b62c750"), false);
  assert.equal(shouldPreferStaticStationDetail("47d719c1b62c750"), false);
  assert.equal(
    shouldPreferStaticStationDetail(
      "DE:47d719c1b62c750",
      { hasBundledFeature: true },
    ),
    false,
  );
});

test("parses the station payload embedded in a generated detail page", () => {
  const payload = parseEmbeddedStationDetailPayload(`
    <script id="station-detail-data" type="application/json">
      {"schema_version":"station-static-detail-v1","station":{"station_id":"BE:provider:station"}}
    </script>
  `);

  assert.equal(payload.station.station_id, "BE:provider:station");
});

test("rejects generated pages without usable station data", () => {
  assert.throws(() => parseEmbeddedStationDetailPayload("<main></main>"), /station_detail_data_missing/);
  assert.throws(
    () => parseEmbeddedStationDetailPayload(
      '<script id="station-detail-data" type="application/json">{"station":null}</script>',
    ),
    /station_detail_data_invalid/,
  );
});
