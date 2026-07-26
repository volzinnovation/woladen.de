import assert from "node:assert/strict";
import test from "node:test";

import {
  parseEmbeddedStationDetailPayload,
  staticStationPagePath,
} from "./station-detail.mjs";

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
