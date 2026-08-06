import assert from "node:assert/strict";
import test from "node:test";

import {
  catalogStationDetailUrl,
  combineStationDetails,
  liveStationDetailUrl,
} from "./station-page.mjs";

test("builds a catalog detail URL for the query-driven station page", () => {
  assert.equal(
    catalogStationDetailUrl(
      "https://live-eu.woladen.de/",
      "AT:econtrol:at-cam-emaltacamp*001",
    ),
    "https://live-eu.woladen.de/v1/catalog/stations/at%3Aecontrol%3Aat-cam-emaltacamp*001",
  );
  assert.equal(catalogStationDetailUrl("https://example.test", ""), "");
});

test("station detail page also fetches the canonical live station record", () => {
  assert.equal(
    liveStationDetailUrl(
      "https://live-eu.woladen.de/",
      "BE:be_energyvision_ocpi_locations:1f00b0f8-481a-6714-b53d-06f945fc8557",
    ),
    "https://live-eu.woladen.de/v1/stations/be%3Abe_energyvision_ocpi_locations%3A1f00b0f8-481a-6714-b53d-06f945fc8557",
  );
});

test("station detail keeps catalogue facts while displaying live EVSE fields", () => {
  const combined = combineStationDetails(
    {
      station: { station_id: "be:provider:station", max_power_kw: 300 },
      chargers: [{ connector_type: "IEC_62196_T2_COMBO", max_power_kw: 300 }],
    },
    {
      station: {
        station_id: "be:provider:station",
        source_observed_at: "2026-08-06T19:35:35Z",
      },
      evses: [{
        provider_evse_id: "BENRGEEDR016711",
        operational_status: "AVAILABLE",
        availability_status: "free",
      }],
    },
  );

  assert.equal(combined.station.max_power_kw, 300);
  assert.deepEqual(combined.chargers, [{
    provider_evse_id: "BENRGEEDR016711",
    operational_status: "AVAILABLE",
    availability_status: "free",
  }]);
  assert.equal(combined.live_station.source_observed_at, "2026-08-06T19:35:35Z");
});
