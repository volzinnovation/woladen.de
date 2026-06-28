import test from "node:test";
import assert from "node:assert/strict";

import {
  getStationMarkerColor,
  hasLiveStationData,
  stationMarkerViewModel,
} from "./map-markers.mjs";

test("station marker color follows amenity tiers", () => {
  assert.equal(getStationMarkerColor({ amenities_total: 12 }), "#f59e0b");
  assert.equal(getStationMarkerColor({ amenities_total: 6 }), "#94a3b8");
  assert.equal(getStationMarkerColor({ amenities_total: 1 }), "#b45309");
  assert.equal(getStationMarkerColor({ amenities_total: 0 }), "#64748b");
});

test("live station marker state requires live fields", () => {
  assert.equal(hasLiveStationData({ live_source_observed_at: "2026-05-11T10:00:00Z" }), true);
  assert.equal(hasLiveStationData({ live_total_evses: 4 }), true);
  assert.equal(hasLiveStationData({ occupancy_total_evses: 4 }), false);
  assert.equal(hasLiveStationData({}), false);
});

test("station marker view model separates live and static states", () => {
  assert.deepEqual(
    stationMarkerViewModel({
      operator: "FastCharge",
      city: "Berlin",
      amenities_total: 7,
      live_fetched_at: "2026-05-11T10:00:00Z",
    }),
    {
      color: "#94a3b8",
      hasLiveData: true,
      outOfOrderProbabilityStatus: "",
      state: "live",
      title: "FastCharge, Berlin",
      className: "station-marker-live",
    },
  );

  const staticMarker = stationMarkerViewModel({ operator: "StaticCharge" });
  assert.equal(staticMarker.state, "static");
  assert.equal(staticMarker.title, "StaticCharge");
});

test("station marker view model flags out-of-order probability risk", () => {
  const marker = stationMarkerViewModel({
    operator: "RiskCharge",
    occupancy_out_of_order_probability: 0.51,
  });

  assert.equal(marker.outOfOrderProbabilityStatus, "mostly_broken");
  assert.equal(marker.title, "RiskCharge · Meist defekt");
  assert.equal(marker.className, "station-marker-static station-marker-outage-risk station-marker-outage-risk-mostly_broken");
});
