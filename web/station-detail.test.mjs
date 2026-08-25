import test from "node:test";
import assert from "node:assert/strict";

import {
  distinctStationOperatorName,
  queryStationPagePath,
} from "./station-detail.mjs";

test("query station page path includes a normalized station id", () => {
  assert.equal(queryStationPagePath(" DE:station-1 "), "./station.html?station=DE%3Astation-1");
});

test("detail operator name appears only when it differs from the station name", () => {
  assert.equal(
    distinctStationOperatorName({ station_name: "City Garage", operator_name: "IONITY" }),
    "IONITY",
  );
  assert.equal(
    distinctStationOperatorName({ station_name: "IONITY", operator_name: " IONITY " }),
    "",
  );
  assert.equal(
    distinctStationOperatorName({ station_name: "City Garage", operator_name: "" }),
    "",
  );
  assert.equal(
    distinctStationOperatorName({ operator_name: "IONITY" }),
    "",
  );
});

test("legacy detail properties use operator when operator_name is absent", () => {
  assert.equal(
    distinctStationOperatorName({ station_name: "City Garage", operator: "IONITY" }),
    "IONITY",
  );
});
