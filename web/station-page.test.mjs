import assert from "node:assert/strict";
import test from "node:test";

import { catalogStationDetailUrl } from "./station-page.mjs";

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
