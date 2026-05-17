import test from "node:test";
import assert from "node:assert/strict";

import {
  LOCATION_ERROR_PERMISSION_DENIED,
  LOCATION_PERMISSION_DENIED,
  getLocationLookupViewModel,
  mapGeolocationError,
  requestBrowserLocation,
  shouldAttemptStartupLocation,
} from "./location.mjs";

test("browser location lookup resolves coordinates from geolocation", async () => {
  const geolocation = {
    getCurrentPosition(success) {
      success({
        coords: {
          latitude: 52.52,
          longitude: 13.405,
        },
      });
    },
  };

  const result = await requestBrowserLocation(geolocation);
  assert.deepEqual(result, {
    lat: 52.52,
    lon: 13.405,
    raw: {
      coords: {
        latitude: 52.52,
        longitude: 13.405,
      },
    },
  });
});

test("station list stays blocked until a location is available", () => {
  const waiting = getLocationLookupViewModel({
    hasLocation: false,
    permissionState: "prompt",
    geolocationSupported: true,
  });
  const ready = getLocationLookupViewModel({
    hasLocation: true,
    permissionState: "granted",
    geolocationSupported: true,
  });

  assert.equal(waiting.blocksStationList, true);
  assert.equal(waiting.actionLabel, "Standort freigeben");
  assert.equal(ready.blocksStationList, false);
});

test("missing location access shows the denied-permission message", () => {
  const error = mapGeolocationError({ code: 1 });
  const viewModel = getLocationLookupViewModel({
    hasLocation: false,
    permissionState: LOCATION_PERMISSION_DENIED,
    errorCode: LOCATION_ERROR_PERMISSION_DENIED,
    geolocationSupported: true,
  });

  assert.equal(error.code, LOCATION_ERROR_PERMISSION_DENIED);
  assert.equal(viewModel.title, "Standortfreigabe benötigt");
  assert.match(viewModel.message, /Aktiviere den Standortzugriff/);
});

test("startup location request runs unless access is blocked or already resolved", () => {
  assert.equal(shouldAttemptStartupLocation({ permissionState: "prompt" }), true);
  assert.equal(shouldAttemptStartupLocation({ permissionState: "unknown" }), true);
  assert.equal(shouldAttemptStartupLocation({ permissionState: "granted" }), true);
  assert.equal(shouldAttemptStartupLocation({ permissionState: "denied" }), false);
  assert.equal(shouldAttemptStartupLocation({ permissionState: "unsupported" }), false);
  assert.equal(shouldAttemptStartupLocation({ geolocationSupported: false }), false);
  assert.equal(shouldAttemptStartupLocation({ alreadyRequested: true }), false);
  assert.equal(shouldAttemptStartupLocation({ hasLocation: true }), false);
});
