import test from "node:test";
import assert from "node:assert/strict";

import {
  LOCATION_ERROR_PERMISSION_DENIED,
  LOCATION_ERROR_POSITION_UNAVAILABLE,
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
            latitude: 53.551086,
            longitude: 9.993682,
        },
      });
    },
  };

  const result = await requestBrowserLocation(geolocation);
  assert.deepEqual(result, {
    lat: 53.551086,
    lon: 9.993682,
    raw: {
      coords: {
              latitude: 53.551086,
            longitude: 9.993682,
      },
    },
  });
});

test("station list stays usable until a location is available", () => {
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

  assert.equal(waiting.blocksStationList, false);
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
  assert.equal(viewModel.blocksStationList, false);
  assert.equal(viewModel.title, "Standortfreigabe benötigt");
  assert.match(viewModel.message, /Aktiviere den Standortzugriff/);
});

test("unavailable browser position does not block the station list", () => {
  const viewModel = getLocationLookupViewModel({
    hasLocation: false,
    errorCode: LOCATION_ERROR_POSITION_UNAVAILABLE,
    geolocationSupported: true,
  });

  assert.equal(viewModel.blocksStationList, false);
  assert.equal(viewModel.actionLabel, "Erneut versuchen");
  assert.match(viewModel.message, /ohne Entfernungssortierung/);
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
