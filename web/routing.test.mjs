import test from "node:test";
import assert from "node:assert/strict";

import {
  normalizeRouteChargerResponse,
  normalizeRouteEndpoint,
  routeFiltersPayload,
} from "./routing.mjs";

test("route filter payload matches backend route API fields", () => {
  assert.deepEqual(
    routeFiltersPayload({
      operator: " IONITY ",
      minPower: 151,
      minAmenityCount: 5.6,
      amenities: new Set(["amenity_cafe", "bad", "amenity_cafe", "amenity_toilets"]),
      amenityNameQuery: " Bakery ",
      availableOnly: true,
      currentlyOpenOnly: true,
    }),
    {
      operator: "IONITY",
      min_power_kw: 151,
      min_amenities_total: 6,
      selected_amenities: ["amenity_cafe", "amenity_toilets"],
      amenity_name_query: "Bakery",
      available_only: false,
      currently_open_only: true,
    },
  );
});

test("route filter payload ignores current availability for later trips", () => {
  assert.equal(routeFiltersPayload({ availableOnly: true }).available_only, false);
  assert.equal(routeFiltersPayload({ available_only: true }).available_only, false);
});

test("route endpoint normalization accepts lon and lng", () => {
  assert.deepEqual(normalizeRouteEndpoint({ lat: "52.52", lng: "13.405", label: "Berlin" }), {
    lat: 52.52,
    lon: 13.405,
    label: "Berlin",
  });
  assert.equal(normalizeRouteEndpoint({ lat: "x", lon: 13.4 }), null);
});

test("route response normalization preserves station contract and route metadata separately", () => {
  const payload = normalizeRouteChargerResponse({
    route: {
      source: "openrouteservice",
      profile: "driving-car",
      distance_m: 12345.6,
      duration_s: 678.1,
      geometry: { type: "LineString", coordinates: [[13.4, 52.5], ["x", 0], [13.5, 52.6]] },
    },
    stations: [
      {
        station: { station_id: "station-1", operator_name: "Test" },
        route: {
          drive_distance_to_route_m: 830.4,
          route_detour_m: 1660.2,
          straight_line_distance_to_route_m: 420.2,
          route_position_m: 132000.2,
          nearest_route_point: { lat: "51.8", lon: "11.9" },
        },
      },
      { route: { drive_distance_to_route_m: 20 } },
    ],
    query: { min_amenities_total: 6 },
    source: "open_static.sqlite3+openrouteservice",
  });

  assert.deepEqual(payload.route.geometry.coordinates, [[13.4, 52.5], [13.5, 52.6]]);
  assert.equal(payload.route.distance_m, 12346);
  assert.equal(payload.stations.length, 1);
  assert.deepEqual(payload.stations[0].station, { station_id: "station-1", operator_name: "Test" });
  assert.deepEqual(payload.stations[0].route, {
    drive_distance_to_route_m: 830,
    route_detour_m: 1660,
    straight_line_distance_to_route_m: 420,
    route_position_m: 132000,
    nearest_route_point: { lat: 51.8, lon: 11.9 },
  });
});
