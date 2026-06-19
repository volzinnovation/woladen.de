import test from "node:test";
import assert from "node:assert/strict";

import {
  MAP_KEYBOARD_PAN_PIXELS,
  getMapKeyboardAction,
  performMapKeyboardAction,
} from "./map-keyboard.mjs";

test("maps game-controller movement keys to Leaflet pan offsets", () => {
  assert.deepEqual(getMapKeyboardAction("a"), {
    type: "pan",
    direction: "west",
    x: -MAP_KEYBOARD_PAN_PIXELS,
    y: 0,
  });
  assert.deepEqual(getMapKeyboardAction("s"), {
    type: "pan",
    direction: "south",
    x: 0,
    y: MAP_KEYBOARD_PAN_PIXELS,
  });
  assert.deepEqual(getMapKeyboardAction("w"), {
    type: "pan",
    direction: "north",
    x: 0,
    y: -MAP_KEYBOARD_PAN_PIXELS,
  });
  assert.deepEqual(getMapKeyboardAction("d"), {
    type: "pan",
    direction: "east",
    x: MAP_KEYBOARD_PAN_PIXELS,
    y: 0,
  });
});

test("maps zoom and search keys for map keyboard mode", () => {
  assert.deepEqual(getMapKeyboardAction("q"), {
    type: "zoom",
    direction: "in",
    delta: 1,
  });
  assert.deepEqual(getMapKeyboardAction("E"), {
    type: "zoom",
    direction: "out",
    delta: -1,
  });
  assert.deepEqual(getMapKeyboardAction(" "), { type: "search" });
  assert.deepEqual(getMapKeyboardAction("Spacebar"), { type: "search" });
  assert.equal(getMapKeyboardAction("Enter"), null);
});

test("allows callers to tune the map keyboard pan distance", () => {
  assert.deepEqual(getMapKeyboardAction("d", { panPixels: 64 }), {
    type: "pan",
    direction: "east",
    x: 64,
    y: 0,
  });
});

test("performs map keyboard actions against the Leaflet map surface", () => {
  const calls = [];
  const map = {
    panBy(offset, options) {
      calls.push(["panBy", offset, options]);
    },
    zoomIn(delta, options) {
      calls.push(["zoomIn", delta, options]);
    },
    zoomOut(delta, options) {
      calls.push(["zoomOut", delta, options]);
    },
  };
  let focusedSearch = false;

  assert.equal(performMapKeyboardAction(getMapKeyboardAction("a"), map), true);
  assert.equal(performMapKeyboardAction(getMapKeyboardAction("q"), map), true);
  assert.equal(performMapKeyboardAction(getMapKeyboardAction("e"), map), true);
  assert.equal(
    performMapKeyboardAction(getMapKeyboardAction(" "), map, {
      focusSearchInput() {
        focusedSearch = true;
      },
    }),
    true,
  );

  assert.deepEqual(calls, [
    ["panBy", [-MAP_KEYBOARD_PAN_PIXELS, 0], { animate: true, duration: 0.15 }],
    ["zoomIn", 1, { animate: true }],
    ["zoomOut", 1, { animate: true }],
  ]);
  assert.equal(focusedSearch, true);
});
