export const MAP_KEYBOARD_PAN_PIXELS = 180;

export function getMapKeyboardAction(key, options = {}) {
  const panPixels = Number.isFinite(Number(options.panPixels))
    ? Number(options.panPixels)
    : MAP_KEYBOARD_PAN_PIXELS;
  const normalizedKey = normalizeMapKeyboardKey(key);

  if (normalizedKey === "a") {
    return { type: "pan", direction: "west", x: -panPixels, y: 0 };
  }
  if (normalizedKey === "s") {
    return { type: "pan", direction: "south", x: 0, y: panPixels };
  }
  if (normalizedKey === "w") {
    return { type: "pan", direction: "north", x: 0, y: -panPixels };
  }
  if (normalizedKey === "d") {
    return { type: "pan", direction: "east", x: panPixels, y: 0 };
  }
  if (normalizedKey === "q") {
    return { type: "zoom", direction: "in", delta: 1 };
  }
  if (normalizedKey === "e") {
    return { type: "zoom", direction: "out", delta: -1 };
  }
  if (normalizedKey === " " || normalizedKey === "space" || normalizedKey === "spacebar") {
    return { type: "search" };
  }
  return null;
}

export function performMapKeyboardAction(action, map, callbacks = {}) {
  if (!action || !map) {
    return false;
  }
  if (action.type === "pan" && typeof map.panBy === "function") {
    map.panBy([action.x, action.y], { animate: true, duration: 0.15 });
    return true;
  }
  if (action.type === "zoom" && action.delta > 0 && typeof map.zoomIn === "function") {
    map.zoomIn(action.delta, { animate: true });
    return true;
  }
  if (action.type === "zoom" && action.delta < 0 && typeof map.zoomOut === "function") {
    map.zoomOut(Math.abs(action.delta), { animate: true });
    return true;
  }
  if (action.type === "search" && typeof callbacks.focusSearchInput === "function") {
    callbacks.focusSearchInput();
    return true;
  }
  return false;
}

function normalizeMapKeyboardKey(key) {
  if (key === " ") {
    return " ";
  }
  return String(key || "").trim().toLowerCase();
}
