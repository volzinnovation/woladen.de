import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));

test("station marker tooltips escape provider-controlled labels before Leaflet renders HTML", async () => {
  const source = await readFile(join(__dirname, "app.js"), "utf8");

  assert.equal(source.includes("bindTooltip(formatStationMarkerLabel"), false);
  assert.match(
    source,
    /bindStationMarker[\s\S]*bindTooltip\(escapeHtml\(formatStationMarkerLabel\(feature\)\)/,
  );
  assert.match(
    source,
    /renderDetailStationMarker[\s\S]*bindTooltip\(escapeHtml\(formatStationMarkerLabel\(feature\)\)/,
  );
});
