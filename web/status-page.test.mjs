import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";

const html = fs.readFileSync(new URL("./status.html", import.meta.url), "utf8");
const moduleText = fs.readFileSync(new URL("./status.mjs", import.meta.url), "utf8");

test("status page exposes the operational service sections", () => {
  for (const id of [
    "operational-components",
    "operational-country-body",
    "operational-provider-body",
    "operational-archive-body",
    "operational-detail-body",
  ]) {
    assert.match(html, new RegExp(`id=\"${id}\"`));
  }
  assert.match(moduleText, /commercial\/v1\/status\?view=operational/);
  assert.match(html, /Historische Fehler bleiben sichtbar/);
});

test("status page keeps the source-backed static data sections", () => {
  assert.match(moduleText, /open_static_summary\.json/);
  assert.match(html, /Tagesarchiv-Index|Top-Laender/);
  assert.match(moduleText, /renderOperationalStatus/);
});
