import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const html = await readFile(
  new URL("./afir.html", import.meta.url),
  "utf8",
);
const css = await readFile(
  new URL("./afir.css", import.meta.url),
  "utf8",
);
const script = await readFile(
  new URL("./afir.mjs", import.meta.url),
  "utf8",
);

test("AFIR page states the complete approved F3 rule", () => {
  assert.match(
    html,
    /vollständigen, eindeutigen Basistarif/,
  );
  assert.match(
    html,
    /Eine Preisänderung ist nicht erforderlich/,
  );
  assert.match(
    html,
    /mehrere konkurrierende Basistarife/,
  );
  assert.match(
    html,
    /eine sachfremde Preisspanne erfüllen F3 nicht/,
  );
});

test("AFIR page exposes an immediate accessible loading progress rail", () => {
  assert.match(html, /id="afir-main"[^>]+aria-busy="true"/);
  assert.match(html, /id="afir-loading"[\s\S]+role="status"/);
  assert.match(html, /aria-live="polite"/);
  assert.match(html, /aria-atomic="true"/);
  assert.match(html, /role="progressbar"/);
  assert.match(html, /aria-valuemin="0"/);
  assert.match(html, /aria-valuemax="100"/);
  assert.doesNotMatch(
    html.match(/id="afir-progress"[\s\S]*?<\/div>/)?.[0] || "",
    /aria-valuenow=/,
  );
  assert.match(css, /\.afir-loading\[hidden\]\s*\{\s*display: none;/);
  assert.match(css, /data-mode="indeterminate"/);
  assert.match(css, /data-state="error"/);
});

test("AFIR requests drive visible success and persistent error progress states", () => {
  assert.match(script, /loadingIndicator\.start/);
  assert.match(script, /loadingIndicator\.received/);
  assert.match(script, /loadingIndicator\.succeed/);
  assert.match(script, /loadingIndicator\.fail/);
  assert.match(
    script,
    /Die Fehlermeldung bleibt direkt unterhalb sichtbar/,
  );
});

test("AFIR aggregate views expose dynamic fields before static fields", () => {
  assert.match(html, /id="afir-eu27-table"[^>]+afir-sortable/);
  assert.match(html, /id="afir-groups-table"[^>]+afir-sortable/);
  const dynamicStart = html.indexOf('id="afir-dynamic-field-title"');
  const staticStart = html.indexOf('id="afir-static-field-title"');
  assert.ok(dynamicStart >= 0);
  assert.ok(staticStart > dynamicStart);
  assert.match(html, /id="afir-dynamic-fields-table"[^>]+afir-sortable/);
  assert.match(html, /id="afir-static-fields-table"[^>]+afir-sortable/);
  assert.match(
    html.slice(dynamicStart, staticStart),
    /Ladepunkte vorher[\s\S]+Ladepunkte in beiden Nachrichten[\s\S]+Medianes Alter/,
  );
  assert.doesNotMatch(
    html.slice(staticStart),
    /Ladepunkte vorher|Ladepunkte in beiden Nachrichten|Medianes Alter/,
  );
});

test("AFIR tables provide accessible sorting and retain aggregate field links", () => {
  assert.match(script, /wireSortableTables\(\)/);
  assert.match(script, /afir-sort-button/);
  assert.match(script, /toggleTableSort/);
  assert.match(script, /afirAggregateFieldsUrl\(state\.level/);
  assert.match(script, /dynamicFields = fields\.filter/);
  assert.match(script, /staticFields = fields\.filter/);
});
