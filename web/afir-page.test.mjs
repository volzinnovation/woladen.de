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

test("AFIR point rows omit an unavailable static source update and omit unit counts", () => {
  assert.match(script, /sourceTimestamp\(group\.static_last_updated\)/);
  assert.match(script, /updatedAt !== UNASSESSED_LABEL/);
  assert.match(script, /state\.level === "point"/);
  assert.match(script, /afir-count-stations/);
  assert.match(script, /afir-count-points/);
  assert.match(script, /updateAggregateCountColumns/);
  assert.match(html, /data-sort-key="station_count"/);
  assert.match(html, /data-sort-key="charging_point_count"/);
});

test("AFIR group row visibility is applied after dynamic rows are rendered", () => {
  const renderGroupsStart = script.indexOf("function renderGroups()");
  const renderPaginationStart = script.indexOf("function renderPagination()");
  const renderGroups = script.slice(renderGroupsStart, renderPaginationStart);
  assert.match(
    renderGroups,
    /body\.append\(row\);[\s\S]*updateAggregateCountColumns\(\);[\s\S]*refreshSortableTable/,
  );
});

test("AFIR aggregate sorting is delegated to the complete backend result set", () => {
  assert.match(html, /id="afir-groups-table"[\s\S]*data-server-sort="true"/);
  assert.match(script, /state\.sortColumn/);
  assert.match(script, /state\.sortDirection/);
  assert.match(script, /sort: state\.sortColumn/);
  assert.match(script, /direction: state\.sortDirection/);
  assert.match(script, /table\.dataset\.serverSort === "true"/);
  assert.match(
    script,
    /tableSortState\.set\(table\.dataset\.tableKey,[\s\S]*updateSortHeaderState\(table, tableColumn, tableDirection\)/,
  );
});

test("AFIR country overview separates technical delivery status from field coverage", () => {
  assert.match(html, /Technische Umsetzung/);
  assert.match(html, /data-sort-key="data_provider_count"/);
  assert.match(html, /id="afir-country-implementation-notes"/);
  assert.match(html, /id="afir-country-implementation-footnotes"/);
  assert.match(script, /IMPLEMENTATION_STATUS_LABELS/);
  assert.match(script, /mixed_datex_ocpi: "Gemischt: DATEX II \/ OCPI"/);
  assert.match(script, /workaround_non_datex: "Ersatzweg, kein DATEX II"/);
  assert.match(script, /country_implementation_catalog/);
  assert.match(script, /data_provider_count/);
  assert.match(script, /source_count/);
  assert.match(script, /renderCountryImplementationNotes/);
});

test("AFIR name search is sent to the backend with a three-character minimum", () => {
  assert.match(html, /<span>Suche nach Name oder Kennung<\/span>/);
  assert.match(html, /id="afir-search-summary"[^>]+role="status"/);
  assert.match(html, /id="afir-search-clear"/);
  assert.match(script, /query\.set\("search", state\.searchQuery\)/);
  assert.match(script, /value\.length < 3/);
  assert.match(script, /search: state\.searchQuery/);
  assert.match(script, /renderSearchSummary\(\)/);
  assert.match(css, /\.afir-search-summary/);
});

test("AFIR breadcrumbs use hierarchy identifiers and display source names", () => {
  assert.match(script, /breadcrumbLabel\(state\.level, group\.dimensions\)/);
  assert.match(script, /country: "country_code"/);
  assert.match(script, /`via \$\{label\}`/);
  assert.match(script, /group\?\.source_uids/);
});

test("AFIR field ratios consistently use the current charging-point denominator", () => {
  assert.match(
    script,
    /previous:\s*\{[\s\S]*?denominatorKey:\s*"denominator"/,
  );
  assert.match(
    script,
    /both:\s*\{[\s\S]*?denominatorKey:\s*"denominator"/,
  );
  assert.match(
    script,
    /const ratio = denominator > 0 \? \(numerator \/ denominator\) \* 100 : null;/,
  );
  assert.doesNotMatch(
    script,
    /previous_distinct_release_point_count/,
  );
});

test("AFIR field rows show the field number without a redundant technical key", () => {
  assert.match(script, /identity\.append\(code\);/);
  assert.doesNotMatch(script, /className = "afir-field-key"/);
  assert.doesNotMatch(script, /identity\.append\(code, key\)/);
});

test("AFIR point field details show each retained current value in German", () => {
  assert.equal(
    (html.match(/data-sort-label="Wert">Wert<\/th>/g) || []).length,
    2,
  );
  assert.match(html, /class="afir-point-value-column"/);
  assert.match(script, /const showPointValues = state\.level === "point"/);
  assert.match(script, /showPointValues \? fieldValue\(field\.value\) : ""/);
  assert.match(script, /field\.value/);
  assert.match(css, /\.afir-field-value/);
});
