import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import {
  DEFAULT_WINDOW_DAYS,
  OVERVIEW_METRICS,
  buildCountryOverviewRows,
  buildDailySourceRows,
  buildManagementSubtitle,
  buildOverviewSeries,
  buildProviderProfileSeries,
  buildProviderReportMetrics,
  buildProviderRows,
  buildRollingProviderRows,
  buildStationRows,
  buildSummaryCards,
  compareTableSortValues,
  countryName,
  dateRangeForWindow,
  mergeRollingCountrySummary,
  normalizeCountryCode,
  normalizeManagementDate,
  normalizeProviderUid,
  providerDisplayName,
  rankedTableTitle,
  shouldShowOverviewChart,
  snapshotPathForDate,
  staticCatalogCountsForCountry,
  statusMetricsAreUsable,
  SUPPORTED_WINDOW_DAYS,
  windowLabel,
} from "./management.mjs";

test("normalizeManagementDate accepts ISO dates and rejects junk", () => {
  assert.equal(normalizeManagementDate("2026-04-17"), "2026-04-17");
  assert.equal(normalizeManagementDate("17.04.2026"), "");
  assert.equal(normalizeManagementDate(""), "");
});

test("snapshotPathForDate builds the dated management JSON path", () => {
  assert.equal(
    snapshotPathForDate("2026-04-17"),
    "./data/management/days/2026/04/17/snapshot.json",
  );
  assert.equal(snapshotPathForDate("not-a-date"), "");
});

test("buildManagementSubtitle omits the redundant date on country reports", () => {
  assert.equal(
    buildManagementSubtitle("2026-04-17"),
    "Störungen und Auslastung öffentlicher Ladestationen in angebundenen europäischen Ländern am Freitag, 17.04.2026",
  );
  assert.equal(
    buildManagementSubtitle("2026-04-17", "NL"),
    "Störungen und Auslastung öffentlicher Ladestationen in Niederlande",
  );
});

test("country and rolling-window helpers normalize management routes", () => {
  assert.equal(normalizeCountryCode(" de "), "DE");
  assert.equal(normalizeCountryCode("Germany"), "");
  assert.equal(normalizeProviderUid(" chargecloud "), "chargecloud");
  assert.equal(normalizeProviderUid("provider with spaces"), "");
  assert.equal(countryName("AT"), "Österreich");
  assert.deepEqual(dateRangeForWindow("2026-07-19", 28), {
    startDate: "2026-06-22",
    endDate: "2026-07-19",
  });
  assert.deepEqual(dateRangeForWindow("2026-07-19", 14), {
    startDate: "2026-07-06",
    endDate: "2026-07-19",
  });
  assert.deepEqual(dateRangeForWindow("2026-07-19", 1), {
    startDate: "2026-07-19",
    endDate: "2026-07-19",
  });
  assert.deepEqual(SUPPORTED_WINDOW_DAYS, [1, 7, 14, 28]);
  assert.equal(DEFAULT_WINDOW_DAYS, 1);
  assert.equal(windowLabel(1), "1 Tag");
  assert.equal(windowLabel(7), "1 Woche");
  assert.equal(windowLabel(14), "2 Wochen");
  assert.equal(windowLabel(28), "4 Wochen");
  assert.equal(windowLabel(90), "");
  assert.equal(shouldShowOverviewChart(1), false);
  assert.equal(shouldShowOverviewChart(7), true);
  assert.equal(shouldShowOverviewChart(14), true);
  assert.equal(shouldShowOverviewChart(28), true);
  assert.equal(shouldShowOverviewChart(90), false);
  assert.deepEqual(dateRangeForWindow("2026-07-19", 90), {
    startDate: "",
    endDate: "",
  });
});

test("ranked table title follows the active sort column", () => {
  assert.equal(rankedTableTitle("Top-Länder nach", "Auslastung"), "Top-Länder nach Auslastung");
  assert.equal(rankedTableTitle("Top-Länder nach", "Stationen"), "Top-Länder nach Stationen");
  assert.equal(rankedTableTitle("Top-Länder nach", ""), "Top-Länder nach Auslastung");
});

test("buildOverviewSeries returns ordered labels and values for the selected metric", () => {
  const series = buildOverviewSeries(
    {
      summary_series: [
        { snapshot_date: "2026-04-16", afir_stations_observed: 14000, stations_with_disruptions: 8 },
        { snapshot_date: "2026-04-17", afir_stations_observed: 14032, stations_with_disruptions: 11 },
      ],
    },
    "afir_stations_observed",
  );

  assert.deepEqual(series.labels, ["16.04.2026", "17.04.2026"]);
  assert.deepEqual(series.values, [14000, 14032]);
  assert.equal(series.label, "Stationen im Tagesarchiv");
});

test("overview metric options cover the management KPI cards", () => {
  assert.deepEqual(Object.keys(OVERVIEW_METRICS), [
    "afir_stations_observed",
    "delta_delivery_without_push_provider_count",
    "stations_with_disruptions",
    "disruptions_at_end_of_day",
    "high_utilization_stations",
    "archive_messages_total",
    "occupancy_share",
    "out_of_order_share",
  ]);
});

test("percentage overview metrics are charted as percentages", () => {
  const series = buildOverviewSeries(
    { summary_series: [{ snapshot_date: "2026-07-19", occupancy_share: 0.125 }] },
    "occupancy_share",
  );
  assert.deepEqual(series.values, [12.5]);
  assert.equal(series.kind, "percent");
});

test("buildSummaryCards exposes the public-facing station metrics", () => {
  const cards = buildSummaryCards({
    summary: {
      afir_stations_observed: 14032,
      daily_afir_stations_observed: 14032,
      stations_with_disruptions: 870,
      disruptions_at_end_of_day: 441,
      high_utilization_stations: 1872,
      archive_messages_total: 1050176,
      observations_total: 30970,
    },
  });

  assert.equal(cards[0].label, "Stationen im Tagesarchiv");
  assert.equal(cards[1].metrics[0].value, "870");
  assert.equal(cards[3].label, "Stationen mit hoher Auslastung");
  assert.equal(cards[4].label, "Empfangene AFIR-Meldungen");
  assert.equal(cards[4].metrics[0].value, "1.050.176");
});

test("buildSummaryCards exposes delta delivery warning when daily coverage can undercount", () => {
  const cards = buildSummaryCards({
    summary: {
      daily_afir_stations_observed: 26315,
      delta_delivery_without_push_provider_count: 1,
      stations_with_disruptions: 870,
      disruptions_at_end_of_day: 441,
      high_utilization_stations: 1872,
      observations_total: 30970,
    },
  });

  assert.equal(cards[1].label, "Delta-Anbieter ohne Push");
  assert.equal(cards[1].metrics[0].value, "1");
});

test("buildSummaryCards exposes PostgreSQL coverage and reliability metrics", () => {
  const cards = buildSummaryCards(
    {
      summary: {
        station_count: 35082,
        measured_station_count: 34781,
        observed_evses: 111137,
        measured_station_coverage: 0.99142,
        static_station_count: 72155,
        static_charger_count: 188200,
        measured_static_station_count: 34700,
        static_stations_without_disruptions: 25500,
        occupancy_share: 0.0963,
        day_occupancy_share: 0.11,
        night_occupancy_share: 0.08,
        out_of_order_share: 0.0741,
        day_out_of_order_share: 0.065,
        night_out_of_order_share: 0.083,
        stations_with_disruptions: 9262,
        disruptions_at_end_of_day: 3679,
        fully_out_of_service_stations: 1828,
        static_disruptions_at_end_of_day: 3600,
        static_fully_out_of_service_stations: 1800,
      },
    },
  );
  assert.equal(cards.length, 6);
  assert.deepEqual(
    cards.map((card) => card.label),
    [
      "Öffentliche Infrastruktur",
      "Stationen mit Live-Daten",
      "Stationen ohne Störung",
      "Am Tagesende",
      "Auslastung",
      "Störungsanteil",
    ],
  );
  assert.deepEqual(
    cards[0].metrics.map((metric) => [metric.label, metric.value]),
    [
      ["Ladepunkte", "188.200"],
      ["Ladestationen", "72.155"],
    ],
  );
  assert.equal(cards[1].metrics[0].value, "34.700");
  assert.match(cards[1].metrics[1].value, /48,1 %/);
  assert.equal(cards[2].metrics[0].value, "25.500");
  assert.deepEqual(
    cards[4].metrics.map((metric) => metric.label),
    ["Gesamt", "Tag", "Nacht"],
  );
  assert.deepEqual(
    cards[4].metrics.map((metric) => metric.value),
    ["9,6 %", "11,0 %", "8,0 %"],
  );
  assert.deepEqual(
    cards[5].metrics.map((metric) => metric.value),
    ["7,4 %", "6,5 %", "8,3 %"],
  );
  assert.deepEqual(
    cards[3].metrics.map((metric) => metric.value),
    ["3.600", "1.800"],
  );
  assert.equal(cards[0].detail, undefined);
  assert.deepEqual(cards[0].metrics.map((metric) => metric.reference), [1, 2]);
  assert.deepEqual(cards.slice(1, 3).map((card) => card.reference), [3, 4]);
  assert.deepEqual(cards[3].metrics.map((metric) => metric.reference), [7, 8]);
  assert.deepEqual(cards.slice(4).map((card) => card.reference), [5, 6]);
});

test("staticCatalogCountsForCountry reads the country catalog baseline", () => {
  assert.deepEqual(
    staticCatalogCountsForCountry(
      {
        countries: [
          { code: "DE", station_count: 72155, charger_count: 188200 },
          { code: "SE", station_count: 8922, charger_count: 26315 },
        ],
      },
      "se",
    ),
    { publicStationCount: 8922, publicChargerCount: 26315 },
  );
  assert.deepEqual(
    staticCatalogCountsForCountry({ countries: [] }, "SE"),
    { publicStationCount: null, publicChargerCount: null },
  );
});

test("management tables use window wording and omit removed classification columns", () => {
  const html = readFileSync(new URL("./management.html", import.meta.url), "utf8");
  const providerWindowTable = html.match(
    /<article id="management-provider-window-panel"[\s\S]*?<\/article>/,
  )?.[0];
  const brokenStationsTable = html.match(
    /<h2>Ladestationen mit den meisten Störungen<\/h2>[\s\S]*?<\/article>/,
  )?.[0];

  assert.ok(providerWindowTable);
  assert.doesNotMatch(providerWindowTable, />Einordnung</);
  assert.ok(brokenStationsTable);
  assert.doesNotMatch(brokenStationsTable, />Status</);
  assert.match(brokenStationsTable, /Berichtszeitraum/);
});

test("management detail layout keeps navigation with date controls and methodology at the end", () => {
  const html = readFileSync(new URL("./management.html", import.meta.url), "utf8");
  const script = readFileSync(new URL("./management.mjs", import.meta.url), "utf8");
  const styles = readFileSync(new URL("./styles.css", import.meta.url), "utf8");
  const detailControls = html.match(
    /<section class="management-panel management-controls">[\s\S]*?<\/section>/,
  )?.[0];
  const chartPanel = html.match(
    /<h2 id="management-overview-title">[\s\S]*?<\/article>/,
  )?.[0];

  assert.match(html, /Öffentliche Ladeinfrastruktur in Europa/);
  assert.ok(detailControls);
  assert.ok(
    detailControls.indexOf("management-detail-back") <
      detailControls.indexOf('id="management-date"'),
  );
  assert.doesNotMatch(detailControls, /management-overview-metric/);
  assert.ok(chartPanel);
  assert.match(chartPanel, /management-overview-metric/);
  assert.match(
    html,
    /<section id="management-overview-chart-section" class="management-grid" hidden>/,
  );
  assert.match(script, /overviewChartSection\.hidden = !showOverviewChart/);
  assert.match(
    styles,
    /\.management-control select \{[\s\S]*?appearance: none;[\s\S]*?padding-right: 2\.6rem;/,
  );
  assert.match(html, /Datenabdeckung nach Land/);
  assert.match(html, /id="management-country-coverage-body"/);
  assert.match(html, /colspan="3"[\s\S]*?scope="colgroup"[\s\S]*?>Auslastung</);
  assert.match(html, /colspan="2"[\s\S]*?scope="colgroup"[\s\S]*?>Stationen mit Live-Daten</);
  assert.match(html, /Leistung einzelner Anbieter\./);
  assert.doesNotMatch(html, />Betreiber</);
  assert.match(html, /Datenquellen am Berichtstag/);
  assert.match(html, />Datenumfang</);
  assert.match(html, />Übertragung</);
  assert.doesNotMatch(html, /Historische Abdeckung, Auslastung, Zuverlässigkeit/);
  assert.doesNotMatch(script, /provider_reports:\s*Array\.isArray\(providerReport/);
  assert.match(html, /config\.js\?v=20260724-management-window1/);
  assert.match(html, /management\.mjs\?v=20260726-management-columns/);
  assert.doesNotMatch(script, /confidencePhrase|Hohe Konfidenz|Mittlere Konfidenz|Niedrige Konfidenz/);
  assert.equal(
    (html.match(/<option value="1" selected>1 Tag<\/option>/g) || []).length,
    2,
  );
  assert.ok(
    html.indexOf('id="management-data-quality"') <
      html.indexOf("Fußnoten zu den Kennzahlen"),
  );
  assert.doesNotMatch(html, /PostgreSQL/);
  assert.doesNotMatch(script, /wird aus PostgreSQL geladen/);
  assert.doesNotMatch(script, /wird aus Datenbank geladen/);
  assert.match(script, /wird geladen/);
});

test("country overview rows merge dynamic coverage with the selected day report", () => {
  const rows = buildCountryOverviewRows(
    {
      countries: [
        { country_code: "DE", first_date: "2026-04-15", observed_days: 96 },
        { country_code: "AT", first_date: "2026-05-04", observed_days: 73 },
      ],
    },
    {
      rows: [
        {
          country_code: "AT",
          station_count: 14611,
          occupancy_share: 0.08,
          static_station_count: 15000,
          static_charger_count: 39000,
          measured_static_station_count: 12000,
          static_stations_without_disruptions: 10800,
        },
        {
          country_code: "DE",
          station_count: 35082,
          occupancy_share: 0.1,
          static_station_count: 72000,
          static_charger_count: 198000,
          measured_static_station_count: 35000,
          static_stations_without_disruptions: 28000,
        },
      ],
    },
    {
      countries: [
        { code: "AT", station_count: 14507, charger_count: 38347 },
        { code: "DE", station_count: 72155, charger_count: 197527 },
      ],
    },
  );
  assert.deepEqual(rows.map((row) => row.country_code), ["DE", "AT"]);
  assert.equal(rows[0].country_name, "Deutschland");
  assert.equal(rows[0].observed_days, 96);
  assert.equal(rows[1].station_count, 14611);
  assert.equal(rows[0].public_charger_count, 198000);
  assert.equal(rows[0].public_station_count, 72000);
  assert.equal(rows[0].live_station_count, 35000);
  assert.equal(rows[0].live_station_share, 35000 / 72000);
  assert.equal(rows[0].stations_without_disruptions, 28000);
  assert.equal(rows[0].stations_without_disruptions_share, 0.8);
});

test("country detail KPIs use the same rolling report metrics as the overview", () => {
  const snapshot = {
    summary: {
      measured_station_coverage: 0.99,
      static_charger_count: 197527,
      static_station_count: 72155,
      measured_static_station_count: 33104,
      static_stations_without_disruptions: 23024,
      occupancy_share: 0.107,
      out_of_order_share: 0.054,
      static_disruptions_at_end_of_day: 3245,
    },
  };
  const report = {
    start_date: "2026-07-17",
    end_date: "2026-07-23",
    rows: [
      {
        country_code: "DE",
        static_charger_count: 197527,
        static_station_count: 72155,
        measured_static_station_count: 33168,
        static_stations_without_disruptions: 15742,
        static_station_coverage: 0.459677,
        static_stations_without_disruptions_share: 0.474614,
        occupancy_share: 0.10634,
        day_occupancy_share: 0.13,
        night_occupancy_share: 0.08,
        out_of_order_share: 0.059356,
        day_out_of_order_share: 0.05,
        night_out_of_order_share: 0.07,
      },
    ],
  };

  const merged = mergeRollingCountrySummary(snapshot, report, "DE");
  assert.equal(merged.summary.measured_static_station_count, 33168);
  assert.equal(merged.summary.static_stations_without_disruptions, 15742);
  assert.equal(merged.summary.occupancy_share, 0.10634);
  assert.equal(merged.summary.out_of_order_share, 0.059356);
  assert.equal(merged.summary.static_disruptions_at_end_of_day, 3245);
  assert.equal(merged.summary.reporting_period_start_date, "2026-07-17");
  assert.equal(snapshot.summary.measured_static_station_count, 33104);
});

test("implausible national zero status mixes are shown as unavailable", () => {
  const finlandSummary = {
    station_count: 3024,
    observations_total: 85710,
    measured_seconds: 1176392298,
    occupied_seconds: 0,
    out_of_order_seconds: 0,
    unavailable_seconds: 0,
    occupancy_share: 0,
    day_occupancy_share: 0,
    night_occupancy_share: 0,
    out_of_order_share: 0,
    day_out_of_order_share: 0,
    night_out_of_order_share: 0,
    measured_station_coverage: 0.95,
    static_station_count: 3674,
    static_charger_count: 19430,
    measured_static_station_count: 2809,
    static_stations_without_disruptions: 2809,
  };

  assert.equal(statusMetricsAreUsable(finlandSummary), false);
  const cards = buildSummaryCards({ summary: finlandSummary });
  assert.equal(
    cards.find((card) => card.label === "Auslastung").metrics[0].value,
    "–",
  );
  assert.match(
    cards.find((card) => card.label === "Störungsanteil").detail,
    /keine auswertbaren/i,
  );

  const overviewRows = buildCountryOverviewRows(
    { countries: [{ country_code: "FI" }] },
    { rows: [{ country_code: "FI", ...finlandSummary }] },
  );
  assert.equal(overviewRows[0].occupancy_share, null);
  assert.equal(overviewRows[0].out_of_order_share, null);
  assert.equal(overviewRows[0].stations_without_disruptions, null);

  const operatorRows = buildRollingProviderRows(
    {
      group_by: "operator",
      rows: [
        {
          country_code: "FI",
          operator_name: "Large Network",
          station_count: 3024,
          status_change_count: 85710,
          measured_seconds: 1176392298,
          occupied_seconds: 0,
          out_of_order_seconds: 0,
          unavailable_seconds: 0,
          occupancy_share: 0,
          out_of_order_share: 0,
        },
      ],
    },
    { rows: [] },
  );
  assert.equal(operatorRows[0].occupancy_share, null);
  assert.equal(operatorRows[0].out_of_order_share, null);
});

test("provider profile series fills all local hours and converts shares to percentages", () => {
  const series = buildProviderProfileSeries({
    rows: [
      { local_hour: 0, occupancy_share: 0.125, out_of_order_share: 0.02 },
      { local_hour: 23, occupancy_share: 0.25, out_of_order_share: 0.04 },
    ],
  });
  assert.equal(series.labels.length, 24);
  assert.equal(series.labels[0], "00:00");
  assert.equal(series.labels.at(-1), "23:00");
  assert.equal(series.occupancy[0], 12.5);
  assert.equal(series.outages.at(-1), 4);
  assert.equal(series.occupancy[12], 0);
});

test("rolling provider rows join transport health and sort by station coverage", () => {
  const rows = buildRollingProviderRows(
    {
      rows: [
        { country_code: "DE", provider_uid: "small", station_count: 10 },
        {
          country_code: "BE",
          operator_brand: "STROOHM",
          source_provider_uids: ["be_monta"],
          station_count: 50,
        },
        { country_code: "DE", provider_uid: "large", station_count: 100 },
      ],
    },
    {
      rows: [
        {
          provider_uid: "large",
          country_code: "DE",
          display_name: "Large Network",
          fetch_failure_messages_total: 2,
          http_error_messages_total: 3,
        },
      ],
    },
  );
  assert.equal(rows[0].provider_uid, "large");
  assert.equal(rows[0].display_name, "Large Network");
  assert.equal(rows[0].transport_failure_count, 5);
  assert.equal(rows[1].operator_brand, "STROOHM");
  assert.equal(rows[1].display_name, "STROOHM");
  assert.equal(rows[1].publisher, "be_monta");
});

test("rolling operator rows support legacy names and require at least 50 stations", () => {
  const rows = buildRollingProviderRows(
    {
      group_by: "operator",
      rows: [
        { country_code: "DE", operator_name: "Legacy Network", station_count: 50 },
        { country_code: "DE", operator_brand: "Large Network", station_count: 100 },
        { country_code: "NO", operator_name: "", station_count: 76 },
        { country_code: "DE", operator_name: "Small Network", station_count: 49 },
      ],
    },
    { rows: [] },
  );

  assert.deepEqual(
    rows.map((row) => [row.operator_brand, row.display_name, row.station_count]),
    [
      ["Large Network", "Large Network", 100],
      ["Unbekannt", "Unbekannt", 76],
      ["Legacy Network", "Legacy Network", 50],
    ],
  );
});

test("provider names never render blank and technical source ids are readable", () => {
  assert.equal(
    providerDisplayName({ operator_name: "" }, { operatorMode: true }),
    "Unbekannt",
  );
  assert.equal(
    providerDisplayName(
      {
        provider_uid: "se_nobil",
        display_name: "se_nobil",
        publisher: "se_nobil_realtime",
      },
    ),
    "SE NOBIL Echtzeit",
  );
  assert.equal(providerDisplayName({}), "Datenquelle nicht angegeben");
});

test("buildStationRows sorts broken and busy station tables for the public page", () => {
  const brokenRows = buildStationRows(
    {
      broken_stations: [
        {
          station_id: "more-currently-broken",
          current_broken_charger_count: 99,
          out_of_order_duration_seconds_total: 100,
          affected_charger_count: 99,
        },
        ...Array.from({ length: 11 }, (_, index) => ({
          station_id: `station-${String.fromCharCode(97 + index)}`,
          current_broken_charger_count: index,
          out_of_order_duration_seconds_total: (index + 1) * 100,
          affected_charger_count: index,
        })),
      ],
    },
    "broken_stations",
  );
  const busyRows = buildStationRows(
    {
      busiest_stations: [
        {
          station_id: "still-plugged-in",
          busy_transition_count: 99,
          day_occupancy_share: 1,
        },
        {
          station_id: "still-plugged-in-rounded",
          busy_transition_count: 98,
          day_occupancy_share: 0.9999,
        },
        ...Array.from({ length: 12 }, (_, index) => ({
          station_id: `station-${String.fromCharCode(97 + index)}`,
          busy_transition_count: index,
          day_occupancy_share: 0.8 + index / 100,
        })),
      ],
    },
    "busiest_stations",
  );

  assert.equal(brokenRows.length, 10);
  assert.deepEqual(brokenRows[0].station_id, "station-k");
  assert.equal(brokenRows.some((row) => row.station_id === "more-currently-broken"), false);
  assert.equal(busyRows.length, 10);
  assert.equal(busyRows.some((row) => row.station_id === "still-plugged-in"), false);
  assert.equal(
    busyRows.some((row) => row.station_id === "still-plugged-in-rounded"),
    false,
  );
  assert.deepEqual(busyRows[0].station_id, "station-l");
  assert.deepEqual(busyRows.at(-1).station_id, "station-c");
});

test("buildProviderRows sorts provider reporting by daily status observation volume", () => {
  const rows = buildProviderRows({
    provider_reports: [
      { provider_uid: "small", display_name: "Small", messages_total: 10, observations_total: 500 },
      { provider_uid: "large", display_name: "Large", messages_total: 200, observations_total: 100 },
      { provider_uid: "medium", display_name: "Medium", messages_total: 100, observations_total: 2000 },
    ],
  });

  assert.deepEqual(
    rows.map((row) => row.provider_uid),
    ["medium", "small", "large"],
  );
});

test("buildProviderRows sorts operator reporting by public station count", () => {
  const rows = buildProviderRows({
    provider_reports: [
      { operator_brand: "Small Operator", static_station_count: 5, observations_total: 500 },
      { operator_brand: "IONITY", static_station_count: 120, observations_total: 100 },
      { operator_brand: "ALDI", static_station_count: 80, observations_total: 2000 },
    ],
  });

  assert.deepEqual(
    rows.map((row) => row.operator_brand),
    ["IONITY", "ALDI", "Small Operator"],
  );
});

test("buildDailySourceRows excludes cached operator aggregates", () => {
  const rows = buildDailySourceRows({
    provider_reports: [
      {
        provider_uid: "no_nobil",
        display_name: "Nobil",
        observations_total: 158080,
      },
      {
        operator_brand: "Oslo Kommune",
        static_station_count: 1006,
        observations_total: 500,
      },
    ],
  });

  assert.deepEqual(rows.map((row) => row.provider_uid), ["no_nobil"]);
});

test("buildProviderReportMetrics derives observation density and missing bundle chargers", () => {
  const metrics = buildProviderReportMetrics({
    received_messages_total: 120,
    observations_total: 720,
    unique_chargers_referenced_total: 6,
    unique_bundle_chargers_referenced_total: 4,
    bundle_mapped_chargers_total: 7,
  });

  assert.equal(metrics.receivedMessagesTotal, 120);
  assert.equal(metrics.observationsTotal, 720);
  assert.equal(metrics.messagesPerCharger, 20);
  assert.equal(metrics.observationsPerCharger, 120);
  assert.equal(metrics.bundleChargersWithoutUpdatesTotal, 3);
});

test("compareTableSortValues handles numeric and text sorting", () => {
  assert.equal(compareTableSortValues("2", "10", "number"), -8);
  assert.equal(compareTableSortValues("Anbieter 2", "Anbieter 10", "text") < 0, true);
});
