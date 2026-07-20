import test from "node:test";
import assert from "node:assert/strict";

import {
  OVERVIEW_METRICS,
  buildCountryOverviewRows,
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
  normalizeCountryCode,
  normalizeManagementDate,
  normalizeProviderUid,
  snapshotPathForDate,
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

test("buildManagementSubtitle renders weekday and date for the selected day", () => {
  assert.equal(
    buildManagementSubtitle("2026-04-17"),
    "Störungen und Auslastung öffentlicher Ladestationen in angebundenen europäischen Ländern am Freitag, 17.04.2026",
  );
  assert.equal(
    buildManagementSubtitle("2026-04-17", "NL"),
    "Störungen und Auslastung öffentlicher Ladestationen in Niederlande am Freitag, 17.04.2026",
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
  assert.equal(cards[1].value, "870");
  assert.equal(cards[3].label, "Stationen mit hoher Auslastung");
  assert.equal(cards[4].label, "Empfangene AFIR-Meldungen");
  assert.equal(cards[4].value, "1.050.176");
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
  assert.equal(cards[1].value, "1");
});

test("buildSummaryCards exposes PostgreSQL coverage and reliability metrics", () => {
  const cards = buildSummaryCards({
    summary: {
      station_count: 35082,
      observed_evses: 111137,
      measured_station_coverage: 0.99142,
      occupancy_share: 0.0963,
      out_of_order_share: 0.0741,
      stations_with_disruptions: 9262,
      disruptions_at_end_of_day: 3679,
      fully_out_of_service_stations: 1828,
    },
  });
  assert.equal(cards.length, 6);
  assert.equal(cards[1].value, "99,1 %");
  assert.equal(cards[2].label, "Auslastung");
  assert.equal(cards[5].value, "1.828");
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
        { country_code: "AT", station_count: 14611, occupancy_share: 0.08 },
        { country_code: "DE", station_count: 35082, occupancy_share: 0.1 },
      ],
    },
  );
  assert.deepEqual(rows.map((row) => row.country_code), ["DE", "AT"]);
  assert.equal(rows[0].country_name, "Deutschland");
  assert.equal(rows[0].observed_days, 96);
  assert.equal(rows[1].station_count, 14611);
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
      busiest_stations: Array.from({ length: 12 }, (_, index) => ({
        station_id: `station-${String.fromCharCode(97 + index)}`,
        busy_transition_count: index,
      })),
    },
    "busiest_stations",
  );

  assert.equal(brokenRows.length, 10);
  assert.deepEqual(brokenRows[0].station_id, "station-k");
  assert.equal(brokenRows.some((row) => row.station_id === "more-currently-broken"), false);
  assert.equal(busyRows.length, 10);
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
