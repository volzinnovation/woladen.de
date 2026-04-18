import test from "node:test";
import assert from "node:assert/strict";

import {
  buildManagementSubtitle,
  buildOverviewSeries,
  buildStationRows,
  buildSummaryCards,
  normalizeManagementDate,
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
    "Störungen und Auslastung der öffentlichen Ladesäulen in Deutschland am Freitag, 17.04.2026",
  );
});

test("buildOverviewSeries returns ordered labels and values for the selected metric", () => {
  const series = buildOverviewSeries(
    {
      summary_series: [
        { snapshot_date: "2026-04-16", stations_with_disruptions: 8 },
        { snapshot_date: "2026-04-17", stations_with_disruptions: 11 },
      ],
    },
    "stations_with_disruptions",
  );

  assert.deepEqual(series.labels, ["16.04.2026", "17.04.2026"]);
  assert.deepEqual(series.values, [8, 11]);
});

test("buildSummaryCards exposes the public-facing station metrics", () => {
  const cards = buildSummaryCards({
    summary: {
      afir_stations_observed: 14032,
      stations_with_disruptions: 870,
      disruptions_at_end_of_day: 441,
      high_utilization_stations: 1872,
      archive_messages_total: 30970,
    },
  });

  assert.equal(cards[0].label, "Stationen mit Live-Daten gemäß AFIR");
  assert.equal(cards[1].value, "870");
  assert.equal(cards[3].label, "Stationen mit hoher Auslastung");
  assert.equal(cards[4].label, "AFIR Datenmeldungen");
});

test("buildStationRows sorts broken and busy station tables for the public page", () => {
  const brokenRows = buildStationRows(
    {
      broken_stations: Array.from({ length: 12 }, (_, index) => ({
        station_id: `station-${String.fromCharCode(97 + index)}`,
        fully_broken_now: index === 11,
        current_broken_charger_count: index,
        out_of_order_duration_seconds_total: index * 100,
        affected_charger_count: index,
      })),
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
  assert.deepEqual(brokenRows[0].station_id, "station-l");
  assert.deepEqual(brokenRows.at(-1).station_id, "station-c");
  assert.equal(busyRows.length, 10);
  assert.deepEqual(busyRows[0].station_id, "station-l");
  assert.deepEqual(busyRows.at(-1).station_id, "station-c");
});
