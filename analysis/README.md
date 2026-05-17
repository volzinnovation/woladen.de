# AFIR Archive Analysis

This directory turns archived live payload logs into CSVs that can be inspected in SQL, pandas, or a BI tool.

## Goal

The first pass is intentionally narrow:

- keep the existing live backend as the source of DATEX normalization
- read daily `live-provider-responses-YYYY-MM-DD.tgz` archives
- emit history tables that let us answer:
  - which providers expose static AFIR data
  - which providers expose dynamic AFIR data on a given day
  - how many mapped stations and EVSEs each provider reaches
  - which EVSEs are `free`, `occupied`, `out_of_order`, or `unknown`
  - how long an EVSE stays in a status before the next observed change
  - which mapped stations are effectively down because all observed EVSEs are `out_of_order`

## Current Workflow

The analysis CLIs work on daily `.tgz` archives named `live-provider-responses-YYYY-MM-DD.tgz`. The expected source is the Hugging Face archive dataset `loffenauer/AFIR`, under `provider-response-archives/`, mirrored into `data/live_archives/` as needed.

### Production station occupancy charts

The app's `Typische Auslastung` charts are generated as static public JSON, not from live API history at request time. The production path is:

1. The live backend logs provider poll and push payloads under `data/live_raw/<provider_uid>/<YYYY-MM-DD>/`.
2. The VPS cron job runs `scripts/live_archive_logs.py` shortly after midnight Europe/Berlin, builds `live-provider-responses-YYYY-MM-DD.tgz`, uploads it to Hugging Face, and cleans up uploaded raw files.
3. `.github/workflows/daily-management-analysis.yml` runs daily for the previous Berlin day. It waits for the target archive, restores or creates `data/occupancy.sqlite3`, imports the trailing window, exports public chart JSON for stations with at least 10 mapped status updates per day, rebuilds `site/`, and commits the management plus chart artifacts back to `main`.
4. `.github/workflows/pages-deploy.yml` is triggered by the successful management workflow and deploys the rebuilt static site.

The core local commands mirror the workflow:

```bash
python analysis/update_occupancy_db_daily.py \
  --date 2026-04-20 \
  --days 7 \
  --db data/occupancy.sqlite3 \
  --require-complete \
  --retain-days 7 \
  --clear-hf-cache
```

```bash
python analysis/export_station_occupancy_from_db.py \
  --date 2026-04-20 \
  --days 7 \
  --db data/occupancy.sqlite3 \
  --output-dir web/data/station-occupancy \
  --min-matching-observations-per-day 10 \
  --require-complete
```

```bash
python scripts/build_site.py
```

The public chart artifacts are:

- `web/data/station-occupancy/index.json`: station id to per-station JSON path.
- `web/data/station-occupancy/<station_id>.json`: compact chart payload consumed by the app.
- `site/data/station-occupancy/`: generated deployment copy after `scripts/build_site.py`.

The chart metric is the mean number of occupied EVSEs per local Berlin hour over the included archive days. Unknown or missing status is not counted as occupied. When multiple providers map to the same internal station, the exporter selects one primary provider per station to avoid duplicate provider mappings.

The import/export pipeline is split on purpose:

- `analysis/update_occupancy_db_daily.py` downloads missing archives and delegates DB import.
- `analysis/build_occupancy_db.py` parses archives, normalizes status events, computes daily/hourly aggregates, and writes SQLite.
- `analysis/occupancy_store.py` owns the SQLite schema and replacement/pruning logic.
- `analysis/export_station_occupancy_from_db.py` builds the compact public per-station JSON tree from SQLite.
- `analysis/batch_station_occupancy.py` contains the shared archive parser, station/provider matching, hourly interval accounting, provider selection, and payload builder.

### Manual one-station chart helper

`analysis.hourly_station_occupancy` is still useful for ad-hoc debugging because it writes a JSON file plus an SVG for one station. It is not the production source for the app's current per-station chart bundle.

Mirror the needed Hugging Face archives first:

```bash
python3 -m analysis.download_hf_archives \
  --date 2026-04-20 \
  --days 7
```

Then build the manual chart from local archives:

```bash
python3 -m analysis.hourly_station_occupancy \
  --station 'https://woladen.de/?station=cf43ec02e883007d' \
  --date 2026-04-20 \
  --days 7
```

To inspect remote availability first:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_download_archive.py --list-available
```

To mirror the newest available archive from the configured Hugging Face dataset:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_download_archive.py --latest-available
```

Or keep the old default of "yesterday in archive timezone":

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_download_archive.py
```

Example:

```bash
python3 -m analysis.afir_history \
  --archive-dir /Users/raphaelvolz/Github/woladen.de/data/live_archives \
  --start-date 2026-04-14 \
  --end-date 2026-04-20 \
  --output-dir /Users/raphaelvolz/Github/woladen.de/analysis/output
```

The first real run is documented in [findings-2026-04-16.md](/Users/raphaelvolz/Github/woladen.de/analysis/findings-2026-04-16.md).

To build a target-station EVSE timeline and a nearby-stations timeline from the generated CSVs:

```bash
python3 -m analysis.station_timeseries \
  --station 'https://woladen.de/?station=DE:cf43ec02e883007d' \
  --analysis-output-dir /Users/raphaelvolz/Github/woladen.de/analysis/output \
  --output-dir /Users/raphaelvolz/Github/woladen.de/analysis/output/stations \
  --radius-meters 12000 \
  --max-nearby 6 \
  --provider-tiers eligible,review
```

To build the provider-quality front door from the latest `provider_daily_summary.csv`:

```bash
python3 -m analysis.provider_quality_report \
  --provider-daily-summary /Users/raphaelvolz/Github/woladen.de/analysis/output/provider_daily_summary.csv \
  --output-dir /Users/raphaelvolz/Github/woladen.de/analysis/output/reports
```

To build a provider mapping-gap remediation report from the latest archive day:

```bash
python3 -m analysis.provider_mapping_gap_report \
  --provider-daily-summary /Users/raphaelvolz/Github/woladen.de/analysis/output/provider_daily_summary.csv \
  --evse-observations /Users/raphaelvolz/Github/woladen.de/analysis/output/evse_observations.csv \
  --output-dir /Users/raphaelvolz/Github/woladen.de/analysis/output/reports
```

To aggregate raw EVSE status-change rows by `provider_evse_id`:

```bash
python3 -m analysis.provider_evse_change_counts \
  --input /Users/raphaelvolz/Github/woladen.de/analysis/output/evse_status_changes.csv \
  --output /Users/raphaelvolz/Github/woladen.de/analysis/output/provider_evse_change_counts.csv
```

## Output Files

- `provider_catalog.csv`: provider metadata from `mobilithek_afir_provider_configs.json` plus static matched station counts.
- `archive_messages.csv`: one row per archived raw log entry. Useful for auditing parse coverage and provider message volume.
- `evse_observations.csv`: one normalized EVSE fact per archived payload observation.
- `evse_status_changes.csv`: status intervals per `provider_uid + provider_evse_id`, with `duration_seconds` clipped to the analysis window end when the last interval is still open.
- `station_daily_summary.csv`: latest observed EVSE state per day, aggregated to `provider_uid + station_id`.
- `provider_daily_summary.csv`: provider/day scorecard with station coverage against static matches, mapped-observation ratio, a conservative competitive-analysis eligibility tier, and end-of-day outage counts.
- `stations/<station_id>/target_evse_status_timeline.csv`: interval series for the chosen provider at one target station.
- `stations/<station_id>/target_evse_status_summary.csv`: duration totals per EVSE for the chosen provider at one target station.
- `stations/<station_id>/target_station_status_timeline.csv`: station-level interval aggregation for the target station.
- `stations/<station_id>/nearby_station_status_timeline.csv`: station-level interval aggregation for the target station plus nearby filtered stations, now tagged with `comparison_bucket` (`target`, `primary`, `review`).
- `stations/<station_id>/nearby_station_status_summary.csv`: duration totals per selected station, including `any_*_seconds` rollups for mixed states and a `comparison_bucket`.
- `stations/<station_id>/nearby_stations.csv`: nearby station catalog with chosen provider, tier, `chosen_comparison_bucket`, distance, and latest station snapshot.
- `stations/<station_id>/nearby_station_candidates.csv`: all candidate providers considered for each chosen station, with candidate rank and a selected/not-selected flag. This is the current audit trail for cross-provider deduplication.
- `stations/<station_id>/summary.md`: short human-readable note for the target station neighborhood, now split into a primary nearby comparison set and a separate review-only section, plus a provider-candidate audit for ambiguous stations.
- `reports/provider_quality_<date>.csv`: provider/day scorecard filtered to rows with messages, now enriched with remediation fields from `provider_mapping_gaps_<date>.csv` when available and tagged with `comparison_set_bucket` (`primary`, `secondary`, `backlog`).
- `reports/provider_quality_<date>.md`: the default front door for one archive day, with the primary comparison set, secondary comparison candidates, remediation backlog, outage watchlist, and an artifact index that points to the heavier CSVs.
- `reports/provider_mapping_gaps_<date>.csv`: provider remediation backlog with unmapped-observation samples, identifier-pattern hints, and suggested remediation categories.
- `reports/provider_mapping_gaps_<date>.md`: short markdown backlog that groups providers into remediation buckets and shows sample unmapped EVSE IDs and site IDs.
- `provider_evse_change_counts.csv`: `evse_status_changes.csv` aggregated by `provider_evse_id`, keeping the most frequently observed `station_id` for each EVSE and sorting by descending change count.

## Important Assumptions

- Historical normalization reuses `backend.datex.extract_dynamic_facts`. That keeps status mapping identical to the live API.
- `event_timestamp` prefers `source_observed_at`; if that is missing, it falls back to the fetch or receive timestamp.
- `station_daily_summary.csv` is provider-specific. The same internal station can appear in multiple provider rows.
- `station_all_evses_out_of_order` is the initial proxy for “station not in operation”. It is deliberately conservative.
- Absence from a payload is not treated as a state change. This matters for delta-delivery providers.
- `provider_daily_summary.csv` now classifies providers for competitive analysis:
  - `eligible`: `mapped_observation_ratio >= 0.5`
  - `review`: `0.2 <= mapped_observation_ratio < 0.5`
  - `exclude`: no messages, no static matches, no parseable messages, or `mapped_observation_ratio < 0.2`
- `provider_quality_report` currently derives comparison buckets from those tiers:
  - `primary`: all `eligible` providers
  - `secondary`: `review` providers with `mapped_observation_ratio >= 0.35` and `dynamic_station_coverage_ratio >= 0.5`
  - `backlog`: everyone else
- `station_timeseries` currently stays provider-selected per station, but now materializes `nearby_station_candidates.csv` so ambiguous station/provider choices are visible before we add any stricter multi-day deduplication rule.

## Suggested First Analyses

- Plot `mapped_stations_observed / static_matched_station_count` from `provider_daily_summary.csv`.
- Start provider-vs-provider comparisons from `reports/provider_quality_<date>.md`, then drill into `provider_daily_summary.csv` only when you need the raw scorecard.
- Treat `comparison_set_bucket = primary` as the default comparison cohort.
- Treat `comparison_set_bucket = secondary` as a visible but still cautionary second-tier cohort.
- Rank providers by `out_of_order_evses_end_of_day` and `stations_all_evses_out_of_order`.
- Use `provider_mapping_gaps_<date>.md` as the first remediation backlog for providers that are still failing the mapping-quality gate.
- Inspect `evse_status_changes.csv` for long `out_of_order` intervals.
- Inspect `archive_messages.csv` only when the provider-level `mapped_observation_ratio` suggests a matching problem.
