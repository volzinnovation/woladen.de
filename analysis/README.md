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

The analysis CLI works on local `.tgz` archives. The expected source is the Hugging Face archive dataset, mirrored into `data/live_archives/` with the repo downloader script.

For the hourly station occupancy chart, mirror the needed Hugging Face archives first:

```bash
python3 -m analysis.download_hf_archives \
  --date 2026-04-20 \
  --days 7
```

Then build the chart from the local archives:

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
