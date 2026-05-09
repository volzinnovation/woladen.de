# TODO

## Done

- Downloaded the available Hugging Face archive into `data/live_archives/`.
- Verified remote archive discovery through `scripts/live_download_archive.py --list-available` and `--latest-available`.
- Ran the history build for `2026-04-16`.
- Persisted `extracted_*_observation_count_total` and `mapped_observation_ratio` into `provider_daily_summary.csv`.
- Added `competitive_analysis_eligible`, `competitive_analysis_tier`, and `competitive_analysis_reason` to `provider_daily_summary.csv`.
- Classified the first real day with a conservative rule: `eligible` at `mapped_observation_ratio >= 0.5`, `review` at `0.2-0.5`, otherwise `exclude`.
- Added `analysis.station_timeseries` to materialize EVSE and nearby-station interval series from the generated archive CSVs.
- Extended `analysis.station_timeseries` with duration summaries per EVSE and per selected station.
- Generated the first neighborhood report for `https://woladen.de/?station=DE:cf43ec02e883007d`.
- Updated neighborhood reports so `review` stations remain visible but are separated from the primary nearby comparison set in both CSV outputs and `summary.md`.
- Kept station reports provider-selected per station for now, but added `nearby_station_candidates.csv` and a provider-candidate audit section so ambiguous cross-provider choices are explicit.
- Added `analysis.provider_quality_report` and generated the first provider-quality report for `2026-04-16`.
- Added `analysis.provider_mapping_gap_report` and generated the first provider remediation backlog for `2026-04-16`.
- Extended `analysis.provider_quality_report` so it now acts as the default front door for a day: primary comparison set, review band, remediation backlog, outage watchlist, and artifact pointers.
- Promoted the `review` band into an explicit second-tier comparison cohort in `analysis.provider_quality_report` when both mapped-observation ratio and mapped-station coverage clear a conservative gate.
- Spot-checked raw payload members for `chargecloud`, `eco_movement`, `enbwmobility`, `qwello`, `wirelane`, and `ladenetz_de_ladestationsdaten`.
- Sampled unmapped EVSE IDs for `qwello`, `wirelane`, `m8mit`, `mobidata_bw_datex`, and `deprecated_chargecloud` to separate coverage gaps from pure normalization issues.
- Kept `station_all_evses_out_of_order` as the first conservative proxy for “station not in operation”, but only after applying a provider mapping-quality filter.

## Next

- Keep the provider scorecard centered on:
  - station coverage against static matches
  - mapped observation ratio
  - outage metrics on mapped EVSEs
- Use `provider_mapping_gaps_<date>.csv` to drive provider-specific remediation work:
  - `qwello` and `wirelane`: expand static/site match coverage for UUID-style IDs already present in dynamic data
  - `m8mit`: reconcile AFIR-style dynamic EVSE IDs against the mixed static identifier inventory
  - `mobidata_bw_datex`: bootstrap static mapping before any competitive comparison
  - `deprecated_chargecloud`: decide whether to merge, remap, or exclude the deprecated feed

## Once We Have 7 Days

- Produce week-over-week provider scorecards:
  - static matched stations
  - dynamic mapped stations per day
  - dynamic/static coverage ratio
  - median and p95 out-of-order duration
  - share of EVSEs observed as `unknown`
- Separate snapshot and delta providers more explicitly in the summaries.
- Add interval splitting across midnight so provider daily summaries can carry exact per-day out-of-order seconds.
- Track first-seen and last-seen dates per provider EVSE and per station.
- Add a “provider regression” report:
  - stations observed yesterday but not today
  - EVSEs that went from `free` or `occupied` to `out_of_order`
  - providers whose mapped station coverage dropped materially day over day

## Later

- Materialize the CSVs into DuckDB or SQLite for cheaper iterative analysis.
- Add dashboard notebooks or a small static report for day-to-day AFIR adoption tracking.
- Join archive history with static provider offer metadata and subscription status to explain missing dynamic coverage.
- Compare live coverage against bundle inventory by operator, city, and charger count bucket.
