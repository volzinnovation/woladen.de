# EnBW Mobility+ Roaming Gap Analysis

## Purpose

Assess whether the EnBW mobility+ charging-station map can be used as a private
comparison source to identify catalog, live-availability, and amenity gaps in
Woladen.

This is a gap-analysis use case only. It is not approval to ingest, cache,
redistribute, or expose EnBW mobility+ map data in the web, Android, iPhone, or
live API products.

## Source Character

The EnBW mobility+ map is an EMP roaming catalog. It includes EnBW-operated
stations and roaming partners that can be used through the EnBW mobility+
product. That makes it different from the `enbwmobility` Mobilithek/AFIR feed in
this repository, which represents EnBW as a CPO/static-live provider.

Observed public map responses can include fields such as:

- station identifier
- coordinates and short address
- operator and operator code
- grouped or ungrouped station marker state
- connector types and maximum power
- total, available, and unknown-state charge-point counts
- authentication/payment/opening/accessibility hints

The map is technically reachable through browser-facing endpoints used by the
EnBW map page and by the unofficial `kegelmeier/ha-enbw` Home Assistant
integration. That access pattern is not a stable or licensed production contract
for Woladen.

## Current Woladen Baseline

Woladen already has EnBW CPO coverage through Mobilithek/AFIR:

- provider UID: `enbwmobility`
- static publication: `AFIR-recharging-stat-EnBWmobility+`
- dynamic publication: `AFIR-recharging-dyn-EnBWmobility+`
- local quality tier: `eligible`

The April 2026 quality report shows strong EnBW CPO mapping quality:

- mapped observation ratio: `0.969525`
- dynamic station coverage: `0.986626`
- mapped stations: `2287`

The remaining EnBW CPO work is mostly identifier reconciliation, not basic source
availability. The roaming-catalog use case is therefore about gaps beyond EnBW's
own CPO feed: roaming partners, partner live availability, and catalog detail
differences.

## Gap Questions

Use the EnBW roaming catalog to answer these questions offline:

1. Which EnBW mobility+ roaming stations are missing from the Woladen catalog?
2. Which stations exist in Woladen but differ materially in EVSE count, connector
   type, maximum power, address, operator, or coordinates?
3. Which roaming operators have live availability in the EnBW map but no usable
   live source in Woladen?
4. Which stations have availability/status disagreement between EnBW mobility+
   and Woladen's preferred source?
5. Which amenities or user-facing details appear in EnBW mobility+ but are
   missing from Woladen's open-static plus amenity pipeline?
6. Which missing stations are true public charging opportunities versus stations
   that are only useful inside EnBW's payment/roaming product context?

## Measurement Plan

Run the analysis from a private script or notebook, not from production clients.

Recommended workflow:

1. Select bounded test geographies: dense urban, autobahn corridor, rural area,
   border region, and non-Germany EU city.
2. Fetch the EnBW mobility+ map data for those bounding boxes with conservative
   request volume and no broad scraping.
3. Normalize EnBW records into a temporary comparison table:
   `source_station_id`, `operator_code`, `operator_name`, `lat`, `lon`,
   `address`, `postcode`, `city`, `country`, `evse_count`, `available_count`,
   `unknown_count`, `max_power_kw`, `connector_types`, `fetched_at`.
4. Join against Woladen catalog stations by exact EVSE IDs or station/operator
   identifiers when available.
5. If identifiers are absent, use geospatial matching only as review-stage
   evidence: distance, postcode, operator similarity, address similarity,
   connector/power similarity, and EVSE-count overlap.
6. Classify each candidate:
   - `covered_exact`: identifier-backed match exists.
   - `covered_probable`: strong reviewed location/operator/EVSE match exists.
   - `catalog_gap`: likely public station missing from Woladen.
   - `live_gap`: station exists, but EnBW shows live availability and Woladen
     does not.
   - `attribute_gap`: station exists, but material metadata differs.
   - `enbw_product_only`: station appears useful only in the EnBW roaming/payment
     context and should not be treated as public baseline coverage.
   - `needs_review`: evidence is insufficient or contradictory.
7. Aggregate by country, operator, connector type, fast-charger status, and
   Woladen source coverage.

Do not let a nearby coordinate silently override contradictory identifier,
operator, or EVSE-count evidence.

## Success Metrics

The source is worth pursuing commercially only if a bounded pilot shows clear
incremental value:

- `catalog_gap_station_count`: stations in EnBW mobility+ not matched to Woladen.
- `fast_catalog_gap_station_count`: missing stations with `max_power_kw >= 50`.
- `live_gap_station_count`: matched stations where EnBW has availability and
  Woladen has no usable live source.
- `attribute_gap_rate`: matched stations with material count/power/connector
  differences.
- `operator_gap_top_n`: operators where EnBW materially improves coverage.
- `false_positive_rate`: reviewed EnBW-only candidates rejected as non-public,
  duplicate, private, or EnBW-product-only.

Promotion threshold should be conservative. A useful signal is not enough; the
source also needs permission, stability, operational limits, attribution terms,
and a defensible data contract.

## Production Boundary

Do not ship the hacked EnBW map API directly.

Blocked production behaviors:

- embedding the EnBW browser subscription key in Woladen clients
- redistributing raw EnBW mobility+ map data without permission
- using unofficial endpoints as a live dependency for user-facing search,
  routing, station details, or occupancy
- broad scraping of the map catalog
- using EnBW roaming data to overwrite higher-trust CPO, AFIR, OCPI, or
  reviewed open-static records

Allowed pre-permission behaviors:

- small bounded manual probes for technical assessment
- private offline comparison artifacts
- aggregate gap counts that do not redistribute raw station records
- evidence packages for an EnBW data-access request

If EnBW grants explicit permission, implement the source server-side behind the
live catalog API. Use bounded polling, TTL caching, source attribution, duplicate
suppression, and priority rules that keep authoritative CPO/open-static data
above EMP roaming-derived data.

## Follow-Up Work

- Build a private `analysis/enbw_roaming_gap_report.py` pilot that emits CSV and
  Markdown summaries only.
- Compare EnBW mobility+ coverage against `live-eu` search, open-static catalog
  data, and existing provider-quality reports.
- Review top unmatched operators before any provider-quality comparison.
- Prepare an EnBW permission request if the pilot shows meaningful incremental
  fast-charger or live-availability coverage.

## References

- EnBW mobility+ app: `https://www.enbw.com/elektromobilitaet/produkte/mobilityplus-app/`
- EnBW station map: `https://www.enbw.com/elektromobilitaet/produkte/mobilityplus-app/ladestation-finden/map`
- Unofficial Home Assistant integration: `https://github.com/kegelmeier/ha-enbw`
- Woladen EnBW AFIR provider config:
  `data/mobilithek_afir_provider_configs.json`
- Woladen provider quality report:
  `analysis/output/reports/provider_quality_2026-04-17.md`
- Woladen provider mapping gap report:
  `analysis/output/reports/provider_mapping_gaps_2026-04-17.md`
