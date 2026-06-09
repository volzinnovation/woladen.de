#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import math
import os
import ssl
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

EARTH_RADIUS_M = 6_371_000.0
DEFAULT_AMENITY_RADIUS_M = 250.0
AMENITY_EXAMPLES_PER_STATION = 12
OSM_ATTRIBUTION = "OpenStreetMap contributors"
OSM_LICENSE = "ODbL-1.0"


@dataclass(frozen=True)
class AmenityRule:
    key: str
    selectors: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class AmenityPoint:
    lat: float
    lon: float
    categories: tuple[str, ...]
    name: str = ""
    opening_hours: str = ""
    osm_type: str = ""
    osm_id: str = ""
    geometry_method: str = "point"


AMENITY_RULES: tuple[AmenityRule, ...] = (
    AmenityRule("restaurant", (("amenity", "restaurant"),)),
    AmenityRule("cafe", (("amenity", "cafe"),)),
    AmenityRule("fast_food", (("amenity", "fast_food"),)),
    AmenityRule("toilets", (("amenity", "toilets"),)),
    AmenityRule("supermarket", (("shop", "supermarket"),)),
    AmenityRule("bakery", (("shop", "bakery"),)),
    AmenityRule("convenience", (("shop", "convenience"),)),
    AmenityRule("pharmacy", (("amenity", "pharmacy"), ("shop", "chemist"), ("shop", "pharmacy"))),
    AmenityRule("hotel", (("tourism", "hotel"),)),
    AmenityRule("museum", (("tourism", "museum"),)),
    AmenityRule("playground", (("leisure", "playground"),)),
    AmenityRule("park", (("leisure", "park"),)),
    AmenityRule("ice_cream", (("amenity", "ice_cream"), ("shop", "ice_cream"))),
)
AMENITY_BUNDLE_CATEGORIES = tuple(rule.key for rule in AMENITY_RULES)


EU27_CH_PBF_MANIFEST: dict[str, dict[str, str]] = {
    "AT": {"url": "https://download.geofabrik.de/europe/austria-latest.osm.pbf"},
    "BE": {"url": "https://download.geofabrik.de/europe/belgium-latest.osm.pbf"},
    "BG": {"url": "https://download.geofabrik.de/europe/bulgaria-latest.osm.pbf"},
    "CH": {"url": "https://download.geofabrik.de/europe/switzerland-latest.osm.pbf"},
    "CY": {"url": "https://download.geofabrik.de/europe/cyprus-latest.osm.pbf"},
    "CZ": {"url": "https://download.geofabrik.de/europe/czech-republic-latest.osm.pbf"},
    "DE": {"url": "https://download.geofabrik.de/europe/germany-latest.osm.pbf"},
    "DK": {"url": "https://download.geofabrik.de/europe/denmark-latest.osm.pbf"},
    "EE": {"url": "https://download.geofabrik.de/europe/estonia-latest.osm.pbf"},
    "ES": {"url": "https://download.geofabrik.de/europe/spain-latest.osm.pbf"},
    "FI": {"url": "https://download.geofabrik.de/europe/finland-latest.osm.pbf"},
    "FR": {"url": "https://download.geofabrik.de/europe/france-latest.osm.pbf"},
    "GR": {"url": "https://download.geofabrik.de/europe/greece-latest.osm.pbf"},
    "HR": {"url": "https://download.geofabrik.de/europe/croatia-latest.osm.pbf"},
    "HU": {"url": "https://download.geofabrik.de/europe/hungary-latest.osm.pbf"},
    "IE": {"url": "https://download.geofabrik.de/europe/ireland-and-northern-ireland-latest.osm.pbf"},
    "IT": {"url": "https://download.geofabrik.de/europe/italy-latest.osm.pbf"},
    "LT": {"url": "https://download.geofabrik.de/europe/lithuania-latest.osm.pbf"},
    "LU": {"url": "https://download.geofabrik.de/europe/luxembourg-latest.osm.pbf"},
    "LV": {"url": "https://download.geofabrik.de/europe/latvia-latest.osm.pbf"},
    "MT": {"url": "https://download.geofabrik.de/europe/malta-latest.osm.pbf"},
    "NL": {"url": "https://download.geofabrik.de/europe/netherlands-latest.osm.pbf"},
    "NO": {"url": "https://download.geofabrik.de/europe/norway-latest.osm.pbf"},
    "PL": {"url": "https://download.geofabrik.de/europe/poland-latest.osm.pbf"},
    "PT": {"url": "https://download.geofabrik.de/europe/portugal-latest.osm.pbf"},
    "RO": {"url": "https://download.geofabrik.de/europe/romania-latest.osm.pbf"},
    "SE": {"url": "https://download.geofabrik.de/europe/sweden-latest.osm.pbf"},
    "SI": {"url": "https://download.geofabrik.de/europe/slovenia-latest.osm.pbf"},
    "SK": {"url": "https://download.geofabrik.de/europe/slovakia-latest.osm.pbf"},
}
COUNTRY_PBF_URLS = {country: item["url"] for country, item in EU27_CH_PBF_MANIFEST.items()}
STATION_AMENITY_FIELDS = (
    "country_code",
    "station_id",
    "amenity_radius_m",
    "amenities_total",
    "amenity_category_counts",
    "amenity_examples",
    "nearest_amenity_kind",
    "nearest_amenity_name",
    "nearest_amenity_distance_m",
    "osm_pbf_url",
    "osm_pbf_sha256",
    "osm_extracted_at",
    "osm_extraction_status",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def text(value: Any) -> str:
    if value is None:
        return ""
    value_text = str(value).strip()
    if value_text.lower() in {"nan", "nat"}:
        return ""
    return " ".join(value_text.split())


def float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def station_attr(station: Any, key: str) -> Any:
    if isinstance(station, Mapping):
        return station.get(key)
    return getattr(station, key, None)


def station_identity(station: Any) -> tuple[str, str, float | None, float | None]:
    return (
        text(station_attr(station, "country_code")).upper(),
        text(station_attr(station, "station_id")),
        float_or_none(station_attr(station, "latitude")),
        float_or_none(station_attr(station, "longitude")),
    )


def classify_tags(tags: Any) -> list[str]:
    matched: list[str] = []
    if not hasattr(tags, "get"):
        return matched
    for rule in AMENITY_RULES:
        for key, value in rule.selectors:
            if tags.get(key) == value:
                matched.append(rule.key)
                break
    return matched


def radius_deltas_deg(radius_m: float, lat_deg: float) -> tuple[float, float]:
    lat_delta = float(radius_m) / 111_320.0
    cos_lat = max(0.1, math.cos(math.radians(lat_deg)))
    lon_delta = float(radius_m) / (111_320.0 * cos_lat)
    return lat_delta, lon_delta


def cell_key(lat: float, lon: float, lat_step: float, lon_step: float) -> tuple[int, int]:
    return (int(math.floor(lat / lat_step)), int(math.floor(lon / lon_step)))


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return EARTH_RADIUS_M * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def pbf_cache_path(country_code: str, cache_dir: Path) -> Path:
    url = COUNTRY_PBF_URLS[country_code.upper()]
    return cache_dir / Path(url).name


def download_pbf(country_code: str, cache_dir: Path, *, progress_every_mb: int = 0) -> Path:
    country_code = country_code.upper()
    target = pbf_cache_path(country_code, cache_dir)
    if target.exists() and target.stat().st_size > 0:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(target.suffix + ".part")
    if temp_path.exists():
        temp_path.unlink()
    request = urllib.request.Request(
        COUNTRY_PBF_URLS[country_code],
        headers={"User-Agent": "woladen.de OSM amenity builder"},
    )
    progress_bytes = max(0, int(progress_every_mb)) * 1024 * 1024
    next_progress_bytes = progress_bytes
    downloaded_bytes = 0
    try:
        response_context = urllib.request.urlopen(request, timeout=600)
    except (ssl.SSLError, urllib.error.URLError):
        response_context = urllib.request.urlopen(
            request,
            timeout=600,
            context=ssl._create_unverified_context(),
        )
    with response_context as response, temp_path.open("wb") as handle:
        total_header = response.headers.get("Content-Length")
        total_bytes = int(total_header) if total_header and total_header.isdigit() else 0
        while True:
            chunk = response.read(4 * 1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
            downloaded_bytes += len(chunk)
            if progress_bytes and downloaded_bytes >= next_progress_bytes:
                if total_bytes:
                    print(
                        f"Downloaded {country_code} OSM PBF: "
                        f"{downloaded_bytes / (1024 * 1024):,.0f} MiB of "
                        f"{total_bytes / (1024 * 1024):,.0f} MiB",
                        flush=True,
                    )
                else:
                    print(
                        f"Downloaded {country_code} OSM PBF: "
                        f"{downloaded_bytes / (1024 * 1024):,.0f} MiB",
                        flush=True,
                    )
                while next_progress_bytes <= downloaded_bytes:
                    next_progress_bytes += progress_bytes
    temp_path.replace(target)
    if progress_bytes:
        print(
            f"Downloaded {country_code} OSM PBF complete: "
            f"{downloaded_bytes / (1024 * 1024):,.0f} MiB",
            flush=True,
        )
    return target


def sha256_file(path: Path, *, chunk_size: int = 4 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def build_coarse_station_cells(
    stations: Iterable[Any],
    *,
    radius_m: float,
    lat_step: float,
    lon_step: float,
) -> set[tuple[int, int]]:
    cells: set[tuple[int, int]] = set()
    for station in stations:
        _, _, lat, lon = station_identity(station)
        if lat is None or lon is None:
            continue
        lat_delta, lon_delta = radius_deltas_deg(radius_m, lat)
        lat_min_idx = int(math.floor((lat - lat_delta) / lat_step))
        lat_max_idx = int(math.floor((lat + lat_delta) / lat_step))
        lon_min_idx = int(math.floor((lon - lon_delta) / lon_step))
        lon_max_idx = int(math.floor((lon + lon_delta) / lon_step))
        for lat_idx in range(lat_min_idx, lat_max_idx + 1):
            for lon_idx in range(lon_min_idx, lon_max_idx + 1):
                cells.add((lat_idx, lon_idx))
    return cells


def collect_amenity_points_from_pbf(
    *,
    pbf_path: Path,
    stations: Sequence[Any],
    radius_m: float,
    pbf_progress_every: int = 0,
    coarse_lat_step: float = 0.02,
    coarse_lon_step: float = 0.03,
) -> tuple[list[AmenityPoint], dict[str, int]]:
    try:
        import osmium  # type: ignore
    except Exception as exc:
        raise RuntimeError("python package 'osmium' is required for OSM PBF amenity extraction") from exc

    station_cells = build_coarse_station_cells(
        stations,
        radius_m=radius_m,
        lat_step=coarse_lat_step,
        lon_step=coarse_lon_step,
    )

    class AmenityCollector(osmium.SimpleHandler):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.points: list[AmenityPoint] = []
            self.nodes_seen = 0
            self.ways_seen = 0
            self.relations_seen = 0
            self.nodes_kept = 0
            self.ways_kept = 0
            self.started = time.monotonic()

        def _maybe_log(self) -> None:
            seen = self.nodes_seen + self.ways_seen + self.relations_seen
            if pbf_progress_every <= 0 or seen <= 0 or seen % pbf_progress_every != 0:
                return
            elapsed = max(0.001, time.monotonic() - self.started)
            rate = seen / elapsed
            print(
                "PBF scan progress: "
                f"objects={seen:,} nodes={self.nodes_seen:,} ways={self.ways_seen:,} "
                f"relations={self.relations_seen:,} kept_points={len(self.points):,} "
                f"rate={rate:,.0f} obj/s",
                flush=True,
            )

        def _append_if_relevant(
            self,
            *,
            lat: float,
            lon: float,
            tags: Any,
            source: str,
            osm_id: Any,
            geometry_method: str,
        ) -> None:
            categories = classify_tags(tags)
            if not categories:
                return
            if cell_key(lat, lon, coarse_lat_step, coarse_lon_step) not in station_cells:
                return
            self.points.append(
                AmenityPoint(
                    lat=lat,
                    lon=lon,
                    categories=tuple(categories),
                    name=text(tags.get("name")),
                    opening_hours=text(tags.get("opening_hours")),
                    osm_type=source,
                    osm_id=text(osm_id),
                    geometry_method=geometry_method,
                )
            )
            if source == "node":
                self.nodes_kept += 1
            elif source == "way":
                self.ways_kept += 1

        def node(self, node: Any) -> None:
            self.nodes_seen += 1
            self._maybe_log()
            if not node.location.valid():
                return
            self._append_if_relevant(
                lat=float(node.location.lat),
                lon=float(node.location.lon),
                tags=node.tags,
                source="node",
                osm_id=node.id,
                geometry_method="point",
            )

        def way(self, way: Any) -> None:
            self.ways_seen += 1
            self._maybe_log()
            if not classify_tags(way.tags):
                return
            sum_lat = 0.0
            sum_lon = 0.0
            count = 0
            for node_ref in way.nodes:
                if not node_ref.location.valid():
                    continue
                sum_lat += float(node_ref.location.lat)
                sum_lon += float(node_ref.location.lon)
                count += 1
            if count <= 0:
                return
            self._append_if_relevant(
                lat=sum_lat / count,
                lon=sum_lon / count,
                tags=way.tags,
                source="way",
                osm_id=way.id,
                geometry_method="node_average",
            )

        def relation(self, relation: Any) -> None:
            self.relations_seen += 1
            self._maybe_log()

    collector = AmenityCollector()
    collector.apply_file(str(pbf_path), locations=True)
    return collector.points, {
        "nodes_seen": collector.nodes_seen,
        "ways_seen": collector.ways_seen,
        "relations_seen": collector.relations_seen,
        "nodes_kept": collector.nodes_kept,
        "ways_kept": collector.ways_kept,
        "points_kept": len(collector.points),
        "station_cells": len(station_cells),
    }


def build_point_grid_index(
    points: Sequence[AmenityPoint],
    *,
    lat_step: float,
    lon_step: float,
) -> dict[tuple[int, int], list[int]]:
    grid: dict[tuple[int, int], list[int]] = defaultdict(list)
    for idx, point in enumerate(points):
        grid[cell_key(point.lat, point.lon, lat_step, lon_step)].append(idx)
    return grid


def build_amenity_example(point: AmenityPoint, *, distance_m: float) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "category": point.categories[0] if point.categories else "",
        "distance_m": int(round(max(0.0, distance_m))),
        "lat": round(float(point.lat), 6),
        "lon": round(float(point.lon), 6),
    }
    if point.name:
        payload["name"] = point.name
    if point.opening_hours:
        payload["opening_hours"] = point.opening_hours
    if point.osm_type and point.osm_id:
        payload["osm_ref"] = f"{point.osm_type}/{point.osm_id}"
    if point.geometry_method:
        payload["geometry_method"] = point.geometry_method
    return payload


def limit_amenity_examples(examples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(item: dict[str, Any]) -> tuple[int, str, str]:
        distance = item.get("distance_m")
        distance_int = int(distance) if isinstance(distance, (int, float)) else 10_000_000
        return (distance_int, text(item.get("category")), text(item.get("name")).lower())

    return sorted(examples, key=sort_key)[:AMENITY_EXAMPLES_PER_STATION]


def empty_station_amenity_row(
    station: Any,
    *,
    radius_m: float,
    osm_pbf_url: str,
    osm_extraction_status: str,
    osm_pbf_sha256: str = "",
    osm_extracted_at: str = "",
) -> dict[str, Any]:
    country_code, station_id, _, _ = station_identity(station)
    return {
        "country_code": country_code,
        "station_id": station_id,
        "amenity_radius_m": f"{float(radius_m):.3f}".rstrip("0").rstrip("."),
        "amenities_total": "0",
        "amenity_category_counts": {},
        "amenity_examples": [],
        "nearest_amenity_kind": "",
        "nearest_amenity_name": "",
        "nearest_amenity_distance_m": "",
        "osm_pbf_url": osm_pbf_url,
        "osm_pbf_sha256": osm_pbf_sha256,
        "osm_extracted_at": osm_extracted_at,
        "osm_extraction_status": osm_extraction_status,
    }


def join_station_amenity_rows(
    *,
    stations: Sequence[Any],
    points: Sequence[AmenityPoint],
    radius_m: float,
    osm_pbf_url: str,
    osm_extraction_status: str = "extracted_from_pbf",
    osm_pbf_sha256: str = "",
    osm_extracted_at: str = "",
) -> list[dict[str, Any]]:
    fine_lat_step = max(0.0005, radius_m / 111_320.0)
    reference_lat = 50.0
    valid_lats = [lat for _, _, lat, _ in (station_identity(station) for station in stations) if lat is not None]
    if valid_lats:
        reference_lat = sum(valid_lats) / len(valid_lats)
    fine_lon_step = max(
        0.0005,
        radius_m / (111_320.0 * max(0.1, math.cos(math.radians(reference_lat)))),
    )
    point_grid = build_point_grid_index(points, lat_step=fine_lat_step, lon_step=fine_lon_step)

    rows: list[dict[str, Any]] = []
    for station in stations:
        country_code, station_id, lat, lon = station_identity(station)
        if lat is None or lon is None:
            rows.append(
                empty_station_amenity_row(
                    station,
                    radius_m=radius_m,
                    osm_pbf_url=osm_pbf_url,
                    osm_pbf_sha256=osm_pbf_sha256,
                    osm_extracted_at=osm_extracted_at,
                    osm_extraction_status=osm_extraction_status,
                )
            )
            continue

        lat_delta, lon_delta = radius_deltas_deg(radius_m, lat)
        lat_idx, lon_idx = cell_key(lat, lon, fine_lat_step, fine_lon_step)
        lat_reach = max(1, int(math.ceil(lat_delta / fine_lat_step)) + 1)
        lon_reach = max(1, int(math.ceil(lon_delta / fine_lon_step)) + 1)
        candidate_indices: set[int] = set()
        for d_lat in range(-lat_reach, lat_reach + 1):
            for d_lon in range(-lon_reach, lon_reach + 1):
                candidate_indices.update(point_grid.get((lat_idx + d_lat, lon_idx + d_lon), []))

        category_counts: Counter[str] = Counter()
        examples: list[dict[str, Any]] = []
        nearest: tuple[float, AmenityPoint] | None = None
        for point_idx in candidate_indices:
            point = points[point_idx]
            distance_m = haversine_distance_m(lat, lon, point.lat, point.lon)
            if distance_m > radius_m:
                continue
            if nearest is None or distance_m < nearest[0]:
                nearest = (distance_m, point)
            for category in point.categories:
                category_counts[category] += 1
                examples.append(
                    build_amenity_example(
                        AmenityPoint(
                            lat=point.lat,
                            lon=point.lon,
                            categories=(category,),
                            name=point.name,
                            opening_hours=point.opening_hours,
                            osm_type=point.osm_type,
                            osm_id=point.osm_id,
                            geometry_method=point.geometry_method,
                        ),
                        distance_m=distance_m,
                    )
                )

        nearest_kind = ""
        nearest_name = ""
        nearest_distance = ""
        if nearest is not None:
            nearest_distance = f"{nearest[0]:.1f}"
            nearest_point = nearest[1]
            nearest_name = nearest_point.name
            if nearest_point.categories:
                nearest_kind = f"amenity:{nearest_point.categories[0]}"

        rows.append(
            {
                "country_code": country_code,
                "station_id": station_id,
                "amenity_radius_m": f"{float(radius_m):.3f}".rstrip("0").rstrip("."),
                "amenities_total": str(sum(category_counts.values())),
                "amenity_category_counts": dict(category_counts),
                "amenity_examples": limit_amenity_examples(examples),
                "nearest_amenity_kind": nearest_kind,
                "nearest_amenity_name": nearest_name,
                "nearest_amenity_distance_m": nearest_distance,
                "osm_pbf_url": osm_pbf_url,
                "osm_pbf_sha256": osm_pbf_sha256,
                "osm_extracted_at": osm_extracted_at,
                "osm_extraction_status": osm_extraction_status,
            }
        )
    return rows


def station_amenity_row_to_csv(row: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for field in STATION_AMENITY_FIELDS:
        value = row.get(field, "")
        if field in {"amenity_category_counts", "amenity_examples"} and not isinstance(value, str):
            value = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        payload[field] = value
    return payload


def legacy_fast_row_to_station_amenity_row(
    *,
    station: Any,
    fast_row: Mapping[str, Any] | None,
    radius_m: float,
    osm_pbf_url: str,
    default_status: str,
) -> dict[str, Any]:
    if fast_row is None:
        return empty_station_amenity_row(
            station,
            radius_m=radius_m,
            osm_pbf_url=osm_pbf_url,
            osm_extraction_status=default_status,
        )
    category_counts = {
        category: int(float(text(fast_row.get(f"amenity_{category}")) or "0"))
        for category in AMENITY_BUNDLE_CATEGORIES
    }
    category_counts = {category: count for category, count in category_counts.items() if count > 0}
    examples_raw = fast_row.get("amenity_examples")
    examples: list[dict[str, Any]] = []
    if isinstance(examples_raw, str) and examples_raw:
        try:
            parsed = json.loads(examples_raw)
            if isinstance(parsed, list):
                examples = [item for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            examples = []
    elif isinstance(examples_raw, list):
        examples = [item for item in examples_raw if isinstance(item, dict)]
    source = text(fast_row.get("amenities_source")) or default_status
    return {
        "country_code": text(station_attr(station, "country_code")).upper(),
        "station_id": text(station_attr(station, "station_id")),
        "amenity_radius_m": f"{float(radius_m):.3f}".rstrip("0").rstrip("."),
        "amenities_total": str(sum(category_counts.values())),
        "amenity_category_counts": category_counts,
        "amenity_examples": examples[:AMENITY_EXAMPLES_PER_STATION],
        "nearest_amenity_kind": "",
        "nearest_amenity_name": "",
        "nearest_amenity_distance_m": "",
        "osm_pbf_url": osm_pbf_url,
        "osm_pbf_sha256": "",
        "osm_extracted_at": "",
        "osm_extraction_status": source,
    }


def amenity_status_by_country(rows: Iterable[Mapping[str, Any]]) -> dict[str, str]:
    statuses: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        country_code = text(row.get("country_code")).upper()
        if not country_code:
            continue
        statuses[country_code].add(text(row.get("osm_extraction_status")) or "unknown")
    result: dict[str, str] = {}
    for country_code, country_statuses in sorted(statuses.items()):
        ordered = sorted(country_statuses)
        result[country_code] = ordered[0] if len(ordered) == 1 else "mixed:" + ",".join(ordered)
    return result


def pbf_file_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"exists": False, "path": str(path)}
    stat = path.stat()
    return {
        "exists": True,
        "path": str(path),
        "bytes": int(stat.st_size),
        "sha256": sha256_file(path),
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(microsecond=0).isoformat(),
    }


def json_dumps_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def local_pbf_status(country_code: str, cache_dir: Path) -> str:
    path = pbf_cache_path(country_code, cache_dir)
    if path.exists() and path.stat().st_size > 0:
        return "pbf_available"
    return "pbf_missing"
