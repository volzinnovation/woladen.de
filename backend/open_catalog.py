from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any


MAX_CATALOG_LIMIT = 100
DEFAULT_CATALOG_RADIUS_M = 50_000
MAX_CATALOG_RADIUS_M = 500_000


class OpenCatalogUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class OpenCatalogStore:
    sqlite_path: Path | None

    @property
    def available(self) -> bool:
        return self.sqlite_path is not None and self.sqlite_path.exists()

    def search(
        self,
        *,
        latitude: float,
        longitude: float,
        radius_m: int = DEFAULT_CATALOG_RADIUS_M,
        limit: int = MAX_CATALOG_LIMIT,
        country_code: str = "",
        min_power_kw: float | None = None,
        connector_type: str = "",
        current_type: str = "",
        operator_query: str = "",
        source_uid: str = "",
    ) -> dict[str, Any]:
        radius_m = max(1, min(int(radius_m), MAX_CATALOG_RADIUS_M))
        limit = max(1, min(int(limit), MAX_CATALOG_LIMIT))
        min_lat, max_lat, min_lon, max_lon = _bounding_box(latitude, longitude, radius_m)
        candidates = max(500, min(5_000, limit * 25))

        with self._connect() as connection:
            rows = self._search_rows(
                connection,
                latitude=latitude,
                longitude=longitude,
                min_lat=min_lat,
                max_lat=max_lat,
                min_lon=min_lon,
                max_lon=max_lon,
                country_code=country_code,
                min_power_kw=min_power_kw,
                connector_type=connector_type,
                current_type=current_type,
                operator_query=operator_query,
                source_uid=source_uid,
                candidate_limit=candidates,
            )

        stations = []
        for row in rows:
            station = _station_from_row(row)
            distance_m = _haversine_m(
                latitude,
                longitude,
                float(station["latitude"]),
                float(station["longitude"]),
            )
            if distance_m > radius_m:
                continue
            station["distance_m"] = round(distance_m)
            stations.append(station)

        stations.sort(key=lambda item: (float(item.get("distance_m") or 0), str(item.get("station_id") or "")))
        return {
            "stations": stations[:limit],
            "query": {
                "lat": latitude,
                "lon": longitude,
                "radius_m": radius_m,
                "limit": limit,
                "country_code": country_code.upper() if country_code else "",
                "min_power_kw": min_power_kw,
                "connector_type": connector_type,
                "current_type": current_type,
                "operator": operator_query,
                "source_uid": source_uid,
            },
            "stats": {
                "candidate_count": len(rows),
                "matching_distance_count": len(stations),
                "returned_count": min(len(stations), limit),
            },
            "source": "open_static.sqlite3",
        }

    def get_station(self, station_id: str) -> dict[str, Any] | None:
        normalized_station_id = str(station_id or "").strip()
        if not normalized_station_id:
            return None

        with self._connect() as connection:
            station_sql = f"""
                {self._station_select_sql(connection)}
                WHERE s.station_id = :station_id COLLATE NOCASE
                  AND s.latitude IS NOT NULL
                  AND s.longitude IS NOT NULL
                LIMIT 1
            """
            row = connection.execute(station_sql, {"station_id": normalized_station_id}).fetchone()
            if row is None:
                return None
            station = _station_from_row(row)
            station_uid = int(row["station_uid"])
            return {
                "station": station,
                "chargers": self._charger_rows(connection, station_uid=station_uid),
                "amenities": self._amenity_row(connection, station_uid=station_uid),
                "source": "open_static.sqlite3",
            }

    def _connect(self) -> sqlite3.Connection:
        if not self.available:
            raise OpenCatalogUnavailable("open_static_sqlite_unavailable")
        assert self.sqlite_path is not None
        connection = sqlite3.connect(f"file:{self.sqlite_path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        return connection

    def _search_rows(
        self,
        connection: sqlite3.Connection,
        *,
        latitude: float,
        longitude: float,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        country_code: str,
        min_power_kw: float | None,
        connector_type: str,
        current_type: str,
        operator_query: str,
        source_uid: str,
        candidate_limit: int,
    ) -> list[sqlite3.Row]:
        where_parts = [
            "s.latitude IS NOT NULL",
            "s.longitude IS NOT NULL",
        ]
        params: dict[str, Any] = {
            "center_lat": latitude,
            "center_lon": longitude,
            "min_lat": min_lat,
            "max_lat": max_lat,
            "min_lon": min_lon,
            "max_lon": max_lon,
            "candidate_limit": candidate_limit,
            "lon_scale": max(math.cos(math.radians(latitude)), 0.01),
        }
        if country_code:
            where_parts.append("UPPER(s.country_code) = :country_code")
            params["country_code"] = country_code.upper()
        if min_power_kw is not None:
            where_parts.append("COALESCE(s.max_power_kw, 0) >= :min_power_kw")
            params["min_power_kw"] = float(min_power_kw)
        if source_uid:
            where_parts.append("LOWER(s.source_uid) = :source_uid")
            params["source_uid"] = source_uid.lower()
        if operator_query:
            where_parts.append("LOWER(COALESCE(s.operator_name, '')) LIKE :operator_query")
            params["operator_query"] = f"%{operator_query.lower()}%"
        if connector_type and self._table_exists(connection, "chargers"):
            where_parts.append(
                """
                EXISTS (
                    SELECT 1
                    FROM chargers AS c
                    WHERE c.station_uid = s.station_uid
                      AND LOWER(COALESCE(c.connector_type, '')) = :connector_type
                )
                """
            )
            params["connector_type"] = connector_type.lower()
        if current_type and self._table_exists(connection, "chargers"):
            where_parts.append(
                """
                EXISTS (
                    SELECT 1
                    FROM chargers AS c
                    WHERE c.station_uid = s.station_uid
                      AND LOWER(COALESCE(c.current_type, '')) = :current_type
                )
                """
            )
            params["current_type"] = current_type.lower()

        distance_select = """
            (((s.latitude - :center_lat) * (s.latitude - :center_lat))
              + ((s.longitude - :center_lon) * :lon_scale * (s.longitude - :center_lon) * :lon_scale))
              AS distance_score
        """
        where_sql = " AND ".join(where_parts)
        if self._table_exists(connection, "station_rtree"):
            sql = f"""
                {self._station_select_sql(connection, distance_select=distance_select)}
                JOIN station_rtree AS r ON r.station_uid = s.station_uid
                WHERE r.min_lat <= :max_lat
                  AND r.max_lat >= :min_lat
                  AND r.min_lon <= :max_lon
                  AND r.max_lon >= :min_lon
                  AND {where_sql}
                ORDER BY distance_score, s.station_uid
                LIMIT :candidate_limit
            """
        else:
            sql = f"""
                {self._station_select_sql(connection, distance_select=distance_select)}
                WHERE s.latitude BETWEEN :min_lat AND :max_lat
                  AND s.longitude BETWEEN :min_lon AND :max_lon
                  AND {where_sql}
                ORDER BY distance_score, s.station_uid
                LIMIT :candidate_limit
            """
        return list(connection.execute(sql, params).fetchall())

    def _station_select_sql(self, connection: sqlite3.Connection, *, distance_select: str = "") -> str:
        amenity_select = """
            COALESCE(a.amenities_total, 0) AS amenities_total,
            COALESCE(a.nearest_amenity_kind, '') AS nearest_amenity_kind,
            COALESCE(a.nearest_amenity_name, '') AS nearest_amenity_name,
            a.nearest_amenity_distance_m AS nearest_amenity_distance_m,
            COALESCE(a.amenity_category_counts_json, '{}') AS amenity_category_counts_json
        """
        amenity_join = ""
        if self._table_exists(connection, "station_amenities"):
            amenity_join = "LEFT JOIN station_amenities AS a ON a.station_uid = s.station_uid"
        else:
            amenity_select = """
                0 AS amenities_total,
                '' AS nearest_amenity_kind,
                '' AS nearest_amenity_name,
                NULL AS nearest_amenity_distance_m,
                '{}' AS amenity_category_counts_json
            """
        maybe_distance = f", {distance_select}" if distance_select else ""
        return f"""
            SELECT
                s.station_uid,
                s.country_code,
                s.station_id,
                {_column_expr(connection, "stations", "source_uid")},
                {_column_expr(connection, "stations", "source_station_id")},
                {_column_expr(connection, "stations", "license")},
                {_column_expr(connection, "stations", "provider_uid")},
                {_column_expr(connection, "stations", "operator_name")},
                {_column_expr(connection, "stations", "station_name")},
                {_column_expr(connection, "stations", "address")},
                {_column_expr(connection, "stations", "postal_code")},
                {_column_expr(connection, "stations", "city")},
                s.latitude,
                s.longitude,
                {_column_expr(connection, "stations", "charger_count", default="0")},
                {_column_expr(connection, "stations", "max_power_kw", default="NULL")},
                {_column_expr(connection, "stations", "connector_types")},
                {_column_expr(connection, "stations", "source_url")},
                {_column_expr(connection, "stations", "public_bundle_status")},
                {_column_expr(connection, "stations", "opening_hours")},
                {_column_expr(connection, "stations", "payment_methods")},
                {_column_expr(connection, "stations", "auth_methods")},
                {_column_expr(connection, "stations", "green_energy", default="NULL")},
                {_column_expr(connection, "stations", "helpdesk_phone")},
                {_column_expr(connection, "stations", "price_display")},
                {_column_expr(connection, "stations", "price_currency")},
                {_column_expr(connection, "stations", "detail_last_updated")},
                {amenity_select}
                {maybe_distance}
            FROM stations AS s
            {amenity_join}
        """

    def _charger_rows(self, connection: sqlite3.Connection, *, station_uid: int) -> list[dict[str, Any]]:
        if not self._table_exists(connection, "chargers"):
            return []
        columns = self._columns(connection, "chargers")
        wanted = [
            "charger_id",
            "source_uid",
            "provider_uid",
            "source_station_id",
            "source_evse_id",
            "connector_id",
            "connector_type",
            "current_type",
            "max_power_kw",
            "operator_name",
            "license",
            "source_url",
            "public_bundle_status",
        ]
        select_columns = [column for column in wanted if column in columns]
        if not select_columns:
            return []
        sql = f"""
            SELECT {", ".join(select_columns)}
            FROM chargers
            WHERE station_uid = :station_uid
            ORDER BY charger_uid
        """
        return [dict(row) for row in connection.execute(sql, {"station_uid": station_uid}).fetchall()]

    def _amenity_row(self, connection: sqlite3.Connection, *, station_uid: int) -> dict[str, Any] | None:
        if not self._table_exists(connection, "station_amenities"):
            return None
        row = connection.execute(
            """
            SELECT *
            FROM station_amenities
            WHERE station_uid = :station_uid
            LIMIT 1
            """,
            {"station_uid": station_uid},
        ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        for key in ("amenity_category_counts_json", "amenity_examples_json"):
            if key in payload:
                payload[key.replace("_json", "")] = _json_value(payload.pop(key), default={})
        return payload

    def _table_exists(self, connection: sqlite3.Connection, table_name: str) -> bool:
        return bool(
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?",
                (table_name,),
            ).fetchone()
        )

    def _columns(self, connection: sqlite3.Connection, table_name: str) -> set[str]:
        return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _column_expr(
    connection: sqlite3.Connection,
    table_name: str,
    column_name: str,
    *,
    default: str = "''",
    table_alias: str = "s",
) -> str:
    columns = {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}
    if column_name in columns:
        return f"{table_alias}.{column_name} AS {column_name}"
    return f"{default} AS {column_name}"


def _station_from_row(row: sqlite3.Row) -> dict[str, Any]:
    payload = {
        "station_id": str(row["station_id"] or ""),
        "country_code": str(row["country_code"] or ""),
        "source_uid": str(row["source_uid"] or ""),
        "source_station_id": str(row["source_station_id"] or ""),
        "license": str(row["license"] or ""),
        "provider_uid": str(row["provider_uid"] or ""),
        "operator_name": str(row["operator_name"] or ""),
        "station_name": str(row["station_name"] or ""),
        "address": str(row["address"] or ""),
        "postal_code": str(row["postal_code"] or ""),
        "city": str(row["city"] or ""),
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "charger_count": int(row["charger_count"] or 0),
        "max_power_kw": _float_or_none(row["max_power_kw"]),
        "connector_types": str(row["connector_types"] or ""),
        "source_url": str(row["source_url"] or ""),
        "public_bundle_status": str(row["public_bundle_status"] or ""),
        "opening_hours": str(row["opening_hours"] or ""),
        "payment_methods": str(row["payment_methods"] or ""),
        "auth_methods": str(row["auth_methods"] or ""),
        "green_energy": _bool_or_none(row["green_energy"]),
        "helpdesk_phone": str(row["helpdesk_phone"] or ""),
        "price_display": str(row["price_display"] or ""),
        "price_currency": str(row["price_currency"] or ""),
        "detail_last_updated": str(row["detail_last_updated"] or ""),
        "amenities_total": int(row["amenities_total"] or 0),
        "nearest_amenity_kind": str(row["nearest_amenity_kind"] or ""),
        "nearest_amenity_name": str(row["nearest_amenity_name"] or ""),
        "nearest_amenity_distance_m": _float_or_none(row["nearest_amenity_distance_m"]),
        "amenity_category_counts": _json_value(row["amenity_category_counts_json"], default={}),
    }
    return payload


def _bounding_box(latitude: float, longitude: float, radius_m: int) -> tuple[float, float, float, float]:
    lat_delta = radius_m / 111_320.0
    lon_scale = max(math.cos(math.radians(latitude)), 0.01)
    lon_delta = radius_m / (111_320.0 * lon_scale)
    return (
        max(-90.0, latitude - lat_delta),
        min(90.0, latitude + lat_delta),
        max(-180.0, longitude - lon_delta),
        min(180.0, longitude + lon_delta),
    )


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _json_value(value: Any, *, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    text = str(value or "").strip()
    if not text:
        return default
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return default


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None
