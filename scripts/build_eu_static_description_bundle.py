#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import sys
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.at_econtrol import (  # noqa: E402
    ECONTROL_PUBLIC_API_DATEX_STATUS_SOURCE_UID,
    ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
    ECONTROL_PUBLIC_API_DATEX_TABLE_URL,
    ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
    ECONTROL_PUBLIC_API_SEARCH_URL,
    iter_datex_status_rows_from_binary_stream as iter_at_datex_status_rows_from_binary_stream,
    iter_datex_table_station_rows_from_binary_stream as iter_at_datex_table_station_rows_from_binary_stream,
    iter_api_search_station_rows_from_binary_stream,
)
from commercial_backend.be_transportdata import (  # noqa: E402
    ENERGYVISION_LOCATIONS_SOURCE_UID,
    ENERGYVISION_LOCATIONS_URL,
    ENERGYVISION_PRODUCTION_OCPI_VERSION,
    ENERGYVISION_TARIFFS_SOURCE_UID,
    ENERGYVISION_TARIFFS_URL,
    GROUP_INDIGO_STATIC_SOURCE_UID,
    GROUP_INDIGO_STATIC_URL,
    ROAD_OCPI_LOCATIONS_SOURCE_UID,
    ROAD_OCPI_LOCATIONS_URL,
    attach_energyvision_tariff_prices,
    iter_energyvision_location_rows_from_binary_stream,
    iter_energyvision_tariff_rows_from_binary_stream,
    iter_group_indigo_static_rows_from_binary_stream,
    iter_road_location_rows_from_binary_stream,
)
from commercial_backend.config import AppConfig  # noqa: E402
from commercial_backend.dk_monta import (  # noqa: E402
    BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
    BE_MONTA_PROVIDER_UID,
    MONTA_AFIR_CHARGE_POINTS_SOURCE_UID as DK_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
    MONTA_AFIR_CHARGE_POINTS_URL as DK_MONTA_AFIR_CHARGE_POINTS_URL,
    MONTA_PROVIDER_UID as DK_MONTA_PROVIDER_UID,
    iter_be_static_rows_from_binary_stream as iter_be_monta_static_rows_from_binary_stream,
    iter_static_rows_from_binary_stream as iter_dk_monta_static_rows_from_binary_stream,
)
from commercial_backend.fi_digitraffic import (  # noqa: E402
    DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID,
    DIGITRAFFIC_DATEX_LOCATIONS_URL,
    iter_location_rows_from_binary_stream as iter_fi_location_rows_from_binary_stream,
)
from commercial_backend.eu_public_static import (  # noqa: E402
    CY_TRAFFIC4CYPRUS_SOURCE_UID,
    CY_TRAFFIC4CYPRUS_URL,
    CZ_MPO_REGISTER_SOURCE_UID,
    CZ_MPO_REGISTER_URL,
    ES_DGT_ELECTROLINERAS_SOURCE_UID,
    ES_DGT_ELECTROLINERAS_URL,
    GR_IDRO_STATIC_ZIP_URL,
    GR_ELECTROKINISI_SOURCE_UID,
    LT_EV_LOCATIONS_SOURCE_UID,
    LT_EV_LOCATIONS_URL,
    LU_CHARGING_STATIONS_SOURCE_UID,
    LU_CHARGING_STATIONS_WFS_URL,
    MT_CHARGING_POINTS_SOURCE_UID,
    MT_CHARGING_POINTS_URL,
    NOBIL_DATADUMP_URL,
    NO_NOBIL_STATIC_SOURCE_UID,
    SE_NOBIL_STATIC_SOURCE_UID,
    iter_cy_rows_from_binary_stream,
    iter_cz_rows_from_binary_stream,
    iter_es_rows_from_binary_stream,
    iter_gr_rows_from_binary_stream,
    iter_lt_rows_from_binary_stream,
    iter_lu_rows_from_binary_stream,
    iter_mt_rows_from_binary_stream,
    iter_no_nobil_rows_from_binary_stream,
    iter_se_nobil_rows_from_binary_stream,
)
from commercial_backend.fr_datagouv import (  # noqa: E402
    BASE_NATIONALE_DATASET_PAGE_URL,
    BASE_NATIONALE_PROVIDER_UID,
    BASE_NATIONALE_STATIC_RESOURCE_URL,
    BASE_NATIONALE_STATIC_SOURCE_UID,
    ECO_MOVEMENT_STATIC_RESOURCE_URL,
    ECO_MOVEMENT_STATIC_SOURCE_UID,
    iter_base_nationale_static_rows_from_binary_stream,
    iter_static_rows_from_binary_stream as iter_fr_static_rows_from_binary_stream,
)
from commercial_backend.hu_nap import (  # noqa: E402
    HU_NAP_CONTRACTED_PROFILE_SUMMARIES_URL,
    HU_NAP_ECO_MOVEMENT_PROVIDER_UID,
    HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
    HU_NAP_MOBILITI_PROVIDER_UID,
    HU_NAP_MOBILITI_STATIC_SOURCE_UID,
    HU_NAP_PORTAL_URL,
    iter_eco_movement_static_rows_from_binary_stream as iter_hu_eco_movement_static_rows_from_binary_stream,
    iter_mobiliti_static_rows_from_binary_stream as iter_hu_mobiliti_static_rows_from_binary_stream,
)
from commercial_backend.lv_transportdata import (  # noqa: E402
    LV_ECO_MOVEMENT_PROVIDER_UID,
    LV_ECO_MOVEMENT_STATIC_CARD_URL,
    LV_ECO_MOVEMENT_STATIC_SOURCE_UID,
    LV_ECO_MOVEMENT_STATUS_PRICE_CARD_URL,
    LV_ECO_MOVEMENT_STATUS_PRICE_SOURCE_UID,
    LV_LVC_EV_CHARGING_STREAM_CARD_URL,
    LV_LVC_EV_CHARGING_STREAM_SOURCE_UID,
    LV_LVC_PROVIDER_UID,
    iter_eco_movement_static_rows_from_binary_stream as iter_lv_eco_movement_static_rows_from_binary_stream,
    iter_lvc_static_rows_from_binary_stream as iter_lv_lvc_static_rows_from_binary_stream,
)
from commercial_backend.pl_eipa import (  # noqa: E402
    EIPA_BROWSER_PROVINCE_SOURCE_UID,
    EIPA_BROWSER_URL,
    EIPA_READER_DOCS_URL,
    EIPA_READER_FILE_SOURCE_UIDS,
    EIPA_READER_POINT_SOURCE_UID,
    EIPA_READER_STATIC_FILE_KEYS,
    iter_browser_rows as iter_pl_browser_rows,
    iter_reader_static_rows as iter_pl_reader_static_rows,
)
from commercial_backend.pt_mobie import (  # noqa: E402
    PT_MOBIE_PROVIDER_UID,
    PT_MOBIE_STATIC_SOURCE_UID,
    PT_MOBIE_STATIC_URL,
    PT_NAP_STATIC_DETAIL_URL,
    iter_static_rows_from_binary_stream as iter_pt_mobie_static_rows_from_binary_stream,
)
from commercial_backend.si_nap import (  # noqa: E402
    SI_NAP_DATASET_TABLE_URL,
    SI_NAP_PROVIDER_UID,
    SI_NAP_TABLE_SOURCE_UID,
    SI_NAP_TABLE_URL,
    iter_table_rows_from_binary_stream as iter_si_table_rows_from_binary_stream,
)
from commercial_backend.store import SQLiteIngestStore  # noqa: E402
import scripts.build_onboarded_static_catalog as onboarded  # noqa: E402
from scripts import osm_amenities  # noqa: E402

COUNTRIES = ("AT", "BE", "FR", "FI", "PL", "NL", "CH", "DE", "DK", "CY", "CZ", "ES", "GR", "HU", "LT", "LU", "LV", "MT", "NO", "PT", "SE", "SI")
STATIC_DESCRIPTION_COUNTRIES = ("AT", "BE", "FR", "FI", "PL", "DK", "CY", "CZ", "ES", "GR", "HU", "LT", "LU", "LV", "MT", "NO", "PT", "SE", "SI")
DE_SOURCE_UID = "de_woladen_bnetza_static_bundle"
DE_SOURCE_NAME = "DE woladen.de BNetzA static bundle"
DE_STATION_ID_PREFIX = "DE:"
PUBLIC_STATUS = "open_static_bundle_release_candidate_pending_human_publication_review_2026-05-03"
APPROVED_STATIC_LICENSE_STATUS = "human_terms_review_approved_for_open_static_bundle_2026-05-03"
PENDING_STATIC_LICENSE_STATUS = "source_terms_pending_human_publication_review_2026-05-03"
LT_VIA_LIETUVA_STATIC_FALLBACK_FILE = "data/lt-EnergyInfrastructureTablePublication.xml"
LT_VIA_LIETUVA_STATIC_FALLBACK_SHA256 = "6f3fa1ab3ea0ea8a782c88a088ba03ea1c3d6bdaee5b59e66f976a16ab219f8d"

EU_OSM_PBF_URLS = {
    country_code: osm_amenities.COUNTRY_PBF_URLS[country_code]
    for country_code in COUNTRIES
}
SOURCE_PROVIDER_UIDS = {
    ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID: "at_econtrol_public_api",
    ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID: "at_econtrol_public_api",
    ENERGYVISION_LOCATIONS_SOURCE_UID: "be_energyvision",
    ROAD_OCPI_LOCATIONS_SOURCE_UID: "be_road",
    GROUP_INDIGO_STATIC_SOURCE_UID: "be_group_indigo",
    BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID: BE_MONTA_PROVIDER_UID,
    DK_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID: DK_MONTA_PROVIDER_UID,
    BASE_NATIONALE_STATIC_SOURCE_UID: BASE_NATIONALE_PROVIDER_UID,
    ECO_MOVEMENT_STATIC_SOURCE_UID: "fr_eco_movement_afir_irve",
    DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID: "fi_digitraffic_afir_datex",
    HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID: HU_NAP_ECO_MOVEMENT_PROVIDER_UID,
    HU_NAP_MOBILITI_STATIC_SOURCE_UID: HU_NAP_MOBILITI_PROVIDER_UID,
    LV_ECO_MOVEMENT_STATIC_SOURCE_UID: LV_ECO_MOVEMENT_PROVIDER_UID,
    LV_LVC_EV_CHARGING_STREAM_SOURCE_UID: LV_LVC_PROVIDER_UID,
    PT_MOBIE_STATIC_SOURCE_UID: PT_MOBIE_PROVIDER_UID,
    SI_NAP_TABLE_SOURCE_UID: SI_NAP_PROVIDER_UID,
    EIPA_BROWSER_PROVINCE_SOURCE_UID: "pl_eipa_browser_province_pages",
    EIPA_READER_POINT_SOURCE_UID: "pl_eipa_reader_json",
    CY_TRAFFIC4CYPRUS_SOURCE_UID: "cy_traffic4cyprus",
    CZ_MPO_REGISTER_SOURCE_UID: "cz_mpo_register",
    ES_DGT_ELECTROLINERAS_SOURCE_UID: "es_dgt_electrolineras",
    GR_ELECTROKINISI_SOURCE_UID: "gr_electrokinisi",
    LT_EV_LOCATIONS_SOURCE_UID: "lt_vialietuva_datex",
    LU_CHARGING_STATIONS_SOURCE_UID: "lu_data_public",
    MT_CHARGING_POINTS_SOURCE_UID: "mt_transport_geoservices",
    NO_NOBIL_STATIC_SOURCE_UID: "no_nobil",
    SE_NOBIL_STATIC_SOURCE_UID: "se_nobil",
}

NORMALIZED_STATION_FIELDS = (
    "country_code",
    "station_id",
    "source_uid",
    "source_station_id",
    "license",
    "provider_uid",
    "operator_name",
    "station_name",
    "address",
    "postal_code",
    "city",
    "latitude",
    "longitude",
    "charger_count",
    "max_power_kw",
    "connector_types",
    "source_url",
    "public_bundle_status",
    "id_rule",
    "opening_hours",
    "payment_methods",
    "auth_methods",
    "green_energy",
    "helpdesk_phone",
    "price_display",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_currency",
    "price_quality",
    "detail_last_updated",
)

NORMALIZED_CHARGER_FIELDS = (
    "country_code",
    "station_id",
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
)

GENERATED_FILES = {
    "stations.csv",
    "chargers.csv",
    "chargers_full.csv",
    "chargers_fast.csv",
    "chargers_fast.geojson",
    "operators.json",
    "summary.json",
    "catalog_summary.json",
    "source_attribution.json",
    "dedupe_report.csv",
    "bundle_quality_report.json",
    "ingestion_metrics.json",
    "station_amenities.csv",
    "mobilithek_afir_provider_configs.json",
    "mobilithek_afir_static_matches.csv",
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        return onboarded._coordinate_text(value)
    return value


def _compact_json(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _safe_fragment(value: Any) -> str:
    text = _text(value).lower()
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-", "*"} else "-" for ch in text)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unknown"


def _join_unique(values: Iterable[Any], *, separator: str = ";") -> str:
    seen: dict[str, None] = {}
    for value in values:
        text = _text(value)
        if text:
            seen.setdefault(text, None)
    return separator.join(seen.keys())


def _country_from_station_id(station_id: str) -> str:
    prefix = _text(station_id).split(":", 1)[0].upper()
    if prefix in COUNTRIES:
        return prefix
    return "DE"


def _public_de_station_id(value: Any) -> str:
    station_id = _text(value)
    if not station_id:
        return ""
    if station_id.lower().startswith(DE_STATION_ID_PREFIX.lower()):
        return f"{DE_STATION_ID_PREFIX}{station_id[len(DE_STATION_ID_PREFIX):]}"
    return f"{DE_STATION_ID_PREFIX}{station_id}"


def _legacy_de_station_id(value: Any) -> str:
    station_id = _text(value)
    if station_id.lower().startswith(DE_STATION_ID_PREFIX.lower()):
        return station_id[len(DE_STATION_ID_PREFIX):]
    return station_id


def _project_de_station_ids(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    projected: list[dict[str, str]] = []
    for row in rows:
        next_row = dict(row)
        station_id = _public_de_station_id(next_row.get("station_id"))
        if station_id:
            next_row["station_id"] = station_id
        projected.append(next_row)
    return projected


def _provider_uid_for_source_uid(source_uid: str) -> str:
    return SOURCE_PROVIDER_UIDS.get(_text(source_uid), _text(source_uid))


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field, "")) for field in fieldnames})


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_optional_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return _read_csv_rows(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _prepare_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name in GENERATED_FILES:
        path = output_dir / name
        if path.exists():
            path.unlink()


def _connect(config: AppConfig) -> sqlite3.Connection:
    conn = sqlite3.connect(config.sqlite_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    return conn


def _latest_payload_rows(
    conn: sqlite3.Connection,
    *,
    source_uid: str,
    all_latest_day: bool,
) -> list[sqlite3.Row]:
    latest = conn.execute(
        """
        SELECT date(first_received_at) AS received_date
        FROM raw_payloads
        WHERE source_uid = ?
        ORDER BY first_received_at DESC
        LIMIT 1
        """,
        (source_uid,),
    ).fetchone()
    if latest is None:
        return []
    if all_latest_day:
        return conn.execute(
            """
            SELECT *
            FROM raw_payloads
            WHERE source_uid = ?
              AND date(first_received_at) = ?
            ORDER BY first_received_at, payload_sha256
            """,
            (source_uid, latest["received_date"]),
        ).fetchall()
    return conn.execute(
        """
        SELECT *
        FROM raw_payloads
        WHERE source_uid = ?
        ORDER BY first_received_at DESC
        LIMIT 1
        """,
        (source_uid,),
    ).fetchall()


def _latest_batched_pull_payload_rows(conn: sqlite3.Connection, *, source_uid: str) -> list[sqlite3.Row]:
    latest = conn.execute(
        """
        SELECT r.*, p.received_at AS receipt_received_at, p.request_query AS request_query
        FROM push_receipts p
        JOIN raw_payloads r ON r.payload_sha256 = p.payload_sha256
        WHERE p.source_uid = ?
        ORDER BY p.received_at DESC
        LIMIT 1
        """,
        (source_uid,),
    ).fetchone()
    if latest is None:
        return []
    batch_id = (urllib.parse.parse_qs(str(latest["request_query"] or "")).get("batch_id") or [""])[0]
    if not batch_id:
        return [latest]
    return conn.execute(
        """
        SELECT r.*, p.received_at AS receipt_received_at, p.request_query AS request_query
        FROM push_receipts p
        JOIN raw_payloads r ON r.payload_sha256 = p.payload_sha256
        WHERE p.source_uid = ?
          AND p.request_query LIKE ?
        ORDER BY p.request_query, p.received_at
        """,
        (source_uid, f"%batch_id={batch_id}%"),
    ).fetchall()


def _row_source(source_uid: str) -> dict[str, str]:
    sources = {
        ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID: {
            "country_code": "AT",
            "url": ECONTROL_PUBLIC_API_SEARCH_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID: {
            "country_code": "AT",
            "url": ECONTROL_PUBLIC_API_DATEX_TABLE_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        ENERGYVISION_LOCATIONS_SOURCE_UID: {
            "country_code": "BE",
            "url": ENERGYVISION_LOCATIONS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        ROAD_OCPI_LOCATIONS_SOURCE_UID: {
            "country_code": "BE",
            "url": ROAD_OCPI_LOCATIONS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        GROUP_INDIGO_STATIC_SOURCE_UID: {
            "country_code": "BE",
            "url": GROUP_INDIGO_STATIC_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID: {
            "country_code": "BE",
            "url": DK_MONTA_AFIR_CHARGE_POINTS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        DK_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID: {
            "country_code": "DK",
            "url": DK_MONTA_AFIR_CHARGE_POINTS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        ECO_MOVEMENT_STATIC_SOURCE_UID: {
            "country_code": "FR",
            "url": ECO_MOVEMENT_STATIC_RESOURCE_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        BASE_NATIONALE_STATIC_SOURCE_UID: {
            "country_code": "FR",
            "url": BASE_NATIONALE_STATIC_RESOURCE_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID: {
            "country_code": "FI",
            "url": DIGITRAFFIC_DATEX_LOCATIONS_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        PT_MOBIE_STATIC_SOURCE_UID: {
            "country_code": "PT",
            "url": PT_MOBIE_STATIC_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        SI_NAP_TABLE_SOURCE_UID: {
            "country_code": "SI",
            "url": SI_NAP_TABLE_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID: {
            "country_code": "HU",
            "url": HU_NAP_PORTAL_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        HU_NAP_MOBILITI_STATIC_SOURCE_UID: {
            "country_code": "HU",
            "url": HU_NAP_PORTAL_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        LV_ECO_MOVEMENT_STATIC_SOURCE_UID: {
            "country_code": "LV",
            "url": LV_ECO_MOVEMENT_STATIC_CARD_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        LV_LVC_EV_CHARGING_STREAM_SOURCE_UID: {
            "country_code": "LV",
            "url": LV_LVC_EV_CHARGING_STREAM_CARD_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        EIPA_BROWSER_PROVINCE_SOURCE_UID: {
            "country_code": "PL",
            "url": EIPA_BROWSER_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        EIPA_READER_POINT_SOURCE_UID: {
            "country_code": "PL",
            "url": EIPA_READER_DOCS_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        CY_TRAFFIC4CYPRUS_SOURCE_UID: {
            "country_code": "CY",
            "url": CY_TRAFFIC4CYPRUS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        CZ_MPO_REGISTER_SOURCE_UID: {
            "country_code": "CZ",
            "url": CZ_MPO_REGISTER_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        ES_DGT_ELECTROLINERAS_SOURCE_UID: {
            "country_code": "ES",
            "url": ES_DGT_ELECTROLINERAS_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        GR_ELECTROKINISI_SOURCE_UID: {
            "country_code": "GR",
            "url": GR_IDRO_STATIC_ZIP_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        LT_EV_LOCATIONS_SOURCE_UID: {
            "country_code": "LT",
            "url": LT_EV_LOCATIONS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        LU_CHARGING_STATIONS_SOURCE_UID: {
            "country_code": "LU",
            "url": LU_CHARGING_STATIONS_WFS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        MT_CHARGING_POINTS_SOURCE_UID: {
            "country_code": "MT",
            "url": MT_CHARGING_POINTS_URL,
            "license": PENDING_STATIC_LICENSE_STATUS,
        },
        NO_NOBIL_STATIC_SOURCE_UID: {
            "country_code": "NO",
            "url": NOBIL_DATADUMP_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
        SE_NOBIL_STATIC_SOURCE_UID: {
            "country_code": "SE",
            "url": NOBIL_DATADUMP_URL,
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
    }
    return sources.get(
        source_uid,
        {
            "country_code": "",
            "url": "",
            "license": APPROVED_STATIC_LICENSE_STATUS,
        },
    )


def _iter_binary_rows(
    *,
    store: SQLiteIngestStore,
    storage_uri: str,
    content_encoding: str,
    row_stream_factory: Callable[..., Iterable[dict[str, Any]]],
) -> Iterable[dict[str, Any]]:
    try:
        with store.open_storage_uri_reader(storage_uri) as payload_stream:
            yield from row_stream_factory(payload_stream, content_encoding=content_encoding)
    except FileNotFoundError as exc:
        print(f"warning: archived payload file missing, skipping {storage_uri}: {exc}", file=sys.stderr)


def _iter_pl_rows(*, store: SQLiteIngestStore, storage_uri: str) -> Iterable[dict[str, Any]]:
    try:
        with store.open_storage_uri_reader(storage_uri) as payload_stream:
            yield from iter_pl_browser_rows(payload_stream.read().decode("utf-8", errors="replace"))
    except FileNotFoundError as exc:
        print(f"warning: archived PL payload file missing, skipping {storage_uri}: {exc}", file=sys.stderr)


def _read_json_storage(store: SQLiteIngestStore, storage_uri: str) -> dict[str, Any]:
    try:
        with store.open_storage_uri_reader(storage_uri) as payload_stream:
            payload = json.loads(payload_stream.read().decode("utf-8"))
    except FileNotFoundError as exc:
        print(f"warning: archived JSON payload file missing, skipping {storage_uri}: {exc}", file=sys.stderr)
        return {}
    return payload if isinstance(payload, dict) else {}


def _latest_pl_reader_static_payloads(
    conn: sqlite3.Connection,
) -> tuple[dict[str, sqlite3.Row], list[str]]:
    payload_rows: dict[str, sqlite3.Row] = {}
    missing: list[str] = []
    for file_key in EIPA_READER_STATIC_FILE_KEYS:
        rows = _latest_payload_rows(
            conn,
            source_uid=EIPA_READER_FILE_SOURCE_UIDS[file_key],
            all_latest_day=False,
        )
        if not rows:
            missing.append(file_key)
            continue
        payload_rows[file_key] = rows[0]
    return payload_rows, missing


def _iter_pl_reader_rows(
    *,
    store: SQLiteIngestStore,
    payload_rows_by_file: dict[str, sqlite3.Row],
) -> Iterable[dict[str, Any]]:
    payloads = {
        file_key: _read_json_storage(store, str(payload_rows_by_file[file_key]["storage_uri"]))
        for file_key in EIPA_READER_STATIC_FILE_KEYS
    }
    yield from iter_pl_reader_static_rows(
        operator_payload=payloads["operator"],
        pool_payload=payloads["pool"],
        station_payload=payloads["station"],
        point_payload=payloads["point"],
        dictionary_payload=payloads["dictionary"],
    )


def _at_status_price_lookup(conn: sqlite3.Connection, store: SQLiteIngestStore) -> dict[str, dict[str, Any]]:
    payload_rows = _latest_payload_rows(
        conn,
        source_uid=ECONTROL_PUBLIC_API_DATEX_STATUS_SOURCE_UID,
        all_latest_day=False,
    )
    lookup: dict[str, dict[str, Any]] = {}
    for payload_row in payload_rows:
        for row in _iter_binary_rows(
            store=store,
            storage_uri=str(payload_row["storage_uri"]),
            content_encoding=str(payload_row["content_encoding"] or ""),
            row_stream_factory=iter_at_datex_status_rows_from_binary_stream,
        ):
            if not _text(row.get("price_display")) and not _text(row.get("price_energy_eur_kwh_min")):
                continue
            lookup[_text(row.get("charger_id"))] = {
                key: row.get(key)
                for key in (
                    "price_display",
                    "price_energy_eur_kwh_min",
                    "price_energy_eur_kwh_max",
                    "price_currency",
                    "price_quality",
                    "price_source_text",
                )
                if row.get(key) not in (None, "")
            }
    return lookup


def _be_energyvision_tariff_rows(conn: sqlite3.Connection, store: SQLiteIngestStore) -> list[dict[str, Any]]:
    tariff_rows: list[dict[str, Any]] = []
    for payload_row in _latest_payload_rows(
        conn,
        source_uid=ENERGYVISION_TARIFFS_SOURCE_UID,
        all_latest_day=False,
    ):
        tariff_rows.extend(
            _iter_binary_rows(
                store=store,
                storage_uri=str(payload_row["storage_uri"]),
                content_encoding=str(payload_row["content_encoding"] or ""),
                row_stream_factory=iter_energyvision_tariff_rows_from_binary_stream,
            )
        )
    return tariff_rows


def _source_rows(config: AppConfig) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    store = SQLiteIngestStore(config)
    rows: list[dict[str, Any]] = []
    source_summaries: dict[str, Any] = {}
    with _connect(config) as conn:
        at_status_price_lookup = _at_status_price_lookup(conn, store)
        be_energyvision_tariff_rows = _be_energyvision_tariff_rows(conn, store)
        at_static_payload_rows = _latest_payload_rows(
            conn,
            source_uid=ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
            all_latest_day=False,
        )
        if at_static_payload_rows:
            at_static_spec = {
                "source_uid": ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
                "factory": iter_at_datex_table_station_rows_from_binary_stream,
                "all_latest_day": False,
            }
        else:
            at_static_spec = {
                "source_uid": ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
                "factory": iter_api_search_station_rows_from_binary_stream,
                "all_latest_day": False,
                "fallback_reason": "at_econtrol_public_api_datex_table_payload_missing",
            }
        fr_static_payload_rows = _latest_payload_rows(
            conn,
            source_uid=BASE_NATIONALE_STATIC_SOURCE_UID,
            all_latest_day=False,
        )
        if fr_static_payload_rows:
            fr_static_spec = {
                "source_uid": BASE_NATIONALE_STATIC_SOURCE_UID,
                "factory": iter_base_nationale_static_rows_from_binary_stream,
                "all_latest_day": False,
            }
        else:
            fr_static_spec = {
                "source_uid": ECO_MOVEMENT_STATIC_SOURCE_UID,
                "factory": iter_fr_static_rows_from_binary_stream,
                "all_latest_day": False,
                "fallback_reason": "fr_base_nationale_irve_static_payload_missing",
            }
        pl_reader_payload_rows, pl_reader_missing = _latest_pl_reader_static_payloads(conn)
        if not pl_reader_missing:
            pl_static_spec = {
                "source_uid": EIPA_READER_POINT_SOURCE_UID,
                "factory": "pl_reader_static",
                "all_latest_day": False,
                "payload_rows_by_file": pl_reader_payload_rows,
            }
        else:
            pl_static_spec = {
                "source_uid": EIPA_BROWSER_PROVINCE_SOURCE_UID,
                "factory": None,
                "all_latest_day": True,
                "fallback_reason": "pl_eipa_reader_static_json_missing:" + ",".join(pl_reader_missing),
            }

    specs = (
        at_static_spec,
        {
            "source_uid": ENERGYVISION_LOCATIONS_SOURCE_UID,
            "factory": iter_energyvision_location_rows_from_binary_stream,
            "all_latest_day": False,
            "tariff_row_count": len(be_energyvision_tariff_rows),
        },
        {
            "source_uid": ROAD_OCPI_LOCATIONS_SOURCE_UID,
            "factory": iter_road_location_rows_from_binary_stream,
            "all_latest_day": False,
            "static_candidate_from_dynamic_payload": True,
        },
        {
            "source_uid": GROUP_INDIGO_STATIC_SOURCE_UID,
            "factory": iter_group_indigo_static_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
            "factory": iter_be_monta_static_rows_from_binary_stream,
            "all_latest_day": True,
        },
        fr_static_spec,
        {
            "source_uid": DK_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
            "factory": iter_dk_monta_static_rows_from_binary_stream,
            "all_latest_day": True,
        },
        {
            "source_uid": DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID,
            "factory": iter_fi_location_rows_from_binary_stream,
            "all_latest_day": False,
        },
        pl_static_spec,
        {
            "source_uid": CY_TRAFFIC4CYPRUS_SOURCE_UID,
            "factory": iter_cy_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": CZ_MPO_REGISTER_SOURCE_UID,
            "factory": iter_cz_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": ES_DGT_ELECTROLINERAS_SOURCE_UID,
            "factory": iter_es_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": GR_ELECTROKINISI_SOURCE_UID,
            "factory": iter_gr_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": LT_EV_LOCATIONS_SOURCE_UID,
            "factory": iter_lt_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": LU_CHARGING_STATIONS_SOURCE_UID,
            "factory": iter_lu_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": MT_CHARGING_POINTS_SOURCE_UID,
            "factory": iter_mt_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": NO_NOBIL_STATIC_SOURCE_UID,
            "factory": iter_no_nobil_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": PT_MOBIE_STATIC_SOURCE_UID,
            "factory": iter_pt_mobie_static_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": SE_NOBIL_STATIC_SOURCE_UID,
            "factory": iter_se_nobil_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": SI_NAP_TABLE_SOURCE_UID,
            "factory": iter_si_table_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
            "factory": iter_hu_eco_movement_static_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": HU_NAP_MOBILITI_STATIC_SOURCE_UID,
            "factory": iter_hu_mobiliti_static_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": LV_ECO_MOVEMENT_STATIC_SOURCE_UID,
            "factory": iter_lv_eco_movement_static_rows_from_binary_stream,
            "all_latest_day": False,
        },
        {
            "source_uid": LV_LVC_EV_CHARGING_STREAM_SOURCE_UID,
            "factory": iter_lv_lvc_static_rows_from_binary_stream,
            "all_latest_day": False,
        },
    )
    with _connect(config) as conn:
        for spec in specs:
            source_uid = str(spec["source_uid"])
            if spec["factory"] == "pl_reader_static":
                payload_rows = list(spec["payload_rows_by_file"].values())
            else:
                if source_uid == ENERGYVISION_LOCATIONS_SOURCE_UID:
                    payload_rows = _latest_batched_pull_payload_rows(conn, source_uid=source_uid)
                else:
                    payload_rows = _latest_payload_rows(
                        conn,
                        source_uid=source_uid,
                        all_latest_day=bool(spec["all_latest_day"]),
                    )
            source_count = 0
            if spec["factory"] == "pl_reader_static":
                iterator = _iter_pl_reader_rows(
                    store=store,
                    payload_rows_by_file=dict(spec["payload_rows_by_file"]),
                )
                for row in iterator:
                    metadata = _row_source(source_uid)
                    enriched = dict(row)
                    enriched["source_url"] = metadata["url"]
                    enriched["license"] = metadata["license"]
                    enriched["public_bundle_status"] = PUBLIC_STATUS
                    rows.append(enriched)
                    source_count += 1
            else:
                for payload_row in payload_rows:
                    if spec["factory"] is None:
                        iterator = _iter_pl_rows(store=store, storage_uri=str(payload_row["storage_uri"]))
                    else:
                        iterator = _iter_binary_rows(
                            store=store,
                            storage_uri=str(payload_row["storage_uri"]),
                            content_encoding=str(payload_row["content_encoding"] or ""),
                            row_stream_factory=spec["factory"],
                        )
                    for row in iterator:
                        metadata = _row_source(source_uid)
                        enriched = dict(row)
                        if source_uid == ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID:
                            enriched.update(at_status_price_lookup.get(_text(enriched.get("charger_id")), {}))
                        elif source_uid == ENERGYVISION_LOCATIONS_SOURCE_UID and be_energyvision_tariff_rows:
                            enriched = next(
                                attach_energyvision_tariff_prices([enriched], be_energyvision_tariff_rows),
                                enriched,
                            )
                        elif source_uid == ROAD_OCPI_LOCATIONS_SOURCE_UID:
                            # The Road payload carries live status next to static OCPI fields.
                            # Keep only static catalog fields in this open-bundle candidate.
                            for private_key in ("source_status", "availability_status", "source_observed_at"):
                                enriched.pop(private_key, None)
                            enriched["date_updated"] = ""
                        elif source_uid in {
                            BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
                            DK_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
                        }:
                            renewable_energy = _text(enriched.get("renewable_energy"))
                            if renewable_energy:
                                enriched["green_energy"] = renewable_energy
                        enriched["source_url"] = metadata["url"]
                        enriched["license"] = metadata["license"]
                        enriched["public_bundle_status"] = PUBLIC_STATUS
                        rows.append(enriched)
                        source_count += 1
            source_summaries[source_uid] = {
                "country_code": _row_source(source_uid)["country_code"],
                "payload_count": len(payload_rows),
                "row_count": source_count,
                **(
                    {
                        "reader_static_payloads": {
                            file_key: {
                                "source_uid": EIPA_READER_FILE_SOURCE_UIDS[file_key],
                                "payload_sha256": str(row["payload_sha256"]),
                                "byte_length": int(row["byte_length"]),
                                "first_received_at": str(row["first_received_at"]),
                            }
                            for file_key, row in spec["payload_rows_by_file"].items()
                        }
                    }
                    if spec["factory"] == "pl_reader_static"
                    else {}
                ),
                **({"fallback_reason": spec["fallback_reason"]} if spec.get("fallback_reason") else {}),
                **({"tariff_row_count": spec["tariff_row_count"]} if "tariff_row_count" in spec else {}),
                **(
                    {"static_candidate_from_dynamic_payload": True}
                    if spec.get("static_candidate_from_dynamic_payload")
                    else {}
                ),
                **_row_source(source_uid),
            }
    return rows, source_summaries


def _infer_postal_city(country_code: str, address: str, postal_code: str, city: str) -> tuple[str, str]:
    if postal_code and city:
        return postal_code, city
    import re

    patterns = {
        "AT": r"\b([0-9]{4})\s+([^,]+)$",
        "FR": r"\b([0-9]{5})\s+([^,]+)$",
        "PL": r"\b([0-9]{2}-[0-9]{3})\s+([^,]+)$",
    }
    pattern = patterns.get(country_code)
    if not pattern:
        return postal_code, city
    match = re.search(pattern, _text(address))
    if not match:
        return postal_code, city
    return postal_code or match.group(1).strip(), city or match.group(2).strip()


def _best_station_source_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def score(row: dict[str, Any]) -> tuple[int, str]:
        fields = (
            "latitude",
            "longitude",
            "operator_name",
            "station_name",
            "address",
            "postal_code",
            "city",
            "price_display",
            "price_energy_eur_kwh_min",
            "opening_hours",
            "payment_methods",
            "auth_methods",
            "helpdesk_phone",
        )
        return sum(1 for field in fields if _text(row.get(field))), _text(row.get("date_updated"))

    return max(rows, key=score)


def _price_float_values(rows: list[dict[str, Any]], field_name: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _float_or_none(row.get(field_name))
        if value is None:
            continue
        if not any(abs(value - existing) < 0.000001 for existing in values):
            values.append(value)
    return values


def _price_decimal_text(value: float | None) -> str:
    if value is None:
        return ""
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _station_price_fields(rows: list[dict[str, Any]]) -> dict[str, str]:
    displays = [_text(row.get("price_display")) for row in rows if _text(row.get("price_display"))]
    currencies = [_text(row.get("price_currency")) for row in rows if _text(row.get("price_currency"))]
    qualities = [_text(row.get("price_quality")) for row in rows if _text(row.get("price_quality"))]
    energy_min_values = _price_float_values(rows, "price_energy_eur_kwh_min")
    energy_max_values = _price_float_values(rows, "price_energy_eur_kwh_max")
    if not (displays or energy_min_values or energy_max_values):
        return {}

    unique_displays = list(dict.fromkeys(displays))
    energy_min = min(energy_min_values) if energy_min_values else None
    energy_max = max(energy_max_values or energy_min_values) if (energy_max_values or energy_min_values) else None
    return {
        "price_display": unique_displays[0] if unique_displays else "",
        "price_energy_eur_kwh_min": _price_decimal_text(energy_min),
        "price_energy_eur_kwh_max": _price_decimal_text(energy_max),
        "price_currency": list(dict.fromkeys(currencies))[0] if currencies else ("EUR" if energy_min is not None else ""),
        "price_quality": "source_tarification_mixed"
        if len(unique_displays) > 1
        else (list(dict.fromkeys(qualities))[0] if qualities else "source_tarification"),
    }


def _flat_rows_to_station_charger_rows(
    flat_rows: list[dict[str, Any]],
) -> tuple[list[onboarded.StationRow], list[onboarded.ChargerRow], list[dict[str, str]]]:
    charger_by_id: dict[str, onboarded.ChargerRow] = {}
    source_rows_by_station: dict[str, list[dict[str, Any]]] = defaultdict(list)
    dedupe_rows: list[dict[str, str]] = []

    for row in flat_rows:
        station_id = _text(row.get("station_id"))
        charger_id = _text(row.get("charger_id"))
        if not station_id or not charger_id:
            continue
        source_rows_by_station[station_id].append(row)
        if charger_id in charger_by_id:
            dedupe_rows.append(
                {
                    "issue": "duplicate_normalized_charger_id",
                    "country_code": _text(row.get("country_code")),
                    "station_id": station_id,
                    "source_uid": _text(row.get("source_uid")),
                    "details": f"{charger_id} replaced by later source row",
                }
            )
        charger_by_id[charger_id] = onboarded.ChargerRow(
            country_code=_text(row.get("country_code")),
            station_id=station_id,
            charger_id=charger_id,
            source_uid=_text(row.get("source_uid")),
            source_station_id=_text(row.get("source_station_id")),
            source_evse_id=_text(row.get("source_evse_id")),
            connector_id=_text(row.get("connector_id")),
            connector_type=_text(row.get("connector_types") or row.get("connector_type")),
            current_type=_text(row.get("current_type")),
            max_power_kw=_float_or_none(row.get("max_power_kw")),
            operator_name=_text(row.get("operator_name")),
        )

    chargers = [charger_by_id[key] for key in sorted(charger_by_id)]
    chargers_by_station = onboarded._chargers_by_station(chargers)
    stations: list[onboarded.StationRow] = []
    for station_id, rows in sorted(source_rows_by_station.items()):
        first = _best_station_source_row(rows)
        country_code = _text(first.get("country_code"))
        address = _text(first.get("address"))
        postal_code, city = _infer_postal_city(
            country_code,
            address,
            _text(first.get("postal_code")),
            _text(first.get("city")),
        )
        station_chargers = chargers_by_station.get(station_id, [])
        powers = [charger.max_power_kw for charger in station_chargers if charger.max_power_kw is not None]
        connector_types = _join_unique(charger.connector_type for charger in station_chargers)
        station_name = _text(first.get("station_name"))
        price_fields = _station_price_fields(rows)
        price_source_texts = list(
            dict.fromkeys(_text(row.get("price_source_text")) for row in rows if _text(row.get("price_source_text")))
        )
        details = {
            "country_code": country_code,
            "provider_uid": _text(first.get("provider_uid")),
            "public_bundle_status": _text(first.get("public_bundle_status")),
            "connector_types": connector_types,
            "source_row_count": len(rows),
        }
        if price_source_texts:
            details["price_source_texts"] = price_source_texts[:5]
        details = {key: value for key, value in details.items() if value not in ("", [], {}, None)}
        stations.append(
            onboarded.StationRow(
                country_code=country_code,
                station_id=station_id,
                source_uid=_text(first.get("source_uid")),
                source_station_id=_text(first.get("source_station_id")),
                operator_name=_text(first.get("operator_name")),
                address=address,
                postal_code=postal_code,
                city=city,
                latitude=_float_or_none(first.get("latitude")),
                longitude=_float_or_none(first.get("longitude")),
                charger_count=len(station_chargers),
                max_power_kw=max(powers, default=None),
                source_url=_text(first.get("source_url")),
                license=_text(first.get("license")),
                id_rule=f"{country_code.lower()}_source_station_id",
                display_name=station_name,
                location_name=station_name or _text(first.get("source_station_id")),
                payment_methods=_join_unique(row.get("payment_methods") for row in rows),
                opening_hours=_join_unique(row.get("opening_hours") for row in rows),
                auth_methods=_join_unique(row.get("auth_methods") for row in rows),
                green_energy=_join_unique(row.get("green_energy") for row in rows),
                helpdesk_phone=_join_unique(row.get("helpdesk_phone") for row in rows),
                detail_last_updated=max((_text(row.get("date_updated")) for row in rows), default=""),
                price_display=price_fields.get("price_display", ""),
                price_energy_eur_kwh_min=price_fields.get("price_energy_eur_kwh_min", ""),
                price_energy_eur_kwh_max=price_fields.get("price_energy_eur_kwh_max", ""),
                price_currency=price_fields.get("price_currency", ""),
                price_quality=price_fields.get("price_quality", ""),
                details=details,
            )
        )
    return stations, chargers, dedupe_rows


def _station_normalized_dict(
    station: onboarded.StationRow,
    chargers_by_station: dict[str, list[onboarded.ChargerRow]],
    *,
    provider_uid: str | None = None,
    public_status: str = PUBLIC_STATUS,
) -> dict[str, Any]:
    chargers = chargers_by_station.get(station.station_id, [])
    license_text = station.license
    if license_text in {"source_terms_pending_review", "CC0-1.0-pending-final-confirmation"}:
        license_text = APPROVED_STATIC_LICENSE_STATUS
    return {
        "country_code": station.country_code,
        "station_id": station.station_id,
        "source_uid": station.source_uid,
        "source_station_id": station.source_station_id,
        "license": license_text,
        "provider_uid": provider_uid or station.source_uid,
        "operator_name": station.operator_name,
        "station_name": station.display_name or station.location_name,
        "address": station.address,
        "postal_code": station.postal_code,
        "city": station.city,
        "latitude": onboarded._coordinate_text(station.latitude),
        "longitude": onboarded._coordinate_text(station.longitude),
        "charger_count": str(station.charger_count),
        "max_power_kw": onboarded._decimal_text(station.max_power_kw),
        "connector_types": _join_unique(charger.connector_type for charger in chargers),
        "source_url": station.source_url,
        "public_bundle_status": public_status,
        "id_rule": station.id_rule,
        "opening_hours": station.opening_hours,
        "payment_methods": station.payment_methods,
        "auth_methods": station.auth_methods,
        "green_energy": station.green_energy,
        "helpdesk_phone": station.helpdesk_phone,
        "price_display": station.price_display,
        "price_energy_eur_kwh_min": station.price_energy_eur_kwh_min,
        "price_energy_eur_kwh_max": station.price_energy_eur_kwh_max,
        "price_currency": station.price_currency,
        "price_quality": station.price_quality,
        "detail_last_updated": station.detail_last_updated,
    }


def _charger_normalized_dict(
    charger: onboarded.ChargerRow,
    station: onboarded.StationRow | None,
    *,
    provider_uid: str | None = None,
    public_status: str = PUBLIC_STATUS,
) -> dict[str, Any]:
    license_text = "" if station is None else station.license
    if license_text in {"source_terms_pending_review", "CC0-1.0-pending-final-confirmation"}:
        license_text = APPROVED_STATIC_LICENSE_STATUS
    return {
        "country_code": charger.country_code,
        "station_id": charger.station_id,
        "charger_id": charger.charger_id,
        "source_uid": charger.source_uid,
        "provider_uid": provider_uid or charger.source_uid,
        "source_station_id": charger.source_station_id,
        "source_evse_id": charger.source_evse_id,
        "connector_id": charger.connector_id,
        "connector_type": charger.connector_type,
        "current_type": charger.current_type,
        "max_power_kw": onboarded._decimal_text(charger.max_power_kw),
        "operator_name": charger.operator_name,
        "license": license_text,
        "source_url": "" if station is None else station.source_url,
        "public_bundle_status": public_status,
    }


def _load_de_metadata(woladen_de_data_dir: Path) -> dict[str, Any]:
    summary = _read_json(woladen_de_data_dir / "summary.json")
    source = summary.get("source") if isinstance(summary.get("source"), dict) else {}
    params = summary.get("params") if isinstance(summary.get("params"), dict) else {}
    records = summary.get("records") if isinstance(summary.get("records"), dict) else {}
    return {
        "source_url": _text(source.get("source_url")) or "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html",
        "fetched_at": _text(source.get("fetched_at")),
        "license": APPROVED_STATIC_LICENSE_STATUS,
        "amenity_backend": _text(params.get("amenity_backend")) or "osm-pbf",
        "summary_records": records,
    }


def _de_decimal(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _de_group_key(lat: Any, lon: Any, operator: Any) -> tuple[str, str, str] | None:
    latitude = _de_decimal(lat)
    longitude = _de_decimal(lon)
    if latitude is None or longitude is None:
        return None
    return (f"{latitude:.7f}", f"{longitude:.7f}", _text(operator))


def _de_station_lookup(full_rows: list[dict[str, str]]) -> tuple[dict[tuple[str, str, str], dict[str, str]], int]:
    lookup: dict[tuple[str, str, str], dict[str, str]] = {}
    duplicate_keys = 0
    for row in full_rows:
        key = _de_group_key(row.get("lat"), row.get("lon"), row.get("operator"))
        if key is None:
            continue
        if key in lookup:
            duplicate_keys += 1
            continue
        lookup[key] = row
    return lookup, duplicate_keys


def _bnetza_register_header_index(path: Path, *, encoding: str) -> int:
    with path.open("r", encoding=encoding, newline="") as handle:
        for index, line in enumerate(handle):
            if "Ladeeinrichtungs-ID" in line and "EVSE-ID1" in line:
                return index
    return 0


def _bnetza_register_encoding(path: Path) -> str:
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            path.read_text(encoding=encoding, errors="strict")[:8192]
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin1"


def _iter_bnetza_register_rows(path: Path) -> Iterable[dict[str, str]]:
    encoding = _bnetza_register_encoding(path)
    header_index = _bnetza_register_header_index(path, encoding=encoding)
    with path.open("r", encoding=encoding, newline="") as handle:
        for _ in range(header_index):
            next(handle, None)
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            source_station_id = _text(row.get("Ladeeinrichtungs-ID"))
            if source_station_id:
                yield row


def _de_connector_current_type(connector_type: str) -> str:
    normalized = connector_type.casefold()
    if normalized.startswith("dc") or " combo " in normalized or "chademo" in normalized:
        return "DC"
    if normalized.startswith("ac") or " typ 2" in normalized or "schuko" in normalized:
        return "AC"
    return ""


def _de_source_evse_id(raw_evse_id: str, source_station_id: str, connector_index: int) -> tuple[str, bool]:
    official = _text(raw_evse_id)
    if official:
        return official, True
    return f"BNETZA:{source_station_id}:POINT:{connector_index}", False


def _de_charger_rows_from_bnetza_register(
    *,
    full_rows: list[dict[str, str]],
    register_path: Path,
    metadata: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    station_lookup, duplicate_station_lookup_keys = _de_station_lookup(full_rows)
    chargers: list[dict[str, Any]] = []
    seen_charger_ids: Counter[str] = Counter()
    raw_rows = 0
    mapped_rows = 0
    unmapped_rows = 0
    official_evse_rows = 0
    synthetic_evse_rows = 0

    if not register_path.exists():
        return [], {
            "status": "bnetza_register_missing",
            "path": str(register_path),
            "raw_rows": 0,
            "mapped_rows": 0,
            "unmapped_rows": 0,
            "duplicate_station_lookup_keys": duplicate_station_lookup_keys,
            "official_evse_rows": 0,
            "synthetic_evse_rows": 0,
        }

    for row in _iter_bnetza_register_rows(register_path):
        raw_rows += 1
        source_station_id = _text(row.get("Ladeeinrichtungs-ID"))
        key = _de_group_key(row.get("Breitengrad"), row.get("Längengrad"), row.get("Betreiber"))
        station_row = station_lookup.get(key) if key is not None else None
        if station_row is None:
            unmapped_rows += 1
            continue
        mapped_rows += 1
        station_id = _text(station_row.get("station_id"))
        connector_entries: list[dict[str, Any]] = []
        for connector_index in range(1, 7):
            connector_type = _text(row.get(f"Steckertypen{connector_index}"))
            connector_power = _de_decimal(row.get(f"Nennleistung Stecker{connector_index}"))
            raw_evse_id = _text(row.get(f"EVSE-ID{connector_index}"))
            if not connector_type and connector_power is None and not raw_evse_id:
                continue
            source_evse_id, is_official_evse_id = _de_source_evse_id(
                raw_evse_id,
                source_station_id,
                connector_index,
            )
            connector_entries.append(
                {
                    "source_evse_id": source_evse_id,
                    "connector_id": str(connector_index),
                    "connector_type": connector_type,
                    "current_type": _de_connector_current_type(connector_type),
                    "max_power_kw": connector_power,
                    "is_official_evse_id": is_official_evse_id,
                }
            )

        source_count = int(_de_decimal(row.get("Anzahl Ladepunkte")) or 0)
        if source_count > len(connector_entries):
            for connector_index in range(len(connector_entries) + 1, source_count + 1):
                source_evse_id, is_official_evse_id = _de_source_evse_id(
                    "",
                    source_station_id,
                    connector_index,
                )
                connector_entries.append(
                    {
                        "source_evse_id": source_evse_id,
                        "connector_id": str(connector_index),
                        "connector_type": "",
                        "current_type": "",
                        "max_power_kw": _de_decimal(row.get("Nennleistung Ladeeinrichtung [kW]")),
                        "is_official_evse_id": is_official_evse_id,
                    }
                )

        for connector in connector_entries:
            base_charger_id = f"de:bnetza:evse:{_safe_fragment(connector['source_evse_id'])}"
            seen_charger_ids[base_charger_id] += 1
            charger_id = base_charger_id
            if seen_charger_ids[base_charger_id] > 1:
                suffix = f"{_safe_fragment(source_station_id)}:{connector['connector_id']}:{seen_charger_ids[base_charger_id]}"
                charger_id = f"{base_charger_id}:{suffix}"
            if connector["is_official_evse_id"]:
                official_evse_rows += 1
            else:
                synthetic_evse_rows += 1
            chargers.append(
                {
                    "country_code": "DE",
                    "station_id": station_id,
                    "charger_id": charger_id,
                    "source_uid": DE_SOURCE_UID,
                    "provider_uid": "de_woladen_bnetza",
                    "source_station_id": source_station_id,
                    "source_evse_id": connector["source_evse_id"],
                    "connector_id": connector["connector_id"],
                    "connector_type": connector["connector_type"],
                    "current_type": connector["current_type"],
                    "max_power_kw": onboarded._decimal_text(connector["max_power_kw"]),
                    "operator_name": _text(row.get("Betreiber")) or _text(station_row.get("operator")),
                    "license": metadata["license"],
                    "source_url": metadata["source_url"],
                    "public_bundle_status": PUBLIC_STATUS,
                }
            )

    return chargers, {
        "status": "derived_evse_level_from_bnetza_register",
        "path": str(register_path),
        "raw_rows": raw_rows,
        "mapped_rows": mapped_rows,
        "unmapped_rows": unmapped_rows,
        "duplicate_station_lookup_keys": duplicate_station_lookup_keys,
        "official_evse_rows": official_evse_rows,
        "synthetic_evse_rows": synthetic_evse_rows,
        "charger_rows": len(chargers),
    }


def _de_station_charger_rows(
    full_rows: list[dict[str, str]],
    woladen_de_data_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    metadata = _load_de_metadata(woladen_de_data_dir)
    stations: list[dict[str, Any]] = []
    for row in full_rows:
        station_id = _text(row.get("station_id"))
        if not station_id:
            continue
        source_station_id = _legacy_de_station_id(station_id)
        connector_types = _text(row.get("connector_types"))
        stations.append(
            {
                "country_code": "DE",
                "station_id": station_id,
                "source_uid": DE_SOURCE_UID,
                "source_station_id": source_station_id,
                "license": metadata["license"],
                "provider_uid": "de_woladen_bnetza",
                "operator_name": _text(row.get("operator")),
                "station_name": _text(row.get("bnetza_display_name")) or _text(row.get("bnetza_location_name")),
                "address": _text(row.get("address")),
                "postal_code": _text(row.get("postcode")),
                "city": _text(row.get("city")),
                "latitude": _text(row.get("lat")),
                "longitude": _text(row.get("lon")),
                "charger_count": _text(row.get("charging_points_count")) or "1",
                "max_power_kw": _text(row.get("max_power_kw")),
                "connector_types": connector_types,
                "source_url": metadata["source_url"],
                "public_bundle_status": PUBLIC_STATUS,
                "id_rule": "woladen_de_public_station_id",
                "opening_hours": _text(row.get("bnetza_opening_hours")),
                "payment_methods": _text(row.get("bnetza_payment_systems")),
                "auth_methods": "",
                "green_energy": "",
                "detail_last_updated": metadata["fetched_at"],
            }
        )
    chargers, charger_stats = _de_charger_rows_from_bnetza_register(
        full_rows=full_rows,
        register_path=woladen_de_data_dir / "bnetza_cache.csv",
        metadata=metadata,
    )
    if not chargers:
        chargers = [
            {
                "country_code": "DE",
                "station_id": _text(row.get("station_id")),
                "charger_id": f"de:woladen:station:{_safe_fragment(row.get('station_id'))}",
                "source_uid": DE_SOURCE_UID,
                "provider_uid": "de_woladen_bnetza",
                "source_station_id": _legacy_de_station_id(row.get("station_id")),
                "source_evse_id": _legacy_de_station_id(row.get("station_id")),
                "connector_id": "",
                "connector_type": _text(row.get("connector_types")),
                "current_type": "",
                "max_power_kw": _text(row.get("max_power_kw")),
                "operator_name": _text(row.get("operator")),
                "license": metadata["license"],
                "source_url": metadata["source_url"],
                "public_bundle_status": "station_level_charger_placeholder_from_woladen_de_bundle",
            }
            for row in full_rows
            if _text(row.get("station_id"))
        ]
        charger_stats["fallback_charger_rows"] = len(chargers)
    metadata["de_normalized_charger_derivation"] = charger_stats
    return stations, chargers, metadata


def _derived_normalized_from_full_rows(
    full_rows: list[dict[str, str]],
    *,
    source_uid: str,
    provider_uid: str,
    source_url: str,
    license_text: str,
    public_status: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    stations: list[dict[str, Any]] = []
    chargers: list[dict[str, Any]] = []
    for row in full_rows:
        station_id = _text(row.get("station_id"))
        if not station_id:
            continue
        country_code = _country_from_station_id(station_id)
        stations.append(
            {
                "country_code": country_code,
                "station_id": station_id,
                "source_uid": _text(row.get("detail_source_uid")) or source_uid,
                "source_station_id": station_id,
                "license": license_text,
                "provider_uid": provider_uid,
                "operator_name": _text(row.get("operator")),
                "station_name": _text(row.get("bnetza_display_name")) or _text(row.get("bnetza_location_name")),
                "address": _text(row.get("address")),
                "postal_code": _text(row.get("postcode")),
                "city": _text(row.get("city")),
                "latitude": _text(row.get("lat")),
                "longitude": _text(row.get("lon")),
                "charger_count": _text(row.get("charging_points_count")) or "1",
                "max_power_kw": _text(row.get("max_power_kw")),
                "connector_types": _text(row.get("connector_types")),
                "source_url": source_url,
                "public_bundle_status": public_status,
                "id_rule": "derived_from_existing_open_bundle_station_id",
                "opening_hours": _text(row.get("bnetza_opening_hours")),
                "payment_methods": _text(row.get("bnetza_payment_systems")),
                "auth_methods": "",
                "green_energy": "",
                "helpdesk_phone": _text(row.get("helpdesk_phone")),
                "detail_last_updated": _text(row.get("detail_last_updated")),
            }
        )
        chargers.append(
            {
                "country_code": country_code,
                "station_id": station_id,
                "charger_id": f"{country_code.lower()}:derived:station:{_safe_fragment(station_id)}",
                "source_uid": _text(row.get("detail_source_uid")) or source_uid,
                "provider_uid": provider_uid,
                "source_station_id": station_id,
                "source_evse_id": station_id,
                "connector_id": "",
                "connector_type": _text(row.get("connector_types")),
                "current_type": "",
                "max_power_kw": _text(row.get("max_power_kw")),
                "operator_name": _text(row.get("operator")),
                "license": license_text,
                "source_url": source_url,
                "public_bundle_status": f"{public_status}; station_level_placeholder",
            }
        )
    return stations, chargers


def _load_ch_nl_normalized(
    *,
    refresh: bool,
    existing_onboarded_full_rows: list[dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], list[dict[str, str]], list[str]]:
    warnings: list[str] = []
    source_summaries: dict[str, Any] = {}
    dedupe_rows: list[dict[str, str]] = []
    if refresh:
        try:
            ch_stations, ch_chargers, ch_dedupe, ch_summary = onboarded._parse_ch_catalog()
            nl_stations, nl_chargers, nl_dedupe, nl_summary = onboarded._parse_nl_catalog()
            stations = ch_stations + nl_stations
            chargers = ch_chargers + nl_chargers
            station_by_id = {station.station_id: station for station in stations}
            chargers_by_station = onboarded._chargers_by_station(chargers)
            normalized_stations = [
                _station_normalized_dict(
                    station,
                    chargers_by_station,
                    provider_uid=onboarded.CH_PROVIDER_UID,
                    public_status=PUBLIC_STATUS,
                )
                for station in ch_stations
            ] + [
                _station_normalized_dict(
                    station,
                    chargers_by_station,
                    public_status=PUBLIC_STATUS,
                )
                for station in nl_stations
            ]
            normalized_chargers = [
                _charger_normalized_dict(
                    charger,
                    station_by_id.get(charger.station_id),
                    provider_uid=onboarded.CH_PROVIDER_UID,
                    public_status=PUBLIC_STATUS,
                )
                for charger in ch_chargers
            ] + [
                _charger_normalized_dict(
                    charger,
                    station_by_id.get(charger.station_id),
                    public_status=PUBLIC_STATUS,
                )
                for charger in nl_chargers
            ]
            source_summaries = {"CH": ch_summary, "NL": nl_summary}
            dedupe_rows = ch_dedupe + nl_dedupe
            return normalized_stations, normalized_chargers, source_summaries, dedupe_rows, warnings
        except Exception as exc:  # noqa: BLE001
            warnings.append(f"CH/NL normalized source refresh failed, falling back to existing station-level bundle rows: {exc}")

    ch_full = [row for row in existing_onboarded_full_rows if _country_from_station_id(_text(row.get("station_id"))) == "CH"]
    nl_full = [row for row in existing_onboarded_full_rows if _country_from_station_id(_text(row.get("station_id"))) == "NL"]
    ch_stations, ch_chargers = _derived_normalized_from_full_rows(
        ch_full,
        source_uid="ch_bfe_ladestationen_static",
        provider_uid="ch_bfe_ladestationen",
        source_url=onboarded.CH_STATIC_DATA_URL,
        license_text=APPROVED_STATIC_LICENSE_STATUS,
        public_status="derived_from_existing_onboarded_static_bundle",
    )
    nl_stations, nl_chargers = _derived_normalized_from_full_rows(
        nl_full,
        source_uid="nl_ndw_dotnl_ocpi_locations",
        provider_uid="nl_ndw_dotnl_ocpi_locations",
        source_url=onboarded.NL_OCPI_LOCATIONS_URL,
        license_text=APPROVED_STATIC_LICENSE_STATUS,
        public_status="derived_from_existing_onboarded_static_bundle",
    )
    source_summaries = {
        "CH": {
            "source": "CH BFE Ladestationen",
            "station_count": len(ch_stations),
            "charger_count": len(ch_chargers),
            "status": "fallback_station_level_from_existing_onboarded_bundle",
        },
        "NL": {
            "source": "NL NDW/DOT-NL Openbare laadpunten Nederland",
            "station_count": len(nl_stations),
            "charger_count": len(nl_chargers),
            "status": "fallback_station_level_from_existing_onboarded_bundle",
        },
    }
    return ch_stations + nl_stations, ch_chargers + nl_chargers, source_summaries, dedupe_rows, warnings


def _copy_optional_seed_files(*, woladen_de_data_dir: Path, output_dir: Path) -> list[str]:
    copied: list[str] = []
    for name in ("mobilithek_afir_provider_configs.json", "mobilithek_afir_static_matches.csv"):
        source = woladen_de_data_dir / name
        if not source.exists():
            continue
        shutil.copyfile(source, output_dir / name)
        copied.append(name)
    return copied


def _amenity_gap_row(station: onboarded.StationRow, *, status: str = "pbf_missing") -> dict[str, Any]:
    return {
        "country_code": station.country_code,
        "station_id": station.station_id,
        "amenity_count": "0",
        "amenity_summary": "",
        "amenity_category_counts": {},
        "amenity_examples": [],
        "nearest_amenity_kind": "",
        "nearest_amenity_name": "",
        "nearest_amenity_distance_m": "",
        "osm_pbf_url": EU_OSM_PBF_URLS.get(station.country_code, ""),
        "osm_extraction_status": status,
    }


def _build_static_amenity_rows(
    stations: list[onboarded.StationRow],
    *,
    include_osm: bool,
    osm_countries: set[str] | None,
    download_osm_pbf: bool,
    pbf_cache_dir: Path,
    amenity_radius_m: float,
    pbf_progress_every: int,
    pbf_download_progress_mb: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    summaries: dict[str, Any] = {}
    stations_by_country: dict[str, list[onboarded.StationRow]] = defaultdict(list)
    for station in stations:
        stations_by_country[station.country_code].append(station)

    for country_code, country_stations in sorted(stations_by_country.items()):
        pbf_url = EU_OSM_PBF_URLS.get(country_code, "")
        pbf_path = osm_amenities.pbf_cache_path(country_code, pbf_cache_dir) if pbf_url else Path()
        status = "skipped"
        points: list[osm_amenities.AmenityPoint] = []
        pbf_sha256 = ""
        extracted_at = ""
        pbf_stats: dict[str, Any] = {}
        selected_for_osm = osm_countries is None or country_code in osm_countries
        if include_osm and pbf_url and selected_for_osm:
            try:
                if download_osm_pbf:
                    pbf_path = osm_amenities.download_pbf(
                        country_code,
                        pbf_cache_dir,
                        progress_every_mb=pbf_download_progress_mb,
                    )
                if pbf_path.exists() and pbf_path.stat().st_size > 0:
                    points, pbf_stats = osm_amenities.collect_amenity_points_from_pbf(
                        pbf_path=pbf_path,
                        stations=country_stations,
                        radius_m=amenity_radius_m,
                        pbf_progress_every=pbf_progress_every,
                    )
                    pbf_sha256 = osm_amenities.sha256_file(pbf_path)
                    extracted_at = osm_amenities.utc_now_iso()
                    status = "extracted_from_pbf"
                else:
                    status = "pbf_missing"
            except RuntimeError as exc:
                status = "pbf_extraction_failed"
                pbf_stats = {"error": str(exc)}
        elif pbf_url:
            status = osm_amenities.local_pbf_status(country_code, pbf_cache_dir)
            if status == "pbf_available":
                status = "pbf_available_not_extracted"

        if points:
            rows.extend(
                osm_amenities.join_station_amenity_rows(
                    stations=country_stations,
                    points=points,
                    radius_m=amenity_radius_m,
                    osm_pbf_url=pbf_url,
                    osm_extraction_status=status,
                    osm_pbf_sha256=pbf_sha256,
                    osm_extracted_at=extracted_at,
                )
            )
        else:
            for station in country_stations:
                rows.append(
                    osm_amenities.empty_station_amenity_row(
                        station,
                        radius_m=amenity_radius_m,
                        osm_pbf_url=pbf_url,
                        osm_extraction_status=status,
                        osm_pbf_sha256=pbf_sha256,
                        osm_extracted_at=extracted_at,
                    )
                )

        summaries[country_code] = {
            "status": status,
            "station_count": len(country_stations),
            "pbf_url": pbf_url,
            "pbf_path": str(pbf_path) if pbf_path else "",
            "pbf_sha256": pbf_sha256,
            "extracted_at": extracted_at,
            "points": len(points),
            **pbf_stats,
        }
    return rows, summaries


def _static_description_bundle_rows(
    stations: list[onboarded.StationRow],
    chargers: list[onboarded.ChargerRow],
    *,
    generated_at: str,
    amenity_rows_by_station: Mapping[str, dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    amenity_rows_by_station = amenity_rows_by_station or {}
    chargers_by_station = onboarded._chargers_by_station(chargers)
    full_rows = [
        onboarded._station_full_bundle_row(station, chargers_by_station.get(station.station_id, []))
        for station in stations
    ]
    fast_station_ids = {
        charger.station_id
        for charger in chargers
        if onboarded._is_fast(charger.max_power_kw, charger.current_type)
    }
    fast_rows = [
        onboarded._station_fast_bundle_row(
            station,
            chargers_by_station.get(station.station_id, []),
            amenity_rows_by_station.get(station.station_id) or _amenity_gap_row(station),
            generated_at,
        )
        for station in stations
        if station.station_id in fast_station_ids
        and station.latitude is not None
        and station.longitude is not None
    ]
    return full_rows, fast_rows


def _combine_dedupe_rows(*row_groups: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in row_groups:
        for row in group:
            rows.append(
                {
                    "issue": _text(row.get("issue")),
                    "country_code": _text(row.get("country_code")),
                    "station_id": _text(row.get("station_id")),
                    "source_uid": _text(row.get("source_uid")),
                    "details": _text(row.get("details")),
                }
            )
    return rows


def _unique_charger_rows(
    chargers: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    assigned: set[str] = set()
    duplicate_counts: Counter[str] = Counter()
    unique_rows: list[dict[str, Any]] = []
    dedupe_rows: list[dict[str, str]] = []
    for row in chargers:
        original_charger_id = _text(row.get("charger_id"))
        if not original_charger_id:
            unique_rows.append(dict(row))
            continue
        duplicate_counts[original_charger_id] += 1
        charger_id = original_charger_id
        if charger_id in assigned:
            suffix_parts = [
                _safe_fragment(row.get("source_station_id") or row.get("station_id")),
                _safe_fragment(row.get("connector_id") or duplicate_counts[original_charger_id]),
                str(duplicate_counts[original_charger_id]),
            ]
            charger_id = f"{original_charger_id}:{':'.join(suffix_parts)}"
            while charger_id in assigned:
                duplicate_counts[original_charger_id] += 1
                suffix_parts[-1] = str(duplicate_counts[original_charger_id])
                charger_id = f"{original_charger_id}:{':'.join(suffix_parts)}"
            dedupe_rows.append(
                {
                    "issue": "duplicate_normalized_charger_id_rekeyed",
                    "country_code": _text(row.get("country_code")),
                    "station_id": _text(row.get("station_id")),
                    "source_uid": _text(row.get("source_uid")),
                    "details": f"{original_charger_id} rekeyed to {charger_id}",
                }
            )
        assigned.add(charger_id)
        unique_row = dict(row)
        unique_row["charger_id"] = charger_id
        unique_rows.append(unique_row)
    return unique_rows, dedupe_rows


def _country_counts(rows: Iterable[dict[str, Any]], *, station_id_field: str = "station_id") -> dict[str, int]:
    counts: Counter[str] = Counter()
    for row in rows:
        country_code = _text(row.get("country_code")) or _country_from_station_id(_text(row.get(station_id_field)))
        counts[country_code] += 1
    return dict(sorted(counts.items()))


def _csv_country_missing_counts(rows: list[dict[str, Any]], fields: Sequence[str]) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        country_code = _text(row.get("country_code")) or _country_from_station_id(_text(row.get("station_id")))
        for field in fields:
            if not _text(row.get(field)):
                counts[country_code][field] += 1
    return {country: dict(counter) for country, counter in sorted(counts.items())}


def _file_report(output_dir: Path) -> dict[str, dict[str, Any]]:
    report: dict[str, dict[str, Any]] = {}
    for path in sorted(output_dir.iterdir()):
        if not path.is_file():
            continue
        size = path.stat().st_size
        report[path.name] = {
            "bytes": size,
            "mib": round(size / (1024 * 1024), 3),
        }
    return report


def _fast_amenity_report(fast_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    totals: dict[str, Counter[str]] = defaultdict(Counter)
    for row in fast_rows:
        country_code = _country_from_station_id(_text(row.get("station_id")))
        totals[country_code]["fast_rows"] += 1
        if _text(row.get("lat")) and _text(row.get("lon")):
            totals[country_code]["with_coordinates"] += 1
        source = _text(row.get("amenities_source")) or "missing"
        totals[country_code][f"source:{source}"] += 1
        if int(float(_text(row.get("amenities_total")) or "0")) > 0:
            totals[country_code]["with_amenities"] += 1
    report: dict[str, dict[str, Any]] = {}
    for country_code, counter in sorted(totals.items()):
        fast_rows_count = counter["fast_rows"]
        source_counts = {
            key.split(":", 1)[1]: value
            for key, value in sorted(counter.items())
            if key.startswith("source:")
        }
        report[country_code] = {
            "fast_rows": fast_rows_count,
            "with_coordinates": counter["with_coordinates"],
            "with_mapped_amenities": counter["with_amenities"],
            "mapped_amenity_coverage_pct": round((counter["with_amenities"] / fast_rows_count) * 100, 2)
            if fast_rows_count
            else 0.0,
            "amenities_source_counts": source_counts,
        }
    return report


def _station_amenity_rows_from_bundle(
    *,
    stations: list[dict[str, Any]],
    fast_rows: list[dict[str, Any]],
    static_amenity_rows: list[dict[str, Any]],
    amenity_radius_m: float,
) -> list[dict[str, Any]]:
    fast_rows_by_station = {_text(row.get("station_id")): row for row in fast_rows}
    static_rows_by_station = {_text(row.get("station_id")): row for row in static_amenity_rows}
    rows: list[dict[str, Any]] = []
    for station in stations:
        station_id = _text(station.get("station_id"))
        country_code = _text(station.get("country_code")).upper()
        if station_id in static_rows_by_station:
            row = static_rows_by_station[station_id]
        else:
            if country_code == "DE":
                default_status = "copied_from_woladen_de_fast_csv"
            elif country_code in {"CH", "NL"}:
                default_status = "copied_from_onboarded_static_fast_csv"
            else:
                default_status = "pbf_missing"
            row = osm_amenities.legacy_fast_row_to_station_amenity_row(
                station=station,
                fast_row=fast_rows_by_station.get(station_id),
                radius_m=amenity_radius_m,
                osm_pbf_url=EU_OSM_PBF_URLS.get(country_code, ""),
                default_status=default_status,
            )
        rows.append(osm_amenities.station_amenity_row_to_csv(row))
    rows.sort(key=lambda row: (_text(row.get("country_code")), _text(row.get("station_id"))))
    return rows


def _build_quality_report(
    *,
    output_dir: Path,
    full_rows: list[dict[str, Any]],
    fast_rows: list[dict[str, Any]],
    stations: list[dict[str, Any]],
    chargers: list[dict[str, Any]],
    source_refresh_warnings: list[str],
    de_charger_derivation: dict[str, Any],
) -> dict[str, Any]:
    full_counts = _country_counts(full_rows)
    fast_counts = _country_counts(fast_rows)
    station_counts = _country_counts(stations)
    charger_counts = _country_counts(chargers)
    quality_notes = [
        "DE fast/full rows are copied from this repo's data directory with DE-prefixed public station IDs while retaining the existing Germany schema and OSM amenity columns.",
        "DE normalized chargers are derived at EVSE/Ladepunkt level from data/bnetza_cache.csv and mapped back to woladen.de station IDs.",
        "CH/NL fast/full rows are copied from the onboarded static catalog so existing OSM amenity-enriched fast rows are preserved.",
        "AT/BE/DK/FR/FI/PL/CY/CZ/ES/GR/LT/LU/LV/MT/NO/SE rows are built from the latest archived static payloads in the public ingest SQLite store.",
        "PL rows prefer the authenticated EIPA reader JSON files and fall back to EIPA browser HTML only when reader static files are missing.",
        "CY/CZ/DK/ES/GR/LT/LU/LV/MT/NO/SE sources follow the country/provider onboarding checklist with raw fixture archival before parser normalization.",
        "AT/BE/DK/FR/FI/PL/CY/CZ/ES/GR/LT/LU/LV/MT/NO/SE amenity columns come from country OSM PBF extraction when requested; otherwise station_amenities.csv records the local PBF availability/missing status.",
        "Existing approved-source license/terms and OSM attribution state is preserved; CY/CZ/DK/GR/LT/LU/LV/MT remain release-candidate rows pending human publication review.",
        "LV Transportdata static rows come from subscribed Eco-Movement and LVC DATEX table/snapshot payloads; Eco-Movement status/price remains private dynamic data and is not copied into the open static bundle.",
        "ES DGT static-publication human review was given on 2026-05-11; SGV OCPI dynamic activation remains private/inactive until PASOS/token access yields an authorized raw sample.",
        "NO/SE NOBIL API keys and direct API responses remain private; static datadump-derived rows use stable station/point IDs and publish only normalized open-static fields.",
        "BE and DK Monta static rows come from credentialed public AFIR charge-point table pages; per-EVSE status and price documents remain private dynamic data and are not copied into the open static bundle.",
    ]
    quality_notes.extend(source_refresh_warnings)
    country_report = {}
    missing_fields = _csv_country_missing_counts(
        full_rows,
        ("lat", "lon", "operator", "address", "city", "postcode", "max_power_kw", "connector_types"),
    )
    fast_amenities = _fast_amenity_report(fast_rows)
    for country_code in COUNTRIES:
        full_total = full_counts.get(country_code, 0)
        missing = missing_fields.get(country_code, {})
        missing_coord = max(missing.get("lat", 0), missing.get("lon", 0))
        country_report[country_code] = {
            "full_station_rows": full_total,
            "fast_station_rows": fast_counts.get(country_code, 0),
            "normalized_station_rows": station_counts.get(country_code, 0),
            "normalized_charger_rows": charger_counts.get(country_code, 0),
            "full_coordinate_coverage_pct": round(((full_total - missing_coord) / full_total) * 100, 2)
            if full_total
            else 0.0,
            "missing_full_fields": missing,
            "fast_amenities": fast_amenities.get(
                country_code,
                {
                    "fast_rows": 0,
                    "with_coordinates": 0,
                    "with_mapped_amenities": 0,
                    "mapped_amenity_coverage_pct": 0.0,
                    "amenities_source_counts": {},
                },
            ),
        }
    file_report = _file_report(output_dir)
    return {
        "generated_at": _utc_now_iso(),
        "bundle_dir": str(output_dir.resolve()),
        "countries": list(COUNTRIES),
        "files": file_report,
        "total_size_bytes": sum(item["bytes"] for item in file_report.values()),
        "total_size_mib": round(sum(item["bytes"] for item in file_report.values()) / (1024 * 1024), 3),
        "row_counts": {
            "chargers_full": len(full_rows),
            "chargers_fast": len(fast_rows),
            "stations": len(stations),
            "chargers": len(chargers),
        },
        "de_normalized_charger_derivation": de_charger_derivation,
        "countries_report": country_report,
        "quality_notes": quality_notes,
        "human_todos": [
            "Download AT/BE/FR/FI/PL/CY/CZ/ES/GR/LT/LU/LV/MT/NO/SE country OSM PBFs and run amenity extraction for any country still showing pbf_missing amenity status.",
            "Keep PL EIPA dynamic JSON in the private raw archive/live-state path; do not publish availability or price fields in the open static bundle.",
            "Investigate the separate PL KPD DATEX SOAP/WSDL path as a future NAP integration after account access and terms are reviewed.",
            "Capture an authorized Spain SGV OCPI dynamic sample from the PASOS/token-gated ruta-e path before private live-state parser activation.",
            "Capture authorized NOBIL Real-time status samples for NO/SE before private live-state parser activation.",
            "Complete human license/attribution review for CY/CZ/DK/GR/LT/LU/MT before public release even though fixture/static fetches are operational.",
        ],
    }


def build_bundle(
    *,
    output_dir: Path,
    woladen_de_data_dir: Path,
    onboarded_static_dir: Path,
    refresh_ch_nl_normalized: bool,
    operator_min_stations: int,
    include_osm: bool = False,
    osm_countries: set[str] | None = None,
    download_osm_pbf: bool = False,
    pbf_cache_dir: Path = REPO_ROOT / "data" / "osm_pbf_cache",
    amenity_radius_m: float = osm_amenities.DEFAULT_AMENITY_RADIUS_M,
    pbf_progress_every: int = 0,
    pbf_download_progress_mb: int = 0,
) -> dict[str, Any]:
    generated_at = _utc_now_iso()
    _prepare_output_dir(output_dir)

    de_full_rows = _project_de_station_ids(_read_csv_rows(woladen_de_data_dir / "chargers_full.csv"))
    de_fast_rows = _project_de_station_ids(_read_csv_rows(woladen_de_data_dir / "chargers_fast.csv"))
    onboarded_full_rows = _read_optional_csv_rows(onboarded_static_dir / "chargers_full.csv")
    onboarded_fast_rows = _read_optional_csv_rows(onboarded_static_dir / "chargers_fast.csv")
    existing_onboarded_dedupe = _read_optional_csv_rows(onboarded_static_dir / "dedupe_report.csv")

    config = AppConfig()
    raw_static_rows, source_summaries = _source_rows(config)
    static_stations, static_chargers, static_dedupe = _flat_rows_to_station_charger_rows(raw_static_rows)
    static_amenity_rows, static_amenity_summaries = _build_static_amenity_rows(
        static_stations,
        include_osm=include_osm,
        osm_countries=osm_countries,
        download_osm_pbf=download_osm_pbf,
        pbf_cache_dir=pbf_cache_dir,
        amenity_radius_m=amenity_radius_m,
        pbf_progress_every=pbf_progress_every,
        pbf_download_progress_mb=pbf_download_progress_mb,
    )
    static_full_rows, static_fast_rows = _static_description_bundle_rows(
        static_stations,
        static_chargers,
        generated_at=generated_at,
        amenity_rows_by_station={row["station_id"]: row for row in static_amenity_rows},
    )

    de_stations, de_chargers, de_metadata = _de_station_charger_rows(de_full_rows, woladen_de_data_dir)
    ch_nl_stations, ch_nl_chargers, ch_nl_summaries, ch_nl_dedupe, refresh_warnings = _load_ch_nl_normalized(
        refresh=refresh_ch_nl_normalized,
        existing_onboarded_full_rows=onboarded_full_rows,
    )

    static_chargers_by_station = onboarded._chargers_by_station(static_chargers)
    static_station_dicts = [
        _station_normalized_dict(
            station,
            static_chargers_by_station,
            provider_uid=_provider_uid_for_source_uid(station.source_uid),
            public_status=PUBLIC_STATUS,
        )
        for station in static_stations
    ]
    static_station_by_id = {station.station_id: station for station in static_stations}
    static_charger_dicts = [
        _charger_normalized_dict(
            charger,
            static_station_by_id.get(charger.station_id),
            provider_uid=_provider_uid_for_source_uid(charger.source_uid),
            public_status=PUBLIC_STATUS,
        )
        for charger in static_chargers
    ]

    full_rows = de_full_rows + onboarded_full_rows + static_full_rows
    fast_rows = de_fast_rows + onboarded_fast_rows + static_fast_rows
    stations = de_stations + ch_nl_stations + static_station_dicts
    chargers = de_chargers + ch_nl_chargers + static_charger_dicts
    chargers, charger_id_dedupe = _unique_charger_rows(chargers)
    station_amenity_rows = _station_amenity_rows_from_bundle(
        stations=stations,
        fast_rows=fast_rows,
        static_amenity_rows=static_amenity_rows,
        amenity_radius_m=amenity_radius_m,
    )

    full_rows.sort(key=lambda row: (_country_from_station_id(_text(row.get("station_id"))), _text(row.get("operator")).lower(), _text(row.get("station_id"))))
    fast_rows.sort(
        key=lambda row: (
            _country_from_station_id(_text(row.get("station_id"))),
            -(_float_or_none(row.get("max_power_kw")) or 0.0),
            _text(row.get("operator")).lower(),
            _text(row.get("station_id")),
        )
    )
    stations.sort(key=lambda row: (_text(row.get("country_code")), _text(row.get("station_id"))))
    chargers.sort(key=lambda row: (_text(row.get("country_code")), _text(row.get("charger_id"))))

    _write_csv(output_dir / "chargers_full.csv", onboarded.FULL_BUNDLE_FIELDS, full_rows)
    _write_csv(output_dir / "chargers_fast.csv", onboarded.FAST_BUNDLE_FIELDS, fast_rows)
    _write_csv(output_dir / "stations.csv", NORMALIZED_STATION_FIELDS, stations)
    _write_csv(output_dir / "chargers.csv", NORMALIZED_CHARGER_FIELDS, chargers)
    _write_csv(output_dir / "station_amenities.csv", osm_amenities.STATION_AMENITY_FIELDS, station_amenity_rows)
    onboarded._write_fast_geojson(output_dir / "chargers_fast.geojson", fast_rows, generated_at)
    copied_seed_files = _copy_optional_seed_files(
        woladen_de_data_dir=woladen_de_data_dir,
        output_dir=output_dir,
    )

    dedupe_rows = _combine_dedupe_rows(existing_onboarded_dedupe, static_dedupe, ch_nl_dedupe, charger_id_dedupe)
    _write_csv(output_dir / "dedupe_report.csv", onboarded.DEDUPE_FIELDS, dedupe_rows)

    operators_payload = onboarded._build_operator_list(fast_rows, operator_min_stations, generated_at)
    (output_dir / "operators.json").write_text(
        json.dumps(operators_payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )
    amenity_status_by_country = osm_amenities.amenity_status_by_country(station_amenity_rows)

    source_attribution = {
        "generated_at": generated_at,
        "public_release_status": "release_candidate_pending_human_publication_review_2026-05-03",
        "sources": {
            "AT": {
                "url": ECONTROL_PUBLIC_API_DATEX_TABLE_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
                "source_name": "AT E-Control DATEX energy infrastructure table publication",
                "secondary_archived_source_uid": ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
                "secondary_archived_source_role": "legacy Vienna proximity probe; not used when DATEX all-country table is available",
            },
            "BE": {
                "url": ENERGYVISION_LOCATIONS_URL,
                "tariffs_url": ENERGYVISION_TARIFFS_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": ENERGYVISION_LOCATIONS_SOURCE_UID,
                "source_name": "BE EnergyVision Public Charging Network OCPI locations",
                "provider_uid": "be_energyvision",
                "ocpi_version": ENERGYVISION_PRODUCTION_OCPI_VERSION,
                "static_dynamic_split": "location, EVSE, connector, operator, and tariff-reference fields are normalized into the open static bundle; live status fields remain private dynamic observations",
                "additional_sources": [
                    {
                        "url": ROAD_OCPI_LOCATIONS_URL,
                        "license": PENDING_STATIC_LICENSE_STATUS,
                        "source_uid": ROAD_OCPI_LOCATIONS_SOURCE_UID,
                        "source_name": "BE Road Public Charging Network OCPI locations",
                        "provider_uid": "be_road",
                        "static_dynamic_split": "location, EVSE, connector, operator, power, and coordinate fields are normalized into this local open-static candidate after licence review; live status fields from the same OCPI payload remain private dynamic observations and are not exported.",
                        "publication_review_status": "pending_transportdata_be_and_provider_licence_attribution_review_2026-06-07",
                    },
                    {
                        "url": GROUP_INDIGO_STATIC_URL,
                        "license": PENDING_STATIC_LICENSE_STATUS,
                        "source_uid": GROUP_INDIGO_STATIC_SOURCE_UID,
                        "source_name": "BE Group INDIGO DATEX II static charging infrastructure",
                        "provider_uid": "be_group_indigo",
                        "static_dynamic_split": "DATEX II table fields are static only; no live availability/status is present in the source payload.",
                        "publication_review_status": "pending_transportdata_be_and_provider_licence_attribution_review_2026-06-07",
                    },
                    {
                        "url": DK_MONTA_AFIR_CHARGE_POINTS_URL,
                        "license": PENDING_STATIC_LICENSE_STATUS,
                        "source_uid": BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
                        "source_name": "BE Monta AFIR charge-point table",
                        "provider_uid": BE_MONTA_PROVIDER_UID,
                        "static_dynamic_split": "Static site, station, EVSE/refill-point, connector, operator, power, and location fields are normalized into the open static release candidate after publication review; Monta per-EVSE status and ad-hoc price documents remain private dynamic data.",
                        "publication_review_status": "pending_monta_public_api_terms_attribution_review_2026-06-08",
                    },
                ],
            },
            "CH": {
                "url": onboarded.CH_STATIC_DATA_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": "ch_bfe_ladestationen_static",
            },
            "CY": {
                "url": CY_TRAFFIC4CYPRUS_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": CY_TRAFFIC4CYPRUS_SOURCE_UID,
                "source_name": "CY Traffic4Cyprus/FixCyprus electric vehicle chargers DATEX II",
            },
            "CZ": {
                "url": CZ_MPO_REGISTER_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": CZ_MPO_REGISTER_SOURCE_UID,
                "source_name": "CZ MPO public charging-station register XLSX",
            },
            "ES": {
                "url": ES_DGT_ELECTROLINERAS_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": ES_DGT_ELECTROLINERAS_SOURCE_UID,
                "source_name": "ES DGT electrolineras DATEX II static charging infrastructure",
                "provider_uid": "es_dgt_electrolineras",
                "nap_dataset_url": "https://nap.dgt.es/es/dataset/puntos-de-recarga-electrica-para-vehiculos",
                "static_dynamic_split": "DGT static site, EVSE, connector, operator, access, and location fields are eligible for the open static bundle after 2026-05-11 human review; SGV/ruta-e OCPI dynamic status remains private/inactive until authorized PASOS/token access and raw sample capture.",
                "dynamic_candidate_url": "https://ocpi.ruta-e.es/versions",
                "dynamic_activation_status": "pasos_token_gated_not_public_consumer_feed",
            },
            "DE": {
                "url": de_metadata["source_url"],
                "license": de_metadata["license"],
                "source_uid": DE_SOURCE_UID,
                "source_name": DE_SOURCE_NAME,
                "copied_from": str((woladen_de_data_dir / "chargers_full.csv").resolve()),
                "fetched_at": de_metadata.get("fetched_at", ""),
                "normalized_chargers": de_metadata.get("de_normalized_charger_derivation", {}),
            },
            "DK": {
                "url": DK_MONTA_AFIR_CHARGE_POINTS_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": DK_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
                "source_name": "DK Monta AFIR charge-point table",
                "provider_uid": DK_MONTA_PROVIDER_UID,
                "credential_handling": "Monta Public API client credentials are stored outside git as GitHub Actions secrets and ignored local files; pull receipts store redacted request metadata only.",
                "static_dynamic_split": "Static site, station, EVSE/refill-point, connector, operator, power, and location fields are normalized into the open static release candidate after publication review; Monta per-EVSE status and ad-hoc price documents remain private dynamic data.",
                "pagination": "GET /api/v1/afir/charge-points?country=DK&page=N&perPage=1000; the bundle generator reads all latest-day archived table pages for this source.",
                "dynamic_candidate_url": "https://public-api.monta.com/api/v1/afir/charge-points/{evseId}/status",
            },
            "FI": {
                "url": DIGITRAFFIC_DATEX_LOCATIONS_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID,
            },
            "FR": {
                "url": BASE_NATIONALE_DATASET_PAGE_URL,
                "resource_url": BASE_NATIONALE_STATIC_RESOURCE_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": BASE_NATIONALE_STATIC_SOURCE_UID,
                "source_name": "FR Base nationale IRVE static consolidation",
                "secondary_archived_source_uid": ECO_MOVEMENT_STATIC_SOURCE_UID,
                "secondary_archived_source_role": "Eco-Movement AFIR dynamic/subset context; not used as primary static bundle when national base is available",
            },
            "GR": {
                "url": GR_IDRO_STATIC_ZIP_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": GR_ELECTROKINISI_SOURCE_UID,
                "source_name": "GR Electrokinisi IDRO static charging-station JSON ZIP",
            },
            "HU": {
                "url": HU_NAP_PORTAL_URL,
                "resource_url": HU_NAP_CONTRACTED_PROFILE_SUMMARIES_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
                "secondary_archived_source_uid": HU_NAP_MOBILITI_STATIC_SOURCE_UID,
                "source_name": "HU NAP subscription DATEX II v3.3 static charging snapshots",
                "provider_uid": f"{HU_NAP_ECO_MOVEMENT_PROVIDER_UID};{HU_NAP_MOBILITI_PROVIDER_UID}",
                "credential_handling": "NAP data-consumer credentials are stored outside git; subscription snapshot URLs are discovered at runtime and are not committed.",
                "static_dynamic_split": "Eco-Movement and MVM Mobiliti static rows are normalized after publication/license review; AMPECO dynamic and other ack-only subscriptions remain private/inactive until a non-empty sample is available.",
            },
            "LT": {
                "url": LT_EV_LOCATIONS_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": LT_EV_LOCATIONS_SOURCE_UID,
                "source_name": "LT Via Lietuva DATEX II public charging infrastructure table",
                "provider_uid": "lt_vialietuva_datex",
                "static_fallback_file": LT_VIA_LIETUVA_STATIC_FALLBACK_FILE,
                "static_fallback_sha256": LT_VIA_LIETUVA_STATIC_FALLBACK_SHA256,
                "static_dynamic_split": "DATEX II table rows are static catalog candidates; during the 2026-06-07 Cloudflare backend-fetch issue the open-static workflow uses the tracked table file as an interim static source. Via Lietuva status publication remains private dynamic and requires provider allowlisting or an official non-challenged backend route before raw status archive/live-state activation.",
            },
            "LU": {
                "url": LU_CHARGING_STATIONS_WFS_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": LU_CHARGING_STATIONS_SOURCE_UID,
                "source_name": "LU public electrical charging stations WFS GeoJSON",
            },
            "LV": {
                "url": LV_ECO_MOVEMENT_STATIC_CARD_URL,
                "secondary_url": LV_LVC_EV_CHARGING_STREAM_CARD_URL,
                "dynamic_status_price_url": LV_ECO_MOVEMENT_STATUS_PRICE_CARD_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "license_name": "Transportdata metadata CC0; source payload publication pending review",
                "source_uid": LV_ECO_MOVEMENT_STATIC_SOURCE_UID,
                "secondary_archived_source_uid": LV_LVC_EV_CHARGING_STREAM_SOURCE_UID,
                "source_name": "LV Transportdata Eco-Movement and LVC DATEX energy infrastructure static snapshots",
                "provider_uid": f"{LV_ECO_MOVEMENT_PROVIDER_UID};{LV_LVC_PROVIDER_UID}",
                "credential_handling": "Transportdata.gov.lv dataset API keys are stored outside git; pull receipts redact x-api-key.",
                "static_dynamic_split": "Eco-Movement and LVC static site, EVSE/refill-point, connector, operator, access, and location fields are normalized into the open static bundle after publication review; Eco-Movement status/price observations remain private dynamic data.",
            },
            "MT": {
                "url": MT_CHARGING_POINTS_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "source_uid": MT_CHARGING_POINTS_SOURCE_UID,
                "source_name": "MT Transport Malta eGIS Charging Points ArcGIS layer",
            },
            "NL": {
                "url": onboarded.NL_OCPI_LOCATIONS_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": "nl_ndw_dotnl_ocpi_locations",
            },
            "NO": {
                "url": NOBIL_DATADUMP_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "license_name": "Creative Commons Attribution 4.0 International License",
                "source_uid": NO_NOBIL_STATIC_SOURCE_UID,
                "source_name": "NO NOBIL API v3 static charging-station datadump",
                "provider_uid": "no_nobil",
                "credential_handling": "API key is stored outside git; pull receipts store redacted query metadata only.",
                "static_dynamic_split": "Static station, point-count, connector, operator, access, and location fields are normalized into the open static bundle; NOBIL Real-time status remains private dynamic data until authorized raw sample capture.",
            },
            "PL": {
                "url": EIPA_READER_DOCS_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "source_uid": EIPA_READER_POINT_SOURCE_UID,
                "source_name": "PL EIPA reader static JSON files",
                "secondary_archived_source_uid": EIPA_BROWSER_PROVINCE_SOURCE_UID,
                "secondary_archived_source_role": "public browser fallback; not used when authenticated reader static JSON files are available",
            },
            "PT": {
                "url": PT_NAP_STATIC_DETAIL_URL,
                "resource_url": PT_MOBIE_STATIC_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "license_name": "NAP metadata: Sem licença - Sem contrato; Livre acesso",
                "source_uid": PT_MOBIE_STATIC_SOURCE_UID,
                "source_name": "PT MOBI.E DATEX II v3 Energy Infrastructure Table Publication",
                "provider_uid": PT_MOBIE_PROVIDER_UID,
                "credential_handling": "MOBI.E static pull is no-auth as observed on 2026-05-13; PT NAP account credentials remain outside git for portal/subscription follow-up only.",
                "static_dynamic_split": "Static site, station, EVSE, connector, operator, access, pricing, and location fields are normalized into the open static bundle after publication review; MOBI.E status publication remains private dynamic data.",
            },
            "SE": {
                "url": NOBIL_DATADUMP_URL,
                "license": APPROVED_STATIC_LICENSE_STATUS,
                "license_name": "Creative Commons Attribution 4.0 International License",
                "source_uid": SE_NOBIL_STATIC_SOURCE_UID,
                "source_name": "SE NOBIL API v3 static charging-station datadump",
                "provider_uid": "se_nobil",
                "credential_handling": "API key is stored outside git; pull receipts store redacted query metadata only.",
                "static_dynamic_split": "Static station, point-count, connector, operator, access, and location fields are normalized into the open static bundle; NOBIL Real-time status remains private dynamic data until authorized raw sample capture.",
            },
            "SI": {
                "url": SI_NAP_DATASET_TABLE_URL,
                "resource_url": SI_NAP_TABLE_URL,
                "license": PENDING_STATIC_LICENSE_STATUS,
                "license_name": "Creative Commons Attribution-ShareAlike 4.0 International",
                "source_uid": SI_NAP_TABLE_SOURCE_UID,
                "source_name": "SI NAP Prometej IDACS Energy Infrastructure Table Publication",
                "provider_uid": SI_NAP_PROVIDER_UID,
                "credential_handling": "NAP B2B OAuth credentials are stored outside git; pull receipts store redacted authorization metadata only.",
                "static_dynamic_split": "Static site, station, EVSE, connector, operator, access, opening-hour, pricing, and location fields are normalized into the open static bundle after publication review; Prometej status publication remains private dynamic data.",
            },
            "OSM": {
                "pbf_urls": EU_OSM_PBF_URLS,
                "license": "ODbL-1.0",
                "attribution": "OpenStreetMap contributors",
                "amenity_radius_m": amenity_radius_m,
                "review_status": APPROVED_STATIC_LICENSE_STATUS,
                "amenity_status_by_country": amenity_status_by_country,
                "static_country_extraction": static_amenity_summaries,
            },
        },
    }
    _write_json(output_dir / "source_attribution.json", source_attribution)

    quality_report = _build_quality_report(
        output_dir=output_dir,
        full_rows=full_rows,
        fast_rows=fast_rows,
        stations=stations,
        chargers=chargers,
        source_refresh_warnings=refresh_warnings,
        de_charger_derivation=de_metadata.get("de_normalized_charger_derivation", {}),
    )
    _write_json(output_dir / "bundle_quality_report.json", quality_report)

    summary = {
        "run": {
            "started_at": generated_at,
            "finished_at": _utc_now_iso(),
        },
        "source": {
            "source_url": "eu27-no-ch-combined-static-bundle",
            "countries": list(COUNTRIES),
            "sources": source_attribution["sources"],
            "onboarded_static_dir": str(onboarded_static_dir.resolve()),
            "woladen_de_data_dir": str(woladen_de_data_dir.resolve()),
            "copied_seed_files": copied_seed_files,
        },
        "params": {
            "min_power_kw": onboarded.FAST_POWER_THRESHOLD_KW,
            "radius_m": amenity_radius_m,
            "amenity_backend": "osm-pbf" if include_osm else "mixed_existing_osm_pbf_and_pbf_missing",
            "operator_min_stations": operator_min_stations,
            "refresh_ch_nl_normalized": refresh_ch_nl_normalized,
            "include_osm": include_osm,
            "download_osm_pbf": download_osm_pbf,
            "pbf_cache_dir": str(pbf_cache_dir.resolve()),
            "pbf_progress_every": pbf_progress_every,
            "pbf_download_progress_mb": pbf_download_progress_mb,
        },
        "records": {
            "raw_rows": len(chargers),
            "full_registry_stations_total": len(full_rows),
            "full_registry_active_stations_total": len(full_rows),
            "fast_chargers_total": len(fast_rows),
            "stations_with_live_occupancy": sum(1 for row in fast_rows if _text(row.get("occupancy_source_uid"))),
            "stations_with_static_details": sum(1 for row in fast_rows if _text(row.get("detail_source_uid"))),
            "stations_with_price": sum(1 for row in fast_rows if _text(row.get("price_display"))),
            "stations_with_opening_hours": sum(1 for row in fast_rows if _text(row.get("opening_hours_display"))),
            "stations_with_helpdesk": sum(1 for row in fast_rows if _text(row.get("helpdesk_phone"))),
            "stations_with_amenities": sum(1 for row in fast_rows if int(float(_text(row.get("amenities_total")) or "0")) > 0),
        },
        "occupancy_lookup": {
            "sources_discovered": 0,
            "sources_used": 0,
            "locations_scanned": 0,
            "matched_locations": 0,
            "matched_stations": 0,
            "matched_evses": 0,
            "errors": [],
            "sources": [],
        },
        "static_detail_lookup": {
            "sources_discovered": len(source_summaries) + len(ch_nl_summaries) + 1,
            "sources_used": sum(1 for item in source_summaries.values() if item.get("row_count", 0))
            + sum(1 for item in ch_nl_summaries.values() if item.get("station_count", 0))
            + 1,
            "matched_stations": len(stations),
            "stations_with_price": sum(1 for row in fast_rows if _text(row.get("price_display"))),
            "stations_with_opening_hours": sum(1 for row in fast_rows if _text(row.get("opening_hours_display"))),
            "stations_with_helpdesk": sum(1 for row in fast_rows if _text(row.get("helpdesk_phone"))),
            "errors": refresh_warnings,
            "sources": {
                "DE": {
                    "source": DE_SOURCE_NAME,
                    "station_count": len(de_stations),
                    "charger_count": len(de_chargers),
                    "status": "copied_from_woladen_de",
                },
                **ch_nl_summaries,
                **source_summaries,
            },
        },
        "amenity_lookup": {
            "backend": "mixed",
            "queries_used": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "deferred": sum(1 for row in fast_rows if _text(row.get("amenities_source")) == "pbf_missing"),
            "lookup_errors": 0,
            "amenity_radius_m": amenity_radius_m,
            "osm_pbf_urls": EU_OSM_PBF_URLS,
            "amenity_status_by_country": amenity_status_by_country,
            "station_amenity_rows": len(station_amenity_rows),
            "pbf_missing_station_rows": sum(
                1 for row in station_amenity_rows if _text(row.get("osm_extraction_status")) == "pbf_missing"
            ),
            "stations_with_mapped_amenities": sum(1 for row in fast_rows if int(float(_text(row.get("amenities_total")) or "0")) > 0),
        },
        "operators": {
            "min_stations": operator_min_stations,
            "listed_operators": operators_payload["total_operators"],
        },
    }
    _write_json(output_dir / "summary.json", summary)

    catalog_summary = {
        "generated_at": generated_at,
        "countries": list(COUNTRIES),
        "station_count": len(stations),
        "charger_count": len(chargers),
        "station_amenity_count": len(station_amenity_rows),
        "full_station_count": len(full_rows),
        "fast_station_count": len(fast_rows),
        "dedupe_issue_count": len(dedupe_rows),
        "source_summaries": summary["static_detail_lookup"]["sources"],
        "quality_report": "bundle_quality_report.json",
        "outputs": sorted(path.name for path in output_dir.iterdir() if path.is_file()),
        "country_counts": {
            "full": _country_counts(full_rows),
            "fast": _country_counts(fast_rows),
            "stations": _country_counts(stations),
            "chargers": _country_counts(chargers),
        },
        "public_release_status": "release_candidate_pending_human_publication_review_2026-05-03",
    }
    _write_json(output_dir / "catalog_summary.json", catalog_summary)
    return catalog_summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the combined EU27+NO+CH open static bundle from DE woladen.de, onboarded country catalogs, and archived public static payloads."
    )
    parser.add_argument("--output-dir", type=Path, default=REPO_ROOT / "data" / "eu27_ch_static")
    parser.add_argument("--woladen-de-data-dir", type=Path, default=REPO_ROOT / "data")
    parser.add_argument("--onboarded-static-dir", type=Path, default=REPO_ROOT / "data" / "onboarded_static")
    parser.add_argument(
        "--refresh-ch-nl-normalized",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch CH/NL source feeds to write EVSE-level normalized join-key rows; fallback uses existing station-level bundle rows.",
    )
    parser.add_argument("--operator-min-stations", type=int, default=100)
    parser.add_argument("--include-osm", action="store_true", help="Extract OSM amenities from country PBFs when local PBFs are available.")
    parser.add_argument(
        "--osm-countries",
        default="",
        help="Comma-separated country codes to extract when --include-osm is set; default extracts all bundle countries.",
    )
    parser.add_argument("--download-osm-pbf", action="store_true", help="Download missing country PBFs before OSM extraction.")
    parser.add_argument("--pbf-cache-dir", type=Path, default=REPO_ROOT / "data" / "osm_pbf_cache")
    parser.add_argument("--amenity-radius-m", type=float, default=osm_amenities.DEFAULT_AMENITY_RADIUS_M)
    parser.add_argument("--pbf-progress-every", type=int, default=0, help="Print PBF scan progress every N OSM objects.")
    parser.add_argument("--pbf-download-progress-mb", type=int, default=0, help="Print PBF download progress every N MiB.")
    args = parser.parse_args(argv)
    osm_countries = {part.strip().upper() for part in args.osm_countries.split(",") if part.strip()} or None
    unknown_osm_countries = sorted((osm_countries or set()) - set(EU_OSM_PBF_URLS))
    if unknown_osm_countries:
        raise ValueError(f"unknown_osm_countries:{','.join(unknown_osm_countries)}")

    summary = build_bundle(
        output_dir=args.output_dir,
        woladen_de_data_dir=args.woladen_de_data_dir,
        onboarded_static_dir=args.onboarded_static_dir,
        refresh_ch_nl_normalized=args.refresh_ch_nl_normalized,
        operator_min_stations=args.operator_min_stations,
        include_osm=args.include_osm,
        osm_countries=osm_countries,
        download_osm_pbf=args.download_osm_pbf,
        pbf_cache_dir=args.pbf_cache_dir,
        amenity_radius_m=args.amenity_radius_m,
        pbf_progress_every=args.pbf_progress_every,
        pbf_download_progress_mb=args.pbf_download_progress_mb,
    )
    print(json.dumps({"ok": True, "output_dir": str(args.output_dir), "summary": summary}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
