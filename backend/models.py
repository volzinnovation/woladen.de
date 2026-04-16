from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProviderTarget:
    provider_uid: str
    display_name: str
    publisher: str
    publication_id: str
    access_mode: str
    fetch_kind: str
    fetch_url: str
    subscription_id: str
    enabled: bool
    delta_delivery: bool
    retention_period_minutes: int | None


@dataclass(frozen=True)
class StationRecord:
    station_id: str
    operator: str
    address: str
    postcode: str
    city: str
    lat: float
    lon: float
    charging_points_count: int
    max_power_kw: float


@dataclass(frozen=True)
class SiteMatch:
    provider_uid: str
    site_id: str
    station_id: str
    score: float


@dataclass(frozen=True)
class EvseMatch:
    provider_uid: str
    evse_id: str
    station_id: str
    site_id: str
    station_ref: str


@dataclass(frozen=True)
class PriceSnapshot:
    display: str
    currency: str
    energy_eur_kwh_min: str
    energy_eur_kwh_max: str
    time_eur_min_min: float | None
    time_eur_min_max: float | None
    quality: str
    complex_tariff: bool


@dataclass(frozen=True)
class DynamicFact:
    provider_uid: str
    site_id: str
    station_ref: str
    evse_id: str
    station_id: str | None
    availability_status: str
    operational_status: str
    price: PriceSnapshot
    next_available_charging_slots: list[Any]
    supplemental_facility_status: list[Any]
    source_observed_at: str


@dataclass(frozen=True)
class FetchResponse:
    body: bytes
    content_type: str
    http_status: int
    headers_text: str = ""
