from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_build_site_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "build_site.py"
    spec = importlib.util.spec_from_file_location("build_site_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_site = _load_build_site_module()


def test_station_query_url_keeps_country_namespace_separator_readable():
    assert build_site.station_query_url("DE:47d719c1b62c750") == "/?station=DE:47d719c1b62c750"


def test_station_query_url_projects_internal_station_ids_to_public_namespace():
    assert build_site.station_query_url("47d719c1b62c750") == "/?station=DE:47d719c1b62c750"


def test_station_page_path_uses_cross_platform_namespace_directory():
    assert build_site.station_page_path("DE:47d719c1b62c750") == "station/DE/47d719c1b62c750.html"


def test_public_bundle_value_projects_station_ids_and_urls_only():
    payload = {
        "station_id": "47d719c1b62c750",
        "station_url": "https://woladen.de/?station=47d719c1b62c750&date=2026-05-07",
        "datex_station_ids": ["47d719c1b62c750"],
    }

    assert build_site.public_bundle_value(payload) == {
        "station_id": "DE:47d719c1b62c750",
        "station_url": "https://woladen.de/?station=DE:47d719c1b62c750&date=2026-05-07",
        "datex_station_ids": ["47d719c1b62c750"],
    }
