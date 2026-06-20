from __future__ import annotations

import importlib.util
import json
import sqlite3
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


def test_station_page_path_preserves_non_de_country_namespace():
    assert build_site.public_station_id("at:econtrol:at-002") == "AT:econtrol:at-002"
    assert build_site.station_page_path("at:econtrol:at-002") == "station/AT/econtrol%3Aat-002.html"


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


def test_copy_station_occupancy_tree_uses_data_source(tmp_path: Path, monkeypatch):
    source_file = tmp_path / "data" / "station-occupancy" / "aa" / "bb" / "station.json"
    source_file.parent.mkdir(parents=True)
    source_file.write_text('{"ok":true}', encoding="utf-8")
    stale_file = tmp_path / "site" / "data" / "station-occupancy" / "stale.json"
    stale_file.parent.mkdir(parents=True)
    stale_file.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(build_site, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(build_site, "SITE_DATA_DIR", tmp_path / "site" / "data")

    build_site.copy_station_occupancy_tree()

    assert (tmp_path / "site" / "data" / "station-occupancy" / "aa" / "bb" / "station.json").read_text(
        encoding="utf-8"
    ) == '{"ok":true}'
    assert not stale_file.exists()


def test_write_sitemap_splits_large_station_url_sets(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(build_site, "SITE_DIR", tmp_path)
    monkeypatch.setattr(build_site, "SITEMAP_MAX_URLS", 2)
    build_site.write_sitemap(
        [
            "station/AT/one.html",
            "station/BE/two.html",
            "station/CH/three.html",
        ],
        {"sitemap-seo-home.xml": ["en/index.html", "de/index.html"]},
    )

    sitemap_index = (tmp_path / "sitemap.xml").read_text(encoding="utf-8")
    assert "<sitemapindex" in sitemap_index
    assert "https://woladen.de/sitemap-pages.xml" in sitemap_index
    assert "https://woladen.de/sitemap-seo-home.xml" in sitemap_index
    assert "https://woladen.de/sitemap-stations-1.xml" in sitemap_index
    assert "https://woladen.de/sitemap-stations-2.xml" in sitemap_index
    assert "https://woladen.de/en/" in (tmp_path / "sitemap-seo-home.xml").read_text(encoding="utf-8")
    assert (tmp_path / "sitemap-stations-1.xml").read_text(encoding="utf-8").count("<url>") == 2
    assert (tmp_path / "sitemap-stations-2.xml").read_text(encoding="utf-8").count("<url>") == 1


def _seo_bundle(language: str) -> dict[str, str]:
    values = {key: f"{language}-{key}" for key in build_site.SEO_REQUIRED_KEYS}
    values.update(
        {
            "brandName": "woladen",
            "primaryTagline": "Plugs for Cars. Perks for People.",
            "humanHook": "The human side of charging.",
            "timeLine": "Because charging time is your time.",
            "coveragePath": "abdeckung" if language == "de" else "coverage",
            "countryTitle": "{country} title",
            "countryH1": "{country} h1",
            "countryDescription": "{country} description",
            "countryIntro": "{country} intro",
            "dataFreshness": "Generated {date}",
        }
    )
    return values


def _seo_bundles() -> dict[str, dict[str, str]]:
    return {language: _seo_bundle(language) for language in build_site.SEO_LANGUAGES}


def test_build_seo_pages_generates_language_paths_and_served_countries_only():
    countries = [
        build_site.SeoCountry(
            "DE",
            station_count=10,
            charger_count=20,
            fast_station_count=5,
            source_name="Deutschland",
            live_station_count=7,
        ),
        build_site.SeoCountry("FR", station_count=12, charger_count=24, fast_station_count=6, source_name="Frankreich"),
    ]

    pages = build_site.build_seo_pages(countries, "2026-06-20T00:00:00Z", _seo_bundles())
    paths = {page.path for page in pages}

    assert "en/index.html" in paths
    assert "de/abdeckung/index.html" in paths
    assert "en/germany/index.html" in paths
    assert "de/deutschland/index.html" in paths
    assert "en/france/index.html" in paths
    assert "en/italy/index.html" not in paths
    assert len([page for page in pages if page.sitemap_group == "sitemap-seo-countries.xml"]) == 8

    germany_page = next(page for page in pages if page.path == "en/germany/index.html")
    assert 'class="link-btn seo-primary-cta" href="/?lang=en"' in germany_page.body_html
    assert 'class="app-install-link" href="https://apps.apple.com/de/app/wo-laden/id6759499459"' in germany_page.body_html
    assert 'class="app-install-promo seo-data-box"' in germany_page.body_html
    assert germany_page.body_html.count('class="seo-cta-pointer" aria-hidden="true"') == 8
    assert germany_page.body_html.count('class="link-btn seo-secondary-cta" href="/?lang=en"') == 3
    assert ">7</strong><span>en-liveInfoStationsMetric</span>" in germany_page.body_html


def test_load_dynamic_station_counts_uses_prefixed_live_state_and_reviewed_match_csv(tmp_path: Path):
    live_db = tmp_path / "live_state.sqlite3"
    conn = sqlite3.connect(live_db)
    try:
        conn.execute("create table station_current_state (station_id text)")
        conn.executemany(
            "insert into station_current_state (station_id) values (?)",
            [
                ("nl:ndw:one",),
                ("fr:irve:duplicate",),
                ("raw-internal-hash",),
            ],
        )
        conn.commit()
    finally:
        conn.close()

    match_csv = tmp_path / "mobilithek_afir_static_matches.csv"
    match_csv.write_text(
        "station_id,station_in_bundle\n"
        "local-de-station,1\n"
        "fr:irve:duplicate,1\n"
        "ignored-station,0\n",
        encoding="utf-8",
    )

    assert build_site.load_dynamic_station_counts_by_country(live_db, match_csv) == {
        "DE": 1,
        "FR": 1,
        "NL": 1,
    }


def test_load_seo_countries_attaches_live_station_counts(tmp_path: Path):
    summary_path = tmp_path / "open_static_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "generated_at": "2026-06-20T00:00:00Z",
                "countries": [
                    {
                        "code": "DE",
                        "name": "Deutschland",
                        "station_count": 10,
                        "charger_count": 20,
                        "fast_station_count": 5,
                    },
                    {
                        "code": "FR",
                        "name": "Frankreich",
                        "station_count": 12,
                        "charger_count": 24,
                        "fast_station_count": 6,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    countries, generated_at = build_site.load_seo_countries(summary_path, {"DE": 7})

    assert generated_at == "2026-06-20T00:00:00Z"
    assert {country.code: country.live_station_count for country in countries} == {"DE": 7, "FR": 0}


def test_render_seo_shell_uses_self_canonical_hreflang_and_root_relative_assets():
    bundles = _seo_bundles()
    page = build_site.SeoPage(
        identity="country:DE",
        language="de",
        path="de/deutschland/index.html",
        title="Ladestopps in Deutschland | woladen",
        description="Deutschland description",
        h1="Ladestopps in Deutschland",
        alternate_paths={
            "en": "en/germany/index.html",
            "de": "de/deutschland/index.html",
            "fr": "fr/allemagne/index.html",
            "nl": "nl/duitsland/index.html",
        },
        body_html="<p>Body</p>",
        sitemap_group="sitemap-seo-countries.xml",
    )

    html = build_site.render_seo_shell(page, bundles)

    assert '<html lang="de">' in html
    assert '<link rel="canonical" href="https://woladen.de/de/deutschland/" />' in html
    assert 'hreflang="x-default" href="https://woladen.de/en/germany/"' in html
    assert 'hreflang="fr" href="https://woladen.de/fr/allemagne/"' in html
    assert 'href="/styles.css' in html
    assert "./styles.css" not in html


def test_load_seo_bundles_fails_when_required_translation_is_missing(tmp_path: Path, monkeypatch):
    i18n_dir = tmp_path / "web" / "i18n"
    i18n_dir.mkdir(parents=True)
    complete = {"seo": _seo_bundle("en")}
    for language in build_site.SEO_LANGUAGES:
        payload = complete
        if language == "fr":
            incomplete = {"seo": _seo_bundle("fr")}
            del incomplete["seo"]["homeTitle"]
            payload = incomplete
        (i18n_dir / f"{language}.json").write_text(json.dumps(payload), encoding="utf-8")

    monkeypatch.setattr(build_site, "WEB_DIR", tmp_path / "web")

    try:
        build_site.load_seo_bundles()
    except ValueError as exc:
        assert "fr: missing seo.homeTitle" in str(exc)
    else:
        raise AssertionError("expected missing SEO translation to fail")
