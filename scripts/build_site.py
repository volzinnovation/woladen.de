#!/usr/bin/env python3
"""Build static site folder from web assets + generated data."""

from __future__ import annotations

import html
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
DATA_DIR = ROOT / "data"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
STATION_DIR = SITE_DIR / "station"
SITE_ORIGIN = "https://woladen.de"

REQUIRED_DATA = [
    "chargers_fast.geojson",
    "operators.json",
    "summary.json",
]

ROOT_URLS = [
    "",
    "privacy.html",
    "imprint.html",
]

AMENITY_LABELS = {
    "bakery": "Bäckerei",
    "cafe": "Café",
    "convenience": "Kiosk",
    "fast_food": "Fast Food",
    "hotel": "Hotel",
    "ice_cream": "Eis",
    "museum": "Museum",
    "park": "Park",
    "pharmacy": "Apotheke",
    "playground": "Spielplatz",
    "restaurant": "Restaurant",
    "supermarket": "Supermarkt",
    "toilets": "Toiletten",
}


def format_text(value: object) -> str:
    return html.escape(str(value or "").strip())


def format_power_kw(value: object) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "0"
    rounded = round(numeric)
    if abs(numeric - rounded) < 0.05:
        return str(int(rounded))
    return f"{numeric:.1f}".rstrip("0").rstrip(".")


def to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def station_page_path(station_id: str) -> str:
    return f"station/{station_id}.html"


def absolute_url(path: str) -> str:
    clean = path.lstrip("/")
    if not clean:
        return f"{SITE_ORIGIN}/"
    return f"{SITE_ORIGIN}/{clean}"


def amenity_summary(properties: dict[str, object]) -> list[str]:
    counts: list[tuple[int, str]] = []
    for key, value in properties.items():
        if not key.startswith("amenity_"):
            continue
        count = to_int(value)
        if count <= 0:
            continue
        category = key.removeprefix("amenity_")
        label = AMENITY_LABELS.get(category, category.replace("_", " ").title())
        counts.append((count, label))
    counts.sort(key=lambda item: (-item[0], item[1]))
    return [label for _, label in counts[:6]]


def render_amenity_items(properties: dict[str, object]) -> str:
    examples = properties.get("amenity_examples")
    if isinstance(examples, list):
        items: list[str] = []
        for example in examples[:8]:
            if not isinstance(example, dict):
                continue
            category = str(example.get("category") or "").strip()
            label = AMENITY_LABELS.get(category, category.replace("_", " ").title() or "Annehmlichkeit")
            name = str(example.get("name") or "").strip() or label
            meta_parts = [label]
            distance = example.get("distance_m")
            if distance not in (None, ""):
                try:
                    meta_parts.append(f"{round(float(distance))} m entfernt")
                except (TypeError, ValueError):
                    pass
            items.append(
                "<li>"
                f"<strong>{html.escape(name)}</strong>"
                f"{html.escape(' • '.join(meta_parts))}"
                "</li>"
            )
        if items:
            return "".join(items)

    summary = amenity_summary(properties)
    if not summary:
        return (
            "<li>"
            "<strong>Keine Details hinterlegt</strong>"
            "Zu dieser Station liegen noch keine POI-Beispiele vor."
            "</li>"
        )
    return "".join(
        "<li>"
        f"<strong>{html.escape(label)}</strong>"
        "In der Nähe dieser Station vorhanden."
        "</li>"
        for label in summary
    )


def build_station_description(properties: dict[str, object]) -> str:
    operator = str(properties.get("operator") or "Unbekannt").strip()
    address = str(properties.get("address") or "").strip()
    city = str(properties.get("city") or "").strip()
    power = format_power_kw(properties.get("max_power_kw"))
    amenities_total = to_int(properties.get("amenities_total"))
    summary = amenity_summary(properties)
    amenity_text = ", ".join(summary[:3]) if summary else "Annehmlichkeiten"
    place = city or address or "Deutschland"
    return (
        f"Schnellladesäule von {operator} in {place}. "
        f"Bis zu {power} kW, {amenities_total} Annehmlichkeiten in der Nähe, darunter {amenity_text}."
    )


def build_station_page(feature: dict[str, object]) -> tuple[str, str]:
    geometry = feature.get("geometry")
    if not isinstance(geometry, dict):
        geometry = {}
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list):
        coordinates = [0.0, 0.0]
    lon = float(coordinates[0]) if len(coordinates) > 0 else 0.0
    lat = float(coordinates[1]) if len(coordinates) > 1 else 0.0

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        properties = {}

    station_id = str(properties.get("station_id") or "").strip()
    operator = str(properties.get("operator") or "Unbekannt").strip()
    address = str(properties.get("address") or "").strip()
    postcode = str(properties.get("postcode") or "").strip()
    city = str(properties.get("city") or "").strip()
    title_city = city or postcode or "Deutschland"
    max_power = format_power_kw(properties.get("max_power_kw"))
    charging_points = to_int(properties.get("charging_points_count"), default=1)
    amenities_total = to_int(properties.get("amenities_total"))
    description = build_station_description(properties)
    amenity_text = ", ".join(amenity_summary(properties)[:4])

    page_path = station_page_path(station_id)
    canonical_url = absolute_url(page_path)
    app_url = f"/?station={station_id}"
    google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
    amenity_items = render_amenity_items(properties)
    amenity_paragraph = (
        f"In der Nähe findest du unter anderem {html.escape(amenity_text)}."
        if amenity_text
        else "Diese Station ist als Direktlink in der woladen.de Web-App hinterlegt."
    )

    page_html = f"""<!doctype html>
<html lang="de">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{format_text(operator)} in {format_text(title_city)} | {format_text(max_power)} kW Schnelllader | woladen.de</title>
    <meta name="description" content="{format_text(description)}" />
    <link rel="canonical" href="{canonical_url}" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{format_text(operator)} in {format_text(title_city)} | woladen.de" />
    <meta property="og:description" content="{format_text(description)}" />
    <meta property="og:url" content="{canonical_url}" />
    <link rel="icon" href="/favicon.ico?v=20260221" sizes="any" />
    <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v=20260221" />
    <link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png?v=20260221" />
    <link rel="apple-touch-icon" sizes="180x180" href="/img/touch-icon.png?v=20260221" />
    <link rel="stylesheet" href="/styles.css" />
  </head>
  <body class="station-page">
    <main class="station-shell">
      <a href="{app_url}" class="legal-back">Zur Web-App</a>
      <section class="station-hero">
        <p class="legal-kicker">Direktlink Schnellladesäule</p>
        <h1>{format_text(operator)}</h1>
        <p class="station-summary">{format_text(address)}<br />{format_text(postcode)} {format_text(city)}</p>
        <div class="station-chip-row">
          <span class="station-chip">⚡ {format_text(max_power)} kW max</span>
          <span class="station-chip">🔌 {charging_points} Ladepunkte</span>
          <span class="station-chip">🏪 {amenities_total} Annehmlichkeiten</span>
        </div>
        <p class="station-summary">{amenity_paragraph}</p>
        <div class="station-actions">
          <a href="{app_url}" class="link-btn">In Web-App öffnen</a>
          <a href="{google_maps_url}" target="_blank" rel="noopener noreferrer" class="link-btn secondary-link">Navigation</a>
        </div>
      </section>

      <section class="legal-card">
        <h2>Station im Überblick</h2>
        <p class="legal-intro">
          woladen.de zeigt Schnellladesäulen mit Aufenthaltsqualität. Diese Stationsseite ist für direkte Links und bessere Auffindbarkeit in Suchmaschinen gedacht.
        </p>
        <h3>Adresse</h3>
        <p>{format_text(address)}<br />{format_text(postcode)} {format_text(city)}</p>
        <h3>Annehmlichkeiten in der Nähe</h3>
        <ul class="station-list">
          {amenity_items}
        </ul>
        <p class="station-note">
          Datenquelle: Bundesnetzagentur Ladesäulenregister und OpenStreetMap. Karten- und POI-Daten © OpenStreetMap-Mitwirkende.
        </p>
      </section>
    </main>
  </body>
</html>
"""
    return page_path, page_html


def write_station_pages() -> list[str]:
    geojson_path = DATA_DIR / "chargers_fast.geojson"
    if not geojson_path.exists():
        return []

    payload = json.loads(geojson_path.read_text(encoding="utf-8"))
    features = payload.get("features")
    if not isinstance(features, list):
        return []

    STATION_DIR.mkdir(parents=True, exist_ok=True)
    page_paths: list[str] = []
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        station_id = str(properties.get("station_id") or "").strip()
        if not station_id:
            continue
        page_path, page_html = build_station_page(feature)
        target = SITE_DIR / page_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page_html, encoding="utf-8")
        page_paths.append(page_path)
    return page_paths


def write_sitemap(page_paths: list[str]) -> None:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path in ROOT_URLS + page_paths:
        lines.append("  <url>")
        lines.append(f"    <loc>{html.escape(absolute_url(path))}</loc>")
        lines.append("  </url>")
    lines.append("</urlset>")
    (SITE_DIR / "sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True, exist_ok=True)

    for src in WEB_DIR.glob("*"):
        target = SITE_DIR / src.name
        if src.is_dir():
            shutil.copytree(src, target)
        else:
            shutil.copy2(src, target)

    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for filename in REQUIRED_DATA:
        source = DATA_DIR / filename
        if source.exists():
            shutil.copy2(source, SITE_DATA_DIR / filename)

    station_page_paths = write_station_pages()
    write_sitemap(station_page_paths)


if __name__ == "__main__":
    main()
