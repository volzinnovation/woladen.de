#!/usr/bin/env python3
"""Build static site folder from web assets + generated data."""

from __future__ import annotations

import html
import json
import math
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
DATA_DIR = ROOT / "data"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
STATION_DIR = SITE_DIR / "station"
SITE_ORIGIN = "https://woladen.de"
SOCIAL_IMAGE_VERSION = "20260411"
SOCIAL_IMAGE_PATH = f"img/social-card-home.png?v={SOCIAL_IMAGE_VERSION}"
SOCIAL_IMAGE_WIDTH = "1200"
SOCIAL_IMAGE_HEIGHT = "630"
SOCIAL_IMAGE_ALT = (
    "Vorschau der woladen.de Web-App mit Schnellladesäulen und Angeboten vor Ort."
)

REQUIRED_DATA = [
    "chargers_fast.geojson",
    "chargers_under_50.geojson",
    "operators.json",
    "station_ratings.json",
    "summary.json",
]

ROOT_URLS = [
    "",
    "management.html",
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


def format_amenity_count(count: int) -> str:
    label = "Angebot vor Ort" if count == 1 else "Angebote vor Ort"
    return f"{count} {label}"


def sanitize_json_value(value: object) -> object:
    if isinstance(value, dict):
        return {key: sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, str):
        return "" if value.strip().lower() in {"nan", "nat"} else value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


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
            label = AMENITY_LABELS.get(category, category.replace("_", " ").title() or "Angebot vor Ort")
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


def build_static_detail_rows(properties: dict[str, object]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []

    def add_row(label: str, key: str) -> None:
        value = str(properties.get(key) or "").strip()
        if value:
            rows.append((label, value))

    add_row("Bezahlen", "payment_methods_display")
    add_row("Zugang", "auth_methods_display")
    add_row("Stecker", "connector_types_display")
    add_row("Stromart", "current_types_display")
    connector_count = to_int(properties.get("connector_count"), default=0)
    if connector_count > 0:
        rows.append(("Anschlüsse", f"{connector_count} Steckplätze"))
    add_row("Service", "service_types_display")

    green_energy = properties.get("green_energy")
    if isinstance(green_energy, bool):
        rows.append(("Strom", "100 % erneuerbar" if green_energy else "Nicht als erneuerbar markiert"))

    return rows


def build_station_description(properties: dict[str, object]) -> str:
    operator = str(properties.get("operator") or "Unbekannt").strip()
    address = str(properties.get("address") or "").strip()
    city = str(properties.get("city") or "").strip()
    power = format_power_kw(properties.get("max_power_kw"))
    amenities_total = to_int(properties.get("amenities_total"))
    summary = amenity_summary(properties)
    amenity_count = format_amenity_count(amenities_total)
    place = city or address or "Deutschland"
    if summary:
        amenity_text = ", ".join(summary[:3])
        return (
            f"Schnellladesäule von {operator} in {place}. "
            f"Bis zu {power} kW, {amenity_count}, darunter {amenity_text}."
        )
    return (
        f"Schnellladesäule von {operator} in {place}. "
        f"Bis zu {power} kW und {amenity_count}."
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
    social_title = f"{operator} in {title_city} | {max_power} kW Schnelllader | woladen.de"
    social_image_url = absolute_url(SOCIAL_IMAGE_PATH)

    page_path = station_page_path(station_id)
    canonical_url = absolute_url(page_path)
    app_url = f"/?station={station_id}"
    google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
    amenity_items = render_amenity_items(properties)
    static_detail_rows = build_static_detail_rows(properties)
    static_detail_items = "".join(
        "<li>"
        f"<strong>{html.escape(label)}</strong>"
        f"{html.escape(value)}"
        "</li>"
        for label, value in static_detail_rows
    )
    detail_source_name = str(properties.get("detail_source_name") or "").strip()
    detail_last_updated = str(properties.get("detail_last_updated") or "").strip()
    detail_source_text = ""
    if detail_source_name and detail_last_updated:
        detail_source_text = f"Details via {html.escape(detail_source_name)} • Stand {html.escape(detail_last_updated)}"
    elif detail_source_name:
        detail_source_text = f"Details via {html.escape(detail_source_name)}"
    elif detail_last_updated:
        detail_source_text = f"Stand {html.escape(detail_last_updated)}"
    amenity_paragraph = (
        f"Vor Ort findest du unter anderem {html.escape(amenity_text)}."
        if amenity_text
        else "Diese Station ist als Direktlink in der woladen.de Web-App hinterlegt."
    )
    price_chip = str(properties.get("price_display") or "").strip()
    opening_hours_chip = str(properties.get("opening_hours_display") or "").strip()

    page_html = f"""<!doctype html>
<html lang="de">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{format_text(operator)} in {format_text(title_city)} | {format_text(max_power)} kW Schnelllader | woladen.de</title>
    <meta name="description" content="{format_text(description)}" />
    <link rel="canonical" href="{canonical_url}" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{format_text(social_title)}" />
    <meta property="og:description" content="{format_text(description)}" />
    <meta property="og:url" content="{canonical_url}" />
    <meta property="og:site_name" content="woladen.de" />
    <meta property="og:locale" content="de_DE" />
    <meta property="og:image" content="{social_image_url}" />
    <meta property="og:image:width" content="{SOCIAL_IMAGE_WIDTH}" />
    <meta property="og:image:height" content="{SOCIAL_IMAGE_HEIGHT}" />
    <meta property="og:image:alt" content="{SOCIAL_IMAGE_ALT}" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{format_text(social_title)}" />
    <meta name="twitter:description" content="{format_text(description)}" />
    <meta name="twitter:image" content="{social_image_url}" />
    <meta name="twitter:image:alt" content="{SOCIAL_IMAGE_ALT}" />
    <link rel="icon" href="/favicon.ico?v=20260411" sizes="any" />
    <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v=20260411" />
    <link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png?v=20260411" />
    <link rel="apple-touch-icon" sizes="180x180" href="/img/touch-icon.png?v=20260411" />
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
          <span class="station-chip">🏪 {format_amenity_count(amenities_total)}</span>
          {f'<span class="station-chip">€ {format_text(price_chip)}</span>' if price_chip else ''}
          {f'<span class="station-chip">🕒 {format_text(opening_hours_chip)}</span>' if opening_hours_chip else ''}
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
        <h3>Angebote vor Ort</h3>
        <ul class="station-list">
          {amenity_items}
        </ul>
        {f'<h3>Details</h3><ul class="station-list">{static_detail_items}</ul>' if static_detail_items else ''}
        {f'<p class="station-note">{detail_source_text}</p>' if detail_source_text else ''}
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

    payload = sanitize_json_value(json.loads(geojson_path.read_text(encoding="utf-8")))
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


def copy_management_data_tree() -> None:
    source_root = DATA_DIR / "management"
    if not source_root.exists():
        return
    target_root = SITE_DATA_DIR / "management"
    for source_path in sorted(source_root.rglob("*")):
        if source_path.is_dir():
            continue
        relative_path = source_path.relative_to(source_root)
        target_path = target_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.suffix.lower() == ".json":
            payload = sanitize_json_value(json.loads(source_path.read_text(encoding="utf-8")))
            target_path.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                encoding="utf-8",
            )
        else:
            shutil.copy2(source_path, target_path)


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
            payload = sanitize_json_value(json.loads(source.read_text(encoding="utf-8")))
            (SITE_DATA_DIR / filename).write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                encoding="utf-8",
            )
    copy_management_data_tree()

    station_page_paths = write_station_pages()
    write_sitemap(station_page_paths)


if __name__ == "__main__":
    main()
