#!/usr/bin/env python3
"""Generate an A0 scientific poster for woladen.de."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from PIL import Image
from reportlab.graphics import renderPDF
from reportlab.graphics.barcode import qr
from reportlab.graphics.shapes import Drawing
from reportlab.lib import colors
from reportlab.lib.pagesizes import A0
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
from reportlab.platypus import Paragraph


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "pdf"
TMP_DIR = ROOT / "tmp" / "pdfs"

SUMMARY_PATH = ROOT / "data" / "summary.json"
OPERATORS_PATH = ROOT / "data" / "operators.json"
CHARGERS_PATH = ROOT / "data" / "chargers_fast.csv"
GERMANY_PATH = ROOT / "data" / "apple-abdeckung-DE.geojson"
SCREENSHOT_PATH = Path(
    os.environ.get(
        "WOLADEN_POSTER_SCREENSHOT",
        ROOT / "output" / "playwright" / "mobile_test.webp",
    )
)
ICON_PATH = ROOT / "web" / "favicon-512.png"

POSTER_PATH = OUTPUT_DIR / "woladen_poster_a0_de.pdf"


PALETTE = {
    "bg": colors.HexColor("#F3F6F4"),
    "panel": colors.HexColor("#FFFFFF"),
    "panel_alt": colors.HexColor("#EEF5F3"),
    "ink": colors.HexColor("#10353B"),
    "muted": colors.HexColor("#4F6B70"),
    "teal": colors.HexColor("#1E9E9B"),
    "teal_dark": colors.HexColor("#157A79"),
    "teal_soft": colors.HexColor("#D8F0EC"),
    "gold": colors.HexColor("#E6A95C"),
    "gray": colors.HexColor("#CBD5D8"),
    "gray_dark": colors.HexColor("#7B8D93"),
    "line": colors.HexColor("#D9E3E1"),
    "header": colors.HexColor("#0E4E57"),
    "header_soft": colors.HexColor("#17717A"),
    "danger": colors.HexColor("#C05C4B"),
}

AMENITY_LABELS = {
    "restaurant": "Restaurant",
    "cafe": "Café",
    "fast_food": "Fast Food",
    "toilets": "Toiletten",
    "supermarket": "Supermarkt",
    "bakery": "Bäckerei",
    "convenience": "Kiosk",
    "pharmacy": "Apotheke",
    "hotel": "Hotel",
    "museum": "Museum",
    "playground": "Spielplatz",
    "park": "Park",
    "ice_cream": "Eis",
}

FONT_REGULAR = "Helvetica"
FONT_SEMIBOLD = "Helvetica-Bold"
FONT_BOLD = "Helvetica-Bold"


@dataclass(frozen=True)
class PosterMetrics:
    raw_rows: int
    fast_chargers_total: int
    stations_with_amenities: int
    amenity_share: float
    median_power_kw: float
    p90_power_kw: float
    median_amenities: float
    mean_amenities: float
    p90_amenities: float
    median_points: float
    mean_points: float
    listed_operators: int
    min_operator_stations: int
    source_url: str
    source_date_label: str
    finished_at_label: str
    radius_m: int
    min_power_kw: float
    pbf_gb: float
    osm_points: int
    operator_rows: list[tuple[str, int]]
    amenity_rows: list[tuple[str, float]]
    stations: list[tuple[float, float, bool]]
    germany_polygons: list[list[tuple[float, float]]]


def register_fonts() -> None:
    global FONT_REGULAR, FONT_SEMIBOLD, FONT_BOLD

    candidates = {
        "WoladenRegular": ROOT / "web" / "img" / "SpaceGrotesk-Regular.ttf",
        "WoladenSemiBold": ROOT / "web" / "img" / "SpaceGrotesk-SemiBold.ttf",
        "WoladenBold": ROOT / "web" / "img" / "SpaceGrotesk-Bold.ttf",
    }

    try:
        for name, path in candidates.items():
            pdfmetrics.registerFont(TTFont(name, str(path)))
        FONT_REGULAR = "WoladenRegular"
        FONT_SEMIBOLD = "WoladenSemiBold"
        FONT_BOLD = "WoladenBold"
    except Exception:
        FONT_REGULAR = "Helvetica"
        FONT_SEMIBOLD = "Helvetica-Bold"
        FONT_BOLD = "Helvetica-Bold"


def de_int(value: int) -> str:
    return f"{value:,}".replace(",", ".")


def de_float(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}".replace(".", ",")


def iso_to_date_label(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.strftime("%d.%m.%Y")


def load_metrics() -> PosterMetrics:
    summary = json.loads(SUMMARY_PATH.read_text())
    operators = json.loads(OPERATORS_PATH.read_text())
    chargers = pd.read_csv(CHARGERS_PATH)
    germany = json.loads(GERMANY_PATH.read_text())

    amenity_cols = [
        col
        for col in chargers.columns
        if col.startswith("amenity_") and col != "amenity_examples"
    ]
    amenity_rows: list[tuple[str, float]] = []
    for col in amenity_cols:
        key = col.replace("amenity_", "")
        share = float((chargers[col] > 0).mean() * 100)
        amenity_rows.append((AMENITY_LABELS.get(key, key), share))
    amenity_rows.sort(key=lambda row: row[1], reverse=True)

    operator_rows = [
        (row["name"], int(row["stations"]))
        for row in operators["operators"][:6]
    ]

    stations = [
        (float(row.lon), float(row.lat), bool(row.amenities_total > 0))
        for row in chargers[["lon", "lat", "amenities_total"]].itertuples(index=False)
    ]

    polygons: list[list[tuple[float, float]]] = []
    if germany["type"] == "Polygon":
        polygons.append([(float(x), float(y)) for x, y in germany["coordinates"][0]])
    elif germany["type"] == "MultiPolygon":
        for polygon in germany["coordinates"]:
            polygons.append([(float(x), float(y)) for x, y in polygon[0]])

    source_url = summary["source"]["source_url"]
    source_date = source_url.rsplit("_", 1)[-1].replace(".csv", "")

    return PosterMetrics(
        raw_rows=int(summary["records"]["raw_rows"]),
        fast_chargers_total=int(summary["records"]["fast_chargers_total"]),
        stations_with_amenities=int(summary["records"]["stations_with_amenities"]),
        amenity_share=float(
            summary["records"]["stations_with_amenities"]
            / summary["records"]["fast_chargers_total"]
            * 100
        ),
        median_power_kw=float(chargers["max_power_kw"].median()),
        p90_power_kw=float(chargers["max_power_kw"].quantile(0.9)),
        median_amenities=float(chargers["amenities_total"].median()),
        mean_amenities=float(chargers["amenities_total"].mean()),
        p90_amenities=float(chargers["amenities_total"].quantile(0.9)),
        median_points=float(chargers["charging_points_count"].median()),
        mean_points=float(chargers["charging_points_count"].mean()),
        listed_operators=int(operators["total_operators"]),
        min_operator_stations=int(operators["min_stations"]),
        source_url=source_url,
        source_date_label=source_date,
        finished_at_label=iso_to_date_label(summary["run"]["finished_at"]),
        radius_m=int(summary["params"]["radius_m"]),
        min_power_kw=float(summary["params"]["min_power_kw"]),
        pbf_gb=float(summary["amenity_lookup"]["osm_pbf_bytes"]) / (1024**3),
        osm_points=int(summary["amenity_lookup"]["osm_pbf_points"]),
        operator_rows=operator_rows,
        amenity_rows=amenity_rows[:6],
        stations=stations,
        germany_polygons=polygons,
    )


def prepare_images() -> tuple[Path, Path]:
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    screenshot_png = TMP_DIR / "woladen_mobile_screenshot.png"
    with Image.open(SCREENSHOT_PATH) as image:
        image = image.convert("RGBA")
        bg = Image.new("RGBA", image.size, (255, 255, 255, 255))
        bg.alpha_composite(image)
        bg.convert("RGB").save(screenshot_png)

    icon_png = TMP_DIR / "woladen_icon.png"
    with Image.open(ICON_PATH) as image:
        image.convert("RGBA").save(icon_png)

    return screenshot_png, icon_png


def make_styles() -> dict[str, ParagraphStyle]:
    return {
        "abstract": ParagraphStyle(
            "abstract",
            fontName=FONT_REGULAR,
            fontSize=28,
            leading=35,
            textColor=colors.white,
        ),
        "meta": ParagraphStyle(
            "meta",
            fontName=FONT_SEMIBOLD,
            fontSize=24,
            leading=28,
            textColor=colors.white,
        ),
        "body": ParagraphStyle(
            "body",
            fontName=FONT_REGULAR,
            fontSize=26,
            leading=33,
            textColor=PALETTE["ink"],
        ),
        "body_muted": ParagraphStyle(
            "body_muted",
            fontName=FONT_REGULAR,
            fontSize=24,
            leading=30,
            textColor=PALETTE["muted"],
        ),
        "bullet": ParagraphStyle(
            "bullet",
            fontName=FONT_REGULAR,
            fontSize=26,
            leading=33,
            textColor=PALETTE["ink"],
            leftIndent=0,
        ),
        "small_bullet": ParagraphStyle(
            "small_bullet",
            fontName=FONT_REGULAR,
            fontSize=24,
            leading=30,
            textColor=PALETTE["ink"],
            leftIndent=0,
        ),
        "footer": ParagraphStyle(
            "footer",
            fontName=FONT_REGULAR,
            fontSize=24,
            leading=30,
            textColor=PALETTE["ink"],
        ),
    }


def paragraph_height(text: str, width: float, style: ParagraphStyle) -> float:
    para = Paragraph(text, style)
    _, height = para.wrap(width, 10_000)
    return height


def draw_paragraph(
    c: canvas.Canvas,
    text: str,
    x: float,
    top: float,
    width: float,
    style: ParagraphStyle,
) -> float:
    para = Paragraph(text, style)
    _, height = para.wrap(width, 10_000)
    para.drawOn(c, x, top - height)
    return height


def draw_bullets(
    c: canvas.Canvas,
    items: Iterable[str],
    x: float,
    top: float,
    width: float,
    style: ParagraphStyle,
) -> float:
    used = 0.0
    for item in items:
        text = f"- {item}"
        used += draw_paragraph(c, text, x, top - used, width, style)
        used += 8
    return used


def draw_panel_shell(
    c: canvas.Canvas,
    x: float,
    bottom: float,
    width: float,
    height: float,
    title: str,
    tint: colors.Color | None = None,
) -> tuple[float, float, float, float]:
    fill = tint or PALETTE["panel"]
    c.saveState()
    c.setFillColor(fill)
    c.roundRect(x, bottom, width, height, 28, stroke=0, fill=1)
    c.setStrokeColor(PALETTE["line"])
    c.setLineWidth(2)
    c.roundRect(x, bottom, width, height, 28, stroke=1, fill=0)
    c.restoreState()

    pad_x = 30
    pad_top = 28
    title_y = bottom + height - pad_top
    c.setFillColor(PALETTE["ink"])
    c.setFont(FONT_BOLD, 40)
    c.drawString(x + pad_x, title_y - 40, title)

    content_top = title_y - 72
    return x + pad_x, content_top, width - 2 * pad_x, height - 112


def draw_metric_card(
    c: canvas.Canvas,
    x: float,
    bottom: float,
    width: float,
    height: float,
    value: str,
    label: str,
    accent: colors.Color,
    detail: str | None = None,
) -> None:
    c.setFillColor(colors.white)
    c.roundRect(x, bottom, width, height, 22, stroke=0, fill=1)
    c.setStrokeColor(PALETTE["line"])
    c.setLineWidth(2)
    c.roundRect(x, bottom, width, height, 22, stroke=1, fill=0)
    c.setFillColor(accent)
    c.roundRect(x + 18, bottom + height - 28, 64, 10, 5, stroke=0, fill=1)
    c.setFont(FONT_BOLD, 54)
    c.setFillColor(PALETTE["ink"])
    c.drawString(x + 22, bottom + height - 92, value)
    c.setFont(FONT_SEMIBOLD, 26)
    c.setFillColor(PALETTE["muted"])
    c.drawString(x + 22, bottom + 42, label)
    if detail:
        c.setFont(FONT_REGULAR, 24)
        c.drawRightString(x + width - 22, bottom + 42, detail)


def draw_header(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    styles: dict[str, ParagraphStyle],
    page_w: float,
    page_h: float,
    margin: float,
    header_h: float,
) -> None:
    header_y = page_h - margin - header_h
    c.setFillColor(PALETTE["header"])
    c.roundRect(margin, header_y, page_w - 2 * margin, header_h, 36, stroke=0, fill=1)

    c.setFillColor(PALETTE["header_soft"])
    c.circle(page_w - 240, header_y + header_h - 100, 130, stroke=0, fill=1)
    c.circle(page_w - 400, header_y + 78, 90, stroke=0, fill=1)
    c.circle(page_w - 140, header_y + 58, 52, stroke=0, fill=1)

    content_x = margin + 40
    content_top = page_h - margin - 42
    content_w = page_w - 2 * margin - 350

    c.setFillColor(colors.white)
    c.setFont(FONT_BOLD, 92)
    c.drawString(content_x, content_top - 84, "woladen.de")

    c.setFont(FONT_SEMIBOLD, 42)
    c.drawString(
        content_x,
        content_top - 140,
        "Datengetriebene Analyse von Schnellladepunkten und Umgebungsangeboten",
    )

    abstract = (
        "Das Projekt kombiniert das Ladesäulenregister der Bundesnetzagentur mit "
        "OpenStreetMap-POIs, um Schnellladepunkte ab 50 kW räumlich anzureichern "
        "und als performanten Demonstrator für Web, iPhone und Android bereitzustellen."
    )
    draw_paragraph(c, abstract, content_x, content_top - 188, content_w, styles["abstract"])

    c.setFont(FONT_SEMIBOLD, 24)
    meta = (
        f"Projektposter | Datenstand: {metrics.finished_at_label} | "
        f"Quelle BNetzA: {metrics.source_date_label}"
    )
    c.drawString(content_x, header_y + 32, meta)

    qr_x = page_w - margin - 250
    qr_y = header_y + 36
    draw_qr_code(c, "https://woladen.de/", qr_x, qr_y, 190)
    c.setFillColor(colors.white)
    c.setFont(FONT_SEMIBOLD, 24)
    c.drawCentredString(qr_x + 95, qr_y - 18, "Demo: woladen.de")


def draw_qr_code(c: canvas.Canvas, value: str, x: float, y: float, size: float) -> None:
    c.setFillColor(colors.white)
    c.roundRect(x - 18, y - 18, size + 36, size + 36, 20, stroke=0, fill=1)
    widget = qr.QrCodeWidget(value)
    bounds = widget.getBounds()
    bw = bounds[2] - bounds[0]
    bh = bounds[3] - bounds[1]
    drawing = Drawing(size, size, transform=[size / bw, 0, 0, size / bh, 0, 0])
    drawing.add(widget)
    renderPDF.draw(drawing, c, x, y)


def draw_problem_panel(
    c: canvas.Canvas,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Motivation und Ziel")
    lead = (
        "Schnellladepunkte werden im Alltag nicht nur nach Ladeleistung ausgewählt. "
        "Während eines Ladefensters von 15 bis 40 Minuten sind Toiletten, Gastronomie "
        "und Nahversorgung zentrale Kontextfaktoren."
    )
    used = draw_paragraph(c, lead, left, top, inner_w, styles["body"])
    used += 16
    bullets = [
        "Öffentliche Register enthalten Standort, Betreiber und Leistung, aber kaum Informationen zur Aufenthaltsqualität.",
        "API-basierte Einzelabfragen für deutschlandweite POI-Anreicherung sind langsam, fragil und schwer reproduzierbar.",
        "Ziel ist ein reproduzierbarer Workflow, der Registerdaten räumlich anreichert und in mobilen Clients direkt nutzbar macht.",
    ]
    draw_bullets(c, bullets, left, top - used, inner_w, styles["bullet"])


def draw_data_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Datengrundlage")
    bullets = [
        f"Bundesnetzagentur-Ladesäulenregister vom {metrics.source_date_label}: {de_int(metrics.raw_rows)} Rohzeilen.",
        f"Filter auf aktive Schnellladepunkte mit mindestens {de_float(metrics.min_power_kw, 0)} kW.",
        f"OpenStreetMap-Deutschlandextrakt ({de_float(metrics.pbf_gb, 2)} GB) als lokaler PBF-Index mit {de_int(metrics.osm_points)} relevanten POI-Punkten.",
        f"Räumliche Zuordnung in einem Radius von {de_int(metrics.radius_m)} m für 13 Amenity-Kategorien.",
        f"Abgeleitete Artefakte: GeoJSON für die Karte, Operatorliste für das UI und ein maschinenlesbares Summary.",
    ]
    draw_bullets(c, bullets, left, top, inner_w, styles["bullet"])


def draw_operator_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Betreiberstruktur")
    chart_top = top - 10
    label_w = inner_w * 0.55
    bar_x = left + label_w + 10
    bar_w = inner_w - label_w - 22
    row_h = 72
    max_value = max(value for _, value in metrics.operator_rows)

    for idx, (label, value) in enumerate(metrics.operator_rows):
        y = chart_top - idx * row_h - 52
        c.setFont(FONT_SEMIBOLD, 24)
        c.setFillColor(PALETTE["ink"])
        short_label = label
        if paragraph_height(short_label, label_w, styles["body_muted"]) > 60:
            parts = short_label.split(" ")
            short_label = " ".join(parts[:3]) + " ..."
        c.drawString(left, y, short_label)
        c.setFillColor(PALETTE["teal_soft"])
        c.roundRect(bar_x, y - 8, bar_w, 26, 13, stroke=0, fill=1)
        fill_w = max(40, bar_w * (value / max_value))
        c.setFillColor(PALETTE["teal"])
        c.roundRect(bar_x, y - 8, fill_w, 26, 13, stroke=0, fill=1)
        c.setFillColor(PALETTE["ink"])
        c.setFont(FONT_BOLD, 24)
        c.drawRightString(bar_x + bar_w, y + 1, de_int(value))

    note = (
        f"Für das Frontend werden {metrics.listed_operators} Betreiber mit mindestens "
        f"{de_int(metrics.min_operator_stations)} Standorten voraggregiert. "
        "Die Visualisierung zeigt die sechs größten Anbieter."
    )
    draw_paragraph(c, note, left, bottom + 86, inner_w, styles["body_muted"])


def draw_pipeline_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Methodik")
    box_h = 110
    gap = 26
    box_w = inner_w
    steps = [
        ("1. Quelle", f"BNetzA-Download und Schema-Normalisierung\n{de_int(metrics.raw_rows)} Rohzeilen"),
        ("2. Filter", f"Aktive Ladepunkte mit >= {de_float(metrics.min_power_kw, 0)} kW"),
        ("3. Kontext", f"OSM-PBF-Scan, Indexierung relevanter POIs\n{de_int(metrics.osm_points)} Punkte"),
        ("4. Join", f"Räumliche Zuordnung im {de_int(metrics.radius_m)}-m-Radius\nAmenity-Zahlen und Beispiele"),
        ("5. Output", "GeoJSON, Operators.json und Summary\nfür Web-, iOS- und Android-Clients"),
    ]

    current_top = top
    for idx, (title, body) in enumerate(steps):
        y = current_top - box_h
        tint = PALETTE["panel_alt"] if idx % 2 == 0 else colors.white
        c.setFillColor(tint)
        c.roundRect(left, y, box_w, box_h, 20, stroke=0, fill=1)
        c.setStrokeColor(PALETTE["line"])
        c.setLineWidth(2)
        c.roundRect(left, y, box_w, box_h, 20, stroke=1, fill=0)
        c.setFont(FONT_BOLD, 28)
        c.setFillColor(PALETTE["teal_dark"])
        c.drawString(left + 22, y + box_h - 36, title)
        draw_paragraph(c, body.replace("\n", "<br/>"), left + 22, y + box_h - 48, box_w - 44, styles["body_muted"])
        current_top = y - gap
        if idx < len(steps) - 1:
            arrow_x = left + box_w / 2
            arrow_top = current_top + gap - 4
            c.setStrokeColor(PALETTE["teal_dark"])
            c.setLineWidth(4)
            c.line(arrow_x, arrow_top, arrow_x, arrow_top - 18)
            c.line(arrow_x, arrow_top - 18, arrow_x - 10, arrow_top - 8)
            c.line(arrow_x, arrow_top - 18, arrow_x + 10, arrow_top - 8)


def draw_map_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Räumliche Verteilung")
    map_bottom = bottom + 90
    map_h = height - 170
    map_w = inner_w

    min_lon = 5.5
    max_lon = 15.7
    min_lat = 47.0
    max_lat = 55.4
    data_w = max_lon - min_lon
    data_h = max_lat - min_lat
    scale = min(map_w / data_w, map_h / data_h)
    draw_w = data_w * scale
    draw_h = data_h * scale
    ox = left + (map_w - draw_w) / 2
    oy = map_bottom + (map_h - draw_h) / 2

    def project(point: tuple[float, float]) -> tuple[float, float]:
        lon, lat = point
        px = ox + (lon - min_lon) * scale
        py = oy + (lat - min_lat) * scale
        return px, py

    c.setFillColor(colors.HexColor("#F7FBFA"))
    c.roundRect(left, map_bottom, map_w, map_h, 20, stroke=0, fill=1)

    c.setStrokeColor(PALETTE["line"])
    c.setLineWidth(1.8)
    for polygon in metrics.germany_polygons:
        if not polygon:
            continue
        path = c.beginPath()
        first_x, first_y = project(polygon[0])
        path.moveTo(first_x, first_y)
        for lon, lat in polygon[1:]:
            px, py = project((lon, lat))
            path.lineTo(px, py)
        path.close()
        c.setFillColor(colors.HexColor("#EAF2F0"))
        c.drawPath(path, stroke=1, fill=1)

    for lon, lat, has_amenity in metrics.stations:
        px, py = project((lon, lat))
        if has_amenity:
            c.setFillColor(PALETTE["teal"])
            radius = 1.35
        else:
            c.setFillColor(PALETTE["gray_dark"])
            radius = 1.0
        c.circle(px, py, radius, stroke=0, fill=1)

    legend_y = bottom + 38
    c.setFillColor(PALETTE["teal"])
    c.circle(left + 18, legend_y + 9, 6, stroke=0, fill=1)
    c.setFillColor(PALETTE["ink"])
    c.setFont(FONT_REGULAR, 24)
    c.drawString(left + 34, legend_y, "Station mit mindestens einem POI")
    c.setFillColor(PALETTE["gray_dark"])
    c.circle(left + 390, legend_y + 9, 6, stroke=0, fill=1)
    c.setFillColor(PALETTE["ink"])
    c.drawString(left + 406, legend_y, "Station ohne POI im Radius")


def draw_amenity_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Amenity-Ergebnisse")
    label_w = inner_w * 0.46
    bar_x = left + label_w + 10
    bar_w = inner_w - label_w - 22
    row_h = 72
    max_value = max(value for _, value in metrics.amenity_rows)

    for idx, (label, value) in enumerate(metrics.amenity_rows):
        y = top - idx * row_h - 48
        c.setFont(FONT_SEMIBOLD, 24)
        c.setFillColor(PALETTE["ink"])
        c.drawString(left, y, label)
        c.setFillColor(PALETTE["teal_soft"])
        c.roundRect(bar_x, y - 8, bar_w, 26, 13, stroke=0, fill=1)
        fill_w = bar_w * (value / max_value)
        c.setFillColor(PALETTE["gold"])
        c.roundRect(bar_x, y - 8, fill_w, 26, 13, stroke=0, fill=1)
        c.setFillColor(PALETTE["ink"])
        c.setFont(FONT_BOLD, 24)
        c.drawRightString(bar_x + bar_w, y + 1, f"{de_float(value)} %")

    note = (
        "Die häufigsten Kontexte sind Nahversorgung und Gastronomie. "
        f"Im Median liegen {de_float(metrics.median_amenities, 0)} POIs im Radius, "
        f"das 90. Perzentil erreicht {de_float(metrics.p90_amenities, 0)}."
    )
    draw_paragraph(c, note, left, bottom + 90, inner_w, styles["body_muted"])


def scale_to_fit(img_w: float, img_h: float, max_w: float, max_h: float) -> tuple[float, float]:
    factor = min(max_w / img_w, max_h / img_h)
    return img_w * factor, img_h * factor


def draw_demo_panel(
    c: canvas.Canvas,
    screenshot_path: Path,
    icon_path: Path,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Demonstrator")

    c.setFillColor(PALETTE["panel_alt"])
    c.roundRect(left, bottom + 52, inner_w, height - 138, 24, stroke=0, fill=1)

    screenshot = Image.open(screenshot_path)
    icon = Image.open(icon_path)
    s_w, s_h = screenshot.size
    i_w, i_h = icon.size

    icon_box = 170
    c.drawImage(
        ImageReader(icon),
        left + 28,
        bottom + height - 264,
        width=icon_box,
        height=icon_box,
        mask="auto",
    )

    metric_text = (
        f"{de_int(metrics.fast_chargers_total)} Stationen<br/>"
        f"{de_float(metrics.amenity_share)} % mit Kontext<br/>"
        f"Median: {de_float(metrics.median_power_kw, 0)} kW"
    )
    draw_paragraph(c, metric_text, left + 220, top - 12, inner_w - 248, styles["body"])

    draw_w, draw_h = scale_to_fit(s_w, s_h, inner_w - 56, height - 420)
    img_x = left + (inner_w - draw_w) / 2
    img_y = bottom + 80
    c.setFillColor(colors.white)
    c.roundRect(img_x - 16, img_y - 16, draw_w + 32, draw_h + 32, 26, stroke=0, fill=1)
    c.setStrokeColor(PALETTE["line"])
    c.setLineWidth(2)
    c.roundRect(img_x - 16, img_y - 16, draw_w + 32, draw_h + 32, 26, stroke=1, fill=0)
    c.drawImage(ImageReader(screenshot), img_x, img_y, width=draw_w, height=draw_h, mask="auto")

    caption = (
        "Die UI priorisiert nahe Stationen, zeigt Filter für Betreiber, Leistung "
        "und Amenities und bietet in den nativen Apps zusätzlich Offline-Bundles und Favoriten."
    )
    draw_paragraph(c, caption, left, bottom + 58, inner_w, styles["body_muted"])


def draw_platform_panel(
    c: canvas.Canvas,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Plattformen")
    gap = 16
    box_h = (height - 146 - 2 * gap) / 3
    label_w = 152
    platforms = [
        ("Web", "GitHub Pages mit Leaflet sowie mobile-first Filter- und Listenansicht."),
        ("iPhone", "SwiftUI + MapKit, Offline-Bundle, Favoriten und Navigationshandoff."),
        ("Android", "Jetpack Compose + OSMDroid mit weitgehend deckungsgleicher Datenlogik."),
    ]
    for idx, (title, body) in enumerate(platforms):
        by = bottom + 34 + (2 - idx) * (box_h + gap)
        c.setFillColor(PALETTE["panel_alt"])
        c.roundRect(left, by, inner_w, box_h, 20, stroke=0, fill=1)
        c.setStrokeColor(PALETTE["line"])
        c.setLineWidth(2)
        c.roundRect(left, by, inner_w, box_h, 20, stroke=1, fill=0)

        c.setFillColor(colors.white)
        c.roundRect(left + 18, by + 18, label_w, box_h - 36, 18, stroke=0, fill=1)
        c.setStrokeColor(PALETTE["line"])
        c.setLineWidth(1.5)
        c.roundRect(left + 18, by + 18, label_w, box_h - 36, 18, stroke=1, fill=0)

        c.setFont(FONT_BOLD, 30)
        c.setFillColor(PALETTE["teal_dark"])
        c.drawCentredString(left + 18 + label_w / 2, by + box_h / 2 - 10, title)

        text_x = left + label_w + 42
        text_top = by + box_h - 18
        draw_paragraph(c, body, text_x, text_top, inner_w - label_w - 60, styles["body_muted"])


def draw_interpretation_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Interpretation und Grenzen")
    bullets = [
        f"{de_float(metrics.amenity_share)} % der Schnellladepunkte besitzen mindestens einen POI im {de_int(metrics.radius_m)}-m-Radius. Kontextbasierte Ranking-Verfahren sind damit deutschlandweit sinnvoll.",
        f"Die Leistungswerte sind hoch: Median {de_float(metrics.median_power_kw, 0)} kW, 90. Perzentil {de_float(metrics.p90_power_kw, 0)} kW, im Mittel {de_float(metrics.mean_points)} Ladepunkte pro Station.",
        "Die lokale PBF-Verarbeitung ersetzt fragile Overpass-Massenabfragen und erhöht Reproduzierbarkeit und Laufzeitstabilität.",
        "Grenzen bleiben die Heterogenität von OSM, rein radiale Distanz statt Wegzeit und fehlende Qualitätsurteile über POIs.",
    ]
    draw_bullets(c, bullets, left, top, inner_w, styles["bullet"])


def draw_findings_panel(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    x: float,
    bottom: float,
    width: float,
    height: float,
    styles: dict[str, ParagraphStyle],
) -> None:
    left, top, inner_w, _ = draw_panel_shell(c, x, bottom, width, height, "Kernbefunde")
    bullets = [
        f"Von {de_int(metrics.fast_chargers_total)} Schnellladepunkten besitzen {de_int(metrics.stations_with_amenities)} mindestens einen relevanten POI im Nahbereich.",
        f"Die Kontextdichte ist heterogen: Median {de_float(metrics.median_amenities, 0)} POIs, Mittelwert {de_float(metrics.mean_amenities)}, 90. Perzentil {de_float(metrics.p90_amenities, 0)}.",
        f"Nahversorgung dominiert. Supermärkte sind an {de_float(metrics.amenity_rows[0][1])} % der Standorte vorhanden, Fast Food an {de_float(metrics.amenity_rows[1][1])} %.",
        f"Die technische Vorverarbeitung reduziert den Datenzugriff der Clients auf drei stabile Artefakte und erlaubt monatliche Aktualisierung.",
    ]
    draw_bullets(c, bullets, left, top, inner_w, styles["bullet"])


def draw_footer(
    c: canvas.Canvas,
    metrics: PosterMetrics,
    styles: dict[str, ParagraphStyle],
    page_w: float,
    margin: float,
    bottom_margin: float,
    footer_h: float,
) -> None:
    footer_y = bottom_margin
    total_w = page_w - 2 * margin
    c.setFillColor(colors.white)
    c.roundRect(margin, footer_y, total_w, footer_h, 28, stroke=0, fill=1)
    c.setStrokeColor(PALETTE["line"])
    c.setLineWidth(2)
    c.roundRect(margin, footer_y, total_w, footer_h, 28, stroke=1, fill=0)

    inner_x = margin + 30
    inner_top = footer_y + footer_h - 28
    gap = 26
    left_w = total_w * 0.43
    mid_w = total_w * 0.32
    right_w = total_w - left_w - mid_w - 2 * gap

    c.setFont(FONT_BOLD, 40)
    c.setFillColor(PALETTE["ink"])
    c.drawString(inner_x, inner_top - 40, "Schlussfolgerung")
    conclusion = (
        "woladen.de zeigt, dass offene Geodaten und deterministische Vorverarbeitung "
        "aus einem reinen Register ein kontextsensitives Ladeinfrastrukturprodukt "
        "machen können. Der Mehrwert entsteht vor allem durch die Kombination aus "
        "amtlicher Quelle, räumlicher OSM-Anreicherung und direkt nutzbaren Clients."
    )
    draw_paragraph(c, conclusion, inner_x, inner_top - 60, left_w - 10, styles["footer"])

    mid_x = inner_x + left_w + gap
    c.setFont(FONT_BOLD, 40)
    c.drawString(mid_x, inner_top - 40, "Nächste Schritte")
    next_steps = [
        "Routen- und Wegzeitbezug statt reinem Radius.",
        "Persistenter Amenity-Preindex für schnellere Builds.",
        "Schema- und Artefakt-Regressionstests.",
        "Nutzerstudie zur Relevanz einzelner Kontextfaktoren.",
    ]
    draw_bullets(c, next_steps, mid_x, inner_top - 68, mid_w - 10, styles["small_bullet"])

    qr_x = margin + total_w - right_w + 32
    qr_y = footer_y + 54
    draw_qr_code(c, "https://woladen.de/", qr_x, qr_y, 150)
    c.setFillColor(PALETTE["ink"])
    c.setFont(FONT_SEMIBOLD, 24)
    c.drawString(qr_x + 182, qr_y + 118, "Demo und Quellcode")
    c.setFont(FONT_REGULAR, 24)
    c.drawString(qr_x + 182, qr_y + 84, "woladen.de")
    c.drawString(qr_x + 182, qr_y + 50, "GitHub: volzinnovation/")
    c.drawString(qr_x + 182, qr_y + 20, "woladen.de")

    refs = (
        f"Datenquellen: Bundesnetzagentur Ladesäulenregister ({metrics.source_date_label}), "
        f"OpenStreetMap / Geofabrik Deutschlandextrakt, Poster generiert am {datetime.now().strftime('%d.%m.%Y')}."
    )
    c.setStrokeColor(PALETTE["line"])
    c.line(margin + 30, footer_y + 26, margin + total_w - 30, footer_y + 26)
    c.setFont(FONT_REGULAR, 24)
    c.setFillColor(PALETTE["muted"])
    c.drawString(margin + 30, footer_y + 36, refs)


def generate() -> Path:
    register_fonts()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics = load_metrics()
    screenshot_path, icon_path = prepare_images()
    styles = make_styles()

    page_w, page_h = A0
    margin = 72
    gutter = 32
    gap = 28
    header_h = 320
    metric_h = 152
    footer_h = 288
    body_h = page_h - 2 * margin - header_h - metric_h - footer_h - 3 * gap
    col_w = (page_w - 2 * margin - 2 * gutter) / 3

    c = canvas.Canvas(str(POSTER_PATH), pagesize=A0)
    c.setTitle("woladen.de Poster A0")
    c.setAuthor("Codex")
    c.setSubject("wissenschaftliches Projektposter")

    c.setFillColor(PALETTE["bg"])
    c.rect(0, 0, page_w, page_h, stroke=0, fill=1)

    draw_header(c, metrics, styles, page_w, page_h, margin, header_h)

    metrics_bottom = page_h - margin - header_h - gap - metric_h
    metric_gap = 22
    metric_w = (page_w - 2 * margin - 3 * metric_gap) / 4
    metric_labels = [
        (
            de_int(metrics.fast_chargers_total),
            "Schnellladepunkte >= 50 kW",
            PALETTE["teal"],
            f"{de_int(metrics.raw_rows)} Rohzeilen",
        ),
        (
            f"{de_float(metrics.amenity_share)} %",
            "mit mindestens einem POI",
            PALETTE["gold"],
            f"Radius {de_int(metrics.radius_m)} m",
        ),
        (
            f"{de_float(metrics.median_power_kw, 0)} kW",
            "Median der Maximalleistung",
            PALETTE["teal_dark"],
            f"P90 {de_float(metrics.p90_power_kw, 0)} kW",
        ),
        (
            de_int(metrics.listed_operators),
            "vorgefilterte Betreiber",
            PALETTE["danger"],
            f">= {de_int(metrics.min_operator_stations)} Standorte",
        ),
    ]
    for idx, (value, label, accent, detail) in enumerate(metric_labels):
        x = margin + idx * (metric_w + metric_gap)
        draw_metric_card(c, x, metrics_bottom, metric_w, metric_h, value, label, accent, detail)

    body_top = metrics_bottom - gap
    left_x = margin
    mid_x = margin + col_w + gutter
    right_x = margin + 2 * (col_w + gutter)

    col1_heights = [462, 506, 622, 722]
    col2_heights = [714, 896, 704]
    col3_heights = [1002, 516, 796]

    col_top = body_top
    panel_bottom = col_top - col1_heights[0]
    draw_problem_panel(c, left_x, panel_bottom, col_w, col1_heights[0], styles)
    col_top = panel_bottom - 24
    panel_bottom = col_top - col1_heights[1]
    draw_data_panel(c, metrics, left_x, panel_bottom, col_w, col1_heights[1], styles)
    col_top = panel_bottom - 24
    panel_bottom = col_top - col1_heights[2]
    draw_operator_panel(c, metrics, left_x, panel_bottom, col_w, col1_heights[2], styles)
    col_top = panel_bottom - 24
    panel_bottom = col_top - col1_heights[3]
    draw_findings_panel(c, metrics, left_x, panel_bottom, col_w, col1_heights[3], styles)

    col_top = body_top
    panel_bottom = col_top - col2_heights[0]
    draw_pipeline_panel(c, metrics, mid_x, panel_bottom, col_w, col2_heights[0], styles)
    col_top = panel_bottom - 24
    panel_bottom = col_top - col2_heights[1]
    draw_map_panel(c, metrics, mid_x, panel_bottom, col_w, col2_heights[1])
    col_top = panel_bottom - 24
    panel_bottom = col_top - col2_heights[2]
    draw_amenity_panel(c, metrics, mid_x, panel_bottom, col_w, col2_heights[2], styles)

    col_top = body_top
    panel_bottom = col_top - col3_heights[0]
    draw_demo_panel(
        c,
        screenshot_path,
        icon_path,
        metrics,
        right_x,
        panel_bottom,
        col_w,
        col3_heights[0],
        styles,
    )
    col_top = panel_bottom - 24
    panel_bottom = col_top - col3_heights[1]
    draw_platform_panel(c, right_x, panel_bottom, col_w, col3_heights[1], styles)
    col_top = panel_bottom - 24
    panel_bottom = col_top - col3_heights[2]
    draw_interpretation_panel(c, metrics, right_x, panel_bottom, col_w, col3_heights[2], styles)

    draw_footer(c, metrics, styles, page_w, margin, margin, footer_h)

    c.showPage()
    c.save()
    return POSTER_PATH


if __name__ == "__main__":
    output = generate()
    print(output)
