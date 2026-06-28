#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "play-store" / "android"
ASSET_DIR = OUTPUT_DIR / "assets"
PLAY_METADATA_DIR = OUTPUT_DIR / "metadata"
METADATA_DIR = OUTPUT_DIR / "metadata" / "de-DE"
I18N_DIR = ROOT / "web" / "i18n"
ICON_SOURCE = (
    ROOT
    / "iphone"
    / "Woladen"
    / "Resources"
    / "Assets.xcassets"
    / "AppIcon.appiconset"
    / "icon-1024.png"
)

GERMAN_TITLE = "woladen"
GERMAN_SHORT_DESCRIPTION = "Bessere Ladestopps in Europa"
GERMAN_FULL_DESCRIPTION = """woladen hilft dir, bessere Ladepausen zu finden: Ladestopps, die frei, verlässlich und angenehm sind.

Du suchst nicht nur eine Ladesäule. Du suchst einen Ort, an dem die Pause funktioniert: genug Leistung, möglichst freie Ladepunkte und etwas Sinnvolles in der Nähe. woladen zeigt dir Ladestationen in Europa zusammen mit Cafés, Bäckereien, Restaurants, Läden, Toiletten, Spielplätzen und anderen Orten rund um den Stopp.

Für längere Fahrten kannst du Start und Ziel eingeben und Ladestationen entlang deiner Route finden.

Mit woladen kannst du:
- verfügbare Ladestopps in Karte, Liste, Route und Favoriten finden
- Schnelllader ab 50 kW als Standard sehen
- nach freien Ladepunkten, Leistung, Steckertyp, Betreiber und Ausstattung filtern
- schnell erkennen, was du während der Ladepause in der Nähe machen kannst
- Live-Status und Details nutzen, wo sie verfügbar sind
- gute Stopps als Favoriten lokal auf deinem Gerät speichern
- deinen Standort optional nutzen, um passende Stopps in der Nähe zu sehen

woladen ist bewusst schlicht: kein Nutzerkonto, keine Werbung, keine In-App-Käufe.

Wenn du deinen Standort freigibst, nutzt woladen ihn nur, um passende Ladestopps in der Nähe zu sortieren und die Karte auszurichten. Favoriten bleiben auf deinem Gerät.

The human side of charging. Because charging time is your time."""
RELEASE_NOTES = """Version 1.3.1 aktualisiert woladen auf den europäischen Live-Katalog.

- API-gestützter europäischer Katalog
- Karte, Liste, Route, Filter und Favoriten
- Mehrsprachige native Texte aus dem Web-Katalog
- Live-Status und Stationsdetails, wo Anbieter sie bereitstellen
- Angepasste Layouts für Android-Smartphones und Tablets"""

PLAY_LOCALE_SOURCES = {
    "cs-CZ": "cs",
    "da-DK": "da",
    "de-DE": "de",
    "el-GR": "el",
    "en-GB": "en",
    "en-US": "en",
    "es-ES": "es",
    "fi-FI": "fi",
    "fr-FR": "fr",
    "hu-HU": "hu",
    "it-IT": "it",
    "lt": "lt",
    "lv": "lv",
    "nl-NL": "nl",
    "no-NO": "nb",
    "pl-PL": "pl",
    "pt-PT": "pt",
    "rm": "rm",
    "sl": "sl",
    "sv-SE": "sv",
    "tr-TR": "tr",
}

FIELD_LIMITS = {
    "title": 30,
    "short_description": 80,
    "full_description": 4000,
    "release_notes": 500,
}


def load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    candidates = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf" if bold else "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Helvetica.ttc",
    ]
    for candidate in candidates:
        path = Path(candidate)
        if path.exists():
            return ImageFont.truetype(str(path), size=size)
    return ImageFont.load_default()


def rounded(image: Image.Image, radius: int) -> Image.Image:
    mask = Image.new("L", image.size, 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, *image.size), radius=radius, fill=255)
    out = Image.new("RGBA", image.size)
    out.paste(image, mask=mask)
    return out


def fit_crop(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    src_w, src_h = image.size
    dst_w, dst_h = size
    src_ratio = src_w / src_h
    dst_ratio = dst_w / dst_h
    if src_ratio > dst_ratio:
        new_h = dst_h
        new_w = round(dst_h * src_ratio)
    else:
        new_w = dst_w
        new_h = round(dst_w / src_ratio)
    resized = image.resize((new_w, new_h), Image.Resampling.LANCZOS)
    left = max(0, (new_w - dst_w) // 2)
    top = max(0, (new_h - dst_h) // 2)
    return resized.crop((left, top, left + dst_w, top + dst_h))


def generate_icon() -> Path:
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    icon = Image.open(ICON_SOURCE).convert("RGBA")
    icon = icon.resize((512, 512), Image.Resampling.LANCZOS)
    out_path = ASSET_DIR / "app-icon-512.png"
    icon.save(out_path, optimize=True)
    return out_path


def generate_feature_graphic() -> Path:
    feature_size = (1024, 500)
    canvas = Image.new("RGBA", feature_size, "#0d6965")

    base = Image.new("RGBA", feature_size, "#0d6965")
    overlay = Image.new("RGBA", feature_size, 0)
    draw = ImageDraw.Draw(overlay)
    draw.ellipse((-120, -160, 520, 420), fill="#17877f")
    draw.ellipse((620, 180, 1200, 760), fill="#1b978e")
    draw.rounded_rectangle((44, 52, 500, 448), radius=44, fill="#f3eee7")
    canvas = Image.blend(base, overlay, 0.32)
    canvas.alpha_composite(overlay)

    draw = ImageDraw.Draw(canvas)
    title_font = load_font(58, bold=True)
    subtitle_font = load_font(26)
    chip_font = load_font(22, bold=True)

    draw.text((82, 92), "woladen", font=title_font, fill="#0e2e2c")
    draw.text((82, 164), "The human side of charging", font=subtitle_font, fill="#163d3a")
    draw.text(
        (82, 208),
        "Because charging time is your time",
        font=subtitle_font,
        fill="#163d3a",
        spacing=8,
    )

    chips = ["Bäckereien", "Restaurants", "Cafés"]
    x = 268
    y = 308
    for chip in chips:
        bbox = draw.textbbox((0, 0), chip, font=chip_font)
        width = bbox[2] - bbox[0] + 34
        draw.rounded_rectangle((x, y, x + width, y + 42), radius=20, fill="#d8efe8")
        draw.text((x + 17, y + 9), chip, font=chip_font, fill="#0d6965")
        x += width + 12

    icon = Image.open(ICON_SOURCE).convert("RGBA").resize((172, 172), Image.Resampling.LANCZOS)
    icon_shadow = Image.new("RGBA", (188, 188), 0)
    shadow_draw = ImageDraw.Draw(icon_shadow)
    shadow_draw.rounded_rectangle((10, 14, 178, 182), radius=44, fill=(0, 0, 0, 84))
    icon_shadow = icon_shadow.filter(ImageFilter.GaussianBlur(12))
    canvas.alpha_composite(icon_shadow, (70, 304))
    canvas.alpha_composite(icon, (78, 296))

    phone_map = Image.open(OUTPUT_DIR / "phone-portrait" / "03-map.png").convert("RGBA")
    phone_detail = Image.open(OUTPUT_DIR / "phone-portrait" / "02-detail.png").convert("RGBA")
    tablet_map = Image.open(OUTPUT_DIR / "tablet-landscape" / "03-map.png").convert("RGBA")

    tablet_card = rounded(fit_crop(tablet_map, (424, 256)), 28)
    tablet_shadow = Image.new("RGBA", (452, 284), 0)
    ImageDraw.Draw(tablet_shadow).rounded_rectangle((10, 12, 438, 272), radius=32, fill=(0, 0, 0, 90))
    tablet_shadow = tablet_shadow.filter(ImageFilter.GaussianBlur(14))
    canvas.alpha_composite(tablet_shadow, (564, 112))
    canvas.alpha_composite(tablet_card, (578, 124))

    phone_map_card = rounded(fit_crop(phone_map, (134, 268)), 28)
    phone_detail_card = rounded(fit_crop(phone_detail, (134, 268)), 28)
    phone_shadow = Image.new("RGBA", (158, 292), 0)
    ImageDraw.Draw(phone_shadow).rounded_rectangle((10, 10, 148, 282), radius=32, fill=(0, 0, 0, 96))
    phone_shadow = phone_shadow.filter(ImageFilter.GaussianBlur(14))

    canvas.alpha_composite(phone_shadow, (640, 190))
    canvas.alpha_composite(phone_map_card, (652, 202))
    canvas.alpha_composite(phone_shadow, (788, 168))
    canvas.alpha_composite(phone_detail_card, (800, 180))

    out_path = ASSET_DIR / "feature-graphic-1024x500.png"
    canvas.convert("RGB").save(out_path, optimize=True)
    return out_path


def checked(label: str, value: str) -> str:
    length = len(value)
    limit = FIELD_LIMITS[label]
    if length > limit:
        raise ValueError(f"{label} has {length} characters, Google Play limit is {limit}")
    return value.rstrip() + "\n"


def nested_value(data: dict[str, object], path: tuple[str, ...]) -> str:
    current: object = data
    for key in path:
        if not isinstance(current, dict):
            return ""
        current = current.get(key, "")
    return current if isinstance(current, str) else ""


def localized_short_description(data: dict[str, object]) -> str:
    title = nested_value(data, ("meta", "title")) or nested_value(data, ("seo", "homeTitle"))
    if title.startswith("woladen - "):
        return title.removeprefix("woladen - ")
    return title or GERMAN_SHORT_DESCRIPTION


def localized_full_description(data: dict[str, object]) -> str:
    parts = [
        nested_value(data, ("seo", "homeIntro")),
        nested_value(data, ("seo", "productMessage")),
        nested_value(data, ("route", "empty")),
        "The human side of charging. Because charging time is your time.",
    ]
    return "\n\n".join(part for part in parts if part)


def localized_metadata(locale: str, source_code: str) -> tuple[str, str, str]:
    if locale == "de-DE":
        return GERMAN_TITLE, GERMAN_SHORT_DESCRIPTION, GERMAN_FULL_DESCRIPTION

    data = json.loads((I18N_DIR / f"{source_code}.json").read_text(encoding="utf-8"))
    return GERMAN_TITLE, localized_short_description(data), localized_full_description(data)


def write_metadata() -> None:
    PLAY_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    for locale, source_code in PLAY_LOCALE_SOURCES.items():
        metadata_dir = PLAY_METADATA_DIR / locale
        metadata_dir.mkdir(parents=True, exist_ok=True)
        title, short_description, full_description = localized_metadata(locale, source_code)
        (metadata_dir / "title.txt").write_text(checked("title", title), encoding="utf-8")
        (metadata_dir / "short-description.txt").write_text(
            checked("short_description", short_description),
            encoding="utf-8",
        )
        (metadata_dir / "full-description.txt").write_text(
            checked("full_description", full_description),
            encoding="utf-8",
        )
        if locale == "de-DE":
            (metadata_dir / "release-notes.txt").write_text(
                checked("release_notes", RELEASE_NOTES),
                encoding="utf-8",
            )

    (PLAY_METADATA_DIR / "support-email.txt").write_text("studios@moonshots.gmbh\n", encoding="utf-8")
    (PLAY_METADATA_DIR / "website-url.txt").write_text("https://woladen.de/\n", encoding="utf-8")
    (PLAY_METADATA_DIR / "privacy-policy-url.txt").write_text("https://woladen.de/privacy.html\n", encoding="utf-8")


def main() -> None:
    generate_icon()
    generate_feature_graphic()
    write_metadata()


if __name__ == "__main__":
    main()
