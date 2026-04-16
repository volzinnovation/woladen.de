#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "play-store" / "android"
ASSET_DIR = OUTPUT_DIR / "assets"
PLAY_METADATA_DIR = OUTPUT_DIR / "metadata"
METADATA_DIR = OUTPUT_DIR / "metadata" / "de-DE"
ICON_SOURCE = (
    ROOT
    / "iphone"
    / "Woladen"
    / "Resources"
    / "Assets.xcassets"
    / "AppIcon.appiconset"
    / "icon-1024.png"
)


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

    draw.text((82, 92), "Woladen", font=title_font, fill="#0e2e2c")
    draw.text((82, 164), "Schnellladen ohne Ladeweile", font=subtitle_font, fill="#163d3a")
    draw.text(
        (82, 208),
        "Deutschlands Schnelllader mit Karte,\nFiltern und Aufenthaltsqualität.",
        font=subtitle_font,
        fill="#163d3a",
        spacing=8,
    )

    chips = ["50+ kW", "Karte", "Favoriten", "OSM-Annehmlichkeiten"]
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


def write_metadata() -> None:
    PLAY_METADATA_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    title = "Woladen"
    short_description = "Deutschlands Schnelllader mit Karte, Filtern und Tipps gegen Ladeweile"
    full_description = """Woladen zeigt dir Schnellladesäulen in Deutschland und hilft dir, gute Ladepausen zu finden. So findest du nicht nur den passenden Schnelllader, sondern auch Orte, an denen sich das Warten lohnt.

Mit Woladen kannst du:
- Schnelllader ab 50 kW in ganz Deutschland finden
- Ladepunkte in Karte und Liste durchsuchen
- nach Anbieter und Annehmlichkeiten filtern
- Favoriten lokal auf deinem Gerät speichern
- deinen Standort optional nutzen, um Ladepunkte in der Nähe schneller zu sehen

Woladen kombiniert das offizielle Ladesäulenregister der Bundesnetzagentur mit Informationen aus OpenStreetMap. Dadurch siehst du zu vielen Standorten direkt, was es in der Umgebung gibt, zum Beispiel Gastronomie, Einkauf, Toiletten oder weitere nützliche Stopps.

Die App ist bewusst schlank gehalten:
- kein Nutzerkonto
- keine Werbung
- keine In-App-Käufe

Wenn du deinen Standort freigibst, wird er verwendet, um die Karte auf deine Umgebung zu fokussieren und nahe Schnelllader zu sortieren. Favoriten bleiben lokal auf deinem Gerät.

Woladen ist ideal für alle, die unterwegs schnell laden und die Ladepause sinnvoll nutzen möchten."""
    release_notes = """Erstveröffentlichung von Woladen für Android.

- Schnelllader ab 50 kW in ganz Deutschland
- Karte, Liste, Filter und Favoriten
- Hinweise zur Aufenthaltsqualität aus OpenStreetMap
- Optionaler Standortzugriff für Ladepunkte in der Nähe"""

    (METADATA_DIR / "title.txt").write_text(title + "\n", encoding="utf-8")
    (METADATA_DIR / "short-description.txt").write_text(short_description + "\n", encoding="utf-8")
    (METADATA_DIR / "full-description.txt").write_text(full_description + "\n", encoding="utf-8")
    (METADATA_DIR / "release-notes.txt").write_text(release_notes + "\n", encoding="utf-8")
    (PLAY_METADATA_DIR / "support-email.txt").write_text("studios@moonshots.gmbh\n", encoding="utf-8")
    (PLAY_METADATA_DIR / "website-url.txt").write_text("https://woladen.de/\n", encoding="utf-8")
    (PLAY_METADATA_DIR / "privacy-policy-url.txt").write_text("https://woladen.de/privacy.html\n", encoding="utf-8")


def main() -> None:
    generate_icon()
    generate_feature_graphic()
    write_metadata()


if __name__ == "__main__":
    main()
