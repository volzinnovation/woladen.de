#!/usr/bin/env python3
"""Generate the woladen.de social preview card."""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1]
IMG_DIR = ROOT / "web" / "img"
OUTPUT_PATH = IMG_DIR / "social-card-home.png"
MAP_PATH = IMG_DIR / "chargers_naturalearth_purple_laea_baden_baden.png"
ICON_PATH = IMG_DIR / "touch-icon.png"
FONT_REGULAR = IMG_DIR / "SpaceGrotesk-Regular.ttf"
FONT_BOLD = IMG_DIR / "SpaceGrotesk-Bold.ttf"
FONT_SEMIBOLD = IMG_DIR / "SpaceGrotesk-SemiBold.ttf"


def font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(path), size)


def fit_crop(image: Image.Image, size: tuple[int, int]) -> Image.Image:
    target_w, target_h = size
    scale = max(target_w / image.width, target_h / image.height)
    resized = image.resize(
        (round(image.width * scale), round(image.height * scale)),
        Image.Resampling.LANCZOS,
    )
    left = max(0, (resized.width - target_w) // 2)
    top = max(0, (resized.height - target_h) // 2)
    return resized.crop((left, top, left + target_w, top + target_h))


def rounded_rect(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], radius: int, fill: str) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill)


def draw_chip(draw: ImageDraw.ImageDraw, text: str, x: int, y: int, chip_font: ImageFont.FreeTypeFont) -> int:
    bbox = draw.textbbox((0, 0), text, font=chip_font)
    width = bbox[2] - bbox[0] + 34
    rounded_rect(draw, (x, y, x + width, y + 42), 21, "#dff5ef")
    draw.text((x + 17, y + 9), text, fill="#0f766e", font=chip_font)
    return x + width + 14


def main() -> None:
    canvas = Image.new("RGB", (1200, 630), "#f8fafc")
    draw = ImageDraw.Draw(canvas)

    map_image = Image.open(MAP_PATH).convert("RGBA")
    map_crop = fit_crop(map_image, (650, 630))
    map_layer = Image.new("RGBA", (650, 630), "#f8fafc")
    map_layer.alpha_composite(map_crop)
    tint = Image.new("RGBA", map_layer.size, (15, 118, 110, 64))
    map_layer = Image.alpha_composite(map_layer, tint)
    canvas.paste(map_layer.convert("RGB"), (550, 0))

    fade = Image.new("RGBA", (1200, 630), 0)
    fade_draw = ImageDraw.Draw(fade)
    for index in range(220):
        alpha = max(0, 255 - index)
        fade_draw.line((520 + index, 0, 520 + index, 630), fill=(248, 250, 252, alpha), width=1)
    canvas = Image.alpha_composite(canvas.convert("RGBA"), fade)
    draw = ImageDraw.Draw(canvas)

    icon = Image.open(ICON_PATH).convert("RGBA").resize((82, 82), Image.Resampling.LANCZOS)
    shadow = Image.new("RGBA", (110, 110), 0)
    ImageDraw.Draw(shadow).rounded_rectangle((14, 16, 96, 98), radius=24, fill=(15, 23, 42, 62))
    shadow = shadow.filter(ImageFilter.GaussianBlur(10))
    canvas.alpha_composite(shadow, (70, 60))
    canvas.alpha_composite(icon, (84, 70))

    brand_font = font(FONT_BOLD, 42)
    headline_font = font(FONT_BOLD, 60)
    sub_font = font(FONT_REGULAR, 30)
    message_font = font(FONT_REGULAR, 24)
    chip_font = font(FONT_SEMIBOLD, 22)
    small_font = font(FONT_SEMIBOLD, 24)

    draw.text((184, 82), "woladen", fill="#0f2f2d", font=brand_font)
    draw.text((84, 178), "Plugs for Cars.\nPerks for People.", fill="#102a28", font=headline_font, spacing=4)
    draw.text(
        (88, 370),
        "The human side of charging.\nBecause charging time is your time.",
        fill="#31514e",
        font=sub_font,
        spacing=6,
    )
    draw.text(
        (88, 466),
        "Find available chargers near bakeries,\nrestaurants, shops, playgrounds and cafés.",
        fill="#31514e",
        font=message_font,
        spacing=5,
    )

    x = 88
    for chip in ("Bakeries", "Restaurants", "Cafés"):
        x = draw_chip(draw, chip, x, 548, chip_font)

    rounded_rect(draw, (780, 64, 1130, 132), 34, "#ffffff")
    draw.text((810, 84), "Smart EV Stops", fill="#0f766e", font=small_font)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(OUTPUT_PATH, quality=92, optimize=True)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
