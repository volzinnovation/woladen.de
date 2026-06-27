#!/usr/bin/env python3
"""Generate the woladen.de social preview card."""

from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw, ImageFilter, ImageFont

ROOT = Path(__file__).resolve().parents[1]
IMG_DIR = ROOT / "web" / "img"
OUTPUT_PATH = IMG_DIR / "social-card-home.png"
MAP_PATH = IMG_DIR / "chargers_naturalearth_purple.png"
ICON_PATH = IMG_DIR / "touch-icon.png"
FONT_REGULAR = IMG_DIR / "SpaceGrotesk-Regular.ttf"
FONT_BOLD = IMG_DIR / "SpaceGrotesk-Bold.ttf"
FONT_SEMIBOLD = IMG_DIR / "SpaceGrotesk-SemiBold.ttf"
AMENITY_ICON_PATHS = (
    IMG_DIR / "amenity_cafe.png",
    IMG_DIR / "shop_bakery.png",
    IMG_DIR / "amenity_restaurant.png",
    IMG_DIR / "leisure_playground.png",
    IMG_DIR / "amenity_toilets.png",
)


def font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(str(path), size)


def fit_crop(image: Image.Image, size: tuple[int, int], *, y_bias: float = 0.5) -> Image.Image:
    target_w, target_h = size
    scale = max(target_w / image.width, target_h / image.height)
    resized = image.resize(
        (round(image.width * scale), round(image.height * scale)),
        Image.Resampling.LANCZOS,
    )
    left = max(0, (resized.width - target_w) // 2)
    top = max(0, round((resized.height - target_h) * y_bias))
    return resized.crop((left, top, left + target_w, top + target_h))


def rounded_rect(draw: ImageDraw.ImageDraw, box: tuple[int, int, int, int], radius: int, fill: str) -> None:
    draw.rounded_rectangle(box, radius=radius, fill=fill)


def draw_chip(draw: ImageDraw.ImageDraw, text: str, x: int, y: int, chip_font: ImageFont.FreeTypeFont) -> int:
    bbox = draw.textbbox((0, 0), text, font=chip_font)
    width = bbox[2] - bbox[0] + 36
    rounded_rect(draw, (x, y, x + width, y + 44), 22, "#0f766e")
    draw.text((x + 18, y + 9), text, fill="#f7fffc", font=chip_font)
    return x + width + 12


def draw_icon_bubble(canvas: Image.Image, icon_path: Path, center: tuple[int, int], size: int = 50) -> None:
    cx, cy = center
    box = (cx - size // 2, cy - size // 2, cx + size // 2, cy + size // 2)
    shadow = Image.new("RGBA", (size + 28, size + 28), 0)
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.ellipse((14, 16, 14 + size, 16 + size), fill=(15, 23, 42, 54))
    shadow = shadow.filter(ImageFilter.GaussianBlur(8))
    canvas.alpha_composite(shadow, (box[0] - 14, box[1] - 14))

    draw = ImageDraw.Draw(canvas)
    draw.ellipse(box, fill="#f8fffc", outline=(15, 118, 110, 90), width=1)
    icon = Image.open(icon_path).convert("RGBA").resize((30, 30), Image.Resampling.LANCZOS)
    canvas.alpha_composite(icon, (cx - 15, cy - 15))


def main() -> None:
    canvas = Image.new("RGB", (1200, 630), "#f8fafc")
    draw = ImageDraw.Draw(canvas)

    map_image = Image.open(MAP_PATH).convert("RGBA")
    map_source = map_image.crop((0, 0, 2920, map_image.height))
    map_crop = fit_crop(map_source, (620, 630), y_bias=0.46)
    map_layer = Image.new("RGBA", (620, 630), "#f8fafc")
    map_layer.alpha_composite(map_crop)
    tint = Image.new("RGBA", map_layer.size, (15, 118, 110, 52))
    map_layer = Image.alpha_composite(map_layer, tint)
    canvas.paste(map_layer.convert("RGB"), (580, 0))

    fade = Image.new("RGBA", (1200, 630), 0)
    fade_draw = ImageDraw.Draw(fade)
    for index in range(220):
        alpha = max(0, 255 - index)
        fade_draw.line((548 + index, 0, 548 + index, 630), fill=(248, 250, 252, alpha), width=1)
    canvas = Image.alpha_composite(canvas.convert("RGBA"), fade)
    draw = ImageDraw.Draw(canvas)

    icon = Image.open(ICON_PATH).convert("RGBA").resize((82, 82), Image.Resampling.LANCZOS)
    shadow = Image.new("RGBA", (110, 110), 0)
    ImageDraw.Draw(shadow).rounded_rectangle((14, 16, 96, 98), radius=24, fill=(15, 23, 42, 62))
    shadow = shadow.filter(ImageFilter.GaussianBlur(10))
    canvas.alpha_composite(shadow, (70, 60))
    canvas.alpha_composite(icon, (84, 70))

    brand_font = font(FONT_BOLD, 42)
    headline_font = font(FONT_BOLD, 54)
    sub_font = font(FONT_REGULAR, 29)
    message_font = font(FONT_REGULAR, 23)
    chip_font = font(FONT_SEMIBOLD, 21)
    small_font = font(FONT_SEMIBOLD, 24)

    draw.text((184, 82), "woladen", fill="#0f2f2d", font=brand_font)
    draw.text((84, 176), "The human side\nof charging", fill="#102a28", font=headline_font, spacing=2)
    draw.text(
        (88, 318),
        "Because charging time\nis your time",
        fill="#31514e",
        font=sub_font,
        spacing=4,
    )
    draw.text(
        (88, 424),
        "Find available and reliable chargers\nwith great things to do nearby.",
        fill="#31514e",
        font=message_font,
        spacing=5,
    )

    x = 88
    for chip in ("Search", "Route", "Favorites"):
        x = draw_chip(draw, chip, x, 548, chip_font)

    rounded_rect(draw, (798, 64, 1130, 132), 34, "#ffffff")
    draw.text((832, 84), "Smart EV Stops", fill="#0f766e", font=small_font)

    icon_centers = ((690, 572), (765, 586), (840, 574), (915, 590), (990, 576))
    for icon_path, center in zip(AMENITY_ICON_PATHS, icon_centers):
        draw_icon_bubble(canvas, icon_path, center)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    canvas.convert("RGB").save(OUTPUT_PATH, quality=92, optimize=True)
    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
