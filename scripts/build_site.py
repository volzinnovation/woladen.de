#!/usr/bin/env python3
"""Build the deployable frontend shell without local catalog payloads."""

from __future__ import annotations

import html
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
SITE_DIR = ROOT / "site"
SITE_ORIGIN = "https://woladen.de"

PUBLIC_PAGES = (
    "",
    "afir.html",
    "management.html",
    "status.html",
    "privacy.html",
    "imprint.html",
    "station.html",
)


def absolute_url(path: str) -> str:
    normalized = str(path or "").lstrip("/")
    return f"{SITE_ORIGIN}/{normalized}"


def write_sitemap() -> None:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path in PUBLIC_PAGES:
        lines.extend(("  <url>", f"    <loc>{html.escape(absolute_url(path))}</loc>", "  </url>"))
    lines.append("</urlset>")
    (SITE_DIR / "sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_robots_txt() -> None:
    (SITE_DIR / "robots.txt").write_text(
        f"User-agent: *\nAllow: /\nSitemap: {absolute_url('sitemap.xml')}\n",
        encoding="utf-8",
    )


def main() -> None:
    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    shutil.copytree(WEB_DIR, SITE_DIR)
    write_sitemap()
    write_robots_txt()


if __name__ == "__main__":
    main()
