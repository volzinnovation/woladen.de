#!/usr/bin/env python3
"""Build static site folder from web assets + generated data."""

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
DATA_DIR = ROOT / "data"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"

REQUIRED_DATA = [
    "chargers_fast.geojson",
    "summary.json",
]



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


if __name__ == "__main__":
    main()
