#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "app-store" / "ios"
METADATA_ROOT = OUTPUT_DIR / "metadata"
METADATA_DIR = METADATA_ROOT / "de-DE"

APP_NAME = "woladen"
SUBTITLE = "Bessere Ladestopps in Europa"
PROMOTIONAL_TEXT = (
    "Finde verfügbare, verlässliche und angenehme Ladestopps nahe Cafés, "
    "Bäckereien, Restaurants, Läden und Spielplätzen - auch entlang deiner Route."
)
DESCRIPTION = """woladen hilft dir, bessere Ladepausen zu finden: Ladestopps, die frei, verlässlich und angenehm sind.

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
KEYWORDS = "E-Auto,Laden,Ladesäule,Schnelllader,EV,Elektroauto,Route,Café,Bäckerei,Restaurant"
RELEASE_NOTES = """Version 1.3.1 aktualisiert woladen auf den europäischen Live-Katalog.

- API-gestützter europäischer Katalog
- Karte, Liste, Route, Filter und Favoriten
- Mehrsprachige native Texte aus dem Web-Katalog
- Live-Status und Stationsdetails, wo Anbieter sie bereitstellen
- Angepasste Layouts für iPhone und iPad"""
SCREENSHOT_NOTES = """01-list.png: Bessere Ladestopps finden
02-detail.png: Details, Live-Infos und Umgebung
03-map.png: Karte mit Schnellladern und Favoriten
04-favorites.png: Lieblingsstopps lokal speichern
05-route.png: Ladestationen entlang der Route
06-info.png: Transparente Quellen und Datenschutz"""

LIMITS = {
    "name": 30,
    "subtitle": 30,
    "promotional_text": 170,
    "description": 4000,
    "keywords": 100,
    "release_notes": 4000,
}


def checked(label: str, value: str) -> str:
    limit = LIMITS[label]
    length = len(value)
    if length > limit:
        raise ValueError(f"{label} has {length} characters, App Store limit is {limit}")
    return value.rstrip() + "\n"


def write_metadata() -> None:
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_ROOT.mkdir(parents=True, exist_ok=True)

    files = {
        METADATA_DIR / "name.txt": checked("name", APP_NAME),
        METADATA_DIR / "subtitle.txt": checked("subtitle", SUBTITLE),
        METADATA_DIR / "promotional-text.txt": checked("promotional_text", PROMOTIONAL_TEXT),
        METADATA_DIR / "description.txt": checked("description", DESCRIPTION),
        METADATA_DIR / "keywords.txt": checked("keywords", KEYWORDS),
        METADATA_DIR / "release-notes.txt": checked("release_notes", RELEASE_NOTES),
        METADATA_DIR / "screenshot-notes.txt": SCREENSHOT_NOTES.rstrip() + "\n",
        METADATA_ROOT / "support-url.txt": "https://woladen.de/\n",
        METADATA_ROOT / "marketing-url.txt": "https://woladen.de/\n",
        METADATA_ROOT / "privacy-policy-url.txt": "https://woladen.de/privacy.html\n",
    }

    for path, content in files.items():
        path.write_text(content, encoding="utf-8")


def main() -> None:
    write_metadata()
    print(f"Saved Apple App Store metadata under {METADATA_ROOT}")


if __name__ == "__main__":
    main()
