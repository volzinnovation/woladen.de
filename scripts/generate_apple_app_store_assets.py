#!/usr/bin/env python3

from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output" / "app-store" / "ios"
METADATA_ROOT = OUTPUT_DIR / "metadata"
METADATA_DIR = METADATA_ROOT / "de-DE"

APP_NAME = "woladen"
SUBTITLE = "Smarte Ladestopps in Europa"
PROMOTIONAL_TEXT = (
    "The human side of charging. Finde bessere Ladestopps nahe Cafés, "
    "Bäckereien, Restaurants, Läden und Spielplätzen - mit Live-Infos, wo verfügbar."
)
DESCRIPTION = """woladen hilft dir, bessere Ladepausen zu finden: Schnelllader für das Auto, nützliche Orte für die Menschen darin.

Die App zeigt Ladestopps in Europa und kombiniert offene Ladeinfrastruktur-Daten, unterstützte Live-Feeds und Umgebungsinformationen aus OpenStreetMap. So siehst du nicht nur, wo du laden kannst, sondern auch, was es direkt in der Nähe gibt.

Mit woladen kannst du:
- Ladestopps in Karte, Liste und Favoriten durchsuchen
- standardmäßig Schnelllader ab 50 kW finden
- nach Verfügbarkeit, Leistung, Steckertyp, Betreiber und Ausstattung filtern
- Cafés, Bäckereien, Restaurants, Läden, Toiletten, Spielplätze und weitere Orte in der Nähe sehen
- Live-Status und Detaildaten nutzen, wo Anbieter sie bereitstellen
- Favoriten lokal auf deinem Gerät speichern
- die App optional mit deinem Standort auf die Umgebung fokussieren
- auf iPhone und iPad mit angepassten Layouts arbeiten

woladen bleibt bewusst schlank: kein Nutzerkonto, keine Werbung, keine In-App-Käufe.

Wenn du deinen Standort freigibst, verwendet woladen ihn nur, um Ladepunkte in der Nähe zu sortieren und die Karte auf deine Umgebung auszurichten. Favoriten bleiben lokal auf deinem Gerät.

The human side of charging. Because charging time is your time."""
KEYWORDS = "E-Auto,Laden,Ladesäule,Schnelllader,EV,Elektroauto,Route,Café,Bäckerei,Restaurant"
RELEASE_NOTES = """Version 1.0.2 aktualisiert woladen auf den neuen europäischen Live-Katalog.

- API-gestützter europäischer Katalog
- iPad-Unterstützung mit angepasstem Layout
- Favoriten in Liste, Karte und Detailansicht klar markiert
- Mehrsprachige native Texte aus dem Web-Katalog
- Live-Status und Stationsdetails mit begrenztem Cache, ohne veraltete Daten als frisch darzustellen"""
SCREENSHOT_NOTES = """01-list.png: Bessere Ladestopps finden
02-detail.png: Details, Live-Infos und Umgebung
03-map.png: Karte mit Schnellladern und Favoriten
04-favorites.png: Lieblingsstopps lokal speichern
05-info.png: Transparente Quellen und Datenschutz"""

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
