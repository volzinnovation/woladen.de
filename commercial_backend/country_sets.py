from __future__ import annotations

EU27_COUNTRIES = (
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "ES",
    "FI",
    "FR",
    "GR",
    "HR",
    "HU",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
)

EXPECTED_COUNTRIES = (*EU27_COUNTRIES, "CH", "NO")
BASELINE_COUNTRIES = ("DE",)
ONBOARDED_COUNTRIES = (
    "AT",
    "BE",
    "CH",
    "CY",
    "CZ",
    "ES",
    "FI",
    "FR",
    "GR",
    "HU",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "NO",
    "PL",
    "PT",
    "SE",
    "SI",
)
PENDING_COUNTRIES: tuple[str, ...] = ()
REVIEWED_COUNTRIES = ("DK",)
OPEN_DISCOVERY_COUNTRIES = tuple(
    country_code
    for country_code in EXPECTED_COUNTRIES
    if country_code
    not in {*BASELINE_COUNTRIES, *ONBOARDED_COUNTRIES, *PENDING_COUNTRIES, *REVIEWED_COUNTRIES}
)

DEFAULT_COUNTRIES = ONBOARDED_COUNTRIES
EU27_COMMERCIAL_COUNTRIES = tuple(country_code for country_code in EU27_COUNTRIES if country_code != "DE")
EXPANSION_COUNTRIES: tuple[str, ...] = ()

BASELINE_COUNTRIES_CSV = ",".join(BASELINE_COUNTRIES)
ONBOARDED_COUNTRIES_CSV = ",".join(ONBOARDED_COUNTRIES)
