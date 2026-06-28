package de.woladen.android.service

import de.woladen.android.model.BundleBuildRecords
import de.woladen.android.model.BundleBuildRun
import de.woladen.android.model.BundleBuildSummary
import de.woladen.android.model.CatalogInfoSummary
import de.woladen.android.model.FilterState
import de.woladen.android.model.OpenStaticBundle
import de.woladen.android.model.OpenStaticCountry
import de.woladen.android.model.OpenStaticSource
import de.woladen.android.model.OpenStaticSummary
import java.util.Locale
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LiveApiClientRequestTest {
    @Test
    fun defaultBaseUrlUsesEuropeanLiveBackend() {
        assertEquals("https://live-eu.woladen.de", LiveApiClient.DEFAULT_BASE_URL)
    }

    @Test
    fun lookupBatchesNormalizeDeduplicateAndLimitToTwentyIds() {
        val stationIds = (1..25).map { " station-$it " } + "station-1" + " "

        val batches = lookupStationIdBatches(stationIds)

        assertEquals(2, batches.size)
        assertEquals((1..20).map { "station-$it" }, batches[0])
        assertEquals((21..25).map { "station-$it" }, batches[1])
        assertTrue(batches.all { it.size <= LiveApiClient.MAX_LOOKUP_STATION_IDS })
    }

    @Test
    fun acceptLanguageUsesSupportedLanguageWithEnglishFallback() {
        val header = NativeLanguage.acceptLanguageHeader(listOf(Locale.forLanguageTag("fr-FR")))

        assertEquals("fr, en;q=0.8", header)
    }

    @Test
    fun acceptLanguageMapsNorwegianAliasToBokmal() {
        val header = NativeLanguage.acceptLanguageHeader(listOf(Locale.forLanguageTag("no-NO")))

        assertEquals("nb, en;q=0.8", header)
    }

    @Test
    fun acceptLanguageFallsBackToEnglishForUnsupportedLanguages() {
        val header = NativeLanguage.acceptLanguageHeader(listOf(Locale.forLanguageTag("ja-JP")))

        assertEquals("en", header)
    }

    @Test
    fun catalogSearchPathUsesTravelCatalogAndClampsLimit() {
        val path = catalogSearchPath(
            latitude = 52.52,
            longitude = 13.405,
            radiusMeters = 20_000,
            limit = 500,
            filterState = FilterState(selectedOperatorNames = setOf("AC/DC GmbH"), minPowerKw = 150.0)
        )

        assertEquals(
            "/v1/catalog/search?lat=52.520000&lon=13.405000&radius_m=20000&limit=100&mode=travel&min_power_kw=150.0&operator=AC%2FDC+GmbH",
            path
        )
    }

    @Test
    fun catalogSearchPathOmitsOperatorWhenMultipleOperatorsAreSelected() {
        val path = catalogSearchPath(
            latitude = 52.52,
            longitude = 13.405,
            radiusMeters = 20_000,
            limit = 100,
            filterState = FilterState(selectedOperatorNames = setOf("IONITY", "EnBW"), minPowerKw = 50.0)
        )

        assertEquals(
            "/v1/catalog/search?lat=52.520000&lon=13.405000&radius_m=20000&limit=100&mode=travel&min_power_kw=50.0",
            path
        )
    }

    @Test
    fun catalogStationDetailPathEncodesStationId() {
        assertEquals(
            "/v1/catalog/stations/DE%3A231a24210456534c",
            catalogStationDetailPath("DE:231a24210456534c")
        )
    }

    @Test
    fun catalogInfoSummaryUsesWebCountryAndSourceContract() {
        val summary = CatalogInfoSummary(
            openStaticSummary = OpenStaticSummary(
                bundle = OpenStaticBundle(
                    stationCount = 287338,
                    chargerCount = 820182,
                    countryCount = 22,
                    schemaVersion = 4
                ),
                countries = listOf(
                    OpenStaticCountry(
                        code = "DE",
                        name = "Deutschland",
                        stationCount = 73204,
                        chargerCount = 111713,
                        fastStationCount = 0
                    ),
                    OpenStaticCountry(
                        code = "AT",
                        name = "Oesterreich",
                        stationCount = 14661,
                        chargerCount = 38771,
                        fastStationCount = 0
                    )
                ),
                generatedAt = "2026-06-21T09:04:42+00:00",
                schemaVersion = 4,
                rawSources = listOf(
                    OpenStaticSource(
                        countryCode = "AT",
                        displayName = "AT E-Control DATEX energy infrastructure table publication",
                        sourceUid = "at_econtrol",
                        sourceUrl = "https://api.e-control.at/charge/1.0/datex2/v3.5/energy-infrastructure-table-publication",
                        license = "",
                        licenseUrl = ""
                    ),
                    OpenStaticSource(
                        countryCode = "DE",
                        displayName = "DE Bundesnetzagentur",
                        sourceUid = "de_bnetza",
                        sourceUrl = "https://example.test/de",
                        license = "",
                        licenseUrl = ""
                    )
                )
            ),
            buildSummary = BundleBuildSummary(
                run = BundleBuildRun(startedAt = "", finishedAt = "2026-06-21T03:19:14+00:00"),
                records = BundleBuildRecords(rawRows = 111713, fullRegistryActiveStationsTotal = 73204)
            )
        )

        assertEquals(287338, summary.stationCount)
        assertEquals(820182, summary.chargerCount)
        assertEquals("2026-06-21T09:04:42+00:00", summary.generatedAt)
        assertEquals("Mobilithek", summary.countrySourceLinks("DE").first().label)
        assertEquals(
            "https://mobilithek.info/offers/842113170303512576",
            summary.countrySourceLinks("DE").first().urlString
        )
        assertEquals(
            "E-Control DATEX energy infrastructure table publication",
            summary.countrySourceLinks("AT").first().label
        )
        assertTrue(summary.dataSourceLinks().any { it.label.startsWith("AT: E-Control") })
    }
}
