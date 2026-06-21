package de.woladen.android.service

import de.woladen.android.model.FilterState
import java.util.Locale
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LiveApiClientRequestTest {
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
            filterState = FilterState(operatorName = "AC/DC GmbH", minPowerKw = 150.0)
        )

        assertEquals(
            "/v1/catalog/search?lat=52.520000&lon=13.405000&radius_m=20000&limit=100&mode=travel&min_power_kw=150.0&operator=AC%2FDC+GmbH",
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
}
