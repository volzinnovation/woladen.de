package de.woladen.android.model

import java.time.Instant
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class LiveFeatureFormattingTest {
    @Test
    fun elapsedLiveTimeFormatsRecentMinutes() {
        val now = Instant.parse("2026-04-17T15:03:18Z")
        assertEquals("13 min ago", formatElapsedLiveTime("2026-04-17T14:50:00Z", now))
    }

    @Test
    fun elapsedLiveTimeFormatsFreshUpdates() {
        val now = Instant.parse("2026-04-17T15:03:18Z")
        assertEquals("now", formatElapsedLiveTime("2026-04-17T15:03:00Z", now))
    }

    @Test
    fun liveSummaryOverridesStaticCatalogOccupancyAndPrice() {
        val feature = sampleFeature(
            properties = sampleProperties(
                occupancyTotalEvses = 2,
                occupancyAvailableEvses = 2,
                priceDisplay = "ab 0,59 €/kWh",
                detailSourceUid = "mobilithek_enbwmobility_static",
                detailSourceName = "EnBWmobility+"
            ),
            liveSummary = LiveStationSummary(
                stationId = "station-1",
                availabilityStatus = AvailabilityStatus.OCCUPIED,
                availableEvses = 1,
                occupiedEvses = 2,
                outOfOrderEvses = 0,
                unknownEvses = 0,
                totalEvses = 3,
                priceDisplay = "ab 0,69 €/kWh",
                priceCurrency = "EUR",
                priceEnergyEurKwhMin = "0.69",
                priceEnergyEurKwhMax = "0.69",
                sourceObservedAt = "2026-04-16T14:10:02Z",
                fetchedAt = "2026-04-16T14:12:16Z",
                ingestedAt = "2026-04-16T14:12:16Z"
            )
        )

        assertEquals("ab 0,69 €/kWh", feature.displayPrice)
        assertEquals("1 free, 2 occupied", feature.occupancySummaryLabel)
        assertEquals(AvailabilityStatus.OCCUPIED, feature.availabilityStatus)
        assertEquals(1, feature.liveEvseRows.size)
        assertTrue(feature.occupancySourceLabel?.contains("EnBWmobility+") == true)
    }

    @Test
    fun staticCatalogValuesRemainWhenNoLiveOverlayExists() {
        val feature = sampleFeature(
            properties = sampleProperties(
                occupancyTotalEvses = 4,
                occupancyAvailableEvses = 3,
                occupancyOccupiedEvses = 1,
                priceDisplay = "ab 0,59 €/kWh"
            )
        )

        assertEquals("ab 0,59 €/kWh", feature.displayPrice)
        assertEquals("3 free, 1 occupied", feature.occupancySummaryLabel)
        assertEquals(AvailabilityStatus.FREE, feature.availabilityStatus)
        assertTrue(feature.liveEvseRows.isEmpty())
    }

    private fun sampleFeature(
        properties: ChargerProperties,
        liveSummary: LiveStationSummary? = null
    ): GeoJsonFeature {
        return GeoJsonFeature(
            id = properties.stationId,
            geometry = GeoJsonPointGeometry(type = "Point", coordinates = listOf(13.4, 52.5)),
            properties = properties,
            liveSummary = liveSummary
        )
    }

    private fun sampleProperties(
        occupancyTotalEvses: Int = 0,
        occupancyAvailableEvses: Int = 0,
        occupancyOccupiedEvses: Int = 0,
        priceDisplay: String = "",
        detailSourceUid: String = "",
        detailSourceName: String = ""
    ): ChargerProperties {
        return ChargerProperties(
            stationId = "station-1",
            operatorName = "IONITY",
            status = "In Betrieb",
            maxPowerKw = 150.0,
            chargingPointsCount = 4,
            maxIndividualPowerKw = 150.0,
            postcode = "10115",
            city = "Berlin",
            address = "Teststraße 1",
            occupancySourceUid = "",
            occupancySourceName = "",
            occupancyStatus = "",
            occupancyLastUpdated = "",
            occupancyTotalEvses = occupancyTotalEvses,
            occupancyAvailableEvses = occupancyAvailableEvses,
            occupancyOccupiedEvses = occupancyOccupiedEvses,
            occupancyChargingEvses = 0,
            occupancyOutOfOrderEvses = 0,
            occupancyUnknownEvses = 0,
            detailSourceUid = detailSourceUid,
            detailSourceName = detailSourceName,
            detailLastUpdated = "",
            datexSiteId = "",
            datexStationIds = "",
            datexChargePointIds = "",
            priceDisplay = priceDisplay,
            priceEnergyEurKwhMin = null,
            priceEnergyEurKwhMax = null,
            priceCurrency = "EUR",
            priceQuality = "",
            openingHoursDisplay = "24/7",
            openingHoursIs24_7 = true,
            helpdeskPhone = "",
            paymentMethodsDisplay = "",
            authMethodsDisplay = "",
            connectorTypesDisplay = "",
            currentTypesDisplay = "",
            connectorCount = 0,
            greenEnergy = null,
            serviceTypesDisplay = "",
            detailsJson = "",
            amenitiesTotal = 0,
            amenitiesSource = "osm-pbf",
            amenityExamples = emptyList(),
            amenityCounts = emptyMap()
        )
    }
}
