package de.woladen.android.model

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class FilterMatchingTest {

    @Test
    fun amenityNameQuery_matchesIgnoringCasePunctuationAndDiacritics() {
        val properties = sampleProperties(
            amenityExamples = listOf(
                AmenityExample(
                    category = "fast_food",
                    name = "McDonald's Café",
                    openingHours = null,
                    distanceM = 42.0,
                    lat = null,
                    lon = null
                )
            )
        )

        assertTrue(properties.matchesAmenityNameQuery("mcdonalds"))
        assertTrue(properties.matchesAmenityNameQuery("cafe"))
        assertFalse(properties.matchesAmenityNameQuery("burger king"))
    }

    @Test
    fun filterState_matchesAllConfiguredFiltersIncludingAmenityName() {
        val properties = sampleProperties(
            operatorName = "EnBW",
            maxPowerKw = 300.0,
            amenityCounts = mapOf("amenity_fast_food" to 2),
            amenityExamples = listOf(
                AmenityExample(
                    category = "fast_food",
                    name = "McDonald's",
                    openingHours = null,
                    distanceM = 25.0,
                    lat = null,
                    lon = null
                )
            )
        )

        val matching = FilterState(
            operatorName = "EnBW",
            minPowerKw = 150.0,
            selectedAmenities = setOf("amenity_fast_food"),
            amenityNameQuery = "McDonald"
        )
        val nonMatching = matching.copy(amenityNameQuery = "Subway")

        assertTrue(properties.matches(matching))
        assertFalse(properties.matches(nonMatching))
    }

    @Test
    fun activeCount_includesAmenityNameQuery() {
        val filters = FilterState(
            operatorName = "IONITY",
            minPowerKw = 150.0,
            selectedAmenities = setOf("amenity_restaurant", "amenity_toilets"),
            amenityNameQuery = "McDonald"
        )

        assertEquals(6, filters.activeCount)
    }

    @Test
    fun availableOnlyRequiresKnownFreeChargingPoint() {
        val available = sampleProperties(occupancyTotalEvses = 4, occupancyAvailableEvses = 1)
        val occupied = sampleProperties(
            occupancyTotalEvses = 4,
            occupancyAvailableEvses = 0,
            occupancyOccupiedEvses = 4
        )

        assertTrue(available.matches(FilterState()))
        assertFalse(occupied.matches(FilterState()))
        assertTrue(occupied.matches(FilterState(availableOnly = false)))
    }

    private fun sampleProperties(
        operatorName: String = "IONITY",
        maxPowerKw: Double = 150.0,
        amenityExamples: List<AmenityExample> = emptyList(),
        amenityCounts: Map<String, Int> = emptyMap(),
        occupancyTotalEvses: Int = 4,
        occupancyAvailableEvses: Int = 1,
        occupancyOccupiedEvses: Int = 0
    ): ChargerProperties {
        return ChargerProperties(
            stationId = "station-1",
            operatorName = operatorName,
            status = "In Betrieb",
            maxPowerKw = maxPowerKw,
            chargingPointsCount = 4,
            maxIndividualPowerKw = maxPowerKw,
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
            detailSourceUid = "",
            detailSourceName = "",
            detailLastUpdated = "",
            datexSiteId = "",
            datexStationIds = "",
            datexChargePointIds = "",
            priceDisplay = "",
            priceEnergyEurKwhMin = null,
            priceEnergyEurKwhMax = null,
            priceCurrency = "",
            priceQuality = "",
            openingHoursDisplay = "",
            openingHoursIs24_7 = false,
            helpdeskPhone = "",
            paymentMethodsDisplay = "",
            authMethodsDisplay = "",
            connectorTypesDisplay = "",
            currentTypesDisplay = "",
            connectorCount = 0,
            greenEnergy = null,
            serviceTypesDisplay = "",
            detailsJson = "",
            amenitiesTotal = amenityCounts.values.sum(),
            amenitiesSource = "osm-pbf",
            amenityExamples = amenityExamples,
            amenityCounts = amenityCounts
        )
    }
}
