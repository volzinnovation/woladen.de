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
            selectedOperatorNames = setOf("EnBW"),
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
            selectedOperatorNames = setOf("IONITY", "EnBW"),
            minPowerKw = 150.0,
            selectedAmenities = setOf("amenity_restaurant", "amenity_toilets"),
            amenityNameQuery = "McDonald"
        )

        assertEquals(6, filters.activeCount)
    }

    @Test
    fun filterState_matchesAnySelectedOperator() {
        val ionity = sampleProperties(operatorName = "IONITY")
        val enbw = sampleProperties(operatorName = "EnBW")
        val other = sampleProperties(operatorName = "Other")
        val filter = FilterState(selectedOperatorNames = setOf("IONITY", "EnBW"))

        assertTrue(ionity.matches(filter))
        assertTrue(enbw.matches(filter))
        assertFalse(other.matches(filter))
    }

    @Test
    fun canonicalOperatorGroupsMatchAcrossCountriesAndTechnicalNames() {
        val stations = listOf(
            Triple("ionity", "FR*ION", "FR"),
            Triple("ionity", "IONITY GmbH", "DE"),
            Triple("enbw", "DE*EBW", "DE"),
            Triple("enbw", "EnBW mobility+", "AT"),
            Triple("tesla", "NL*TSL", "NL"),
            Triple("tesla", "Tesla Supercharger", "NO")
        )
        for ((group, name, country) in stations) {
            val properties = sampleProperties(operatorName = name).copy(
                countryCode = country,
                operatorGroupIds = setOf(group)
            )
            for (selected in listOf("ionity", "enbw", "tesla")) {
                assertEquals(
                    "$selected / $name / $country",
                    selected == group,
                    properties.matches(FilterState(selectedOperatorNames = setOf(selected)))
                )
            }
        }
    }

    @Test
    fun operatorGroupMetadataTakesPrecedenceOverDisplayName() {
        val properties = sampleProperties(operatorName = "ionity").copy(operatorGroupIds = setOf("tesla"))

        assertFalse(properties.matches(FilterState(selectedOperatorNames = setOf("ionity"))))
        assertTrue(properties.matches(FilterState(selectedOperatorNames = setOf("tesla"))))
    }

    @Test
    fun conflictingSingularGroupCannotBroadenAuthoritativePluralGroups() {
        val properties = sampleProperties(operatorName = "ionity").copy(
            operatorGroupIds = resolvedOperatorGroupIds(listOf(" tesla ", "tesla", " "), "ionity")
        )

        assertEquals(setOf("tesla"), properties.operatorGroupIds)
        assertFalse(properties.matches(FilterState(selectedOperatorNames = setOf("ionity"))))
        assertTrue(properties.matches(FilterState(selectedOperatorNames = setOf("tesla"))))
        assertEquals(setOf("ionity"), resolvedOperatorGroupIds(listOf(" "), " ionity "))
        assertTrue(resolvedOperatorGroupIds(emptyList(), " ").isEmpty())
    }

    @Test
    fun savedOperatorNamesAndAliasesMigrateToCanonicalIdsAndDiscardUnsupportedChoices() {
        val operators = listOf(
            OperatorEntry(id = "ionity", name = "IONITY", stations = 0, aliases = listOf("IONITY GmbH")),
            OperatorEntry(id = "enbw", name = "EnBW", stations = 0, aliases = listOf("EnBW mobility+")),
            OperatorEntry(id = "tesla", name = "Tesla", stations = 0, aliases = listOf("Tesla Supercharger"))
        )
        val filter = FilterState(selectedOperatorNames = setOf(" IONITY GMBH ", "enbw", "Tesla Supercharger", "unsupported"))

        assertEquals(setOf("ionity", "enbw", "tesla"), filter.canonicalized(using = operators).normalizedOperatorNames)
        assertTrue(FilterState(selectedOperatorNames = setOf("unsupported"))
            .canonicalized(using = operators).normalizedOperatorNames.isEmpty())
    }

    @Test
    fun canonicalOperatorIdWinsOverAnotherBrandsAlias() {
        val filter = FilterState(selectedOperatorNames = setOf(" IONITY "))
        val operators = listOf(
            OperatorEntry(id = "other", name = "Other", stations = 0, aliases = listOf("IONITY")),
            OperatorEntry(id = "ionity", name = "IONITY", stations = 0)
        )

        assertEquals(setOf("ionity"), filter.canonicalized(using = operators).normalizedOperatorNames)
    }

    @Test
    fun missingOperatorCatalogPreservesSavedChoicesUntilAuthoritativeListArrives() {
        val filter = FilterState(selectedOperatorNames = setOf("IONITY GmbH", "unknown"))

        assertEquals(filter, filter.canonicalized(using = emptyList()))
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
