package de.woladen.android.model

private const val MAX_REASONABLE_DISPLAY_POWER_KW = 400.0

data class GeoJsonFeatureCollection(
    val generatedAt: String?,
    val features: List<GeoJsonFeature>
)

data class GeoJsonFeature(
    val id: String,
    val geometry: GeoJsonPointGeometry,
    val properties: ChargerProperties
) {
    val latitude: Double get() = geometry.latitude
    val longitude: Double get() = geometry.longitude
}

data class GeoJsonPointGeometry(
    val type: String,
    val coordinates: List<Double>
) {
    val longitude: Double get() = if (coordinates.size == 2) coordinates[0] else 0.0
    val latitude: Double get() = if (coordinates.size == 2) coordinates[1] else 0.0
}

data class ChargerProperties(
    val stationId: String,
    val operatorName: String,
    val status: String,
    val maxPowerKw: Double,
    val chargingPointsCount: Int,
    val maxIndividualPowerKw: Double,
    val postcode: String,
    val city: String,
    val address: String,
    val occupancySourceUid: String,
    val occupancySourceName: String,
    val occupancyStatus: String,
    val occupancyLastUpdated: String,
    val occupancyTotalEvses: Int,
    val occupancyAvailableEvses: Int,
    val occupancyOccupiedEvses: Int,
    val occupancyChargingEvses: Int,
    val occupancyOutOfOrderEvses: Int,
    val occupancyUnknownEvses: Int,
    val detailSourceUid: String,
    val detailSourceName: String,
    val detailLastUpdated: String,
    val datexSiteId: String,
    val datexStationIds: String,
    val datexChargePointIds: String,
    val priceDisplay: String,
    val priceEnergyEurKwhMin: Double?,
    val priceEnergyEurKwhMax: Double?,
    val priceCurrency: String,
    val priceQuality: String,
    val openingHoursDisplay: String,
    val openingHoursIs24_7: Boolean,
    val helpdeskPhone: String,
    val paymentMethodsDisplay: String,
    val authMethodsDisplay: String,
    val connectorTypesDisplay: String,
    val currentTypesDisplay: String,
    val connectorCount: Int,
    val greenEnergy: Boolean?,
    val serviceTypesDisplay: String,
    val detailsJson: String,
    val amenitiesTotal: Int,
    val amenitiesSource: String,
    val amenityExamples: List<AmenityExample>,
    val amenityCounts: Map<String, Int>
) {
    val displayedMaxPowerKw: Double
        get() {
            val maxIndividual = sanitizeDisplayedPowerKw(maxIndividualPowerKw)
            if (maxIndividual > 0.0) {
                return maxIndividual
            }
            return sanitizeDisplayedPowerKw(maxPowerKw)
        }

    fun topAmenities(limit: Int = 3): List<AmenityCount> {
        return amenityCounts
            .filterValues { it > 0 }
            .map { AmenityCount(it.key, it.value) }
            .sortedWith(compareByDescending<AmenityCount> { it.count }.thenBy { it.key })
            .take(limit)
    }

    val occupancySummaryLabel: String?
        get() {
            if (occupancyTotalEvses <= 0) {
                return null
            }
            val knownEvses = maxOf(0, occupancyTotalEvses - occupancyUnknownEvses)
            if (knownEvses > 0 && occupancyUnknownEvses > 0) {
                val parts = mutableListOf<String>()
                if (occupancyAvailableEvses > 0) {
                    parts += "${occupancyAvailableEvses} frei"
                }
                if (occupancyOccupiedEvses > 0) {
                    parts += "${occupancyOccupiedEvses} belegt"
                }
                if (occupancyOutOfOrderEvses > 0) {
                    parts += "${occupancyOutOfOrderEvses} defekt"
                }
                parts += "${occupancyUnknownEvses} unbekannt"
                return parts.joinToString(", ")
            }
            if (occupancyAvailableEvses > 0) {
                return "${occupancyAvailableEvses}/${occupancyTotalEvses} frei"
            }
            if (occupancyOccupiedEvses > 0) {
                return "${occupancyOccupiedEvses}/${occupancyTotalEvses} belegt"
            }
            if (occupancyUnknownEvses <= 0 && occupancyOutOfOrderEvses >= occupancyTotalEvses) {
                return "Außer Betrieb"
            }
            return "Belegung unbekannt"
        }

    val occupancySourceLabel: String?
        get() {
            if (occupancyTotalEvses <= 0) {
                return null
            }
            if (occupancySourceName.startsWith("Mobilithek")) {
                return "Live via $occupancySourceName"
            }
            if (occupancySourceUid.startsWith("mobilithek_")) {
                if (occupancySourceName.isBlank()) {
                    return "Live via Mobilithek"
                }
                return "Live via Mobilithek ($occupancySourceName)"
            }
            if (occupancySourceName.isBlank()) {
                return "Live via MobiData BW"
            }
            return "Live via MobiData BW ($occupancySourceName)"
        }

    val hasPrimaryDetailHighlights: Boolean
        get() = priceDisplay.isNotBlank() || openingHoursDisplay.isNotBlank()

    val staticDetailRows: List<DetailRow>
        get() = buildList {
            if (paymentMethodsDisplay.isNotBlank()) add(DetailRow("Bezahlen", paymentMethodsDisplay))
            if (authMethodsDisplay.isNotBlank()) add(DetailRow("Zugang", authMethodsDisplay))
            if (connectorTypesDisplay.isNotBlank()) add(DetailRow("Stecker", connectorTypesDisplay))
            if (currentTypesDisplay.isNotBlank()) add(DetailRow("Stromart", currentTypesDisplay))
            if (connectorCount > 0) add(DetailRow("Anschlüsse", "$connectorCount Steckplätze"))
            if (serviceTypesDisplay.isNotBlank()) add(DetailRow("Service", serviceTypesDisplay))
            greenEnergy?.let { add(DetailRow("Strom", if (it) "100 % erneuerbar" else "Nicht als erneuerbar markiert")) }
        }

    val detailSourceLabel: String?
        get() {
            val sourceName = detailSourceName.trim()
            val timestamp = formatDetailTimestamp(detailLastUpdated)
            if (sourceName.isEmpty() && timestamp == null) {
                return null
            }
            if (timestamp != null) {
                return if (sourceName.isEmpty()) {
                    "Stand $timestamp"
                } else {
                    "Details via $sourceName • Stand $timestamp"
                }
            }
            return "Details via $sourceName"
        }
}

data class AmenityExample(
    val category: String,
    val name: String?,
    val openingHours: String?,
    val distanceM: Double?,
    val lat: Double?,
    val lon: Double?
)

data class AmenityCount(
    val key: String,
    val count: Int
)

data class DetailRow(
    val label: String,
    val value: String
)

private fun sanitizeDisplayedPowerKw(value: Double): Double {
    if (!value.isFinite() || value <= 0.0) {
        return 0.0
    }
    return minOf(value, MAX_REASONABLE_DISPLAY_POWER_KW)
}

private fun formatDetailTimestamp(value: String): String? {
    if (value.isBlank()) return null
    return try {
        val instant = java.time.Instant.parse(value)
        val formatter = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy, HH:mm")
            .withZone(java.time.ZoneId.systemDefault())
        formatter.format(instant)
    } catch (_: Exception) {
        value
    }
}
