package de.woladen.android.model

import de.woladen.android.R
import de.woladen.android.util.AppStrings
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle
import java.util.Locale

private const val MAX_REASONABLE_DISPLAY_POWER_KW = 400.0

data class GeoJsonFeatureCollection(
    val generatedAt: String?,
    val features: List<GeoJsonFeature>
)

data class GeoJsonFeature(
    val id: String,
    val geometry: GeoJsonPointGeometry,
    val properties: ChargerProperties,
    val liveSummary: LiveStationSummary? = null,
    val liveDetail: LiveStationDetail? = null
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
                    parts += AppStrings.count(R.string.i18n_availability_available, occupancyAvailableEvses)
                }
                if (occupancyOccupiedEvses > 0) {
                    parts += AppStrings.count(R.string.i18n_availability_occupiedcount, occupancyOccupiedEvses)
                }
                if (occupancyOutOfOrderEvses > 0) {
                    parts += AppStrings.count(R.string.i18n_availability_outofordercount, occupancyOutOfOrderEvses)
                }
                parts += AppStrings.count(R.string.i18n_availability_unknowncount, occupancyUnknownEvses)
                return parts.joinToString(", ")
            }
            if (occupancyAvailableEvses > 0) {
                return "${occupancyAvailableEvses}/${occupancyTotalEvses} ${AppStrings.get(R.string.i18n_availability_free).lowercase()}"
            }
            if (occupancyOccupiedEvses > 0) {
                return "${occupancyOccupiedEvses}/${occupancyTotalEvses} ${AppStrings.get(R.string.i18n_availability_occupied).lowercase()}"
            }
            if (occupancyUnknownEvses <= 0 && occupancyOutOfOrderEvses >= occupancyTotalEvses) {
                return AppStrings.get(R.string.i18n_availability_out_of_order)
            }
            return AppStrings.get(R.string.i18n_availability_summaryunknown)
        }

    val occupancySourceLabel: String?
        get() {
            if (occupancyTotalEvses <= 0) {
                return null
            }
            if (occupancySourceName.startsWith("Mobilithek")) {
                return liveVia(occupancySourceName)
            }
            if (occupancySourceUid.startsWith("mobilithek_")) {
                if (occupancySourceName.isBlank()) {
                    return liveVia("Mobilithek")
                }
                return liveVia("Mobilithek ($occupancySourceName)")
            }
            if (occupancySourceName.isBlank()) {
                return liveVia("MobiData BW")
            }
            return liveVia("MobiData BW ($occupancySourceName)")
        }

    val hasPrimaryDetailHighlights: Boolean
        get() = priceDisplay.isNotBlank() || openingHoursDisplay.isNotBlank()

    val staticDetailRows: List<DetailRow>
        get() = buildList {
            if (paymentMethodsDisplay.isNotBlank()) add(DetailRow(AppStrings.get(R.string.i18n_staticdetails_payment), paymentMethodsDisplay))
            if (authMethodsDisplay.isNotBlank()) add(DetailRow(AppStrings.get(R.string.i18n_staticdetails_access), authMethodsDisplay))
            if (connectorTypesDisplay.isNotBlank()) add(DetailRow(AppStrings.get(R.string.i18n_staticdetails_connectors), connectorTypesDisplay))
            if (currentTypesDisplay.isNotBlank()) add(DetailRow(AppStrings.get(R.string.i18n_staticdetails_currenttype), currentTypesDisplay))
            if (connectorCount > 0) {
                add(
                    DetailRow(
                        AppStrings.get(R.string.i18n_staticdetails_connectors),
                        AppStrings.count(R.string.i18n_staticdetails_sockets, connectorCount)
                    )
                )
            }
            if (serviceTypesDisplay.isNotBlank()) add(DetailRow(AppStrings.get(R.string.i18n_staticdetails_service), serviceTypesDisplay))
            greenEnergy?.let {
                add(
                    DetailRow(
                        AppStrings.get(R.string.i18n_staticdetails_energy),
                        AppStrings.get(
                            if (it) {
                                R.string.i18n_staticdetails_renewable
                            } else {
                                R.string.i18n_staticdetails_notrenewable
                            }
                        )
                    )
                )
            }
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
                    AppStrings.get(R.string.i18n_station_updated, mapOf("date" to timestamp))
                } else {
                    AppStrings.get(R.string.i18n_station_detailssource, mapOf("source" to sourceName, "date" to timestamp))
                }
            }
            return AppStrings.get(R.string.i18n_station_detailssourceonly, mapOf("source" to sourceName))
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
        val formatter = DateTimeFormatter.ofLocalizedDateTime(FormatStyle.SHORT)
            .withLocale(Locale.getDefault())
            .withZone(ZoneId.systemDefault())
        formatter.format(instant)
    } catch (_: Exception) {
        value
    }
}

private fun liveVia(source: String): String =
    AppStrings.get(R.string.i18n_station_livevia, mapOf("source" to source))
