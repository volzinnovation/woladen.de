package de.woladen.android.repository

import android.util.JsonReader
import android.util.JsonToken
import de.woladen.android.model.AmenityExample
import de.woladen.android.model.ChargerProperties
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.GeoJsonFeatureCollection
import de.woladen.android.model.GeoJsonPointGeometry
import de.woladen.android.model.OperatorCatalog
import de.woladen.android.model.OperatorEntry
import java.io.Reader

class ChargerRepository(private val dataBundleManager: DataBundleManager) {
    data class LoadResult(
        val features: List<GeoJsonFeature>,
        val operators: List<OperatorEntry>
    )

    suspend fun loadData(): LoadResult {
        val featureCollection = dataBundleManager.useBundleReader(
            "chargers_fast.geojson",
            ::parseFeatureCollection
        )
        val operatorCatalog = dataBundleManager.useBundleReader(
            "operators.json",
            ::parseOperatorCatalog
        )

        val sortedOperators = operatorCatalog.operators.sortedWith(
            compareByDescending<OperatorEntry> { it.stations }.thenBy { it.name }
        )

        return LoadResult(
            features = featureCollection.features,
            operators = sortedOperators
        )
    }

    private fun parseFeatureCollection(reader: Reader): GeoJsonFeatureCollection {
        JsonReader(reader).use { jsonReader ->
            var generatedAt: String? = null
            val features = mutableListOf<GeoJsonFeature>()

            jsonReader.beginObject()
            while (jsonReader.hasNext()) {
                when (jsonReader.nextName()) {
                    "generated_at" -> generatedAt = nextStringOrNull(jsonReader)
                    "features" -> parseFeaturesArray(jsonReader, features)
                    else -> jsonReader.skipValue()
                }
            }
            jsonReader.endObject()

            return GeoJsonFeatureCollection(
                generatedAt = generatedAt,
                features = features
            )
        }
    }

    private fun parseFeaturesArray(reader: JsonReader, target: MutableList<GeoJsonFeature>) {
        reader.beginArray()
        while (reader.hasNext()) {
            parseFeature(reader)?.let(target::add)
        }
        reader.endArray()
    }

    private fun parseFeature(reader: JsonReader): GeoJsonFeature? {
        var geometry: GeoJsonPointGeometry? = null
        var properties: ChargerProperties? = null

        reader.beginObject()
        while (reader.hasNext()) {
            when (reader.nextName()) {
                "geometry" -> geometry = parseGeometry(reader)
                "properties" -> properties = parseProperties(reader)
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        val finalGeometry = geometry ?: return null
        val finalProperties = properties ?: return null
        return GeoJsonFeature(
            id = finalProperties.stationId,
            geometry = finalGeometry,
            properties = finalProperties
        )
    }

    private fun parseGeometry(reader: JsonReader): GeoJsonPointGeometry {
        var type = "Point"
        var lon = 0.0
        var lat = 0.0

        reader.beginObject()
        while (reader.hasNext()) {
            when (reader.nextName()) {
                "type" -> type = nextStringOrNull(reader) ?: "Point"
                "coordinates" -> {
                    reader.beginArray()
                    if (reader.hasNext()) lon = nextLossyDouble(reader, 0.0)
                    if (reader.hasNext()) lat = nextLossyDouble(reader, 0.0)
                    while (reader.hasNext()) {
                        reader.skipValue()
                    }
                    reader.endArray()
                }
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        return GeoJsonPointGeometry(
            type = type,
            coordinates = listOf(lon, lat)
        )
    }

    private fun parseProperties(reader: JsonReader): ChargerProperties {
        var stationId = ""
        var operatorName = ""
        var status = ""
        var maxPowerKw = 0.0
        var chargingPointsCount = 1
        var maxIndividualPowerKw: Double? = null
        var postcode = ""
        var city = ""
        var address = ""
        var occupancySourceUid = ""
        var occupancySourceName = ""
        var occupancyStatus = ""
        var occupancyLastUpdated = ""
        var occupancyTotalEvses = 0
        var occupancyAvailableEvses = 0
        var occupancyOccupiedEvses = 0
        var occupancyChargingEvses = 0
        var occupancyOutOfOrderEvses = 0
        var occupancyUnknownEvses = 0
        var detailSourceUid = ""
        var detailSourceName = ""
        var detailLastUpdated = ""
        var datexSiteId = ""
        var datexStationIds = ""
        var datexChargePointIds = ""
        var priceDisplay = ""
        var priceEnergyEurKwhMin: Double? = null
        var priceEnergyEurKwhMax: Double? = null
        var priceCurrency = ""
        var priceQuality = ""
        var openingHoursDisplay = ""
        var openingHoursIs24_7 = false
        var helpdeskPhone = ""
        var paymentMethodsDisplay = ""
        var authMethodsDisplay = ""
        var connectorTypesDisplay = ""
        var currentTypesDisplay = ""
        var connectorCount = 0
        var greenEnergy: Boolean? = null
        var serviceTypesDisplay = ""
        var detailsJson = ""
        var amenitiesTotal = 0
        var amenitiesSource = ""
        val amenityExamples = mutableListOf<AmenityExample>()
        val amenityCounts = linkedMapOf<String, Int>()

        reader.beginObject()
        while (reader.hasNext()) {
            val name = reader.nextName()
            when {
                name == "station_id" -> stationId = nextStringOrNull(reader).orEmpty()
                name == "operator" -> operatorName = nextStringOrNull(reader).orEmpty()
                name == "status" -> status = nextStringOrNull(reader).orEmpty()
                name == "max_power_kw" -> maxPowerKw = nextLossyDouble(reader, 0.0)
                name == "charging_points_count" -> chargingPointsCount = nextLossyInt(reader, 1)
                name == "max_individual_power_kw" -> {
                    maxIndividualPowerKw = nextLossyDoubleOrNull(reader)
                }
                name == "postcode" -> postcode = nextStringOrNull(reader).orEmpty()
                name == "city" -> city = nextStringOrNull(reader).orEmpty()
                name == "address" -> address = nextStringOrNull(reader).orEmpty()
                name == "occupancy_source_uid" -> occupancySourceUid = nextStringOrNull(reader).orEmpty()
                name == "occupancy_source_name" -> occupancySourceName = nextStringOrNull(reader).orEmpty()
                name == "occupancy_status" -> occupancyStatus = nextStringOrNull(reader).orEmpty()
                name == "occupancy_last_updated" -> occupancyLastUpdated = nextStringOrNull(reader).orEmpty()
                name == "occupancy_total_evses" -> occupancyTotalEvses = nextLossyInt(reader, 0)
                name == "occupancy_available_evses" -> occupancyAvailableEvses = nextLossyInt(reader, 0)
                name == "occupancy_occupied_evses" -> occupancyOccupiedEvses = nextLossyInt(reader, 0)
                name == "occupancy_charging_evses" -> occupancyChargingEvses = nextLossyInt(reader, 0)
                name == "occupancy_out_of_order_evses" -> occupancyOutOfOrderEvses = nextLossyInt(reader, 0)
                name == "occupancy_unknown_evses" -> occupancyUnknownEvses = nextLossyInt(reader, 0)
                name == "detail_source_uid" -> detailSourceUid = nextStringOrNull(reader).orEmpty()
                name == "detail_source_name" -> detailSourceName = nextStringOrNull(reader).orEmpty()
                name == "detail_last_updated" -> detailLastUpdated = nextStringOrNull(reader).orEmpty()
                name == "datex_site_id" -> datexSiteId = nextStringOrNull(reader).orEmpty()
                name == "datex_station_ids" -> datexStationIds = nextStringOrNull(reader).orEmpty()
                name == "datex_charge_point_ids" -> datexChargePointIds = nextStringOrNull(reader).orEmpty()
                name == "price_display" -> priceDisplay = nextStringOrNull(reader).orEmpty()
                name == "price_energy_eur_kwh_min" -> priceEnergyEurKwhMin = nextLossyDoubleOrNull(reader)
                name == "price_energy_eur_kwh_max" -> priceEnergyEurKwhMax = nextLossyDoubleOrNull(reader)
                name == "price_currency" -> priceCurrency = nextStringOrNull(reader).orEmpty()
                name == "price_quality" -> priceQuality = nextStringOrNull(reader).orEmpty()
                name == "opening_hours_display" -> openingHoursDisplay = nextStringOrNull(reader).orEmpty()
                name == "opening_hours_is_24_7" -> {
                    openingHoursIs24_7 = when (reader.peek()) {
                        JsonToken.BOOLEAN -> reader.nextBoolean()
                        JsonToken.NUMBER -> nextLossyInt(reader, 0) > 0
                        JsonToken.STRING -> nextStringOrNull(reader).orEmpty().lowercase() in listOf("1", "true", "yes", "ja")
                        JsonToken.NULL -> {
                            reader.nextNull()
                            false
                        }
                        else -> {
                            reader.skipValue()
                            false
                        }
                    }
                }
                name == "helpdesk_phone" -> helpdeskPhone = nextStringOrNull(reader).orEmpty()
                name == "payment_methods_display" -> paymentMethodsDisplay = nextStringOrNull(reader).orEmpty()
                name == "auth_methods_display" -> authMethodsDisplay = nextStringOrNull(reader).orEmpty()
                name == "connector_types_display" -> connectorTypesDisplay = nextStringOrNull(reader).orEmpty()
                name == "current_types_display" -> currentTypesDisplay = nextStringOrNull(reader).orEmpty()
                name == "connector_count" -> connectorCount = nextLossyInt(reader, 0)
                name == "green_energy" -> {
                    greenEnergy = when (reader.peek()) {
                        JsonToken.BOOLEAN -> reader.nextBoolean()
                        JsonToken.STRING -> when (nextStringOrNull(reader).orEmpty().lowercase()) {
                            "true", "yes", "ja", "1" -> true
                            "false", "no", "nein", "0" -> false
                            else -> null
                        }
                        JsonToken.NULL -> {
                            reader.nextNull()
                            null
                        }
                        else -> {
                            reader.skipValue()
                            null
                        }
                    }
                }
                name == "service_types_display" -> serviceTypesDisplay = nextStringOrNull(reader).orEmpty()
                name == "details_json" -> detailsJson = nextStringOrNull(reader).orEmpty()
                name == "amenities_total" -> amenitiesTotal = nextLossyInt(reader, 0)
                name == "amenities_source" -> amenitiesSource = nextStringOrNull(reader).orEmpty()
                name == "amenity_examples" -> parseAmenityExamples(reader, amenityExamples)
                name.startsWith("amenity_") -> amenityCounts[name] = nextLossyInt(reader, 0)
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        return ChargerProperties(
            stationId = stationId,
            operatorName = operatorName,
            status = status,
            maxPowerKw = maxPowerKw,
            chargingPointsCount = chargingPointsCount,
            maxIndividualPowerKw = maxIndividualPowerKw ?: maxPowerKw,
            postcode = postcode,
            city = city,
            address = address,
            occupancySourceUid = occupancySourceUid,
            occupancySourceName = occupancySourceName,
            occupancyStatus = occupancyStatus,
            occupancyLastUpdated = occupancyLastUpdated,
            occupancyTotalEvses = occupancyTotalEvses,
            occupancyAvailableEvses = occupancyAvailableEvses,
            occupancyOccupiedEvses = occupancyOccupiedEvses,
            occupancyChargingEvses = occupancyChargingEvses,
            occupancyOutOfOrderEvses = occupancyOutOfOrderEvses,
            occupancyUnknownEvses = occupancyUnknownEvses,
            detailSourceUid = detailSourceUid,
            detailSourceName = detailSourceName,
            detailLastUpdated = detailLastUpdated,
            datexSiteId = datexSiteId,
            datexStationIds = datexStationIds,
            datexChargePointIds = datexChargePointIds,
            priceDisplay = priceDisplay,
            priceEnergyEurKwhMin = priceEnergyEurKwhMin,
            priceEnergyEurKwhMax = priceEnergyEurKwhMax,
            priceCurrency = priceCurrency,
            priceQuality = priceQuality,
            openingHoursDisplay = openingHoursDisplay,
            openingHoursIs24_7 = openingHoursIs24_7,
            helpdeskPhone = helpdeskPhone,
            paymentMethodsDisplay = paymentMethodsDisplay,
            authMethodsDisplay = authMethodsDisplay,
            connectorTypesDisplay = connectorTypesDisplay,
            currentTypesDisplay = currentTypesDisplay,
            connectorCount = connectorCount,
            greenEnergy = greenEnergy,
            serviceTypesDisplay = serviceTypesDisplay,
            detailsJson = detailsJson,
            amenitiesTotal = amenitiesTotal,
            amenitiesSource = amenitiesSource,
            amenityExamples = amenityExamples,
            amenityCounts = amenityCounts
        )
    }

    private fun parseAmenityExamples(reader: JsonReader, target: MutableList<AmenityExample>) {
        reader.beginArray()
        while (reader.hasNext()) {
            var category = ""
            var name: String? = null
            var openingHours: String? = null
            var distanceM: Double? = null
            var lat: Double? = null
            var lon: Double? = null

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    "category" -> category = nextStringOrNull(reader).orEmpty()
                    "name" -> name = nextStringOrNull(reader)?.ifBlank { null }
                    "opening_hours" -> openingHours = nextStringOrNull(reader)?.ifBlank { null }
                    "distance_m" -> distanceM = nextLossyDoubleOrNull(reader)
                    "lat" -> lat = nextLossyDoubleOrNull(reader)
                    "lon" -> lon = nextLossyDoubleOrNull(reader)
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            target += AmenityExample(
                category = category,
                name = name,
                openingHours = openingHours,
                distanceM = distanceM,
                lat = lat,
                lon = lon
            )
        }
        reader.endArray()
    }

    private fun parseOperatorCatalog(reader: Reader): OperatorCatalog {
        JsonReader(reader).use { jsonReader ->
            var generatedAt: String? = null
            var minStations = 0
            var totalOperators = 0
            val operators = mutableListOf<OperatorEntry>()

            jsonReader.beginObject()
            while (jsonReader.hasNext()) {
                when (jsonReader.nextName()) {
                    "generated_at" -> generatedAt = nextStringOrNull(jsonReader)
                    "min_stations" -> minStations = nextLossyInt(jsonReader, 0)
                    "total_operators" -> totalOperators = nextLossyInt(jsonReader, 0)
                    "operators" -> parseOperatorsArray(jsonReader, operators)
                    else -> jsonReader.skipValue()
                }
            }
            jsonReader.endObject()

            return OperatorCatalog(
                generatedAt = generatedAt,
                minStations = minStations,
                totalOperators = if (totalOperators > 0) totalOperators else operators.size,
                operators = operators
            )
        }
    }

    private fun parseOperatorsArray(reader: JsonReader, target: MutableList<OperatorEntry>) {
        reader.beginArray()
        while (reader.hasNext()) {
            var name = ""
            var stations = 0

            reader.beginObject()
            while (reader.hasNext()) {
                when (reader.nextName()) {
                    "name" -> name = nextStringOrNull(reader).orEmpty()
                    "stations" -> stations = nextLossyInt(reader, 0)
                    else -> reader.skipValue()
                }
            }
            reader.endObject()

            target += OperatorEntry(name = name, stations = stations)
        }
        reader.endArray()
    }

    private fun nextStringOrNull(reader: JsonReader): String? {
        return when (reader.peek()) {
            JsonToken.NULL -> {
                reader.nextNull()
                null
            }
            JsonToken.STRING, JsonToken.NUMBER -> reader.nextString()
            JsonToken.BOOLEAN -> reader.nextBoolean().toString()
            else -> {
                reader.skipValue()
                null
            }
        }
    }

    private fun nextLossyInt(reader: JsonReader, fallback: Int): Int {
        return nextStringOrNull(reader)?.replace(',', '.')?.toDoubleOrNull()?.toInt() ?: fallback
    }

    private fun nextLossyDouble(reader: JsonReader, fallback: Double): Double {
        return nextStringOrNull(reader)?.replace(',', '.')?.toDoubleOrNull() ?: fallback
    }

    private fun nextLossyDoubleOrNull(reader: JsonReader): Double? {
        return nextStringOrNull(reader)?.replace(',', '.')?.toDoubleOrNull()
    }
}
