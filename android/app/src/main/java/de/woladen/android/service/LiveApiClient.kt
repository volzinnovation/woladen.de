package de.woladen.android.service

import android.util.JsonReader
import android.util.JsonToken
import de.woladen.android.BuildConfig
import de.woladen.android.model.AmenityExample
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.BundleBuildRecords
import de.woladen.android.model.BundleBuildRun
import de.woladen.android.model.BundleBuildSummary
import de.woladen.android.model.CatalogInfoSummary
import de.woladen.android.model.CatalogCharger
import de.woladen.android.model.CatalogSearchResponse
import de.woladen.android.model.CatalogStation
import de.woladen.android.model.CatalogStationDetail
import de.woladen.android.model.FilterState
import de.woladen.android.model.OpenStaticBundle
import de.woladen.android.model.OpenStaticCountry
import de.woladen.android.model.OpenStaticSource
import de.woladen.android.model.OpenStaticSummary
import de.woladen.android.model.LiveEvse
import de.woladen.android.model.LiveJsonValue
import de.woladen.android.model.LiveStationDetail
import de.woladen.android.model.LiveStationLookupResponse
import de.woladen.android.model.LiveStationSummary
import de.woladen.android.model.RouteChargerResponse
import de.woladen.android.model.RouteEndpoint
import de.woladen.android.model.RouteFilterPayload
import de.woladen.android.model.RouteGeometry
import de.woladen.android.model.RouteNearestPoint
import de.woladen.android.model.RouteStationCandidate
import de.woladen.android.model.RouteStationMetadata
import de.woladen.android.model.RouteSummary
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.io.Reader
import java.net.HttpURLConnection
import java.net.Proxy
import java.net.URL
import java.net.URLEncoder
import java.util.Locale
import kotlin.math.roundToInt

class LiveApiClient(
    private val baseUrl: String = DEFAULT_BASE_URL,
    private val acceptLanguage: String = NativeLanguage.acceptLanguageHeader()
) {
    val isEnabled: Boolean
        get() = baseUrl.isNotBlank()

    suspend fun lookupStations(stationIds: List<String>): LiveStationLookupResponse = withContext(Dispatchers.IO) {
        val lookupStationIds = normalizeLookupStationIds(stationIds)
        require(lookupStationIds.size <= MAX_LOOKUP_STATION_IDS) {
            "lookupStations accepts at most $MAX_LOOKUP_STATION_IDS station IDs per request"
        }
        if (lookupStationIds.isEmpty()) {
            return@withContext LiveStationLookupResponse(stations = emptyList(), missingStationIds = emptyList())
        }

        val connection = openConnection(
            path = "/v1/stations/lookup",
            method = "POST",
            timeoutMs = LOOKUP_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        connection.setRequestProperty("Content-Type", "application/json")
        connection.doOutput = true
        connection.outputStream.bufferedWriter().use { writer ->
            writer.write(lookupRequestBody(lookupStationIds))
        }
        readResponse(connection, ::parseLookupResponse)
    }

    suspend fun stationDetail(stationId: String): LiveStationDetail = withContext(Dispatchers.IO) {
        val encodedStationId = URLEncoder.encode(stationId, Charsets.UTF_8.name()).replace("+", "%20")
        val connection = openConnection(
            path = "/v1/stations/$encodedStationId",
            method = "GET",
            timeoutMs = DETAIL_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        readResponse(connection, ::parseStationDetail)
    }

    suspend fun catalogSearch(
        latitude: Double,
        longitude: Double,
        radiusMeters: Int,
        limit: Int,
        filterState: FilterState
    ): CatalogSearchResponse = withContext(Dispatchers.IO) {
        val connection = openConnection(
            path = catalogSearchPath(
                latitude = latitude,
                longitude = longitude,
                radiusMeters = radiusMeters,
                limit = limit,
                filterState = filterState
            ),
            method = "GET",
            timeoutMs = CATALOG_SEARCH_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        parseCatalogSearchResponse(readJsonResponse(connection))
    }

    suspend fun catalogStationDetail(stationId: String): CatalogStationDetail = withContext(Dispatchers.IO) {
        val connection = openConnection(
            path = catalogStationDetailPath(stationId),
            method = "GET",
            timeoutMs = CATALOG_DETAIL_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        parseCatalogStationDetail(readJsonResponse(connection))
    }

    suspend fun catalogInfoSummary(): CatalogInfoSummary = withContext(Dispatchers.IO) {
        val connection = openConnection(
            path = "/v1/catalog/summary",
            method = "GET",
            timeoutMs = CATALOG_SUMMARY_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        parseCatalogInfoSummaryPayload(readJsonResponse(connection))
    }

    suspend fun routeChargers(
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        filters: RouteFilterPayload
    ): RouteChargerResponse = withContext(Dispatchers.IO) {
        val connection = openConnection(
            path = "/v1/routes/chargers",
            method = "POST",
            timeoutMs = ROUTE_CHARGER_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        connection.setRequestProperty("Content-Type", "application/json")
        connection.doOutput = true
        connection.outputStream.bufferedWriter().use { writer ->
            writer.write(routeChargerRequestBody(origin, destination, filters))
        }
        parseRouteChargerResponse(readJsonResponse(connection))
    }

    suspend fun webOpenStaticSummary(): OpenStaticSummary = withContext(Dispatchers.IO) {
        val connection = openAbsoluteConnection(
            url = DEFAULT_WEB_OPEN_STATIC_SUMMARY_URL,
            method = "GET",
            timeoutMs = WEB_SUMMARY_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        parseOpenStaticSummaryPayload(readJsonResponse(connection))
    }

    suspend fun webBuildSummary(): BundleBuildSummary = withContext(Dispatchers.IO) {
        val connection = openAbsoluteConnection(
            url = DEFAULT_WEB_BUILD_SUMMARY_URL,
            method = "GET",
            timeoutMs = WEB_SUMMARY_TIMEOUT_MS
        )
        connection.setRequestProperty("Accept", "application/json")
        parseBundleBuildSummaryPayload(readJsonResponse(connection))
    }

    private fun openConnection(path: String, method: String, timeoutMs: Int): HttpURLConnection {
        val normalizedBaseUrl = baseUrl.trimEnd('/')
        val connection = URL("$normalizedBaseUrl$path").openConnection(Proxy.NO_PROXY) as HttpURLConnection
        connection.requestMethod = method
        connection.connectTimeout = timeoutMs
        connection.readTimeout = timeoutMs
        connection.instanceFollowRedirects = true
        if (acceptLanguage.isNotBlank()) {
            connection.setRequestProperty("Accept-Language", acceptLanguage)
        }
        return connection
    }

    private fun openAbsoluteConnection(url: String, method: String, timeoutMs: Int): HttpURLConnection {
        val connection = URL(url).openConnection(Proxy.NO_PROXY) as HttpURLConnection
        connection.requestMethod = method
        connection.connectTimeout = timeoutMs
        connection.readTimeout = timeoutMs
        connection.instanceFollowRedirects = true
        if (acceptLanguage.isNotBlank()) {
            connection.setRequestProperty("Accept-Language", acceptLanguage)
        }
        return connection
    }

    private fun <T> readResponse(connection: HttpURLConnection, parser: (Reader) -> T): T {
        try {
            val statusCode = connection.responseCode
            if (statusCode !in 200..299) {
                throw IOException("HTTP $statusCode")
            }
            connection.inputStream.bufferedReader().use { reader ->
                return parser(reader)
            }
        } finally {
            connection.disconnect()
        }
    }

    private fun readJsonResponse(connection: HttpURLConnection): JSONObject {
        try {
            val statusCode = connection.responseCode
            if (statusCode !in 200..299) {
                throw IOException("HTTP $statusCode")
            }
            val raw = connection.inputStream.bufferedReader().use { it.readText() }
            return JSONObject(raw)
        } finally {
            connection.disconnect()
        }
    }

    private fun lookupRequestBody(stationIds: List<String>): String {
        val ids = stationIds.joinToString(",") { id -> "\"${escapeJson(id)}\"" }
        return "{\"station_ids\":[$ids]}"
    }

    private fun parseLookupResponse(reader: Reader): LiveStationLookupResponse {
        JsonReader(reader).use { jsonReader ->
            val stations = mutableListOf<LiveStationSummary>()
            val missingStationIds = mutableListOf<String>()

            jsonReader.beginObject()
            while (jsonReader.hasNext()) {
                when (jsonReader.nextName()) {
                    "stations" -> {
                        jsonReader.beginArray()
                        while (jsonReader.hasNext()) {
                            stations += parseLiveStationSummary(jsonReader)
                        }
                        jsonReader.endArray()
                    }
                    "missing_station_ids" -> {
                        jsonReader.beginArray()
                        while (jsonReader.hasNext()) {
                            nextStringOrNull(jsonReader)?.takeIf { it.isNotBlank() }?.let(missingStationIds::add)
                        }
                        jsonReader.endArray()
                    }
                    else -> jsonReader.skipValue()
                }
            }
            jsonReader.endObject()

            return LiveStationLookupResponse(
                stations = stations,
                missingStationIds = missingStationIds
            )
        }
    }

    private fun parseStationDetail(reader: Reader): LiveStationDetail {
        JsonReader(reader).use { jsonReader ->
            var station: LiveStationSummary? = null
            val evses = mutableListOf<LiveEvse>()

            jsonReader.beginObject()
            while (jsonReader.hasNext()) {
                when (jsonReader.nextName()) {
                    "station" -> station = parseLiveStationSummary(jsonReader)
                    "evses" -> {
                        jsonReader.beginArray()
                        while (jsonReader.hasNext()) {
                            evses += parseLiveEvse(jsonReader)
                        }
                        jsonReader.endArray()
                    }
                    else -> jsonReader.skipValue()
                }
            }
            jsonReader.endObject()

            return LiveStationDetail(
                station = station ?: throw IOException("missing station in live detail"),
                evses = evses
            )
        }
    }

    private fun parseLiveStationSummary(reader: JsonReader): LiveStationSummary {
        var stationId = ""
        var availabilityStatus = AvailabilityStatus.UNKNOWN
        var availableEvses = 0
        var occupiedEvses = 0
        var outOfOrderEvses = 0
        var unknownEvses = 0
        var totalEvses = 0
        var priceDisplay = ""
        var priceCurrency = ""
        var priceEnergyEurKwhMin = ""
        var priceEnergyEurKwhMax = ""
        var sourceObservedAt = ""
        var fetchedAt = ""
        var ingestedAt = ""
        var dailyAnalysisDataAvailable = false
        var frequentlyOutOfOrderDailyAnalysis = false
        var frequentlyOccupiedDailyAnalysis = false
        var dailyAnalysisOutOfOrderColor = ""
        var dailyAnalysisOccupiedColor = ""

        reader.beginObject()
        while (reader.hasNext()) {
            when (reader.nextName()) {
                "station_id" -> stationId = nextStringOrNull(reader).orEmpty()
                "availability_status" -> availabilityStatus = AvailabilityStatus.fromRaw(nextStringOrNull(reader))
                "available_evses" -> availableEvses = nextLossyInt(reader, 0)
                "occupied_evses" -> occupiedEvses = nextLossyInt(reader, 0)
                "out_of_order_evses" -> outOfOrderEvses = nextLossyInt(reader, 0)
                "unknown_evses" -> unknownEvses = nextLossyInt(reader, 0)
                "total_evses" -> totalEvses = nextLossyInt(reader, 0)
                "price_display" -> priceDisplay = nextStringOrNull(reader).orEmpty()
                "price_currency" -> priceCurrency = nextStringOrNull(reader).orEmpty()
                "price_energy_eur_kwh_min" -> priceEnergyEurKwhMin = nextStringOrNull(reader).orEmpty()
                "price_energy_eur_kwh_max" -> priceEnergyEurKwhMax = nextStringOrNull(reader).orEmpty()
                "source_observed_at" -> sourceObservedAt = nextStringOrNull(reader).orEmpty()
                "fetched_at" -> fetchedAt = nextStringOrNull(reader).orEmpty()
                "ingested_at" -> ingestedAt = nextStringOrNull(reader).orEmpty()
                "daily_analysis_data_available" -> dailyAnalysisDataAvailable = nextLossyBoolean(reader, false)
                "frequently_out_of_order_daily_analysis" -> frequentlyOutOfOrderDailyAnalysis = nextLossyBoolean(reader, false)
                "frequently_occupied_daily_analysis" -> frequentlyOccupiedDailyAnalysis = nextLossyBoolean(reader, false)
                "daily_analysis_out_of_order_color" -> dailyAnalysisOutOfOrderColor = nextStringOrNull(reader).orEmpty()
                "daily_analysis_occupied_color" -> dailyAnalysisOccupiedColor = nextStringOrNull(reader).orEmpty()
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        return LiveStationSummary(
            stationId = stationId,
            availabilityStatus = availabilityStatus,
            availableEvses = availableEvses,
            occupiedEvses = occupiedEvses,
            outOfOrderEvses = outOfOrderEvses,
            unknownEvses = unknownEvses,
            totalEvses = totalEvses,
            priceDisplay = priceDisplay,
            priceCurrency = priceCurrency,
            priceEnergyEurKwhMin = priceEnergyEurKwhMin,
            priceEnergyEurKwhMax = priceEnergyEurKwhMax,
            sourceObservedAt = sourceObservedAt,
            fetchedAt = fetchedAt,
            ingestedAt = ingestedAt,
            dailyAnalysisDataAvailable = dailyAnalysisDataAvailable,
            frequentlyOutOfOrderDailyAnalysis = frequentlyOutOfOrderDailyAnalysis,
            frequentlyOccupiedDailyAnalysis = frequentlyOccupiedDailyAnalysis,
            dailyAnalysisOutOfOrderColor = dailyAnalysisOutOfOrderColor,
            dailyAnalysisOccupiedColor = dailyAnalysisOccupiedColor
        )
    }

    private fun parseLiveEvse(reader: JsonReader): LiveEvse {
        var providerEvseId = ""
        var availabilityStatus = AvailabilityStatus.UNKNOWN
        var operationalStatus = ""
        var priceDisplay = ""
        var sourceObservedAt = ""
        var fetchedAt = ""
        var ingestedAt = ""
        var nextAvailableChargingSlots: List<LiveJsonValue> = emptyList()
        var supplementalFacilityStatus: List<LiveJsonValue> = emptyList()

        reader.beginObject()
        while (reader.hasNext()) {
            when (reader.nextName()) {
                "provider_evse_id" -> providerEvseId = nextStringOrNull(reader).orEmpty()
                "availability_status" -> availabilityStatus = AvailabilityStatus.fromRaw(nextStringOrNull(reader))
                "operational_status" -> operationalStatus = nextStringOrNull(reader).orEmpty()
                "price_display" -> priceDisplay = nextStringOrNull(reader).orEmpty()
                "source_observed_at" -> sourceObservedAt = nextStringOrNull(reader).orEmpty()
                "fetched_at" -> fetchedAt = nextStringOrNull(reader).orEmpty()
                "ingested_at" -> ingestedAt = nextStringOrNull(reader).orEmpty()
                "next_available_charging_slots" -> nextAvailableChargingSlots = parseLiveJsonArray(reader)
                "supplemental_facility_status" -> supplementalFacilityStatus = parseLiveJsonArray(reader)
                else -> reader.skipValue()
            }
        }
        reader.endObject()

        return LiveEvse(
            providerEvseId = providerEvseId,
            availabilityStatus = availabilityStatus,
            operationalStatus = operationalStatus,
            priceDisplay = priceDisplay,
            sourceObservedAt = sourceObservedAt,
            fetchedAt = fetchedAt,
            ingestedAt = ingestedAt,
            nextAvailableChargingSlots = nextAvailableChargingSlots,
            supplementalFacilityStatus = supplementalFacilityStatus
        )
    }

    private fun parseCatalogSearchResponse(payload: JSONObject): CatalogSearchResponse {
        val stationArray = payload.optJSONArray("stations") ?: JSONArray()
        val stations = mutableListOf<CatalogStation>()
        for (index in 0 until stationArray.length()) {
            stationArray.optJSONObject(index)?.let { stations += parseCatalogStation(it) }
        }

        val returnedCount = payload
            .optJSONObject("stats")
            ?.optInt("returned_count", stations.size)
            ?: stations.size

        return CatalogSearchResponse(
            stations = stations,
            source = payload.optCleanString("source"),
            returnedCount = returnedCount
        )
    }

    private fun parseRouteChargerResponse(payload: JSONObject): RouteChargerResponse {
        val stationArray = payload.optJSONArray("stations") ?: JSONArray()
        val stations = mutableListOf<RouteStationCandidate>()
        for (index in 0 until stationArray.length()) {
            val item = stationArray.optJSONObject(index) ?: continue
            val station = item.optJSONObject("station") ?: continue
            stations += RouteStationCandidate(
                station = parseCatalogStation(station),
                route = parseRouteStationMetadata(item.optJSONObject("route") ?: JSONObject())
            )
        }

        return RouteChargerResponse(
            route = parseRouteSummary(payload.optJSONObject("route") ?: JSONObject()),
            stations = stations,
            source = payload.optCleanString("source")
        )
    }

    private fun parseRouteSummary(payload: JSONObject): RouteSummary {
        val geometry = payload.optJSONObject("geometry")
        return RouteSummary(
            source = payload.optCleanString("source"),
            profile = payload.optCleanString("profile"),
            distanceM = payload.optNullableDouble("distance_m").toNonNegativeInt(),
            durationS = payload.optNullableDouble("duration_s").toNonNegativeInt(),
            geometry = RouteGeometry(
                type = geometry?.optCleanString("type").orEmpty().ifBlank { "LineString" },
                coordinates = parseRouteCoordinates(geometry?.optJSONArray("coordinates"))
            )
        )
    }

    private fun parseRouteCoordinates(payload: JSONArray?): List<List<Double>> {
        if (payload == null) return emptyList()
        val coordinates = mutableListOf<List<Double>>()
        for (index in 0 until payload.length()) {
            val point = payload.optJSONArray(index) ?: continue
            val lon = point.optNullableDouble(0) ?: continue
            val lat = point.optNullableDouble(1) ?: continue
            coordinates += listOf(lon, lat)
        }
        return coordinates
    }

    private fun parseRouteStationMetadata(payload: JSONObject): RouteStationMetadata {
        val nearest = payload.optJSONObject("nearest_route_point")
        return RouteStationMetadata(
            driveDistanceToRouteM = payload.optNullableDouble("drive_distance_to_route_m").toNonNegativeInt(),
            routeDetourM = payload.optNullableDouble("route_detour_m").toNonNegativeInt(),
            straightLineDistanceToRouteM = payload.optNullableDouble("straight_line_distance_to_route_m").toNonNegativeInt(),
            routePositionM = payload.optNullableDouble("route_position_m").toNonNegativeInt(),
            nearestRoutePoint = nearest?.let {
                val lat = it.optNullableDouble("lat")
                val lon = it.optNullableDouble("lon") ?: it.optNullableDouble("lng")
                if (lat != null && lon != null) RouteNearestPoint(lat = lat, lon = lon) else null
            }
        )
    }

    private fun parseCatalogStationDetail(payload: JSONObject): CatalogStationDetail {
        val station = payload.optJSONObject("station")
            ?: throw IOException("missing station in catalog detail")
        val amenities = payload.optJSONObject("amenities")
        val detailAmenityCounts = parseAmenityCounts(amenities?.opt("amenity_category_counts"))
        val detailAmenityExamples = parseCatalogAmenityExamples(amenities?.optJSONArray("amenity_examples"))

        return CatalogStationDetail(
            station = parseCatalogStation(station),
            chargers = parseCatalogChargers(payload.optJSONArray("chargers")),
            amenitiesTotal = amenities?.optNullableInt("amenities_total"),
            amenityCounts = detailAmenityCounts,
            amenityExamples = detailAmenityExamples
        )
    }

    private fun parseCatalogStation(payload: JSONObject): CatalogStation {
        val stationId = payload.optCleanString("station_id")
        val chargerCount = payload.optNullableInt("charger_count") ?: 0
        val liveSummary = parseCatalogLiveSummary(payload, stationId, fallbackTotalEvses = chargerCount)

        return CatalogStation(
            stationId = stationId,
            countryCode = payload.optCleanString("country_code"),
            sourceUid = payload.optCleanString("source_uid"),
            sourceStationId = payload.optCleanString("source_station_id"),
            license = payload.optCleanString("license"),
            providerUid = payload.optCleanString("provider_uid"),
            operatorName = payload.optCleanString("operator_name"),
            stationName = payload.optCleanString("station_name"),
            address = payload.optCleanString("address"),
            postalCode = payload.optCleanString("postal_code"),
            city = payload.optCleanString("city"),
            latitude = payload.optNullableDouble("latitude") ?: 0.0,
            longitude = payload.optNullableDouble("longitude") ?: 0.0,
            chargerCount = chargerCount,
            maxPowerKw = payload.optNullableDouble("max_power_kw"),
            connectorTypes = payload.optCleanString("connector_types"),
            sourceUrl = payload.optCleanString("source_url"),
            publicBundleStatus = payload.optCleanString("public_bundle_status"),
            openingHours = payload.optCleanString("opening_hours"),
            paymentMethods = payload.optCleanString("payment_methods"),
            authMethods = payload.optCleanString("auth_methods"),
            greenEnergy = payload.optNullableBoolean("green_energy"),
            helpdeskPhone = payload.optCleanString("helpdesk_phone"),
            priceDisplay = payload.optCleanString("price_display"),
            priceCurrency = payload.optCleanString("price_currency"),
            detailLastUpdated = payload.optCleanString("detail_last_updated"),
            amenitiesTotal = payload.optNullableInt("amenities_total") ?: 0,
            amenityCounts = parseAmenityCounts(payload.opt("amenity_category_counts")),
            nearestAmenityKind = payload.optCleanString("nearest_amenity_kind"),
            nearestAmenityName = payload.optCleanString("nearest_amenity_name"),
            nearestAmenityDistanceM = payload.optNullableDouble("nearest_amenity_distance_m"),
            liveSummary = liveSummary
        )
    }

    private fun parseCatalogLiveSummary(
        payload: JSONObject,
        stationId: String,
        fallbackTotalEvses: Int
    ): LiveStationSummary? {
        val hasLiveFields = listOf(
            "availability_status",
            "available_evses",
            "occupied_evses",
            "out_of_order_evses",
            "unknown_evses",
            "total_evses",
            "source_observed_at",
            "fetched_at",
            "ingested_at",
            "daily_analysis_data_available",
            "frequently_out_of_order_daily_analysis",
            "frequently_occupied_daily_analysis",
            "daily_analysis_out_of_order_color",
            "daily_analysis_occupied_color"
        ).any(payload::has)

        if (!hasLiveFields) {
            return null
        }

        val totalEvses = payload.optNullableInt("total_evses") ?: fallbackTotalEvses
        val availableEvses = payload.optNullableInt("available_evses") ?: 0
        val occupiedEvses = payload.optNullableInt("occupied_evses") ?: 0
        val outOfOrderEvses = payload.optNullableInt("out_of_order_evses") ?: 0
        val unknownEvses = payload.optNullableInt("unknown_evses")
            ?: maxOf(0, totalEvses - availableEvses - occupiedEvses - outOfOrderEvses)

        return LiveStationSummary(
            stationId = stationId,
            availabilityStatus = AvailabilityStatus.fromRaw(payload.optCleanString("availability_status")),
            availableEvses = availableEvses,
            occupiedEvses = occupiedEvses,
            outOfOrderEvses = outOfOrderEvses,
            unknownEvses = unknownEvses,
            totalEvses = totalEvses,
            priceDisplay = payload.optCleanString("price_display"),
            priceCurrency = payload.optCleanString("price_currency"),
            priceEnergyEurKwhMin = payload.optCleanString("price_energy_eur_kwh_min"),
            priceEnergyEurKwhMax = payload.optCleanString("price_energy_eur_kwh_max"),
            sourceObservedAt = payload.optCleanString("source_observed_at"),
            fetchedAt = payload.optCleanString("fetched_at"),
            ingestedAt = payload.optCleanString("ingested_at"),
            dailyAnalysisDataAvailable = payload.optNullableBoolean("daily_analysis_data_available") ?: false,
            frequentlyOutOfOrderDailyAnalysis = payload.optNullableBoolean("frequently_out_of_order_daily_analysis") ?: false,
            frequentlyOccupiedDailyAnalysis = payload.optNullableBoolean("frequently_occupied_daily_analysis") ?: false,
            dailyAnalysisOutOfOrderColor = payload.optCleanString("daily_analysis_out_of_order_color"),
            dailyAnalysisOccupiedColor = payload.optCleanString("daily_analysis_occupied_color")
        )
    }

    private fun parseCatalogChargers(payload: JSONArray?): List<CatalogCharger> {
        if (payload == null) return emptyList()
        val chargers = mutableListOf<CatalogCharger>()
        for (index in 0 until payload.length()) {
            val charger = payload.optJSONObject(index) ?: continue
            chargers += CatalogCharger(
                chargerId = charger.optCleanString("charger_id"),
                sourceUid = charger.optCleanString("source_uid"),
                providerUid = charger.optCleanString("provider_uid"),
                sourceStationId = charger.optCleanString("source_station_id"),
                sourceEvseId = charger.optCleanString("source_evse_id"),
                connectorId = charger.optCleanString("connector_id"),
                connectorType = charger.optCleanString("connector_type"),
                currentType = charger.optCleanString("current_type"),
                maxPowerKw = charger.optNullableDouble("max_power_kw"),
                operatorName = charger.optCleanString("operator_name"),
                license = charger.optCleanString("license"),
                sourceUrl = charger.optCleanString("source_url"),
                publicBundleStatus = charger.optCleanString("public_bundle_status")
            )
        }
        return chargers
    }

    private fun parseCatalogAmenityExamples(payload: JSONArray?): List<AmenityExample> {
        if (payload == null) return emptyList()
        val examples = mutableListOf<AmenityExample>()
        for (index in 0 until payload.length()) {
            val item = payload.optJSONObject(index) ?: continue
            val rawCategory = firstNonBlank(
                item.optCleanString("category"),
                item.optCleanString("kind"),
                item.optCleanString("amenity_kind")
            )
            val category = rawCategory.removePrefix("amenity_")
            if (category.isBlank()) continue
            examples += AmenityExample(
                category = category,
                name = item.optCleanString("name").ifBlank { null },
                openingHours = firstNonBlank(
                    item.optCleanString("opening_hours"),
                    item.optCleanString("hours")
                ).ifBlank { null },
                distanceM = item.optNullableDouble("distance_m"),
                lat = item.optNullableDouble("lat") ?: item.optNullableDouble("latitude"),
                lon = item.optNullableDouble("lon") ?: item.optNullableDouble("longitude")
            )
        }
        return examples
    }

    private fun parseAmenityCounts(value: Any?): Map<String, Int> {
        val json = when (value) {
            null, JSONObject.NULL -> return emptyMap()
            is JSONObject -> value
            is String -> runCatching { JSONObject(value) }.getOrNull() ?: return emptyMap()
            else -> return emptyMap()
        }

        val result = linkedMapOf<String, Int>()
        val keys = json.keys()
        while (keys.hasNext()) {
            val rawKey = keys.next()
            val count = json.optNullableInt(rawKey) ?: continue
            if (count <= 0) continue
            val key = normalizeAmenityKey(rawKey)
            if (key.isNotBlank()) {
                result[key] = count
            }
        }
        return result
    }

    private fun parseLiveJsonArray(reader: JsonReader): List<LiveJsonValue> {
        if (reader.peek() == JsonToken.NULL) {
            reader.nextNull()
            return emptyList()
        }

        val items = mutableListOf<LiveJsonValue>()
        reader.beginArray()
        while (reader.hasNext()) {
            items += parseLiveJsonValue(reader)
        }
        reader.endArray()
        return items
    }

    private fun parseLiveJsonValue(reader: JsonReader): LiveJsonValue {
        return when (reader.peek()) {
            JsonToken.BEGIN_OBJECT -> {
                val entries = LinkedHashMap<String, LiveJsonValue>()
                reader.beginObject()
                while (reader.hasNext()) {
                    entries[reader.nextName()] = parseLiveJsonValue(reader)
                }
                reader.endObject()
                LiveJsonValue.ObjectValue(entries)
            }
            JsonToken.BEGIN_ARRAY -> {
                val items = mutableListOf<LiveJsonValue>()
                reader.beginArray()
                while (reader.hasNext()) {
                    items += parseLiveJsonValue(reader)
                }
                reader.endArray()
                LiveJsonValue.ArrayValue(items)
            }
            JsonToken.BOOLEAN -> LiveJsonValue.BoolValue(reader.nextBoolean())
            JsonToken.NUMBER -> LiveJsonValue.NumberValue(nextLossyDouble(reader, 0.0))
            JsonToken.STRING -> LiveJsonValue.StringValue(reader.nextString())
            JsonToken.NULL -> {
                reader.nextNull()
                LiveJsonValue.NullValue
            }
            else -> {
                reader.skipValue()
                LiveJsonValue.NullValue
            }
        }
    }

    private fun nextStringOrNull(reader: JsonReader): String? {
        return when (reader.peek()) {
            JsonToken.STRING -> reader.nextString()
            JsonToken.NUMBER -> reader.nextString()
            JsonToken.BOOLEAN -> reader.nextBoolean().toString()
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

    private fun nextLossyInt(reader: JsonReader, fallback: Int): Int {
        return when (reader.peek()) {
            JsonToken.NUMBER -> reader.nextDouble().toInt()
            JsonToken.STRING -> {
                val value = reader.nextString()
                value.toIntOrNull() ?: value.toDoubleOrNull()?.toInt() ?: fallback
            }
            JsonToken.NULL -> {
                reader.nextNull()
                fallback
            }
            else -> {
                reader.skipValue()
                fallback
            }
        }
    }

    private fun nextLossyDouble(reader: JsonReader, fallback: Double): Double {
        return when (reader.peek()) {
            JsonToken.NUMBER -> reader.nextDouble()
            JsonToken.STRING -> reader.nextString().replace(',', '.').toDoubleOrNull() ?: fallback
            JsonToken.NULL -> {
                reader.nextNull()
                fallback
            }
            else -> {
                reader.skipValue()
                fallback
            }
        }
    }

    private fun nextLossyBoolean(reader: JsonReader, fallback: Boolean): Boolean {
        return when (reader.peek()) {
            JsonToken.BOOLEAN -> reader.nextBoolean()
            JsonToken.NUMBER -> reader.nextDouble().toInt() != 0
            JsonToken.STRING -> when (reader.nextString().trim().lowercase(Locale.ROOT)) {
                "1", "true", "yes", "y", "ja" -> true
                "0", "false", "no", "n", "nein" -> false
                else -> fallback
            }
            JsonToken.NULL -> {
                reader.nextNull()
                fallback
            }
            else -> {
                reader.skipValue()
                fallback
            }
        }
    }

    private fun escapeJson(value: String): String {
        return buildString {
            value.forEach { character ->
                when (character) {
                    '\\' -> append("\\\\")
                    '"' -> append("\\\"")
                    '\n' -> append("\\n")
                    '\r' -> append("\\r")
                    '\t' -> append("\\t")
                    else -> append(character)
                }
            }
        }
    }

    private fun JSONObject.optCleanString(name: String): String {
        if (!has(name) || isNull(name)) return ""
        return optString(name, "").trim()
    }

    private fun JSONObject.optNullableInt(name: String): Int? {
        if (!has(name) || isNull(name)) return null
        val value = opt(name)
        return when (value) {
            is Number -> value.toDouble().toInt()
            is String -> value.trim().replace(',', '.').let { text ->
                text.toIntOrNull() ?: text.toDoubleOrNull()?.toInt()
            }
            else -> null
        }
    }

    private fun JSONObject.optNullableDouble(name: String): Double? {
        if (!has(name) || isNull(name)) return null
        val value = opt(name)
        return when (value) {
            is Number -> value.toDouble().takeIf { it.isFinite() }
            is String -> value.trim().replace(',', '.').toDoubleOrNull()?.takeIf { it.isFinite() }
            else -> null
        }
    }

    private fun JSONObject.optNullableBoolean(name: String): Boolean? {
        if (!has(name) || isNull(name)) return null
        val value = opt(name)
        return when (value) {
            is Boolean -> value
            is Number -> value.toInt() != 0
            is String -> when (value.trim().lowercase(Locale.ROOT)) {
                "1", "true", "yes", "y", "ja" -> true
                "0", "false", "no", "n", "nein" -> false
                else -> null
            }
            else -> null
        }
    }

    private fun JSONArray.optNullableDouble(index: Int): Double? {
        if (index < 0 || index >= length() || isNull(index)) return null
        val value = opt(index)
        return when (value) {
            is Number -> value.toDouble().takeIf { it.isFinite() }
            is String -> value.trim().replace(',', '.').toDoubleOrNull()?.takeIf { it.isFinite() }
            else -> null
        }
    }

    private fun Double?.toNonNegativeInt(): Int {
        val value = this ?: return 0
        return maxOf(0, value.roundToInt())
    }

    private fun firstNonBlank(vararg values: String): String {
        return values.firstOrNull { it.isNotBlank() }.orEmpty()
    }

    private fun normalizeAmenityKey(rawKey: String): String {
        val key = rawKey.trim().lowercase(Locale.ROOT)
        if (key.isBlank()) return ""
        return if (key.startsWith("amenity_")) key else "amenity_$key"
    }

    companion object {
        val DEFAULT_BASE_URL: String = BuildConfig.LIVE_API_BASE_URL
        const val DEFAULT_WEB_OPEN_STATIC_SUMMARY_URL = "https://woladen.de/data/open_static_summary.json"
        const val DEFAULT_WEB_BUILD_SUMMARY_URL = "https://woladen.de/data/summary.json"
        const val MAX_LOOKUP_STATION_IDS = 20
        const val MAX_CATALOG_SEARCH_RESULTS = 100
        private const val LOOKUP_TIMEOUT_MS = 3_500
        private const val DETAIL_TIMEOUT_MS = 4_000
        private const val CATALOG_SEARCH_TIMEOUT_MS = 4_500
        private const val CATALOG_DETAIL_TIMEOUT_MS = 4_500
        private const val CATALOG_SUMMARY_TIMEOUT_MS = 5_000
        private const val ROUTE_CHARGER_TIMEOUT_MS = 120_000
        private const val WEB_SUMMARY_TIMEOUT_MS = 5_000
    }
}

internal fun routeChargerRequestBody(
    origin: RouteEndpoint,
    destination: RouteEndpoint,
    filters: RouteFilterPayload
): String {
    return JSONObject()
        .put("origin", routeEndpointJson(origin))
        .put("destination", routeEndpointJson(destination))
        .put(
            "filters",
            JSONObject()
                .put("operator", filters.operator)
                .put("min_power_kw", filters.minPowerKw)
                .put("min_amenities_total", filters.minAmenitiesTotal)
                .put("selected_amenities", JSONArray(filters.selectedAmenities))
                .put("amenity_name_query", filters.amenityNameQuery)
                .put("available_only", filters.availableOnly)
                .put("currently_open_only", filters.currentlyOpenOnly)
        )
        .put("filter_mode", "route_calculation")
        .toString()
}

private fun routeEndpointJson(endpoint: RouteEndpoint): JSONObject {
    return JSONObject()
        .put("lat", endpoint.lat)
        .put("lon", endpoint.lon)
        .put("label", endpoint.label)
}

internal fun catalogInfoSummaryFromJson(rawJson: String): CatalogInfoSummary =
    parseCatalogInfoSummaryPayload(JSONObject(rawJson))

internal fun parseCatalogInfoSummaryPayload(payload: JSONObject): CatalogInfoSummary {
    if (payload.has("bundle") || payload.has("countries") || payload.has("sources")) {
        return CatalogInfoSummary(
            openStaticSummary = parseOpenStaticSummaryPayload(payload),
            buildSummary = null
        )
    }

    return CatalogInfoSummary(
        openStaticSummary = payload.optJSONObject("open_static_summary")?.let(::parseOpenStaticSummaryPayload)
            ?: payload.optJSONObject("openStaticSummary")?.let(::parseOpenStaticSummaryPayload),
        buildSummary = payload.optJSONObject("summary")?.let(::parseBundleBuildSummaryPayload)
            ?: payload.optJSONObject("build_summary")?.let(::parseBundleBuildSummaryPayload)
            ?: payload.optJSONObject("buildSummary")?.let(::parseBundleBuildSummaryPayload)
    )
}

internal fun parseOpenStaticSummaryPayload(payload: JSONObject): OpenStaticSummary {
    return OpenStaticSummary(
        bundle = payload.optJSONObject("bundle")?.let(::parseOpenStaticBundlePayload),
        countries = parseOpenStaticCountries(payload.optJSONArray("countries")),
        generatedAt = payload.summaryCleanString("generated_at"),
        schemaVersion = payload.summaryNullableInt("schema_version") ?: 0,
        rawSources = parseOpenStaticSources(payload.optJSONArray("sources"))
    )
}

internal fun parseBundleBuildSummaryPayload(payload: JSONObject): BundleBuildSummary {
    return BundleBuildSummary(
        run = payload.optJSONObject("run")?.let {
            BundleBuildRun(
                startedAt = it.summaryCleanString("started_at"),
                finishedAt = it.summaryCleanString("finished_at")
            )
        },
        records = payload.optJSONObject("records")?.let {
            BundleBuildRecords(
                rawRows = it.summaryNullableInt("raw_rows") ?: 0,
                fullRegistryActiveStationsTotal = it.summaryNullableInt("full_registry_active_stations_total") ?: 0
            )
        }
    )
}

private fun parseOpenStaticBundlePayload(payload: JSONObject): OpenStaticBundle {
    return OpenStaticBundle(
        stationCount = payload.summaryNullableInt("station_count") ?: 0,
        chargerCount = payload.summaryNullableInt("charger_count") ?: 0,
        countryCount = payload.summaryNullableInt("country_count") ?: 0,
        schemaVersion = payload.summaryNullableInt("schema_version") ?: 0
    )
}

private fun parseOpenStaticCountries(payload: JSONArray?): List<OpenStaticCountry> {
    if (payload == null) return emptyList()
    val countries = mutableListOf<OpenStaticCountry>()
    for (index in 0 until payload.length()) {
        val country = payload.optJSONObject(index) ?: continue
        countries += OpenStaticCountry(
            code = firstSummaryNonBlank(
                country.summaryCleanString("code"),
                country.summaryCleanString("country_code")
            ).uppercase(Locale.ROOT),
            name = firstSummaryNonBlank(
                country.summaryCleanString("name"),
                country.summaryCleanString("country_name")
            ),
            stationCount = firstSummaryInt(country, "station_count", "stationCount", "stations"),
            chargerCount = firstSummaryInt(country, "charger_count", "chargerCount", "chargers"),
            fastStationCount = firstSummaryInt(country, "fast_station_count", "fastStationCount", "fast_stations")
        )
    }
    return countries
}

private fun parseOpenStaticSources(payload: JSONArray?): List<OpenStaticSource> {
    if (payload == null) return emptyList()
    val sources = mutableListOf<OpenStaticSource>()
    for (index in 0 until payload.length()) {
        val source = payload.optJSONObject(index) ?: continue
        sources += OpenStaticSource(
            countryCode = firstSummaryNonBlank(
                source.summaryCleanString("country_code"),
                source.summaryCleanString("countryCode")
            ).uppercase(Locale.ROOT),
            sourceUid = firstSummaryNonBlank(
                source.summaryCleanString("source_uid"),
                source.summaryCleanString("sourceUid")
            ),
            displayName = firstSummaryNonBlank(
                source.summaryCleanString("display_name"),
                source.summaryCleanString("displayName"),
                source.summaryCleanString("source_name"),
                source.summaryCleanString("sourceName"),
                source.summaryCleanString("source_uid"),
                source.summaryCleanString("sourceUid")
            ),
            sourceUrl = normalizedSummarySourceUrl(
                firstSummaryNonBlank(
                    source.summaryCleanString("source_url"),
                    source.summaryCleanString("sourceUrl"),
                    source.summaryCleanString("url")
                )
            ),
            license = source.summaryCleanString("license"),
            licenseUrl = firstSummaryNonBlank(
                source.summaryCleanString("license_url"),
                source.summaryCleanString("licenseUrl")
            )
        )
    }
    return sources
}

private fun firstSummaryInt(payload: JSONObject, vararg names: String): Int =
    names.firstNotNullOfOrNull(payload::summaryNullableInt) ?: 0

private fun firstSummaryNonBlank(vararg values: String): String =
    values.firstOrNull { it.isNotBlank() }.orEmpty()

private fun normalizedSummarySourceUrl(value: String): String =
    value.trim().trimEnd('/')

private fun JSONObject.summaryCleanString(name: String): String {
    if (!has(name) || isNull(name)) return ""
    return optString(name, "").trim()
}

private fun JSONObject.summaryNullableInt(name: String): Int? {
    if (!has(name) || isNull(name)) return null
    return when (val value = opt(name)) {
        is Number -> value.toDouble().toInt()
        is String -> value.trim().replace(',', '.').let { text ->
            text.toIntOrNull() ?: text.toDoubleOrNull()?.toInt()
        }
        else -> null
    }
}

internal fun normalizeLookupStationIds(stationIds: List<String>): List<String> {
    return stationIds
        .map { it.trim() }
        .filter { it.isNotBlank() }
        .distinct()
}

internal fun lookupStationIdBatches(
    stationIds: List<String>,
    maxBatchSize: Int = LiveApiClient.MAX_LOOKUP_STATION_IDS
): List<List<String>> {
    require(maxBatchSize > 0) { "maxBatchSize must be positive" }
    return normalizeLookupStationIds(stationIds).chunked(maxBatchSize)
}

internal fun catalogSearchPath(
    latitude: Double,
    longitude: Double,
    radiusMeters: Int,
    limit: Int,
    filterState: FilterState
): String {
    val params = linkedMapOf(
        "lat" to "%.6f".format(Locale.ROOT, latitude),
        "lon" to "%.6f".format(Locale.ROOT, longitude),
        "radius_m" to radiusMeters.coerceIn(1, 500_000).toString(),
        "limit" to limit.coerceIn(1, LiveApiClient.MAX_CATALOG_SEARCH_RESULTS).toString(),
        "mode" to "travel",
        "min_power_kw" to "%.1f".format(Locale.ROOT, filterState.minPowerKw.coerceAtLeast(0.0))
    )
    val operator = filterState.operatorName.trim()
    if (operator.isNotBlank()) {
        params["operator"] = operator
    }

    return "/v1/catalog/search?" + params.entries.joinToString("&") { (key, value) ->
        "${urlEncode(key)}=${urlEncode(value)}"
    }
}

internal fun catalogStationDetailPath(stationId: String): String {
    val encodedStationId = URLEncoder.encode(stationId.trim(), Charsets.UTF_8.name()).replace("+", "%20")
    return "/v1/catalog/stations/$encodedStationId"
}

private fun urlEncode(value: String): String {
    return URLEncoder.encode(value, Charsets.UTF_8.name())
}

internal object NativeLanguage {
    private val supportedLanguages = GeneratedNativeI18n.SUPPORTED_LANGUAGE_CODES

    fun acceptLanguageHeader(locales: List<Locale> = listOf(Locale.getDefault())): String {
        val resolvedLanguage = locales
            .asSequence()
            .mapNotNull { normalizeLanguageTag(it.toLanguageTag()) }
            .firstOrNull()
            ?: GeneratedNativeI18n.FALLBACK_LANGUAGE_CODE

        return if (resolvedLanguage == GeneratedNativeI18n.FALLBACK_LANGUAGE_CODE) {
            GeneratedNativeI18n.FALLBACK_LANGUAGE_CODE
        } else {
            "$resolvedLanguage, ${GeneratedNativeI18n.FALLBACK_LANGUAGE_CODE};q=0.8"
        }
    }

    fun normalizeLanguageTag(languageTag: String): String? {
        val language = languageTag
            .substringBefore('-')
            .substringBefore('_')
            .lowercase(Locale.ROOT)

        val alias = when (language) {
            "no" -> "nb"
            else -> language
        }

        return alias.takeIf { supportedLanguages.contains(it) }
    }
}
