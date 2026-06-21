package de.woladen.android.repository

import de.woladen.android.model.AmenityExample
import de.woladen.android.model.CatalogCharger
import de.woladen.android.model.CatalogStation
import de.woladen.android.model.CatalogStationDetail
import de.woladen.android.model.ChargerProperties
import de.woladen.android.model.FilterState
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.GeoJsonPointGeometry
import de.woladen.android.model.OperatorEntry
import de.woladen.android.service.LiveApiClient
import kotlin.math.roundToInt

class ChargerRepository(
    private val liveApiClient: LiveApiClient = LiveApiClient()
) {
    data class CatalogLoadResult(
        val features: List<GeoJsonFeature>,
        val source: String,
        val returnedCount: Int
    )

    private data class CatalogSearchKey(
        val latitudeE5: Int,
        val longitudeE5: Int,
        val radiusMeters: Int,
        val limit: Int,
        val minPowerKwTenth: Int,
        val operatorName: String
    )

    private data class CacheEntry<T>(
        val value: T,
        val storedAtMs: Long = System.currentTimeMillis()
    )

    private val cacheLock = Any()
    private val catalogSearchCache = BoundedLruCache<CatalogSearchKey, CacheEntry<CatalogLoadResult>>(MAX_SEARCH_CACHE_ENTRIES)
    private val stationSummaryCache = BoundedLruCache<String, GeoJsonFeature>(MAX_STATION_CACHE_ENTRIES)
    private val stationDetailCache = BoundedLruCache<String, CacheEntry<GeoJsonFeature>>(MAX_DETAIL_CACHE_ENTRIES)

    suspend fun searchCatalog(
        latitude: Double,
        longitude: Double,
        radiusMeters: Int,
        limit: Int,
        filterState: FilterState
    ): CatalogLoadResult {
        val key = CatalogSearchKey(
            latitudeE5 = (latitude * 100_000).roundToInt(),
            longitudeE5 = (longitude * 100_000).roundToInt(),
            radiusMeters = radiusMeters,
            limit = limit,
            minPowerKwTenth = (filterState.minPowerKw * 10).roundToInt(),
            operatorName = filterState.operatorName.trim()
        )

        var staleSearch: CatalogLoadResult? = null
        val now = System.currentTimeMillis()
        synchronized(cacheLock) {
            catalogSearchCache[key]?.let { entry ->
                if (now - entry.storedAtMs <= SEARCH_FRESH_TTL_MS) {
                    return entry.value
                }
                if (now - entry.storedAtMs <= SEARCH_STALE_TTL_MS) {
                    staleSearch = entry.value
                }
            }
        }

        val response = runCatching {
            liveApiClient.catalogSearch(
                latitude = latitude,
                longitude = longitude,
                radiusMeters = radiusMeters,
                limit = limit,
                filterState = filterState
            )
        }.getOrElse { error ->
            staleSearch?.let { return it }
            throw error
        }
        val features = response.stations.map(::catalogStationToFeature)
        val result = CatalogLoadResult(
            features = features,
            source = response.source,
            returnedCount = response.returnedCount
        )

        synchronized(cacheLock) {
            catalogSearchCache[key] = CacheEntry(result)
            features.forEach { stationSummaryCache[it.properties.stationId] = it }
        }

        return result
    }

    suspend fun loadCatalogStationDetail(stationId: String): GeoJsonFeature {
        val normalizedStationId = stationId.trim()
        var staleDetail: GeoJsonFeature? = null
        val now = System.currentTimeMillis()
        synchronized(cacheLock) {
            stationDetailCache[normalizedStationId]?.let { entry ->
                if (now - entry.storedAtMs <= DETAIL_FRESH_TTL_MS) {
                    return entry.value
                }
                if (now - entry.storedAtMs <= DETAIL_STALE_TTL_MS) {
                    staleDetail = entry.value
                }
            }
        }

        val detail = runCatching {
            liveApiClient.catalogStationDetail(normalizedStationId)
        }.getOrElse { error ->
            staleDetail?.let { return it }
            throw error
        }
        val feature = catalogStationToFeature(detail.station, detail)
        synchronized(cacheLock) {
            stationDetailCache[normalizedStationId] = CacheEntry(feature)
            stationSummaryCache[normalizedStationId] = feature
        }
        return feature
    }

    fun invalidateCache() {
        synchronized(cacheLock) {
            catalogSearchCache.clear()
            stationDetailCache.clear()
            stationSummaryCache.clear()
        }
    }

    fun cachedFeaturesForStationIds(stationIds: Set<String>): List<GeoJsonFeature> {
        if (stationIds.isEmpty()) return emptyList()
        synchronized(cacheLock) {
            return stationIds.mapNotNull { stationId ->
                stationDetailCache[stationId]?.value ?: stationSummaryCache[stationId]
            }
        }
    }

    private fun catalogStationToFeature(
        station: CatalogStation,
        detail: CatalogStationDetail? = null
    ): GeoJsonFeature {
        val chargers = detail?.chargers.orEmpty()
        val chargerCount = firstPositive(station.chargerCount, chargers.size, 1)
        val maxPowerKw = station.maxPowerKw ?: chargers.mapNotNull { it.maxPowerKw }.maxOrNull() ?: 0.0
        val maxIndividualPowerKw = chargers.mapNotNull { it.maxPowerKw }.maxOrNull() ?: maxPowerKw
        val amenityCounts = detail?.amenityCounts?.takeIf { it.isNotEmpty() } ?: station.amenityCounts
        val amenityExamples = detail?.amenityExamples?.takeIf { it.isNotEmpty() }
            ?: nearestAmenityExample(station)
        val amenitiesTotal = detail?.amenitiesTotal ?: station.amenitiesTotal
        val connectorTypesDisplay = formatTokenDisplay(
            chargers.map { it.connectorType }.takeIf { values -> values.any { it.isNotBlank() } }
                ?: splitCatalogTokens(station.connectorTypes)
        )
        val currentTypesDisplay = formatTokenDisplay(chargers.map { it.currentType })
        val sourceLabel = firstNonBlank(station.sourceUid, station.providerUid, station.sourceUrl, station.publicBundleStatus)

        return GeoJsonFeature(
            id = station.stationId,
            geometry = GeoJsonPointGeometry(
                type = "Point",
                coordinates = listOf(station.longitude, station.latitude)
            ),
            properties = ChargerProperties(
                stationId = station.stationId,
                operatorName = firstNonBlank(station.operatorName, station.stationName, "Unbekannt"),
                status = station.publicBundleStatus,
                maxPowerKw = maxPowerKw,
                chargingPointsCount = chargerCount,
                maxIndividualPowerKw = maxIndividualPowerKw,
                postcode = station.postalCode,
                city = station.city,
                address = station.address,
                occupancySourceUid = station.sourceUid,
                occupancySourceName = sourceLabel,
                occupancyStatus = station.liveSummary?.availabilityStatus?.rawValue.orEmpty(),
                occupancyLastUpdated = firstNonBlank(
                    station.liveSummary?.sourceObservedAt.orEmpty(),
                    station.liveSummary?.fetchedAt.orEmpty(),
                    station.liveSummary?.ingestedAt.orEmpty()
                ),
                occupancyTotalEvses = station.liveSummary?.totalEvses ?: 0,
                occupancyAvailableEvses = station.liveSummary?.availableEvses ?: 0,
                occupancyOccupiedEvses = station.liveSummary?.occupiedEvses ?: 0,
                occupancyChargingEvses = 0,
                occupancyOutOfOrderEvses = station.liveSummary?.outOfOrderEvses ?: 0,
                occupancyUnknownEvses = station.liveSummary?.unknownEvses ?: 0,
                detailSourceUid = station.sourceUid,
                detailSourceName = sourceLabel,
                detailLastUpdated = station.detailLastUpdated,
                datexSiteId = "",
                datexStationIds = "",
                datexChargePointIds = "",
                priceDisplay = station.priceDisplay,
                priceEnergyEurKwhMin = null,
                priceEnergyEurKwhMax = null,
                priceCurrency = station.priceCurrency,
                priceQuality = "",
                openingHoursDisplay = station.openingHours,
                openingHoursIs24_7 = station.openingHours.equals("24/7", ignoreCase = true),
                helpdeskPhone = station.helpdeskPhone,
                paymentMethodsDisplay = station.paymentMethods,
                authMethodsDisplay = station.authMethods,
                connectorTypesDisplay = connectorTypesDisplay,
                currentTypesDisplay = currentTypesDisplay,
                connectorCount = chargerCount,
                greenEnergy = station.greenEnergy,
                serviceTypesDisplay = "",
                detailsJson = "",
                amenitiesTotal = amenitiesTotal,
                amenitiesSource = "open_static.sqlite3",
                amenityExamples = amenityExamples,
                amenityCounts = amenityCounts
            ),
            liveSummary = station.liveSummary
        )
    }

    private fun nearestAmenityExample(station: CatalogStation): List<AmenityExample> {
        val category = station.nearestAmenityKind.removePrefix("amenity_")
        if (category.isBlank()) return emptyList()
        return listOf(
            AmenityExample(
                category = category,
                name = station.nearestAmenityName.ifBlank { null },
                openingHours = null,
                distanceM = station.nearestAmenityDistanceM,
                lat = null,
                lon = null
            )
        )
    }

    private fun firstPositive(vararg values: Int): Int {
        return values.firstOrNull { it > 0 } ?: 0
    }

    private fun firstNonBlank(vararg values: String): String {
        return values.firstOrNull { it.isNotBlank() }.orEmpty()
    }

    private fun splitCatalogTokens(value: String): List<String> {
        return value
            .split(",", ";", "|")
            .map { it.trim() }
            .filter { it.isNotBlank() }
    }

    private fun formatTokenDisplay(values: List<String>): String {
        return values
            .flatMap(::splitCatalogTokens)
            .distinctBy { it.lowercase() }
            .joinToString(" | ")
    }

    companion object {
        private const val MAX_SEARCH_CACHE_ENTRIES = 48
        private const val MAX_STATION_CACHE_ENTRIES = 600
        private const val MAX_DETAIL_CACHE_ENTRIES = 180
        private const val SEARCH_FRESH_TTL_MS = 5 * 60 * 1000L
        private const val SEARCH_STALE_TTL_MS = 24 * 60 * 60 * 1000L
        private const val DETAIL_FRESH_TTL_MS = 24 * 60 * 60 * 1000L
        private const val DETAIL_STALE_TTL_MS = 7 * 24 * 60 * 60 * 1000L
    }
}

private class BoundedLruCache<K, V>(
    private val maxEntries: Int
) : LinkedHashMap<K, V>(16, 0.75f, true) {
    override fun removeEldestEntry(eldest: MutableMap.MutableEntry<K, V>?): Boolean {
        return size > maxEntries
    }
}
