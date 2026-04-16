package de.woladen.android.viewmodel

import android.app.Application
import android.location.Location
import android.net.Uri
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import de.woladen.android.model.ActiveDataBundleInfo
import de.woladen.android.model.FilterState
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.LiveStationDetail
import de.woladen.android.model.LiveStationSummary
import de.woladen.android.model.OperatorEntry
import de.woladen.android.model.matches
import de.woladen.android.repository.ChargerRepository
import de.woladen.android.repository.DataBundleManager
import de.woladen.android.service.LiveApiClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.PriorityQueue
import kotlin.math.atan2
import kotlin.math.cos
import kotlin.math.roundToInt
import kotlin.math.sin
import kotlin.math.sqrt

class AppViewModel(application: Application) : AndroidViewModel(application) {

    enum class AppTab {
        LIST,
        MAP,
        FAVORITES,
        INFO
    }

    var allFeatures: List<GeoJsonFeature> by mutableStateOf(emptyList())
        private set

    var discoveredFeatures: List<GeoJsonFeature> by mutableStateOf(emptyList())
        private set

    var operators: List<OperatorEntry> by mutableStateOf(emptyList())
        private set

    var filterState: FilterState by mutableStateOf(FilterState())

    var selectedFeature: GeoJsonFeature? by mutableStateOf(null)
        private set

    var selectedTab: AppTab by mutableStateOf(AppTab.LIST)

    var loadError: String? by mutableStateOf(null)
        private set

    var isLoading: Boolean by mutableStateOf(false)
        private set

    var isAwaitingFirstLocationFix: Boolean by mutableStateOf(false)
        private set

    var activeBundleInfo: ActiveDataBundleInfo? by mutableStateOf(null)
        private set

    private val dataBundleManager = DataBundleManager(application.applicationContext)
    private val repository = ChargerRepository(dataBundleManager)
    private val liveApiClient = LiveApiClient()

    private val maxVisibleChargers = 20
    private val maxDiscoveredHistory = 200
    private val liveRefreshIntervalMs = 15_000L

    private var filterPool: List<GeoJsonFeature> = emptyList()
    private val discoveredById: MutableMap<String, GeoJsonFeature> = linkedMapOf()
    private val discoveredOrder: MutableList<String> = mutableListOf()
    private var didSeedFromUserLocation = false
    private var lastUserLocationCenter: Pair<Double, Double>? = null

    private val liveSummaryFetchedAtByStationId: MutableMap<String, Long> = mutableMapOf()
    private val liveDetailFetchedAtByStationId: MutableMap<String, Long> = mutableMapOf()
    private val pendingLiveSummaryStationIds: MutableSet<String> = mutableSetOf()
    private val pendingLiveDetailStationIds: MutableSet<String> = mutableSetOf()

    private var refreshNearbyJob: Job? = null
    private var liveSummaryRefreshJob: Job? = null
    private var selectedFeatureRefreshJob: Job? = null

    init {
        startLiveSummaryRefreshLoop()
    }

    override fun onCleared() {
        refreshNearbyJob?.cancel()
        liveSummaryRefreshJob?.cancel()
        selectedFeatureRefreshJob?.cancel()
        super.onCleared()
    }

    fun load(userLocation: Location?) {
        isLoading = true
        loadError = null

        viewModelScope.launch {
            val result = withContext(Dispatchers.IO) {
                runCatching {
                    val loaded = repository.loadData()
                    val bundle = dataBundleManager.activeBundleInfo()
                    Triple(loaded.features, loaded.operators, bundle)
                }
            }

            isLoading = false
            result.onSuccess { (features, operators, bundle) ->
                resetLiveState()
                allFeatures = features
                this@AppViewModel.operators = operators
                activeBundleInfo = bundle
                loadError = null
                didSeedFromUserLocation = false
                applyFilters(userLocation)
            }.onFailure { error ->
                loadError = error.localizedMessage
                allFeatures = emptyList()
                filterPool = emptyList()
                discoveredFeatures = emptyList()
                operators = emptyList()
                activeBundleInfo = null
                isAwaitingFirstLocationFix = false
                refreshNearbyJob?.cancel()
                resetLiveState()
            }
        }
    }

    fun reloadDataAfterBundleUpdate(userLocation: Location?) {
        load(userLocation)
    }

    suspend fun installBundleFromTreeUri(treeUri: Uri, userLocation: Location?): Result<Unit> {
        val result = withContext(Dispatchers.IO) {
            runCatching {
                dataBundleManager.installBundleFromTreeUri(treeUri)
            }
        }
        if (result.isSuccess) {
            reloadDataAfterBundleUpdate(userLocation)
            applyFilters(userLocation)
        }
        return result
    }

    suspend fun removeInstalledBundle(userLocation: Location?): Result<Unit> {
        val result = withContext(Dispatchers.IO) {
            runCatching {
                dataBundleManager.removeInstalledBundle()
            }
        }
        if (result.isSuccess) {
            reloadDataAfterBundleUpdate(userLocation)
        }
        return result
    }

    fun applyFilters(userLocation: Location?) {
        filterPool = allFeatures.filter { feature -> feature.properties.matches(filterState) }
        resetDiscoveredList()
        if (userLocation != null) {
            didSeedFromUserLocation = true
            lastUserLocationCenter = userLocation.latitude to userLocation.longitude
            isAwaitingFirstLocationFix = false
            refreshNearbyAsync(userLocation.latitude, userLocation.longitude)
        } else {
            didSeedFromUserLocation = false
            lastUserLocationCenter = null
            isAwaitingFirstLocationFix = allFeatures.isNotEmpty()
            discoveredFeatures = emptyList()
        }
    }

    fun handleMapCenterChange(latitude: Double, longitude: Double) {
        didSeedFromUserLocation = true
        isAwaitingFirstLocationFix = false
        refreshNearbyAsync(latitude, longitude)
    }

    fun seedFromInitialUserLocation(location: Location?) {
        if (location == null || allFeatures.isEmpty()) return
        if (!didSeedFromUserLocation) {
            // Start charger discovery from the first real location fix.
            applyFilters(location)
        }
    }

    fun reloadListForCurrentLocation(location: Location?) {
        if (allFeatures.isEmpty()) return
        if (location == null) {
            isAwaitingFirstLocationFix = true
            return
        }
        applyFilters(location)
    }

    fun reloadMapForCenter(latitude: Double?, longitude: Double?) {
        if (allFeatures.isEmpty()) return
        if (latitude == null || longitude == null) {
            isAwaitingFirstLocationFix = true
            return
        }
        handleMapCenterChange(latitude, longitude)
    }

    fun refreshNearbyFromUserLocation(location: Location?, force: Boolean = false) {
        if (allFeatures.isEmpty()) return
        if (location == null) {
            isAwaitingFirstLocationFix = true
            return
        }
        if (!didSeedFromUserLocation) {
            applyFilters(location)
            return
        }
        if (!shouldRefreshUserLocation(lastUserLocationCenter, location.latitude, location.longitude, force)) {
            return
        }
        lastUserLocationCenter = location.latitude to location.longitude
        isAwaitingFirstLocationFix = false
        refreshNearbyAsync(location.latitude, location.longitude)
    }

    fun selectFeature(feature: GeoJsonFeature) {
        val stationId = feature.properties.stationId
        selectedFeature = featureForStationId(stationId) ?: feature
        startSelectedFeatureRefresh(stationId)
    }

    fun clearSelectedFeature() {
        selectedFeature = null
        selectedFeatureRefreshJob?.cancel()
        selectedFeatureRefreshJob = null
    }

    fun featureForStationId(stationId: String): GeoJsonFeature? {
        return allFeatures.firstOrNull { it.properties.stationId == stationId }
            ?: discoveredFeatures.firstOrNull { it.properties.stationId == stationId }
            ?: selectedFeature?.takeIf { it.properties.stationId == stationId }
    }

    suspend fun refreshFavoritesLiveSummaries(favorites: Set<String>, force: Boolean = false) {
        requestLiveSummaries(favorites.toList(), force = force)
    }

    fun favoritesFeatures(favorites: Set<String>, userLocation: Location?): List<GeoJsonFeature> {
        val items = allFeatures.filter { favorites.contains(it.properties.stationId) }.toMutableList()
        if (userLocation != null) {
            items.sortBy {
                distanceMeters(
                    userLocation.latitude,
                    userLocation.longitude,
                    it.latitude,
                    it.longitude
                )
            }
        }
        return items
    }

    fun distanceText(userLocation: Location?, latitude: Double, longitude: Double): String? {
        if (userLocation == null) return null
        val meters = distanceMeters(userLocation.latitude, userLocation.longitude, latitude, longitude)
        return if (meters >= 1000.0) {
            "%.1f km".format(meters / 1000.0)
        } else {
            "${meters.roundToInt()} m"
        }
    }

    fun markerTint(feature: GeoJsonFeature): String {
        val total = feature.properties.amenitiesTotal
        return when {
            total > 10 -> "gold"
            total > 5 -> "silver"
            total > 0 -> "bronze"
            else -> "gray"
        }
    }

    fun humanReadableBundleSource(): String {
        val info = activeBundleInfo ?: return "unbekannt"
        return if (info.source == "installed") {
            "Installiertes Datenbundle (${info.manifest.version})"
        } else {
            "In der App gebündeltes Baseline-Datenbundle"
        }
    }

    suspend fun requestLiveDetailIfNeeded(stationId: String, force: Boolean = false) {
        val trimmedStationId = stationId.trim()
        if (trimmedStationId.isBlank()) return
        if (!liveApiClient.isEnabled) return
        if (pendingLiveDetailStationIds.contains(trimmedStationId)) return

        val now = System.currentTimeMillis()
        if (!force) {
            val lastFetch = liveDetailFetchedAtByStationId[trimmedStationId]
            if (lastFetch != null && now - lastFetch < liveRefreshIntervalMs) {
                return
            }
        }

        pendingLiveDetailStationIds += trimmedStationId
        try {
            val detail = liveApiClient.stationDetail(trimmedStationId)
            liveDetailFetchedAtByStationId[trimmedStationId] = now
            liveSummaryFetchedAtByStationId[trimmedStationId] = now
            applyLiveDetail(trimmedStationId, detail)
        } catch (_: Exception) {
            // Keep offline behavior intact by silently falling back to bundled data.
        } finally {
            pendingLiveDetailStationIds -= trimmedStationId
        }
    }

    suspend fun requestLiveSummaries(stationIds: List<String>, force: Boolean = false) {
        if (!liveApiClient.isEnabled) return

        val normalizedIds = stationIds
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .distinct()

        if (normalizedIds.isEmpty()) return

        val now = System.currentTimeMillis()
        val eligibleIds = normalizedIds.filter { stationId ->
            if (pendingLiveSummaryStationIds.contains(stationId)) {
                return@filter false
            }
            if (force) {
                return@filter true
            }
            val lastFetch = liveSummaryFetchedAtByStationId[stationId]
            lastFetch == null || now - lastFetch >= liveRefreshIntervalMs
        }

        if (eligibleIds.isEmpty()) return

        pendingLiveSummaryStationIds += eligibleIds
        try {
            val response = liveApiClient.lookupStations(eligibleIds)
            val fetchedAt = System.currentTimeMillis()
            (response.stations.map { it.stationId } + response.missingStationIds).forEach { stationId ->
                liveSummaryFetchedAtByStationId[stationId] = fetchedAt
            }
            applyLiveSummaries(response.stations.associateBy { it.stationId }, response.missingStationIds.toSet())
        } catch (_: Exception) {
            // Keep offline behavior intact by silently falling back to bundled data.
        } finally {
            pendingLiveSummaryStationIds -= eligibleIds.toSet()
        }
    }

    private fun startLiveSummaryRefreshLoop() {
        liveSummaryRefreshJob?.cancel()
        liveSummaryRefreshJob = viewModelScope.launch {
            while (isActive) {
                requestLiveSummaries(trackedStationIds())
                delay(liveRefreshIntervalMs)
            }
        }
    }

    private fun trackedStationIds(): List<String> {
        val ids = linkedSetOf<String>()
        discoveredFeatures.mapTo(ids) { it.properties.stationId }
        selectedFeature?.properties?.stationId?.let(ids::add)
        return ids.toList()
    }

    private fun startSelectedFeatureRefresh(stationId: String) {
        selectedFeatureRefreshJob?.cancel()
        selectedFeatureRefreshJob = viewModelScope.launch {
            requestLiveDetailIfNeeded(stationId, force = true)
            while (isActive) {
                delay(liveRefreshIntervalMs)
                if (selectedFeature?.properties?.stationId != stationId) {
                    return@launch
                }
                requestLiveDetailIfNeeded(stationId, force = true)
            }
        }
    }

    private fun applyLiveSummaries(
        summariesByStationId: Map<String, LiveStationSummary>,
        missingStationIds: Set<String>
    ) {
        val affectedIds = summariesByStationId.keys + missingStationIds
        if (affectedIds.isEmpty()) return

        updateFeatureCollections(affectedIds.toSet()) { feature ->
            val stationId = feature.properties.stationId
            when {
                summariesByStationId.containsKey(stationId) -> feature.copy(
                    liveSummary = summariesByStationId.getValue(stationId)
                )
                missingStationIds.contains(stationId) -> feature.copy(liveSummary = null)
                else -> feature
            }
        }
    }

    private fun applyLiveDetail(stationId: String, detail: LiveStationDetail) {
        updateFeatureCollections(setOf(stationId)) { feature ->
            feature.copy(
                liveSummary = detail.station,
                liveDetail = detail
            )
        }
    }

    private fun updateFeatureCollections(
        stationIds: Set<String>,
        updater: (GeoJsonFeature) -> GeoJsonFeature
    ) {
        if (stationIds.isEmpty()) return

        allFeatures = allFeatures.map { feature ->
            if (stationIds.contains(feature.properties.stationId)) updater(feature) else feature
        }
        filterPool = filterPool.map { feature ->
            if (stationIds.contains(feature.properties.stationId)) updater(feature) else feature
        }
        val updatedDiscoveredById = linkedMapOf<String, GeoJsonFeature>()
        for ((id, feature) in discoveredById) {
            updatedDiscoveredById[id] =
                if (stationIds.contains(feature.properties.stationId)) updater(feature) else feature
        }
        discoveredById.clear()
        discoveredById.putAll(updatedDiscoveredById)
        discoveredFeatures = discoveredFeatures.map { feature ->
            if (stationIds.contains(feature.properties.stationId)) updater(feature) else feature
        }
        selectedFeature = selectedFeature?.let { feature ->
            if (stationIds.contains(feature.properties.stationId)) updater(feature) else feature
        }
    }

    private fun resetLiveState() {
        liveSummaryFetchedAtByStationId.clear()
        liveDetailFetchedAtByStationId.clear()
        pendingLiveSummaryStationIds.clear()
        pendingLiveDetailStationIds.clear()
        clearSelectedFeature()
    }

    private fun resetDiscoveredList() {
        refreshNearbyJob?.cancel()
        discoveredById.clear()
        discoveredOrder.clear()
        discoveredFeatures = emptyList()
    }

    private fun refreshNearbyAsync(centerLat: Double, centerLon: Double) {
        val poolSnapshot = filterPool
        if (poolSnapshot.isEmpty()) {
            discoveredFeatures = emptyList()
            return
        }
        refreshNearbyJob?.cancel()
        refreshNearbyJob = viewModelScope.launch {
            val nearest = withContext(Dispatchers.Default) {
                selectNearest(
                    pool = poolSnapshot,
                    centerLat = centerLat,
                    centerLon = centerLon,
                    maxCount = maxVisibleChargers
                )
            }
            if (!isActive) return@launch
            for (feature in nearest) {
                if (!discoveredById.containsKey(feature.id)) {
                    discoveredOrder += feature.id
                }
                discoveredById[feature.id] = feature
            }
            while (discoveredOrder.size > maxDiscoveredHistory) {
                val removedId = discoveredOrder.removeAt(0)
                discoveredById.remove(removedId)
            }
            discoveredFeatures = discoveredOrder.mapNotNull { discoveredById[it] }
            requestLiveSummaries(nearest.map { it.properties.stationId })
        }
    }

    private fun selectNearest(
        pool: List<GeoJsonFeature>,
        centerLat: Double,
        centerLon: Double,
        maxCount: Int
    ): List<GeoJsonFeature> {
        if (pool.isEmpty() || maxCount <= 0) return emptyList()
        val heap = PriorityQueue<Pair<GeoJsonFeature, Double>>(compareByDescending { it.second })
        for (feature in pool) {
            val distance = distanceMeters(centerLat, centerLon, feature.latitude, feature.longitude)
            if (heap.size < maxCount) {
                heap += feature to distance
            } else {
                val farthest = heap.peek() ?: continue
                if (distance < farthest.second) {
                    heap.poll()
                    heap += feature to distance
                }
            }
        }
        return heap
            .toList()
            .sortedBy { it.second }
            .map { it.first }
    }

    private fun distanceMeters(
        latitudeA: Double,
        longitudeA: Double,
        latitudeB: Double,
        longitudeB: Double
    ): Double {
        val latRadA = Math.toRadians(latitudeA)
        val latRadB = Math.toRadians(latitudeB)
        val dLat = latRadB - latRadA
        val dLon = Math.toRadians(longitudeB - longitudeA)

        val a = sin(dLat / 2) * sin(dLat / 2) +
            cos(latRadA) * cos(latRadB) * sin(dLon / 2) * sin(dLon / 2)
        val c = 2 * atan2(sqrt(a), sqrt(1 - a))
        return EARTH_RADIUS_METERS * c
    }

    companion object {
        private const val EARTH_RADIUS_METERS = 6_371_000.0
    }
}

internal fun shouldRefreshUserLocation(
    lastCenter: Pair<Double, Double>?,
    latitude: Double,
    longitude: Double,
    force: Boolean = false
): Boolean {
    if (force) return true
    val last = lastCenter ?: return true
    return haversineDistanceMeters(last.first, last.second, latitude, longitude) > 250.0
}

private fun haversineDistanceMeters(
    latitudeA: Double,
    longitudeA: Double,
    latitudeB: Double,
    longitudeB: Double
): Double {
    val latRadA = Math.toRadians(latitudeA)
    val latRadB = Math.toRadians(latitudeB)
    val dLat = latRadB - latRadA
    val dLon = Math.toRadians(longitudeB - longitudeA)

    val a = sin(dLat / 2) * sin(dLat / 2) +
        cos(latRadA) * cos(latRadB) * sin(dLon / 2) * sin(dLon / 2)
    val c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return 6_371_000.0 * c
}
