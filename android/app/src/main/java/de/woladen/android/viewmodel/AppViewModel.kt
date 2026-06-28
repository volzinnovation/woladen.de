package de.woladen.android.viewmodel

import android.app.Application
import android.content.Context
import android.location.Location
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import de.woladen.android.R
import de.woladen.android.model.ActiveCatalogSourceInfo
import de.woladen.android.model.CatalogInfoSummary
import de.woladen.android.model.CatalogSourceManifest
import de.woladen.android.model.FilterState
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.LiveStationDetail
import de.woladen.android.model.LiveStationSummary
import de.woladen.android.model.OperatorEntry
import de.woladen.android.model.RouteEndpoint
import de.woladen.android.model.RouteFilterPayload
import de.woladen.android.model.RouteSummary
import de.woladen.android.model.matches
import de.woladen.android.repository.ChargerRepository
import de.woladen.android.service.LiveApiClient
import de.woladen.android.service.lookupStationIdBatches
import de.woladen.android.util.AppStrings
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
        ROUTE,
        FAVORITES,
        INFO
    }

    var allFeatures: List<GeoJsonFeature> by mutableStateOf(emptyList())
        private set

    var discoveredFeatures: List<GeoJsonFeature> by mutableStateOf(emptyList())
        private set

    var operators: List<OperatorEntry> by mutableStateOf(emptyList())
        private set

    private val filterPreferences = application.getSharedPreferences("woladen", Context.MODE_PRIVATE)

    private var filterStateBacking: FilterState by mutableStateOf(loadSavedFilterState())

    var filterState: FilterState
        get() = filterStateBacking
        set(value) {
            filterStateBacking = value
            saveFilterState(value)
        }

    var selectedFeature: GeoJsonFeature? by mutableStateOf(null)
        private set

    var selectedTab: AppTab by mutableStateOf(AppTab.LIST)

    var loadError: String? by mutableStateOf(null)
        private set

    var isLoading: Boolean by mutableStateOf(false)
        private set

    var isAwaitingFirstLocationFix: Boolean by mutableStateOf(false)
        private set

    var activeCatalogInfo: ActiveCatalogSourceInfo? by mutableStateOf(null)
        private set

    var infoSummary: CatalogInfoSummary? by mutableStateOf(null)
        private set

    var infoSummaryError: String? by mutableStateOf(null)
        private set

    var isLoadingInfoSummary: Boolean by mutableStateOf(false)
        private set

    var routeSummary: RouteSummary? by mutableStateOf(null)
        private set

    var routeFeatures: List<GeoJsonFeature> by mutableStateOf(emptyList())
        private set

    var routeError: String? by mutableStateOf(null)
        private set

    var isLoadingRoute: Boolean by mutableStateOf(false)
        private set

    private val liveApiClient = LiveApiClient()
    private val repository = ChargerRepository(liveApiClient)

    private val maxVisibleChargers = 1_000
    private val maxKnownCatalogFeatures = 1_000
    private val catalogSearchRadiusMeters = 20_000
    private val catalogSearchLimit = LiveApiClient.MAX_CATALOG_SEARCH_RESULTS
    private val liveRefreshIntervalMs = 15_000L

    private var filterPool: List<GeoJsonFeature> = emptyList()
    private val discoveredById: MutableMap<String, GeoJsonFeature> = linkedMapOf()
    private val discoveredOrder: MutableList<String> = mutableListOf()
    private var didSeedFromUserLocation = false
    private var lastUserLocationCenter: Pair<Double, Double>? = null
    private var lastCatalogCenter: Pair<Double, Double>? = null

    private val liveSummaryFetchedAtByStationId: MutableMap<String, Long> = mutableMapOf()
    private val liveDetailFetchedAtByStationId: MutableMap<String, Long> = mutableMapOf()
    private val pendingLiveSummaryStationIds: MutableSet<String> = mutableSetOf()
    private val pendingLiveDetailStationIds: MutableSet<String> = mutableSetOf()
    private val pendingCatalogDetailStationIds: MutableSet<String> = mutableSetOf()
    private var routeCalculatedFilters: RouteFilterPayload? = null

    private var refreshNearbyJob: Job? = null
    private var liveSummaryRefreshJob: Job? = null
    private var selectedFeatureRefreshJob: Job? = null
    private var infoSummaryJob: Job? = null
    private var routeSearchJob: Job? = null

    init {
        startLiveSummaryRefreshLoop()
    }

    override fun onCleared() {
        refreshNearbyJob?.cancel()
        liveSummaryRefreshJob?.cancel()
        selectedFeatureRefreshJob?.cancel()
        infoSummaryJob?.cancel()
        routeSearchJob?.cancel()
        super.onCleared()
    }

    private fun loadSavedFilterState(): FilterState {
        val selectedOperators = filterPreferences
            .getStringSet(FILTER_SELECTED_OPERATOR_NAMES_KEY, emptySet())
            .orEmpty()
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .toSet()
            .ifEmpty {
                filterPreferences.getString(FILTER_OPERATOR_NAME_KEY, "").orEmpty()
                    .trim()
                    .takeIf { it.isNotBlank() }
                    ?.let { setOf(it) }
                    .orEmpty()
            }
        val savedRouteMaxDistanceKm = filterPreferences.getFloat(FILTER_ROUTE_MAX_DISTANCE_KM_KEY, -1f).toDouble()
        return FilterState(
            selectedOperatorNames = selectedOperators,
            minPowerKw = filterPreferences.getFloat(FILTER_MIN_POWER_KW_KEY, 50f).toDouble(),
            minAmenityCount = filterPreferences.getFloat(FILTER_MIN_AMENITY_COUNT_KEY, 0f).toDouble(),
            selectedAmenities = filterPreferences
                .getStringSet(FILTER_SELECTED_AMENITIES_KEY, emptySet())
                .orEmpty()
                .toSet(),
            amenityNameQuery = filterPreferences.getString(FILTER_AMENITY_NAME_QUERY_KEY, "").orEmpty(),
            availableOnly = filterPreferences.getBoolean(FILTER_AVAILABLE_ONLY_KEY, true),
            currentlyOpenOnly = filterPreferences.getBoolean(FILTER_CURRENTLY_OPEN_ONLY_KEY, false),
            routeMaxDistanceFromLocationKm = savedRouteMaxDistanceKm.takeIf { it > 0.0 }?.coerceAtMost(400.0)
        )
    }

    private fun saveFilterState(state: FilterState) {
        val selectedOperators = state.normalizedOperatorNames
        filterPreferences.edit()
            .putStringSet(FILTER_SELECTED_OPERATOR_NAMES_KEY, selectedOperators)
            .putString(FILTER_OPERATOR_NAME_KEY, selectedOperators.singleOrNull().orEmpty())
            .putFloat(FILTER_MIN_POWER_KW_KEY, state.minPowerKw.toFloat())
            .putFloat(FILTER_MIN_AMENITY_COUNT_KEY, state.minAmenityCount.toFloat())
            .putStringSet(FILTER_SELECTED_AMENITIES_KEY, state.selectedAmenities)
            .putString(FILTER_AMENITY_NAME_QUERY_KEY, state.amenityNameQuery)
            .putBoolean(FILTER_AVAILABLE_ONLY_KEY, state.availableOnly)
            .putBoolean(FILTER_CURRENTLY_OPEN_ONLY_KEY, state.currentlyOpenOnly)
            .putFloat(FILTER_ROUTE_MAX_DISTANCE_KM_KEY, state.routeMaxDistanceFromLocationKm?.toFloat() ?: -1f)
            .apply()
    }

    fun load(userLocation: Location?) {
        if (userLocation == null) {
            waitForLocation()
            return
        }

        isLoading = true
        loadError = null

        viewModelScope.launch {
            resetLiveState()
            filterPool = emptyList()
            discoveredFeatures = emptyList()
            allFeatures = emptyList()
            operators = emptyList()
            activeCatalogInfo = ActiveCatalogSourceInfo(
                source = "catalog_api",
                manifest = CatalogSourceManifest(
                    version = "live-eu",
                    generatedAt = "network",
                    schema = "v1/catalog/search"
                )
            )
            loadError = null

            didSeedFromUserLocation = true
            lastUserLocationCenter = userLocation.latitude to userLocation.longitude
            val center = userLocation.latitude to userLocation.longitude
            isAwaitingFirstLocationFix = false
            refreshNearbyAsync(center.first, center.second)
        }
    }

    fun reloadCatalog(userLocation: Location?) {
        if (userLocation == null) {
            waitForLocation()
            return
        }
        repository.invalidateCache()
        load(userLocation)
    }

    fun applyFilters(userLocation: Location?) {
        resetDiscoveredList()
        if (userLocation != null) {
            didSeedFromUserLocation = true
            lastUserLocationCenter = userLocation.latitude to userLocation.longitude
            refreshNearbyAsync(userLocation.latitude, userLocation.longitude)
        } else if (lastCatalogCenter != null) {
            val center = lastCatalogCenter ?: return
            refreshNearbyAsync(center.first, center.second)
        } else {
            waitForLocation()
            return
        }
        isAwaitingFirstLocationFix = false
    }

    fun handleMapCenterChange(latitude: Double, longitude: Double) {
        didSeedFromUserLocation = true
        isAwaitingFirstLocationFix = false
        refreshNearbyAsync(latitude, longitude)
    }

    fun seedFromInitialUserLocation(location: Location?) {
        if (location == null) return
        if (!didSeedFromUserLocation) {
            // Start charger discovery from the first real location fix.
            applyFilters(location)
        }
    }

    fun reloadListForCurrentLocation(location: Location?) {
        if (location == null) {
            waitForLocation()
            return
        }
        applyFilters(location)
    }

    fun reloadMapForCenter(latitude: Double?, longitude: Double?) {
        if (latitude == null || longitude == null) {
            waitForLocation()
            return
        }
        handleMapCenterChange(latitude, longitude)
    }

    fun refreshNearbyFromUserLocation(location: Location?, force: Boolean = false) {
        if (location == null) {
            if (lastCatalogCenter != null && force) {
                val center = lastCatalogCenter ?: return
                refreshNearbyAsync(center.first, center.second)
            } else if (lastCatalogCenter == null) {
                waitForLocation()
            }
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

    private fun waitForLocation() {
        refreshNearbyJob?.cancel()
        isLoading = false
        loadError = null
        isAwaitingFirstLocationFix = true
        if (allFeatures.isEmpty()) {
            filterPool = emptyList()
            discoveredFeatures = emptyList()
            operators = emptyList()
            activeCatalogInfo = null
        }
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
            ?: repository.cachedFeaturesForStationIds(setOf(stationId)).firstOrNull()
            ?: selectedFeature?.takeIf { it.properties.stationId == stationId }
    }

    suspend fun refreshFavoritesLiveSummaries(favorites: Set<String>, force: Boolean = false) {
        refreshFavoriteCatalogDetails(favorites)
        requestLiveSummaries(favorites.toList(), force = force)
    }

    suspend fun refreshFavoriteCatalogDetails(favorites: Set<String>) {
        val favoriteStationIds = favorites
            .map { it.trim() }
            .filter { it.isNotBlank() }

        for (stationId in favoriteStationIds) {
            requestCatalogDetailIfNeeded(stationId)
        }
    }

    fun favoritesFeatures(favorites: Set<String>, userLocation: Location?): List<GeoJsonFeature> {
        val cachedFeatures = repository.cachedFeaturesForStationIds(favorites)
        val byStationId = linkedMapOf<String, GeoJsonFeature>()
        allFeatures
            .filter { favorites.contains(it.properties.stationId) }
            .forEach { byStationId[it.properties.stationId] = it }
        cachedFeatures.forEach { byStationId.putIfAbsent(it.properties.stationId, it) }
        val items = byStationId.values.toMutableList()
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

    fun searchRoute(origin: RouteEndpoint, destination: RouteEndpoint) {
        routeSearchJob?.cancel()
        isLoadingRoute = true
        routeError = null
        routeSummary = null
        routeFeatures = emptyList()
        routeCalculatedFilters = null
        val routeFilter = routeEffectiveFilter()
        val calculatedFilters = RouteFilterPayload.from(routeFilter)
        routeSearchJob = viewModelScope.launch {
            val result = runCatching {
                repository.routeChargers(
                    origin = origin,
                    destination = destination,
                    filterState = routeFilter
                )
            }

            if (!isActive) return@launch
            isLoadingRoute = false
            result.onSuccess { routeResult ->
                routeSummary = routeResult.route
                routeFeatures = routeResult.features
                routeCalculatedFilters = calculatedFilters
                routeError = null
                requestLiveSummaries(routeResult.features.map { it.properties.stationId }, force = true)
            }.onFailure {
                routeSummary = null
                routeFeatures = emptyList()
                routeCalculatedFilters = null
                routeError = AppStrings.get(R.string.i18n_route_searcherror)
            }
        }
    }

    fun clearRoute() {
        routeSearchJob?.cancel()
        isLoadingRoute = false
        routeSummary = null
        routeFeatures = emptyList()
        routeCalculatedFilters = null
        routeError = null
    }

    fun routeDisplayFeatures(userLocation: Location? = null): List<GeoJsonFeature> {
        val filter = routeEffectiveFilter()
        return routeFeatures
            .filter { it.properties.matches(filter) }
            .filter { feature -> feature.matchesRouteRange(filter, userLocation) }
            .sortedWith { left, right -> compareRouteFeatures(left, right) }
    }

    fun routeFiltersRequireRecalculation(): Boolean {
        val baseline = routeCalculatedFilters ?: return false
        val current = RouteFilterPayload.from(routeEffectiveFilter())
        if (baseline.operator.isNotBlank() && current.operator != baseline.operator) return true
        if (baseline.operator.isBlank() && current.operator.isNotBlank()) return false
        if (current.minPowerKw < baseline.minPowerKw) return true
        if (current.minAmenitiesTotal < baseline.minAmenitiesTotal) return true
        val currentAmenities = current.selectedAmenities.toSet()
        if (baseline.selectedAmenities.any { it !in currentAmenities }) return true
        if (baseline.amenityNameQuery.isNotBlank()) {
            val baselineName = baseline.amenityNameQuery.lowercase()
            val currentName = current.amenityNameQuery.lowercase()
            if (currentName.isBlank() || !currentName.contains(baselineName)) return true
        }
        if (baseline.currentlyOpenOnly && !current.currentlyOpenOnly) return true
        return false
    }

    fun routeFilterActiveCount(): Int {
        return routeEffectiveFilter().activeCount
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

    fun humanReadableCatalogSource(): String {
        val info = activeCatalogInfo ?: return AppStrings.get(R.string.i18n_info_sourceunknown)
        return when (info.source) {
            "catalog_api" -> "Live-Katalog via live-eu.woladen.de"
            else -> "Live-Katalog via live-eu.woladen.de"
        }
    }

    fun loadInfoSummaryIfNeeded() {
        if (infoSummary != null || isLoadingInfoSummary) return
        loadInfoSummary(forceRefresh = false)
    }

    fun reloadInfoSummary() {
        loadInfoSummary(forceRefresh = true)
    }

    private fun loadInfoSummary(forceRefresh: Boolean) {
        infoSummaryJob?.cancel()
        isLoadingInfoSummary = true
        infoSummaryError = null
        infoSummaryJob = viewModelScope.launch {
            if (forceRefresh) {
                repository.invalidateInfoSummaryCache()
            }

            val result = runCatching {
                repository.infoSummary()
            }

            if (!isActive) return@launch
            isLoadingInfoSummary = false
            result
                .onSuccess { summary ->
                    infoSummary = summary
                    infoSummaryError = null
                }
                .onFailure { error ->
                    infoSummaryError = error.localizedMessage
                        ?: AppStrings.get(R.string.i18n_info_countryloaderror)
                }
        }
    }

    suspend fun requestCatalogDetailIfNeeded(stationId: String) {
        val trimmedStationId = stationId.trim()
        if (trimmedStationId.isBlank()) return
        if (!liveApiClient.isEnabled) return
        if (pendingCatalogDetailStationIds.contains(trimmedStationId)) return

        pendingCatalogDetailStationIds += trimmedStationId
        try {
            val feature = repository.loadCatalogStationDetail(trimmedStationId)
            applyCatalogFeature(feature)
        } catch (_: Exception) {
            // Search results remain usable when static detail is temporarily unavailable.
        } finally {
            pendingCatalogDetailStationIds -= trimmedStationId
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
            // The catalog row remains visible when the live detail endpoint is temporarily unavailable.
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
            for (batch in lookupStationIdBatches(eligibleIds)) {
                val response = liveApiClient.lookupStations(batch)
                val fetchedAt = System.currentTimeMillis()
                (response.stations.map { it.stationId } + response.missingStationIds).forEach { stationId ->
                    liveSummaryFetchedAtByStationId[stationId] = fetchedAt
                }
                applyLiveSummaries(
                    response.stations.associateBy { it.stationId },
                    response.missingStationIds.toSet()
                )
            }
        } catch (_: Exception) {
            // Keep the last API/cache state visible when live summaries are temporarily unavailable.
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
        routeFeatures.mapTo(ids) { it.properties.stationId }
        selectedFeature?.properties?.stationId?.let(ids::add)
        return ids.toList()
    }

    private fun startSelectedFeatureRefresh(stationId: String) {
        selectedFeatureRefreshJob?.cancel()
        selectedFeatureRefreshJob = viewModelScope.launch {
            requestCatalogDetailIfNeeded(stationId)
            requestLiveDetailIfNeeded(stationId, force = true)
            while (isActive) {
                delay(liveRefreshIntervalMs)
                if (selectedFeature?.properties?.stationId != stationId) {
                    return@launch
                }
                requestCatalogDetailIfNeeded(stationId)
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

    private fun applyCatalogFeature(feature: GeoJsonFeature) {
        mergeKnownFeatures(listOf(feature))
        val stationId = feature.properties.stationId
        updateFeatureCollections(setOf(stationId)) { existing ->
            mergeCatalogFeature(existing, feature)
        }
    }

    private fun mergeKnownFeatures(features: List<GeoJsonFeature>) {
        if (features.isEmpty()) return
        val byStationId = linkedMapOf<String, GeoJsonFeature>()
        for (feature in allFeatures) {
            byStationId[feature.properties.stationId] = feature
        }
        for (feature in features) {
            val stationId = feature.properties.stationId
            byStationId[stationId] = byStationId[stationId]?.let { existing ->
                mergeCatalogFeature(existing, feature)
            } ?: feature
        }
        val removedStationIds = mutableSetOf<String>()
        while (byStationId.size > maxKnownCatalogFeatures) {
            val firstKey = byStationId.keys.firstOrNull() ?: break
            byStationId.remove(firstKey)
            removedStationIds += firstKey
        }
        allFeatures = byStationId.values.toList()
        filterPool = allFeatures.filter { feature -> feature.properties.matches(filterState) }
        removeDiscoveredStationIds(removedStationIds)
        rebuildOperators()
    }

    private fun mergeCatalogFeature(existing: GeoJsonFeature, incoming: GeoJsonFeature): GeoJsonFeature {
        val existingHasRicherAmenities =
            existing.properties.amenityExamples.size > incoming.properties.amenityExamples.size
        return incoming.copy(
            properties = if (existingHasRicherAmenities) existing.properties else incoming.properties,
            liveSummary = incoming.liveSummary ?: existing.liveSummary,
            liveDetail = existing.liveDetail ?: incoming.liveDetail
        )
    }

    private fun rebuildOperators() {
        operators = allFeatures
            .asSequence()
            .map { it.properties.operatorName.trim() }
            .filter { it.isNotBlank() }
            .groupingBy { it }
            .eachCount()
            .map { (name, count) -> OperatorEntry(name = name, stations = count) }
            .sortedWith(compareBy<OperatorEntry> { it.name.lowercase() }.thenBy { it.name })
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
        routeFeatures = routeFeatures.map { feature ->
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
        pendingCatalogDetailStationIds.clear()
        clearSelectedFeature()
    }

    private fun resetDiscoveredList() {
        refreshNearbyJob?.cancel()
        discoveredById.clear()
        discoveredOrder.clear()
        discoveredFeatures = emptyList()
    }

    private fun removeDiscoveredStationIds(stationIds: Set<String>) {
        if (stationIds.isEmpty()) return
        discoveredOrder.removeAll { stationIds.contains(it) }
        stationIds.forEach { discoveredById.remove(it) }
        discoveredFeatures = discoveredOrder.mapNotNull { discoveredById[it] }
    }

    private fun refreshNearbyAsync(centerLat: Double, centerLon: Double) {
        lastCatalogCenter = centerLat to centerLon
        isAwaitingFirstLocationFix = false
        refreshNearbyJob?.cancel()
        refreshNearbyJob = viewModelScope.launch {
            val cachedPool = allFeatures.filter { feature -> feature.properties.matches(filterState) }
            filterPool = cachedPool
            if (cachedPool.isNotEmpty()) {
                publishNearestCatalogFeatures(centerLat, centerLon, cachedPool)
            }

            isLoading = allFeatures.isEmpty() || cachedPool.isEmpty()
            val result = runCatching {
                repository.searchCatalog(
                    latitude = centerLat,
                    longitude = centerLon,
                    radiusMeters = catalogSearchRadiusMeters,
                    limit = catalogSearchLimit,
                    filterState = filterState
                )
            }

            if (!isActive) return@launch
            isLoading = false
            result.onSuccess { catalogResult ->
                mergeKnownFeatures(catalogResult.features)
                filterPool = allFeatures.filter { feature -> feature.properties.matches(filterState) }
                publishNearestCatalogFeatures(centerLat, centerLon, filterPool)
                requestLiveSummaries(discoveredFeatures.map { it.properties.stationId })
                loadError = null
            }.onFailure {
                if (discoveredFeatures.isEmpty()) {
                    loadError = catalogLoadErrorMessage()
                }
            }
        }
    }

    private suspend fun publishNearestCatalogFeatures(
        centerLat: Double,
        centerLon: Double,
        poolSnapshot: List<GeoJsonFeature>
    ) {
        val nearest = withContext(Dispatchers.Default) {
            selectNearest(
                pool = poolSnapshot,
                centerLat = centerLat,
                centerLon = centerLon,
                maxDistanceMeters = catalogSearchRadiusMeters.toDouble(),
                maxCount = maxVisibleChargers
            )
        }
        discoveredById.clear()
        discoveredOrder.clear()
        for (feature in nearest) {
            discoveredOrder += feature.id
            discoveredById[feature.id] = feature
        }
        discoveredFeatures = discoveredOrder.mapNotNull { discoveredById[it] }
    }

    private fun selectNearest(
        pool: List<GeoJsonFeature>,
        centerLat: Double,
        centerLon: Double,
        maxDistanceMeters: Double,
        maxCount: Int
    ): List<GeoJsonFeature> {
        if (pool.isEmpty() || maxCount <= 0) return emptyList()
        val heap = PriorityQueue<Pair<GeoJsonFeature, Double>>(compareByDescending { it.second })
        for (feature in pool) {
            val distance = distanceMeters(centerLat, centerLon, feature.latitude, feature.longitude)
            if (distance > maxDistanceMeters) continue
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

    private fun routeEffectiveFilter(): FilterState {
        return filterState.copy(availableOnly = false)
    }

    private fun GeoJsonFeature.matchesRouteRange(filter: FilterState, userLocation: Location?): Boolean {
        val maxDistanceKm = filter.routeMaxDistanceFromLocationKm ?: return true
        val location = userLocation ?: return false
        val maxDistanceMeters = maxDistanceKm.coerceAtLeast(0.0) * 1000.0
        return distanceMeters(location.latitude, location.longitude, latitude, longitude) <= maxDistanceMeters
    }

    private fun compareRouteFeatures(left: GeoJsonFeature, right: GeoJsonFeature): Int {
        val positionDiff = (left.routeMetadata?.routePositionM ?: Int.MAX_VALUE)
            .compareTo(right.routeMetadata?.routePositionM ?: Int.MAX_VALUE)
        if (positionDiff != 0) return positionDiff
        val accessDiff = (left.routeMetadata?.driveDistanceToRouteM ?: Int.MAX_VALUE)
            .compareTo(right.routeMetadata?.driveDistanceToRouteM ?: Int.MAX_VALUE)
        if (accessDiff != 0) return accessDiff
        val amenitiesDiff = right.properties.amenitiesTotal.compareTo(left.properties.amenitiesTotal)
        if (amenitiesDiff != 0) return amenitiesDiff
        return right.properties.displayedMaxPowerKw.compareTo(left.properties.displayedMaxPowerKw)
    }

    companion object {
        private const val EARTH_RADIUS_METERS = 6371000.0
        private const val FILTER_OPERATOR_NAME_KEY = "filter.operatorName"
        private const val FILTER_SELECTED_OPERATOR_NAMES_KEY = "filter.selectedOperatorNames"
        private const val FILTER_MIN_POWER_KW_KEY = "filter.minPowerKw"
        private const val FILTER_MIN_AMENITY_COUNT_KEY = "filter.minAmenityCount"
        private const val FILTER_SELECTED_AMENITIES_KEY = "filter.selectedAmenities"
        private const val FILTER_AMENITY_NAME_QUERY_KEY = "filter.amenityNameQuery"
        private const val FILTER_AVAILABLE_ONLY_KEY = "filter.availableOnly"
        private const val FILTER_CURRENTLY_OPEN_ONLY_KEY = "filter.currentlyOpenOnly"
        private const val FILTER_ROUTE_MAX_DISTANCE_KM_KEY = "filter.routeMaxDistanceFromLocationKm"
    }
}

internal fun catalogLoadErrorMessage(): String =
    AppStrings.get(R.string.i18n_errors_catalogmessage).ifBlank {
        "No network connection. Sorry, live search will not work until this device is online."
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
