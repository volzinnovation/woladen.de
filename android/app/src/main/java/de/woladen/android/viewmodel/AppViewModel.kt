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
import de.woladen.android.model.OperatorEntry
import de.woladen.android.model.matches
import de.woladen.android.repository.ChargerRepository
import de.woladen.android.repository.DataBundleManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
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

    var selectedTab: AppTab by mutableStateOf(AppTab.LIST)

    var loadError: String? by mutableStateOf(null)
        private set

    var isLoading: Boolean by mutableStateOf(false)
        private set

    var activeBundleInfo: ActiveDataBundleInfo? by mutableStateOf(null)
        private set

    private val dataBundleManager = DataBundleManager(application.applicationContext)
    private val repository = ChargerRepository(dataBundleManager)

    private val maxVisibleChargers = 20
    private val maxDiscoveredHistory = 200
    private var filterPool: List<GeoJsonFeature> = emptyList()
    private val discoveredById: MutableMap<String, GeoJsonFeature> = linkedMapOf()
    private val discoveredOrder: MutableList<String> = mutableListOf()
    private var didSeedFromUserLocation = false
    private var refreshNearbyJob: Job? = null

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
                refreshNearbyJob?.cancel()
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
        didSeedFromUserLocation = false
        if (userLocation != null) {
            didSeedFromUserLocation = true
            refreshNearbyAsync(userLocation.latitude, userLocation.longitude)
        }
    }

    fun handleMapCenterChange(latitude: Double, longitude: Double) {
        refreshNearbyAsync(latitude, longitude)
    }

    fun seedFromInitialUserLocation(location: Location?) {
        if (location == null || allFeatures.isEmpty()) return
        if (!didSeedFromUserLocation) {
            if (discoveredFeatures.isEmpty()) {
                applyFilters(location)
            } else {
                refreshNearbyAsync(location.latitude, location.longitude)
            }
            didSeedFromUserLocation = true
        }
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
