package de.woladen.android.car

import android.Manifest
import android.annotation.SuppressLint
import android.content.Intent
import android.location.Location
import android.location.LocationManager
import android.net.Uri
import android.text.SpannableString
import android.text.Spanned
import androidx.car.app.CarContext
import androidx.car.app.OnRequestPermissionsListener
import androidx.car.app.Screen
import androidx.car.app.model.Action
import androidx.car.app.model.ActionStrip
import androidx.car.app.model.CarColor
import androidx.car.app.model.CarLocation
import androidx.car.app.model.Distance
import androidx.car.app.model.DistanceSpan
import androidx.car.app.model.ItemList
import androidx.car.app.model.ListTemplate
import androidx.car.app.model.Metadata
import androidx.car.app.model.Place
import androidx.car.app.model.PlaceListMapTemplate
import androidx.car.app.model.PlaceMarker
import androidx.car.app.model.Row
import androidx.car.app.model.Template
import de.woladen.android.app.WoladenApplication
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.availabilityCounts
import de.woladen.android.model.displayPrice
import de.woladen.android.model.liveUpdatedLabel
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.model.occupancySourceLabel
import de.woladen.android.repository.ChargerRepository
import de.woladen.android.store.FavoritesStore
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.withContext
import java.util.Locale
import kotlin.math.roundToInt

/** Nearby charger finder rendered with the Android Auto POI templates. */
internal class WoladenNearbyScreen(carContext: CarContext) : Screen(carContext) {
    private val application = carContext.applicationContext as WoladenApplication
    private val repository: ChargerRepository = application.chargerRepository
    private val favoritesStore: FavoritesStore = application.favoritesStore
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)

    private var location: Location? = null
    private var stations: List<GeoJsonFeature> = emptyList()
    private var loading = true
    private var error: String? = null
    private var favoritesMode = false
    private var locationPermissionRequested = false

    init {
        loadStations()
    }

    override fun onGetTemplate(): Template {
        val title = if (favoritesMode) "Favorite chargers" else "Nearby fast chargers"
        val action = Action.Builder()
            .setTitle(if (favoritesMode) "Nearby" else "Favorites")
            .setOnClickListener {
                favoritesMode = !favoritesMode
                loadStations()
            }
            .build()
        val builder = PlaceListMapTemplate.Builder()
            .setTitle(title)
            .setActionStrip(ActionStrip.Builder().addAction(action).build())
            .setCurrentLocationEnabled(location != null)

        if (loading) {
            builder.setLoading(true)
        } else {
            builder.setItemList(buildItemList())
        }
        return builder.build()
    }

    private fun buildItemList(): ItemList {
        val message = error ?: if (favoritesMode) {
            "No favorite chargers saved on your phone"
        } else {
            "No fast chargers found nearby"
        }
        if (stations.isEmpty()) {
            return ItemList.Builder().setNoItemsMessage(message).build()
        }

        val builder = ItemList.Builder()
        stations.take(MAX_CAR_ITEMS).forEach { station ->
            builder.addItem(buildStationRow(station))
        }
        return builder.build()
    }

    private fun buildStationRow(station: GeoJsonFeature): Row {
        val distanceMeters = location?.let { origin -> distanceMeters(origin, station) }
        val distanceLabel = distanceMeters?.let { formatDistance(it) } ?: ""
        val status = station.availabilityStatus.label
        val counts = station.availabilityCounts
        val availability = station.occupancySummaryLabel
            ?: if (counts.total > 0) status else "Live status unavailable"
        val power = station.properties.displayedMaxPowerKw
            .takeIf { it > 0.0 }
            ?.let { "${formatNumber(it)} kW" }
        val amenities = station.properties.topAmenities(2)
            .takeIf { it.isNotEmpty() }
            ?.joinToString(" • ") { "${it.count} ${it.key.removePrefix("amenity_")}" }
        val detail = listOfNotNull(
            availability,
            power,
            station.displayPrice.takeIf { it.isNotBlank() },
            amenities,
            station.occupancySourceLabel ?: station.liveUpdatedLabel
        ).joinToString(" • ")
        val title = station.properties.operatorName.ifBlank { station.properties.city }
            .ifBlank { station.id }
        val text = if (distanceLabel.isBlank()) detail else "$distanceLabel • $detail"
        val spannedText = SpannableString(text)
        if (distanceMeters != null && text.startsWith(distanceLabel)) {
            spannedText.setSpan(
                DistanceSpan.create(
                    Distance.create(distanceMeters / 1000.0, Distance.UNIT_KILOMETERS_P1)
                ),
                0,
                distanceLabel.length,
                Spanned.SPAN_EXCLUSIVE_EXCLUSIVE
            )
        }
        val marker = PlaceMarker.Builder()
            .setColor(CarColor.BLUE)
            .setLabel("EV")
            .build()
        val metadata = Metadata.Builder()
            .setPlace(
                Place.Builder(CarLocation.create(station.latitude, station.longitude))
                    .setMarker(marker)
                    .build()
            )
            .build()
        return Row.Builder()
            .setTitle(title)
            .addText(spannedText)
            .setMetadata(metadata)
            .setOnClickListener {
                screenManager.push(WoladenStationDetailScreen(carContext, station))
            }
            .build()
    }

    private fun loadStations() {
        if (!hasLocationPermission()) {
            requestLocationPermission()
            return
        }
        favoritesStore.refreshFromDisk()
        val currentLocation = lastKnownLocation()
        if (!favoritesMode && currentLocation == null) {
            location = null
            stations = emptyList()
            loading = false
            error = "Location is unavailable or stale. Refresh location on your phone and try again."
            invalidate()
            return
        }
        location = currentLocation
        loading = true
        error = null
        invalidate()
        scope.launch {
            val result = runCatching {
                if (favoritesMode) {
                    loadFavorites()
                } else {
                    loadNearby(currentLocation!!)
                }
            }
            result.onSuccess {
                stations = it
                error = null
            }.onFailure {
                stations = emptyList()
                error = "Could not load chargers. Try again from your phone."
            }
            loading = false
            invalidate()
        }
    }

    private suspend fun loadNearby(origin: Location): List<GeoJsonFeature> {
        val result = withContext(Dispatchers.IO) {
            application.filterStateStore.refresh()
            repository.searchCatalog(
                latitude = origin.latitude,
                longitude = origin.longitude,
                radiusMeters = SEARCH_RADIUS_METERS,
                limit = SEARCH_LIMIT,
                filterState = application.filterStateStore.state
            )
        }
        return enrichLive(result.features)
            .sortedBy { distanceMeters(origin, it) }
    }

    private suspend fun loadFavorites(): List<GeoJsonFeature> {
        val ids = favoritesStore.favorites.take(MAX_CAR_ITEMS)
        if (ids.isEmpty()) return emptyList()
        val features = coroutineScope {
            ids.map { id ->
                async(Dispatchers.IO) {
                    runCatching { repository.loadCatalogStationDetail(id) }.getOrNull()
                }
            }.awaitAll().filterNotNull()
        }
        return enrichLive(features)
    }

    private suspend fun enrichLive(features: List<GeoJsonFeature>): List<GeoJsonFeature> {
        if (features.isEmpty()) return emptyList()
        val liveById = withTimeoutOrNull(LIVE_LOOKUP_TIMEOUT_MS) {
            runCatching {
                withContext(Dispatchers.IO) {
                    application.liveApiClient.lookupStations(features.map { it.properties.stationId })
                        .stations
                        .associateBy { it.stationId }
                }
            }.getOrDefault(emptyMap())
        } ?: emptyMap()
        return features.map { feature ->
            liveById[feature.properties.stationId]?.let { feature.copy(liveSummary = it) } ?: feature
        }
    }

    private fun requestLocationPermission() {
        if (locationPermissionRequested) return
        locationPermissionRequested = true
        loading = false
        error = "Location permission is required to find nearby chargers"
        invalidate()
        carContext.requestPermissions(
            listOf(Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.ACCESS_COARSE_LOCATION),
            object : OnRequestPermissionsListener {
                override fun onRequestPermissionsResult(
                    grantedPermissions: List<String>,
                    rejectedPermissions: List<String>
                ) {
                    if (grantedPermissions.any {
                            it == Manifest.permission.ACCESS_FINE_LOCATION ||
                                it == Manifest.permission.ACCESS_COARSE_LOCATION
                        }
                    ) {
                        locationPermissionRequested = false
                        loadStations()
                    } else {
                        loading = false
                        error = "Location permission was denied. Enable it on your phone to search nearby."
                        invalidate()
                    }
                }
            }
        )
    }

    private fun hasLocationPermission(): Boolean {
        return carContext.checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) ==
            android.content.pm.PackageManager.PERMISSION_GRANTED ||
            carContext.checkSelfPermission(Manifest.permission.ACCESS_COARSE_LOCATION) ==
            android.content.pm.PackageManager.PERMISSION_GRANTED
    }

    @SuppressLint("MissingPermission")
    private fun lastKnownLocation(): Location? {
        val manager = carContext.getSystemService(LocationManager::class.java) ?: return null
        val candidate = manager.getProviders(true)
            .mapNotNull { provider -> runCatching { manager.getLastKnownLocation(provider) }.getOrNull() }
            .maxByOrNull { it.time }
        return candidate?.takeIf {
            it.time <= 0L || System.currentTimeMillis() - it.time <= MAX_LOCATION_AGE_MS
        }
    }

    companion object {
        private const val SEARCH_RADIUS_METERS = 20_000
        private const val SEARCH_LIMIT = 20
        private const val MAX_CAR_ITEMS = 6
        private const val LIVE_LOOKUP_TIMEOUT_MS = 3_000L
        private const val MAX_LOCATION_AGE_MS = 15 * 60 * 1_000L
    }
}

internal class WoladenStationDetailScreen(
    carContext: CarContext,
    initialFeature: GeoJsonFeature
) : Screen(carContext) {
    private val application = carContext.applicationContext as WoladenApplication
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private var feature = initialFeature
    private var loading = true

    init {
        scope.launch {
            feature = runCatching {
                val detail = withContext(Dispatchers.IO) {
                    application.chargerRepository.loadCatalogStationDetail(initialFeature.properties.stationId)
                }
                val live = withContext(Dispatchers.IO) {
                    runCatching { application.liveApiClient.stationDetail(initialFeature.properties.stationId) }.getOrNull()
                }
                if (live == null) detail else detail.copy(liveSummary = live.station, liveDetail = live)
            }.getOrElse { initialFeature }
            loading = false
            invalidate()
        }
    }

    override fun onGetTemplate(): Template {
        val properties = feature.properties
        val stationTitle = properties.operatorName.ifBlank { properties.city }.ifBlank { feature.id }
        val rows = mutableListOf<Row>()
        rows += Row.Builder()
            .setTitle("Navigate")
            .addText("Open this charger in the car navigation app")
            .setOnClickListener {
                carContext.startCarApp(
                    Intent(CarContext.ACTION_NAVIGATE, Uri.parse("geo:${feature.latitude},${feature.longitude}"))
                )
            }
            .build()
        rows += Row.Builder()
            .setTitle(if (application.favoritesStore.isFavorite(feature.id)) "Remove favorite" else "Save favorite")
            .addText("Synced with woladen on your phone")
            .setOnClickListener {
                application.favoritesStore.toggle(feature.id)
                invalidate()
            }
            .build()
        val address = listOf(properties.address, properties.postcode, properties.city)
            .filter { it.isNotBlank() }
            .joinToString(", ")
        if (address.isNotBlank()) rows += Row.Builder().setTitle("Address").addText(address).build()
        val availability = feature.occupancySummaryLabel
            ?: feature.availabilityStatus.label
        rows += Row.Builder().setTitle("Availability").addText(availability).build()
        feature.occupancySourceLabel?.let {
            rows += Row.Builder().setTitle("Live data").addText(it).build()
        }
        properties.displayedMaxPowerKw.takeIf { it > 0.0 }?.let {
            rows += Row.Builder().setTitle("Power").addText("${formatNumber(it)} kW • ${properties.chargingPointsCount} charging points").build()
        }
        feature.displayPrice.takeIf { it.isNotBlank() }?.let {
            rows += Row.Builder().setTitle("Price").addText(it).build()
        }
        properties.openingHoursDisplay.takeIf { it.isNotBlank() }?.let {
            rows += Row.Builder().setTitle("Opening hours").addText(it).build()
        }
        properties.topAmenities(2).takeIf { it.isNotEmpty() }?.let { amenities ->
            rows += Row.Builder().setTitle("Nearby").addText(amenities.joinToString(" • ") { "${it.count} ${it.key.removePrefix("amenity_")}" }).build()
        }
        if (loading) rows += Row.Builder().setTitle("Updating live status…").build()
        return ListTemplate.Builder()
            .setTitle(stationTitle)
            .setHeaderAction(Action.BACK)
            .setSingleList(ItemList.Builder().apply { rows.take(10).forEach(::addItem) }.build())
            .build()
    }

}

private fun distanceMeters(origin: Location, station: GeoJsonFeature): Float {
    val result = FloatArray(1)
    Location.distanceBetween(origin.latitude, origin.longitude, station.latitude, station.longitude, result)
    return result[0]
}

private fun formatDistance(meters: Float): String {
    return if (meters < 1000f) {
        "${meters.roundToInt()} m"
    } else {
        String.format(Locale.getDefault(), "%.1f km", meters / 1000.0)
    }
}

private fun formatNumber(value: Double): String {
    return if (value % 1.0 == 0.0) value.toInt().toString() else String.format(Locale.getDefault(), "%.1f", value)
}
