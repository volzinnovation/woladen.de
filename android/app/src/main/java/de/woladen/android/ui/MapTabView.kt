package de.woladen.android.ui

import android.content.Context
import android.content.Intent
import android.location.Geocoder
import android.location.Location
import android.net.Uri
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.FilterList
import androidx.compose.material.icons.filled.MyLocation
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextField
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import de.woladen.android.R
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.ui.components.MainMapView
import de.woladen.android.viewmodel.AppViewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.maplibre.android.camera.CameraUpdateFactory
import org.maplibre.android.geometry.LatLng
import org.maplibre.android.geometry.LatLngBounds
import org.maplibre.android.maps.MapLibreMap
import java.util.Locale

private const val MAP_GPS_REFRESH_INTERVAL_MS = 5 * 60 * 1000L

@Composable
fun MapTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoriteStationIds: Set<String>,
    onRequestLocationPermission: () -> Unit,
    onShowFilter: () -> Unit
) {
    val lifecycleOwner = LocalLifecycleOwner.current
    var mapViewRef by remember { mutableStateOf<MapLibreMap?>(null) }
    var centerOnNextLocationUpdate by remember { mutableStateOf(false) }
    var hasCenteredInitialLocation by remember { mutableStateOf(false) }
    var lastQueriedCenter by remember { mutableStateOf<Pair<Double, Double>?>(null) }
    var searchQuery by rememberSaveable { mutableStateOf("") }
    var searchError by remember { mutableStateOf<String?>(null) }
    var isSearchingPlace by remember { mutableStateOf(false) }
    val context = LocalContext.current
    val coroutineScope = rememberCoroutineScope()
    val routeSummary = viewModel.routeSummary
    val currentLocation = locationService.currentLocation
    val mapFeatures = if (routeSummary != null) {
        viewModel.routeDisplayFeatures(currentLocation)
    } else {
        viewModel.discoveredFeatures
    }
    val routeCoordinates = routeSummary?.geometry?.coordinates.orEmpty()

    fun centerMap(location: Location) {
        val map = mapViewRef ?: return
        centerOnNextLocationUpdate = false
        map.animateCamera(
            CameraUpdateFactory.newLatLngZoom(
                LatLng(location.latitude, location.longitude),
                12.8
            )
        )
        lastQueriedCenter = location.latitude to location.longitude
        if (routeSummary == null) {
            viewModel.handleMapCenterChange(location.latitude, location.longitude)
        }
    }

    fun focusMapOnRoute() {
        val map = mapViewRef ?: return
        val points = routeCoordinates.mapNotNull { point ->
            if (point.size < 2) {
                null
            } else {
                val lon = point[0]
                val lat = point[1]
                if (lat.isFinite() && lon.isFinite()) LatLng(lat, lon) else null
            }
        } + mapFeatures.map { LatLng(it.latitude, it.longitude) }
        if (points.isEmpty()) return
        val boundsBuilder = LatLngBounds.Builder()
        points.forEach(boundsBuilder::include)
        runCatching {
            map.animateCamera(CameraUpdateFactory.newLatLngBounds(boundsBuilder.build(), 44))
        }
        hasCenteredInitialLocation = true
    }

    fun searchPlace() {
        val query = searchQuery.trim()
        if (query.isBlank() || isSearchingPlace) return
        val map = mapViewRef ?: return
        searchError = null
        isSearchingPlace = true
        coroutineScope.launch {
            val address = withContext(Dispatchers.IO) {
                @Suppress("DEPRECATION")
                runCatching {
                    Geocoder(context, Locale.getDefault())
                        .getFromLocationName(query, 1)
                        ?.firstOrNull()
                }.getOrNull()
            }
            if (address == null) {
                searchError = context.getString(R.string.i18n_search_noresults)
            } else {
                val lat = address.latitude
                val lon = address.longitude
                map.animateCamera(CameraUpdateFactory.newLatLngZoom(LatLng(lat, lon), 12.8))
                lastQueriedCenter = lat to lon
                hasCenteredInitialLocation = true
                if (routeSummary == null) {
                    viewModel.handleMapCenterChange(lat, lon)
                }
            }
            isSearchingPlace = false
        }
    }

    Box(modifier = Modifier.fillMaxSize()) {
        MainMapView(
            features = mapFeatures,
            routeCoordinates = routeCoordinates,
            userLocation = currentLocation,
            favoriteStationIds = favoriteStationIds,
            markerTint = viewModel::markerTint,
            onFeatureTap = { feature -> viewModel.selectFeature(feature) },
            onFeatureLongPress = { feature -> openGoogleNavigation(context, feature) },
            onMapIdle = { lat, lon ->
                if (routeSummary != null) return@MainMapView
                if (!hasCenteredInitialLocation) return@MainMapView
                val shouldQuery = shouldQuery(lastQueriedCenter, lat, lon)
                if (shouldQuery) {
                    lastQueriedCenter = lat to lon
                    viewModel.handleMapCenterChange(lat, lon)
                }
            },
            onMapReady = { mapViewRef = it },
            modifier = Modifier
                .fillMaxSize()
                .testTag("map-view-host")
        )

        Column(
            modifier = Modifier
                .align(Alignment.TopCenter)
                .padding(horizontal = 16.dp, vertical = 12.dp)
                .widthIn(max = 620.dp)
                .fillMaxWidth(),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Surface(
                shape = RoundedCornerShape(percent = 50),
                tonalElevation = 4.dp,
                shadowElevation = 6.dp
            ) {
                Row(
                    modifier = Modifier.padding(8.dp),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    IconButton(
                        onClick = {
                            centerOnNextLocationUpdate = true
                            if (locationService.authorizationStatus == LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE) {
                                locationService.requestSingleLocation()
                                locationService.startUpdates()
                                locationService.currentLocation?.let(::centerMap)
                            } else {
                                onRequestLocationPermission()
                            }
                        },
                        modifier = Modifier
                            .background(
                                color = MaterialTheme.colorScheme.surfaceVariant,
                                shape = CircleShape
                            )
                            .testTag("map-location-button")
                    ) {
                        Icon(Icons.Filled.MyLocation, contentDescription = stringResource(R.string.i18n_aria_locate))
                    }

                    TextField(
                        value = searchQuery,
                        onValueChange = {
                            searchQuery = it
                            searchError = null
                        },
                        placeholder = { Text(stringResource(R.string.i18n_search_placeholder)) },
                        singleLine = true,
                        keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                        keyboardActions = KeyboardActions(onSearch = { searchPlace() }),
                        modifier = Modifier
                            .weight(1f)
                            .testTag("map-search-input")
                    )

                    Box {
                        IconButton(
                            onClick = onShowFilter,
                            modifier = Modifier
                                .background(
                                    color = MaterialTheme.colorScheme.surfaceVariant,
                                    shape = CircleShape
                                )
                                .testTag("map-filter-button")
                        ) {
                            Icon(Icons.Filled.FilterList, contentDescription = stringResource(R.string.i18n_aria_filteropen))
                        }
                        if (viewModel.filterState.activeCount > 0) {
                            Text(
                                text = viewModel.filterState.activeCount.toString(),
                                style = MaterialTheme.typography.labelSmall,
                                color = MaterialTheme.colorScheme.onPrimary,
                                modifier = Modifier
                                    .align(Alignment.TopEnd)
                                    .background(MaterialTheme.colorScheme.primary, CircleShape)
                                    .padding(horizontal = 5.dp)
                            )
                        }
                    }
                }
            }

            searchError?.let {
                Surface(shape = RoundedCornerShape(percent = 50), tonalElevation = 3.dp) {
                    Text(
                        text = it,
                        style = MaterialTheme.typography.labelMedium,
                        modifier = Modifier.padding(horizontal = 12.dp, vertical = 7.dp)
                    )
                }
            }
        }

        if (viewModel.isLoading && viewModel.allFeatures.isEmpty()) {
            Surface(
                modifier = Modifier
                    .align(Alignment.TopStart)
                    .padding(12.dp)
            ) {
                Row(
                    modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp),
                    horizontalArrangement = Arrangement.spacedBy(8.dp),
                    verticalAlignment = Alignment.CenterVertically
                ) {
                    CircularProgressIndicator(modifier = Modifier.padding(2.dp))
                    Text(stringResource(R.string.i18n_list_loading))
                }
            }
        }

        if (viewModel.isAwaitingFirstLocationFix) {
            Surface(
                modifier = Modifier
                    .align(Alignment.Center)
                    .padding(horizontal = 24.dp)
            ) {
                Column(
                    modifier = Modifier.padding(horizontal = 16.dp, vertical = 12.dp),
                    verticalArrangement = Arrangement.spacedBy(4.dp)
                ) {
                    Text(mapInitialLocationTitle(locationService.authorizationStatus))
                    Text(mapInitialLocationDescription(locationService.authorizationStatus))
                }
            }
        }
    }

    LaunchedEffect(Unit) {
        locationService.activate()
    }

    LaunchedEffect(mapViewRef, locationService.authorizationStatus) {
        if (routeSummary != null) return@LaunchedEffect
        if (mapViewRef == null || hasCenteredInitialLocation) return@LaunchedEffect
        val location = locationService.currentLocation
        if (location != null) {
            centerMap(location)
            hasCenteredInitialLocation = true
        } else if (locationService.authorizationStatus == LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE) {
            centerOnNextLocationUpdate = true
            locationService.activate()
        } else {
            centerOnNextLocationUpdate = true
            locationService.activate()
            viewModel.reloadMapForCenter(null, null)
        }
    }

    LaunchedEffect(mapViewRef, locationService.currentLocation) {
        if (routeSummary != null) return@LaunchedEffect
        val location = locationService.currentLocation
        if (location != null && (centerOnNextLocationUpdate || !hasCenteredInitialLocation)) {
            centerMap(location)
            hasCenteredInitialLocation = true
        }
    }

    LaunchedEffect(mapViewRef, locationService.authorizationStatus) {
        while (isActive) {
            delay(MAP_GPS_REFRESH_INTERVAL_MS)
            if (mapViewRef == null) continue
            if (viewModel.routeSummary != null) continue
            if (locationService.authorizationStatus != LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE) {
                continue
            }
            locationService.requestSingleLocation()
            val location = locationService.currentLocation ?: continue
            lastQueriedCenter = location.latitude to location.longitude
            viewModel.refreshNearbyFromUserLocation(location, force = true)
        }
    }

    DisposableEffect(lifecycleOwner, locationService, lastQueriedCenter) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_RESUME) {
                locationService.activate()
                if (viewModel.routeSummary != null) {
                    focusMapOnRoute()
                    return@LifecycleEventObserver
                }
                val location = locationService.currentLocation
                if (location != null) {
                    viewModel.reloadMapForCenter(
                        latitude = lastQueriedCenter?.first ?: location.latitude,
                        longitude = lastQueriedCenter?.second ?: location.longitude
                    )
                }
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
        }
    }

    LaunchedEffect(mapViewRef, routeSummary, routeCoordinates.size, mapFeatures.size) {
        if (routeSummary != null) {
            focusMapOnRoute()
        }
    }
}

private fun shouldQuery(lastQueriedCenter: Pair<Double, Double>?, lat: Double, lon: Double): Boolean {
    val last = lastQueriedCenter ?: return true
    val out = FloatArray(1)
    Location.distanceBetween(last.first, last.second, lat, lon, out)
    // iOS parity: only refresh discovered-nearby candidates after a meaningful map movement.
    return out[0] > 250f
}

private fun openGoogleNavigation(context: Context, feature: GeoJsonFeature) {
    val url = "https://www.google.com/maps/dir/?api=1&destination=${feature.latitude},${feature.longitude}"
    val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
    runCatching {
        context.startActivity(intent)
    }
}

@Composable
private fun mapInitialLocationTitle(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.DENIED -> stringResource(R.string.i18n_location_deniedtitle)
        else -> stringResource(R.string.i18n_location_pendingtitle)
    }
}

@Composable
private fun mapInitialLocationDescription(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.NOT_DETERMINED ->
            stringResource(R.string.i18n_location_idlemessage)
        LocationAuthorizationStatus.DENIED ->
            stringResource(R.string.i18n_location_deniedmessage)
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE ->
            stringResource(R.string.i18n_location_pendingmessage)
    }
}
