package de.woladen.android.ui

import android.location.Location
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.FilterList
import androidx.compose.material.icons.filled.MyLocation
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.ui.components.MainMapView
import de.woladen.android.viewmodel.AppViewModel
import org.maplibre.android.camera.CameraUpdateFactory
import org.maplibre.android.geometry.LatLng
import org.maplibre.android.maps.MapLibreMap

@Composable
fun MapTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    onRequestLocationPermission: () -> Unit,
    onShowFilter: () -> Unit
) {
    var mapViewRef by remember { mutableStateOf<MapLibreMap?>(null) }
    var centerOnNextLocationUpdate by remember { mutableStateOf(false) }
    var lastQueriedCenter by remember { mutableStateOf<Pair<Double, Double>?>(null) }

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
        viewModel.handleMapCenterChange(location.latitude, location.longitude)
    }

    Box(modifier = Modifier.fillMaxSize()) {
        MainMapView(
            features = viewModel.discoveredFeatures,
            userLocation = locationService.currentLocation,
            markerTint = viewModel::markerTint,
            onFeatureTap = { feature -> viewModel.selectedFeature = feature },
            onMapIdle = { lat, lon ->
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

        Row(
            modifier = Modifier
                .align(Alignment.TopEnd)
                .padding(top = 12.dp, end = 16.dp),
            horizontalArrangement = Arrangement.spacedBy(12.dp)
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
                modifier = Modifier.background(
                    color = MaterialTheme.colorScheme.surfaceVariant,
                    shape = CircleShape
                ).testTag("map-location-button")
            ) {
                Icon(Icons.Filled.MyLocation, contentDescription = "Standort")
            }

            IconButton(
                onClick = onShowFilter,
                modifier = Modifier.background(
                    color = MaterialTheme.colorScheme.surfaceVariant,
                    shape = CircleShape
                ).testTag("map-filter-button")
            ) {
                Icon(Icons.Filled.FilterList, contentDescription = "Filter")
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
                    Text("Lade Ladepunkte...")
                }
            }
        }
    }

    LaunchedEffect(mapViewRef, locationService.currentLocation) {
        val location = locationService.currentLocation
        if (location != null && centerOnNextLocationUpdate) {
            centerMap(location)
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
