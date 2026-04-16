package de.woladen.android.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.FilterList
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.displayPrice
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.ui.components.AmenityIcon
import de.woladen.android.ui.components.markerColorForKey
import de.woladen.android.util.AmenityCatalog
import de.woladen.android.viewmodel.AppViewModel

@Composable
fun ListTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    onShowFilter: () -> Unit
) {
    val lifecycleOwner = LocalLifecycleOwner.current

    LaunchedEffect(Unit) {
        locationService.activate()
        viewModel.reloadListForCurrentLocation(locationService.currentLocation)
    }

    LaunchedEffect(locationService.currentLocation) {
        viewModel.refreshNearbyFromUserLocation(locationService.currentLocation)
    }

    DisposableEffect(lifecycleOwner, locationService) {
        val observer = LifecycleEventObserver { _, event ->
            if (event == Lifecycle.Event.ON_RESUME) {
                locationService.activate()
                viewModel.reloadListForCurrentLocation(locationService.currentLocation)
            }
        }
        lifecycleOwner.lifecycle.addObserver(observer)
        onDispose {
            lifecycleOwner.lifecycle.removeObserver(observer)
        }
    }

    Box(modifier = Modifier.fillMaxSize()) {
        when {
            viewModel.loadError != null -> {
                EmptyState(
                    title = "Fehler beim Laden",
                    subtitle = viewModel.loadError.orEmpty(),
                    modifier = Modifier.align(Alignment.Center)
                )
            }

            viewModel.isLoading && viewModel.allFeatures.isEmpty() -> {
                CircularProgressIndicator(modifier = Modifier.align(Alignment.Center))
            }

            viewModel.isAwaitingFirstLocationFix -> {
                EmptyState(
                    title = initialLocationTitle(locationService.authorizationStatus),
                    subtitle = initialLocationDescription(locationService.authorizationStatus),
                    modifier = Modifier.align(Alignment.Center)
                )
            }

            viewModel.discoveredFeatures.isEmpty() -> {
                EmptyState(
                    title = "Keine Ladepunkte",
                    modifier = Modifier.align(Alignment.Center)
                )
            }

            else -> {
                LazyColumn(
                    modifier = Modifier.fillMaxSize()
                ) {
                    items(viewModel.discoveredFeatures, key = { it.id }) { feature ->
                        StationRow(
                            feature = feature,
                            distanceText = viewModel.distanceText(
                                userLocation = locationService.currentLocation,
                                latitude = feature.latitude,
                                longitude = feature.longitude
                            ),
                            markerColor = Color(markerColorForKey(viewModel.markerTint(feature))),
                            onClick = { viewModel.selectFeature(feature) }
                        )
                    }
                }
            }
        }

        IconButton(
            onClick = onShowFilter,
            modifier = Modifier
                .align(Alignment.TopEnd)
                .testTag("list-filter-button")
                .padding(top = 10.dp, end = 14.dp)
                .background(MaterialTheme.colorScheme.surfaceVariant, CircleShape)
        ) {
            Icon(Icons.Filled.FilterList, contentDescription = "Filter")
        }
    }
}

private fun initialLocationTitle(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.DENIED -> "Standortfreigabe benötigt"
        else -> "Warte auf ersten GPS-Fix"
    }
}

private fun initialLocationDescription(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.NOT_DETERMINED ->
            "Nahe Ladepunkte werden geladen, sobald dein Standort freigegeben ist."
        LocationAuthorizationStatus.DENIED ->
            "Aktiviere den Standortzugriff, damit die Liste nahe Ladepunkte laden kann."
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE ->
            "Die Liste lädt Ladepunkte, sobald der erste Standort bestimmt wurde."
    }
}

@Composable
private fun StationRow(
    feature: GeoJsonFeature,
    distanceText: String?,
    markerColor: Color,
    onClick: () -> Unit
) {
    Surface(
        modifier = Modifier
            .fillMaxWidth()
            .testTag("station-row")
            .clickable(onClick = onClick)
    ) {
        Column(modifier = Modifier.padding(horizontal = 16.dp, vertical = 10.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Row(
                    modifier = Modifier.weight(1f),
                    verticalAlignment = Alignment.CenterVertically,
                    horizontalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Box(
                        modifier = Modifier
                            .background(markerColor, CircleShape)
                            .padding(5.dp)
                    )
                    Text(
                        text = feature.properties.operatorName,
                        style = MaterialTheme.typography.titleSmall,
                        maxLines = 1
                    )
                }
                if (distanceText != null) {
                    Text(
                        text = distanceText,
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }

            Text(
                text = "${feature.properties.city} • ${feature.properties.displayedMaxPowerKw.toInt()} kW • ${feature.properties.chargingPointsCount} Ladepunkte",
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )

            val amenities = feature.properties.topAmenities()
            val occupancy = feature.occupancySummaryLabel
            val priceDisplay = feature.displayPrice.trim()
            if (occupancy != null || priceDisplay.isNotBlank()) {
                Row(
                    modifier = Modifier
                        .padding(top = 6.dp)
                        .horizontalScroll(rememberScrollState()),
                    horizontalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    occupancy?.let {
                        ListChip(
                            text = it,
                            containerColor = occupancyColor(feature).copy(alpha = 0.16f),
                            contentColor = occupancyColor(feature)
                        )
                    }

                    if (priceDisplay.isNotBlank()) {
                        ListChip(
                            text = priceDisplay,
                            prefix = "€",
                            containerColor = Color(0x1F15803D),
                            contentColor = Color(0xFF15803D)
                        )
                    }
                }
            }

            if (amenities.isNotEmpty()) {
                Row(
                    modifier = Modifier
                        .padding(top = 6.dp)
                        .horizontalScroll(rememberScrollState()),
                    horizontalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    for (item in amenities) {
                        Row(
                            modifier = Modifier
                                .background(MaterialTheme.colorScheme.surfaceVariant, RoundedCornerShape(12.dp))
                                .padding(horizontal = 8.dp, vertical = 4.dp),
                            horizontalArrangement = Arrangement.spacedBy(4.dp),
                            verticalAlignment = Alignment.CenterVertically
                        ) {
                            AmenityIcon(
                                key = item.key,
                                contentDescription = AmenityCatalog.labelFor(item.key),
                                modifier = Modifier.padding(0.dp)
                            )
                            Text(
                                text = item.count.toString(),
                                style = MaterialTheme.typography.labelSmall
                            )
                        }
                    }
                }
            }
        }
    }
}

private fun occupancyColor(feature: GeoJsonFeature): Color {
    return when (feature.availabilityStatus) {
        AvailabilityStatus.FREE -> Color(0xFF0F766E)
        AvailabilityStatus.OCCUPIED -> Color(0xFFB45309)
        AvailabilityStatus.OUT_OF_ORDER -> Color(0xFFB91C1C)
        AvailabilityStatus.UNKNOWN -> Color.Gray
    }
}

@Composable
private fun ListChip(
    text: String,
    prefix: String? = null,
    containerColor: Color,
    contentColor: Color
) {
    Row(
        modifier = Modifier
            .background(containerColor, RoundedCornerShape(12.dp))
            .padding(horizontal = 8.dp, vertical = 4.dp),
        horizontalArrangement = Arrangement.spacedBy(4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        if (!prefix.isNullOrBlank()) {
            Text(
                text = prefix,
                style = MaterialTheme.typography.labelSmall,
                color = contentColor
            )
        }
        Text(
            text = text,
            style = MaterialTheme.typography.labelSmall,
            color = contentColor
        )
    }
}

@Composable
private fun EmptyState(
    title: String,
    subtitle: String? = null,
    modifier: Modifier = Modifier
) {
    Column(
        modifier = modifier.padding(24.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(title, style = MaterialTheme.typography.titleMedium)
        if (!subtitle.isNullOrBlank()) {
            Text(
                text = subtitle,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}
