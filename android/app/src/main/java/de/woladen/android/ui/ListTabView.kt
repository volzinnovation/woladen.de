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
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.model.GeoJsonFeature
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
                            onClick = { viewModel.selectedFeature = feature }
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
            val occupancy = feature.properties.occupancySummaryLabel
            val priceDisplay = feature.properties.priceDisplay.trim()
            if (occupancy != null || priceDisplay.isNotBlank() || amenities.isNotEmpty()) {
                Row(
                    modifier = Modifier
                        .padding(top = 6.dp)
                        .horizontalScroll(rememberScrollState()),
                    horizontalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    occupancy?.let {
                        ListChip(
                            text = it,
                            containerColor = Color(0x1F0F766E),
                            contentColor = Color(0xFF0F766E)
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
