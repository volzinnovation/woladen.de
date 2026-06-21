package de.woladen.android.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.displayPrice
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.viewmodel.AppViewModel
import kotlinx.coroutines.delay

@Composable
fun FavoritesTabView(
    viewModel: AppViewModel,
    favoritesStore: FavoritesStore,
    locationService: LocationService
) {
    val items = viewModel.favoritesFeatures(favoritesStore.favorites, locationService.currentLocation)
    val favoriteSignature = favoritesStore.favorites.sorted().joinToString("|")

    LaunchedEffect(favoriteSignature) {
        while (true) {
            viewModel.refreshFavoriteCatalogDetails(favoritesStore.favorites)
            viewModel.refreshFavoritesLiveSummaries(favoritesStore.favorites, force = true)
            delay(15_000)
        }
    }

    if (items.isEmpty()) {
        Column(
            modifier = Modifier.fillMaxSize(),
            horizontalAlignment = Alignment.CenterHorizontally,
            verticalArrangement = Arrangement.Center
        ) {
            Text("Keine Favoriten", style = MaterialTheme.typography.titleMedium)
        }
        return
    }

    LazyColumn(
        modifier = Modifier.fillMaxSize()
    ) {
        items(items, key = { it.id }) { feature ->
            FavoriteRow(
                feature = feature,
                onOpen = { viewModel.selectFeature(feature) },
                onRemove = { favoritesStore.remove(feature.properties.stationId) }
            )
        }
    }
}

@Composable
private fun FavoriteRow(
    feature: GeoJsonFeature,
    onOpen: () -> Unit,
    onRemove: () -> Unit
) {
    Surface(modifier = Modifier.fillMaxWidth()) {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .testTag("favorites-row")
                .padding(horizontal = 14.dp, vertical = 10.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Column(
                modifier = Modifier
                    .weight(1f)
                    .clickable(onClick = onOpen)
            ) {
                Text(feature.properties.operatorName, style = MaterialTheme.typography.titleMedium)
                Text(feature.properties.city, color = MaterialTheme.colorScheme.onSurfaceVariant)
                Text(
                    "${feature.properties.displayedMaxPowerKw.toInt()} kW max • ${feature.properties.chargingPointsCount} Ladepunkte",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                val occupancy = feature.occupancySummaryLabel
                val priceDisplay = feature.displayPrice
                if (occupancy != null || priceDisplay.isNotBlank()) {
                    Row(
                        modifier = Modifier.horizontalScroll(rememberScrollState()),
                        horizontalArrangement = Arrangement.spacedBy(6.dp)
                    ) {
                        occupancy?.let {
                            FavoriteChip(
                                text = it,
                                containerColor = favoriteOccupancyColor(feature).copy(alpha = 0.16f),
                                contentColor = favoriteOccupancyColor(feature)
                            )
                        }
                        if (priceDisplay.isNotBlank()) {
                            FavoriteChip(
                                text = priceDisplay,
                                prefix = "€",
                                containerColor = Color(0x1F15803D),
                                contentColor = Color(0xFF15803D)
                            )
                        }
                    }
                }
            }

            IconButton(onClick = onRemove) {
                Icon(Icons.Outlined.Delete, contentDescription = "Entfernen")
            }
        }
    }
}

@Composable
private fun FavoriteChip(
    text: String,
    prefix: String? = null,
    containerColor: Color,
    contentColor: Color
) {
    Row(
        modifier = Modifier
            .background(containerColor, androidx.compose.foundation.shape.RoundedCornerShape(12.dp))
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

private fun favoriteOccupancyColor(feature: GeoJsonFeature): Color {
    return when (feature.availabilityStatus) {
        de.woladen.android.model.AvailabilityStatus.FREE -> Color(0xFF0F766E)
        de.woladen.android.model.AvailabilityStatus.OCCUPIED -> Color(0xFFB45309)
        de.woladen.android.model.AvailabilityStatus.OUT_OF_ORDER -> Color(0xFFB91C1C)
        de.woladen.android.model.AvailabilityStatus.UNKNOWN -> Color.Gray
    }
}
