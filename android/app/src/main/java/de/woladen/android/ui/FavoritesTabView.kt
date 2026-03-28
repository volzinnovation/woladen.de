package de.woladen.android.ui

import androidx.compose.foundation.clickable
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
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.viewmodel.AppViewModel

@Composable
fun FavoritesTabView(
    viewModel: AppViewModel,
    favoritesStore: FavoritesStore,
    locationService: LocationService
) {
    val items = viewModel.favoritesFeatures(favoritesStore.favorites, locationService.currentLocation)

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
                onOpen = { viewModel.selectedFeature = feature },
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
            }

            IconButton(onClick = onRemove) {
                Icon(Icons.Outlined.Delete, contentDescription = "Entfernen")
            }
        }
    }
}
