package de.woladen.android.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.Menu
import androidx.compose.material.icons.outlined.Map
import androidx.compose.material.icons.outlined.Star
import androidx.compose.material3.Divider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.vector.ImageVector
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.testTagsAsResourceId
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.model.FilterState
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.util.AmenityCatalog
import de.woladen.android.viewmodel.AppViewModel

@OptIn(ExperimentalComposeUiApi::class)
@Composable
fun WoladenAppScreen(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoritesStore: FavoritesStore,
    onRequestLocationPermission: () -> Unit
) {
    var showingFilter by rememberSaveable { mutableStateOf(false) }

    Box(
        modifier = Modifier
            .fillMaxSize()
            .background(MaterialTheme.colorScheme.background)
            .semantics { testTagsAsResourceId = true }
    ) {
        Column(
            modifier = Modifier
                .fillMaxSize()
                .statusBarsPadding()
        ) {
            Box(
                modifier = Modifier.weight(1f)
            ) {
                when (viewModel.selectedTab) {
                    AppViewModel.AppTab.LIST -> {
                        ListTabView(
                            viewModel = viewModel,
                            locationService = locationService,
                            onShowFilter = { showingFilter = true }
                        )
                    }

                    AppViewModel.AppTab.MAP -> {
                        MapTabView(
                            viewModel = viewModel,
                            locationService = locationService,
                            onRequestLocationPermission = onRequestLocationPermission,
                            onShowFilter = { showingFilter = true }
                        )
                    }

                    AppViewModel.AppTab.FAVORITES -> {
                        FavoritesTabView(
                            viewModel = viewModel,
                            favoritesStore = favoritesStore,
                            locationService = locationService
                        )
                    }

                    AppViewModel.AppTab.INFO -> {
                        InfoTabView(
                            viewModel = viewModel,
                            locationService = locationService,
                            onRequestLocationPermission = onRequestLocationPermission
                        )
                    }
                }
            }

            BottomTabBar(
                selectedTab = viewModel.selectedTab,
                onTabSelected = { viewModel.selectedTab = it },
                modifier = Modifier.navigationBarsPadding()
            )
        }
    }

    if (showingFilter) {
        FilterSheetView(
            filter = viewModel.filterState,
            operators = viewModel.operators,
            availableAmenityKeys = availableAmenityKeys(viewModel),
            onDismiss = { showingFilter = false },
            onApply = { newFilter: FilterState ->
                viewModel.filterState = newFilter
                viewModel.applyFilters(locationService.currentLocation)
                showingFilter = false
            }
        )
    }

    viewModel.selectedFeature?.let { feature ->
        StationDetailSheet(
            feature = feature,
            isFavorite = favoritesStore.isFavorite(feature.properties.stationId),
            onToggleFavorite = {
                favoritesStore.toggle(feature.properties.stationId)
            },
            onDismiss = {
                viewModel.selectedFeature = null
            }
        )
    }
}

private fun availableAmenityKeys(viewModel: AppViewModel): List<String> {
    val keys = linkedSetOf<String>()
    for (feature in viewModel.allFeatures) {
        for ((key, count) in feature.properties.amenityCounts) {
            if (count > 0) keys += key
        }
    }
    return keys.sortedBy { AmenityCatalog.labelFor(it) }
}

private data class TabItem(
    val tab: AppViewModel.AppTab,
    val title: String,
    val icon: ImageVector
)

@Composable
private fun BottomTabBar(
    selectedTab: AppViewModel.AppTab,
    onTabSelected: (AppViewModel.AppTab) -> Unit,
    modifier: Modifier = Modifier
) {
    val items = listOf(
        TabItem(AppViewModel.AppTab.LIST, "Liste", Icons.Outlined.Menu),
        TabItem(AppViewModel.AppTab.MAP, "Karte", Icons.Outlined.Map),
        TabItem(AppViewModel.AppTab.FAVORITES, "Favoriten", Icons.Outlined.Star),
        TabItem(AppViewModel.AppTab.INFO, "Info", Icons.Outlined.Info)
    )

    Column(
        modifier = modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
    ) {
        Divider()
        Row(modifier = Modifier.fillMaxWidth()) {
            for (item in items) {
                val selected = selectedTab == item.tab
                TextButton(
                    onClick = { onTabSelected(item.tab) },
                    modifier = Modifier
                        .weight(1f)
                        .testTag("tab-${item.tab.name.lowercase()}")
                        .padding(horizontal = 8.dp, vertical = 6.dp)
                        .clip(RoundedCornerShape(12.dp))
                        .background(
                            if (selected) {
                                MaterialTheme.colorScheme.primary.copy(alpha = 0.14f)
                            } else {
                                MaterialTheme.colorScheme.surface
                            }
                        )
                ) {
                    Column(horizontalAlignment = Alignment.CenterHorizontally) {
                        Icon(
                            imageVector = item.icon,
                            contentDescription = item.title,
                            tint = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Text(
                            text = item.title,
                            color = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                            style = MaterialTheme.typography.labelSmall
                        )
                    }
                }
            }
        }
    }
}
