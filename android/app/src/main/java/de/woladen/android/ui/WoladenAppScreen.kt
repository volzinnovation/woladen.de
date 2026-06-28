package de.woladen.android.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.Image
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.BoxWithConstraints
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxHeight
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.VerticalDivider
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.ExperimentalComposeUiApi
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.semantics.testTagsAsResourceId
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import de.woladen.android.R
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
        BoxWithConstraints(modifier = Modifier.fillMaxSize()) {
            val usesWideLayout = maxWidth >= 840.dp
            if (usesWideLayout) {
                WideAppLayout(
                    viewModel = viewModel,
                    locationService = locationService,
                    favoritesStore = favoritesStore,
                    onRequestLocationPermission = onRequestLocationPermission,
                    onShowFilter = { showingFilter = true }
                )
            } else {
                CompactAppLayout(
                    viewModel = viewModel,
                    locationService = locationService,
                    favoritesStore = favoritesStore,
                    onRequestLocationPermission = onRequestLocationPermission,
                    onShowFilter = { showingFilter = true }
                )
            }
            if (!usesWideLayout) {
                viewModel.selectedFeature?.let { feature ->
                    StationDetailSheet(
                        feature = feature,
                        isFavorite = favoritesStore.isFavorite(feature.properties.stationId),
                        favoritesStore = favoritesStore,
                        onToggleFavorite = {
                            favoritesStore.toggle(feature.properties.stationId)
                        },
                        onDismiss = {
                            viewModel.clearSelectedFeature()
                        }
                    )
                }
            }

            if (showingFilter) {
                FilterSheetView(
                    filter = viewModel.filterState,
                    operators = viewModel.operators,
                    availableAmenityKeys = availableAmenityKeys(viewModel),
                    useWideDialog = usesWideLayout,
                    onDismiss = { showingFilter = false },
                    onApply = { newFilter: FilterState ->
                        viewModel.filterState = newFilter
                        viewModel.applyFilters(locationService.currentLocation)
                        showingFilter = false
                    }
                )
            }
        }
    }

}

@Composable
private fun CompactAppLayout(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoritesStore: FavoritesStore,
    onRequestLocationPermission: () -> Unit,
    onShowFilter: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxSize()
            .statusBarsPadding()
    ) {
        Box(modifier = Modifier.weight(1f)) {
            AppTabContent(
                viewModel = viewModel,
                locationService = locationService,
                favoritesStore = favoritesStore,
                onRequestLocationPermission = onRequestLocationPermission,
                onShowFilter = onShowFilter
            )
        }

        BottomTabBar(
            selectedTab = viewModel.selectedTab,
            onTabSelected = { viewModel.selectedTab = it },
            modifier = Modifier.navigationBarsPadding()
        )
    }
}

@Composable
private fun WideAppLayout(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoritesStore: FavoritesStore,
    onRequestLocationPermission: () -> Unit,
    onShowFilter: () -> Unit
) {
    Box(
        modifier = Modifier
            .fillMaxSize()
            .statusBarsPadding()
    ) {
        Row(modifier = Modifier.fillMaxSize()) {
            WideNavigationRail(
                selectedTab = viewModel.selectedTab,
                onTabSelected = { viewModel.selectedTab = it }
            )
            VerticalDivider()
            Box(modifier = Modifier.weight(1f)) {
                AppTabContent(
                    viewModel = viewModel,
                    locationService = locationService,
                    favoritesStore = favoritesStore,
                    onRequestLocationPermission = onRequestLocationPermission,
                    onShowFilter = onShowFilter
                )
            }
        }
        if (viewModel.selectedTab != AppViewModel.AppTab.INFO) {
            viewModel.selectedFeature?.let { feature ->
                StationDetailWideDialog(
                    feature = feature,
                    isFavorite = favoritesStore.isFavorite(feature.properties.stationId),
                    favoritesStore = favoritesStore,
                    onToggleFavorite = {
                        favoritesStore.toggle(feature.properties.stationId)
                    },
                    onDismiss = {
                        viewModel.clearSelectedFeature()
                    }
                )
            }
        }
    }
}

@Composable
private fun AppTabContent(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoritesStore: FavoritesStore,
    onRequestLocationPermission: () -> Unit,
    onShowFilter: () -> Unit
) {
    when (viewModel.selectedTab) {
        AppViewModel.AppTab.LIST -> {
            ListTabView(
                viewModel = viewModel,
                locationService = locationService,
                favoriteStationIds = favoritesStore.favorites,
                onToggleFavorite = { stationId -> favoritesStore.toggle(stationId) },
                onShowFilter = onShowFilter
            )
        }
        AppViewModel.AppTab.MAP -> {
            MapTabView(
                viewModel = viewModel,
                locationService = locationService,
                favoriteStationIds = favoritesStore.favorites,
                onRequestLocationPermission = onRequestLocationPermission,
                onShowFilter = onShowFilter
            )
        }
        AppViewModel.AppTab.ROUTE -> {
            RouteTabView(
                viewModel = viewModel,
                locationService = locationService,
                favoritesStore = favoritesStore,
                onShowFilter = onShowFilter
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
    val iconResId: Int
)

@Composable
private fun WideNavigationRail(
    selectedTab: AppViewModel.AppTab,
    onTabSelected: (AppViewModel.AppTab) -> Unit
) {
    val items = tabItems()
    Column(
        modifier = Modifier
            .fillMaxHeight()
            .width(108.dp)
            .background(MaterialTheme.colorScheme.surface)
            .padding(horizontal = 8.dp, vertical = 14.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
        horizontalAlignment = Alignment.CenterHorizontally
    ) {
        for (item in items) {
            val selected = selectedTab == item.tab
            TextButton(
                onClick = { onTabSelected(item.tab) },
                contentPadding = PaddingValues(horizontal = 0.dp, vertical = 8.dp),
                modifier = Modifier
                    .testTag("rail-${item.tab.name.lowercase()}")
                    .width(78.dp)
                    .clip(RoundedCornerShape(16.dp))
                    .background(
                        if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else Color.Transparent
                    )
            ) {
                Column(horizontalAlignment = Alignment.CenterHorizontally, verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    Icon(
                        painter = painterResource(item.iconResId),
                        contentDescription = item.title,
                        tint = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                        modifier = Modifier.size(34.dp)
                    )
                    Text(
                        text = item.title,
                        color = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                        style = bottomNavigationLabelStyle(),
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis,
                        textAlign = TextAlign.Center
                    )
                }
            }
        }
    }
}

@Composable
fun WoladenBrandIntro(
    showProductMessage: Boolean,
    modifier: Modifier = Modifier
) {
    Column(
        modifier = modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.background)
            .padding(horizontal = 16.dp, vertical = if (showProductMessage) 10.dp else 6.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(10.dp, Alignment.CenterHorizontally)
        ) {
            Image(
                painter = painterResource(R.mipmap.ic_launcher),
                contentDescription = null,
                modifier = Modifier
                    .size(34.dp)
                    .clip(RoundedCornerShape(9.dp)),
                contentScale = ContentScale.Crop
            )
            Text(
                text = "woladen",
                style = MaterialTheme.typography.titleSmall,
                color = MaterialTheme.colorScheme.primary,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }

        if (showProductMessage) {
            Text(
                text = stringResource(R.string.i18n_seo_productmessage),
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
    HorizontalDivider()
}

@Composable
private fun BottomTabBar(
    selectedTab: AppViewModel.AppTab,
    onTabSelected: (AppViewModel.AppTab) -> Unit,
    modifier: Modifier = Modifier
) {
    val items = tabItems()

    Column(
        modifier = modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.surface)
    ) {
        HorizontalDivider()
        Row(modifier = Modifier.fillMaxWidth()) {
            for (item in items) {
                val selected = selectedTab == item.tab
                TextButton(
                    onClick = { onTabSelected(item.tab) },
                    contentPadding = PaddingValues(horizontal = 0.dp, vertical = 8.dp),
                    modifier = Modifier
                        .weight(1f)
                        .testTag("tab-${item.tab.name.lowercase()}")
                        .padding(horizontal = 3.dp, vertical = 6.dp)
                        .clip(RoundedCornerShape(12.dp))
                        .background(
                            if (selected) {
                                MaterialTheme.colorScheme.primary.copy(alpha = 0.14f)
                            } else {
                                MaterialTheme.colorScheme.surface
                            }
                        )
                ) {
                    Column(
                        horizontalAlignment = Alignment.CenterHorizontally,
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Icon(
                            painter = painterResource(item.iconResId),
                            contentDescription = item.title,
                            tint = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.size(34.dp)
                        )
                        Text(
                            text = item.title,
                            color = if (selected) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                            style = bottomNavigationLabelStyle(),
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis,
                            textAlign = TextAlign.Center,
                            modifier = Modifier.fillMaxWidth()
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun tabItems(): List<TabItem> {
    return listOf(
        TabItem(AppViewModel.AppTab.LIST, stringResource(R.string.i18n_nav_list), R.drawable.ic_nav_list),
        TabItem(AppViewModel.AppTab.MAP, stringResource(R.string.i18n_nav_map), R.drawable.ic_nav_map),
        TabItem(AppViewModel.AppTab.ROUTE, stringResource(R.string.i18n_nav_route), R.drawable.ic_nav_route),
        TabItem(AppViewModel.AppTab.FAVORITES, stringResource(R.string.i18n_nav_favorites), R.drawable.ic_nav_favorites),
        TabItem(AppViewModel.AppTab.INFO, stringResource(R.string.i18n_nav_info), R.drawable.ic_nav_info)
    )
}

@Composable
private fun bottomNavigationLabelStyle() = MaterialTheme.typography.labelSmall.copy(
    fontSize = 14.sp,
    lineHeight = 18.sp
)
