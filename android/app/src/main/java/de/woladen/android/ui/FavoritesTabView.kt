package de.woladen.android.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.isSystemInDarkTheme
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
import androidx.compose.runtime.getValue
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import de.woladen.android.R
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.displayPrice
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.service.LocationService
import de.woladen.android.store.FAVORITE_CATEGORY_UNCATEGORIZED
import de.woladen.android.store.FAVORITE_FILTER_ALL
import de.woladen.android.store.FavoritesStore
import de.woladen.android.store.categoryKey
import de.woladen.android.viewmodel.AppViewModel
import kotlinx.coroutines.delay

@Composable
fun FavoritesTabView(
    viewModel: AppViewModel,
    favoritesStore: FavoritesStore,
    locationService: LocationService
) {
    var categoryFilter by rememberSaveable { mutableStateOf(FAVORITE_FILTER_ALL) }
    val filterItems = favoriteFilterItems(favoritesStore)
    if (filterItems.none { it.id == categoryFilter }) {
        categoryFilter = FAVORITE_FILTER_ALL
    }
    val matchingIds = favoritesStore.favoriteIdsMatching(categoryFilter)
    val favoriteFeatures = viewModel.favoritesFeatures(matchingIds, locationService.currentLocation)
    val favoriteSignature = favoritesStore.favorites.sorted().joinToString("|")

    LaunchedEffect(favoriteSignature) {
        while (true) {
            viewModel.refreshFavoriteCatalogDetails(favoritesStore.favorites)
            viewModel.refreshFavoritesLiveSummaries(favoritesStore.favorites, force = true)
            delay(15_000)
        }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        FavoriteCategoryFilterBar(
            items = filterItems,
            selected = categoryFilter,
            onSelected = { categoryFilter = it }
        )

        if (favoriteFeatures.isEmpty()) {
            val hasSavedFavorites = favoritesStore.favorites.isNotEmpty()
            Column(
                modifier = Modifier.fillMaxSize(),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.Center
            ) {
                Text(
                    if (hasSavedFavorites) {
                        stringResource(R.string.i18n_favorites_loading)
                    } else {
                        stringResource(R.string.i18n_favorites_empty)
                    },
                    style = MaterialTheme.typography.titleMedium
                )
                if (!hasSavedFavorites) {
                    Text(
                        stringResource(R.string.i18n_favorites_emptyhelp),
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
            }
            return@Column
        }

        val groups = favoriteGroups(favoritesStore, categoryFilter, favoriteFeatures)
        LazyColumn(
            modifier = Modifier.fillMaxSize()
        ) {
            for (group in groups) {
                item(key = "heading-${group.id}") {
                    Row(
                        modifier = Modifier
                            .fillMaxWidth()
                            .padding(horizontal = 14.dp, vertical = 8.dp),
                        verticalAlignment = Alignment.CenterVertically
                    ) {
                        Text(group.label, style = MaterialTheme.typography.titleMedium)
                        Text(
                            groupCountLabel(group.features.size),
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            modifier = Modifier.padding(start = 8.dp)
                        )
                    }
                }
                items(group.features, key = { "${group.id}-${it.id}" }) { feature ->
                    FavoriteRow(
                        feature = feature,
                        categories = favoritesStore.categoriesFor(feature.properties.stationId),
                        onOpen = { viewModel.selectFeature(feature) },
                        onRemove = { favoritesStore.remove(feature.properties.stationId) }
                    )
                }
            }
        }
    }
}

@Composable
private fun FavoriteCategoryFilterBar(
    items: List<FavoriteFilterItem>,
    selected: String,
    onSelected: (String) -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .horizontalScroll(rememberScrollState())
            .padding(horizontal = 14.dp, vertical = 8.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        for (item in items) {
            val active = selected == item.id
            Row(
                modifier = Modifier
                    .background(
                        if (active) MaterialTheme.colorScheme.primary.copy(alpha = 0.14f)
                        else MaterialTheme.colorScheme.surfaceVariant,
                        androidx.compose.foundation.shape.RoundedCornerShape(16.dp)
                    )
                    .clickable { onSelected(item.id) }
                    .padding(horizontal = 10.dp, vertical = 8.dp),
                horizontalArrangement = Arrangement.spacedBy(6.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Text(
                    item.label,
                    style = MaterialTheme.typography.labelLarge,
                    color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
                )
                Text(
                    item.count.toString(),
                    style = MaterialTheme.typography.labelSmall,
                    color = if (active) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier
                        .background(MaterialTheme.colorScheme.surface, androidx.compose.foundation.shape.RoundedCornerShape(12.dp))
                        .padding(horizontal = 6.dp, vertical = 2.dp)
                )
            }
        }
    }
}

@Composable
private fun FavoriteRow(
    feature: GeoJsonFeature,
    categories: List<String>,
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
                    "${feature.properties.displayedMaxPowerKw.toInt()} kW max • ${chargingPointLabel(feature.properties.chargingPointsCount)}",
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
                if (categories.isNotEmpty()) {
                    Text(
                        categories.joinToString(" • "),
                        style = MaterialTheme.typography.labelSmall,
                        color = MaterialTheme.colorScheme.primary,
                        maxLines = 1
                    )
                }
                val occupancy = feature.occupancySummaryLabel
                val priceDisplay = feature.displayPrice
                val priceColor = favoritePriceChipColor()
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
                                containerColor = priceColor.copy(alpha = 0.14f),
                                contentColor = priceColor
                            )
                        }
                    }
                }
            }

            IconButton(onClick = onRemove) {
                Icon(Icons.Outlined.Delete, contentDescription = stringResource(R.string.i18n_aria_removefavorite))
            }
        }
    }
}

private data class FavoriteFilterItem(
    val id: String,
    val label: String,
    val count: Int
)

private data class FavoriteGroup(
    val id: String,
    val label: String,
    val features: List<GeoJsonFeature>
)

@Composable
private fun favoriteFilterItems(favoritesStore: FavoritesStore): List<FavoriteFilterItem> {
    val filterItems = mutableListOf(
        FavoriteFilterItem(
            id = FAVORITE_FILTER_ALL,
            label = stringResource(R.string.i18n_favorites_all),
            count = favoritesStore.favorites.size
        )
    )
    for (category in favoritesStore.sortedCategories()) {
        val key = categoryKey(category)
        filterItems += FavoriteFilterItem(
            id = key,
            label = category,
            count = favoritesStore.countMatching(key)
        )
    }
    val uncategorizedCount = favoritesStore.countMatching(FAVORITE_CATEGORY_UNCATEGORIZED)
    if (uncategorizedCount > 0) {
        filterItems += FavoriteFilterItem(
            id = FAVORITE_CATEGORY_UNCATEGORIZED,
            label = stringResource(R.string.i18n_favorites_uncategorized),
            count = uncategorizedCount
        )
    }
    return filterItems
}

@Composable
private fun favoriteGroups(
    favoritesStore: FavoritesStore,
    selectedFilter: String,
    features: List<GeoJsonFeature>
): List<FavoriteGroup> {
    if (selectedFilter != FAVORITE_FILTER_ALL) {
        val label = favoriteFilterItems(favoritesStore).firstOrNull { it.id == selectedFilter }?.label ?: selectedFilter
        return listOf(FavoriteGroup(selectedFilter, label, features))
    }

    val groups = mutableListOf<FavoriteGroup>()
    for (category in favoritesStore.sortedCategories()) {
        val key = categoryKey(category)
        val groupFeatures = features.filter { feature ->
            favoritesStore.categoriesFor(feature.properties.stationId).any { categoryKey(it) == key }
        }
        if (groupFeatures.isNotEmpty()) {
            groups += FavoriteGroup(key, category, groupFeatures)
        }
    }
    val uncategorized = features.filter {
        favoritesStore.categoriesFor(it.properties.stationId).isEmpty()
    }
    if (uncategorized.isNotEmpty()) {
        groups += FavoriteGroup(
            FAVORITE_CATEGORY_UNCATEGORIZED,
            stringResource(R.string.i18n_favorites_uncategorized),
            uncategorized
        )
    }
    return groups.ifEmpty {
        listOf(FavoriteGroup(FAVORITE_FILTER_ALL, stringResource(R.string.i18n_favorites_all), features))
    }
}

@Composable
private fun groupCountLabel(count: Int): String {
    val template = stringResource(
        if (count == 1) {
            R.string.i18n_favorites_groupcountone
        } else {
            R.string.i18n_favorites_groupcountmany
        }
    )
    return template.replace("{count}", count.toString())
}

@Composable
private fun chargingPointLabel(count: Int): String {
    val template = stringResource(
        if (count == 1) {
            R.string.i18n_station_chargingpointone
        } else {
            R.string.i18n_station_chargingpointmany
        }
    )
    return template.replace("{count}", count.toString())
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

@Composable
private fun favoriteOccupancyColor(feature: GeoJsonFeature): Color {
    val isDarkMode = isSystemInDarkTheme()
    return when (feature.availabilityStatus) {
        de.woladen.android.model.AvailabilityStatus.FREE -> if (isDarkMode) Color(0xFF5EEAD4) else Color(0xFF0F766E)
        de.woladen.android.model.AvailabilityStatus.OCCUPIED -> if (isDarkMode) Color(0xFFFBBF24) else Color(0xFFB45309)
        de.woladen.android.model.AvailabilityStatus.OUT_OF_ORDER -> if (isDarkMode) Color(0xFFF87171) else Color(0xFFB91C1C)
        de.woladen.android.model.AvailabilityStatus.UNKNOWN -> MaterialTheme.colorScheme.onSurfaceVariant
    }
}

@Composable
private fun favoritePriceChipColor(): Color =
    if (isSystemInDarkTheme()) Color(0xFF86EFAC) else Color(0xFF15803D)
