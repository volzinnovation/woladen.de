package de.woladen.android.ui

import android.content.Context
import android.content.Intent
import android.net.Uri
import androidx.compose.foundation.Image
import androidx.compose.foundation.ExperimentalFoundationApi
import androidx.compose.foundation.background
import androidx.compose.foundation.combinedClickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.PaddingValues
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.lazy.grid.GridCells
import androidx.compose.foundation.lazy.grid.LazyVerticalGrid
import androidx.compose.foundation.lazy.grid.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.FilterList
import androidx.compose.material.icons.filled.Star
import androidx.compose.material.ExperimentalMaterialApi
import androidx.compose.material.pullrefresh.PullRefreshIndicator
import androidx.compose.material.pullrefresh.pullRefresh
import androidx.compose.material.pullrefresh.rememberPullRefreshState
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedCard
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalLifecycleOwner
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.painterResource
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver
import de.woladen.android.R
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.FilterState
import de.woladen.android.model.StationCardState
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.displayPrice
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.model.stationCardState
import androidx.compose.material.icons.outlined.NearMe
import androidx.compose.material.icons.outlined.Star
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.ui.components.AmenityIcon
import de.woladen.android.ui.components.markerColorForKey
import de.woladen.android.util.AmenityCatalog
import de.woladen.android.viewmodel.AppViewModel

@OptIn(ExperimentalMaterialApi::class)
@Composable
fun ListTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoriteStationIds: Set<String>,
    onToggleFavorite: (String) -> Unit,
    onShowFilter: () -> Unit
) {
    val lifecycleOwner = LocalLifecycleOwner.current
    var isPullRefreshing by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        locationService.activate()
        viewModel.reloadListForCurrentLocation(locationService.currentLocation)
    }

    LaunchedEffect(locationService.currentLocation) {
        viewModel.refreshNearbyFromUserLocation(locationService.currentLocation)
    }

    LaunchedEffect(viewModel.isLoading, isPullRefreshing) {
        if (isPullRefreshing && !viewModel.isLoading) {
            isPullRefreshing = false
        }
    }

    fun refreshList() {
        isPullRefreshing = true
        locationService.activate()
        val location = locationService.currentLocation
        if (location != null) {
            viewModel.reloadCatalog(location)
        } else {
            viewModel.reloadListForCurrentLocation(null)
        }
    }

    val pullRefreshState = rememberPullRefreshState(
        refreshing = isPullRefreshing,
        onRefresh = ::refreshList
    )

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

    Column(modifier = Modifier.fillMaxSize()) {
        ListHeader(viewModel = viewModel, onShowFilter = onShowFilter)
        if (viewModel.filterState.activeCount > 0) {
            ActiveFilterSummary(
                viewModel = viewModel,
                locationService = locationService
            )
        }

        Box(
            modifier = Modifier
                .weight(1f)
                .fillMaxWidth()
                .pullRefresh(pullRefreshState)
        ) {
            when {
                viewModel.loadError != null -> {
                    EmptyState(
                        title = stringResource(R.string.i18n_errors_catalogtitle),
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
                        title = stringResource(R.string.i18n_list_empty),
                        modifier = Modifier.align(Alignment.Center)
                    )
                }

                else -> {
                    LazyVerticalGrid(
                        columns = GridCells.Adaptive(minSize = 360.dp),
                        modifier = Modifier.fillMaxSize(),
                        contentPadding = PaddingValues(
                            start = 14.dp,
                            top = 8.dp,
                            end = 14.dp,
                            bottom = 12.dp
                        ),
                        horizontalArrangement = Arrangement.spacedBy(10.dp),
                        verticalArrangement = Arrangement.spacedBy(10.dp)
                    ) {
                        items(viewModel.discoveredFeatures, key = { it.id }) { feature ->
                            StationRow(
                                feature = feature,
                                distanceText = viewModel.distanceText(
                                    userLocation = locationService.currentLocation,
                                    latitude = feature.latitude,
                                    longitude = feature.longitude
                                ),
                                isFavorite = favoriteStationIds.contains(feature.properties.stationId),
                                markerColor = Color(markerColorForKey(viewModel.markerTint(feature))),
                                onClick = { viewModel.selectFeature(feature) },
                                onToggleFavorite = { onToggleFavorite(feature.properties.stationId) }
                            )
                        }
                    }
                }
            }
            PullRefreshIndicator(
                refreshing = isPullRefreshing,
                state = pullRefreshState,
                modifier = Modifier.align(Alignment.TopCenter),
                backgroundColor = MaterialTheme.colorScheme.surface,
                contentColor = MaterialTheme.colorScheme.primary
            )
        }
    }
}

@Composable
private fun ListHeader(
    viewModel: AppViewModel,
    onShowFilter: () -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.background)
            .padding(horizontal = 16.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Row(
            modifier = Modifier.weight(1f),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(10.dp)
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
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.primary,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
        TextButton(
            onClick = onShowFilter,
            modifier = Modifier
                .testTag("list-filter-button")
                .clip(RoundedCornerShape(12.dp))
                .background(MaterialTheme.colorScheme.surfaceVariant)
        ) {
            Icon(Icons.Filled.FilterList, contentDescription = null)
            Text(stringResource(R.string.i18n_filters_title))
            if (viewModel.filterState.activeCount > 0) {
                Text(" ${viewModel.filterState.activeCount}")
            }
        }
    }
    HorizontalDivider()
}

@Composable
private fun ActiveFilterSummary(
    viewModel: AppViewModel,
    locationService: LocationService
) {
    val labels = activeFilterLabels(viewModel.filterState)
    if (labels.isEmpty()) return
    val summaryText = stringResource(R.string.i18n_filters_selectedonly)
        .replace("{labels}", labels.joinToString(" · "))

    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.06f))
            .padding(horizontal = 14.dp, vertical = 6.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Icon(
            imageVector = Icons.Filled.FilterList,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.primary
        )
        Text(
            text = summaryText,
            style = MaterialTheme.typography.labelMedium,
            color = MaterialTheme.colorScheme.primary,
            modifier = Modifier.weight(1f),
            maxLines = 1,
            overflow = TextOverflow.Ellipsis
        )
        if (viewModel.filterState.hasClearableFilters) {
            IconButton(
                onClick = {
                    viewModel.filterState = viewModel.filterState.clearableState
                    val location = locationService.currentLocation
                    if (location != null) {
                        viewModel.reloadCatalog(location)
                    } else {
                        viewModel.applyFilters(null)
                    }
                },
                modifier = Modifier
                    .size(42.dp)
                    .clip(CircleShape)
                    .background(MaterialTheme.colorScheme.primary.copy(alpha = 0.12f))
            ) {
                Icon(
                    imageVector = Icons.Filled.Close,
                    contentDescription = stringResource(R.string.i18n_filters_reset),
                    tint = MaterialTheme.colorScheme.primary,
                    modifier = Modifier.size(20.dp)
                )
            }
        }
    }
    HorizontalDivider()
}

private val FilterState.hasClearableFilters: Boolean
    get() = operatorName.isNotBlank() ||
        amenityNameQuery.isNotBlank() ||
        availableOnly ||
        currentlyOpenOnly ||
        selectedAmenities.isNotEmpty() ||
        minPowerKw.toInt() != 50 ||
        minAmenityCount.toInt() != 0

@Composable
private fun activeFilterLabels(filter: FilterState): List<String> {
    val labels = mutableListOf<String>()
    if (filter.operatorName.isNotBlank()) {
        labels += filter.operatorName
    }
    val query = filter.amenityNameQuery.trim()
    if (query.isNotBlank()) {
        labels += stringResource(R.string.i18n_filters_nameprefix).replace("{value}", query)
    }
    if (filter.availableOnly) {
        labels += stringResource(R.string.i18n_filters_availableonly)
    }
    if (filter.currentlyOpenOnly) {
        labels += stringResource(R.string.i18n_filters_currentlyopen)
    }
    if (filter.minPowerKw > 0.0) {
        labels += stringResource(R.string.i18n_filters_minpowerlabel)
            .replace("{value}", filter.minPowerKw.toInt().toString())
    }
    if (filter.minAmenityCount > 0.0) {
        labels += stringResource(R.string.i18n_filters_minamenitieslabel)
            .replace("{value}", filter.minAmenityCount.toInt().toString())
    }
    labels += filter.selectedAmenities
        .map { AmenityCatalog.labelFor(it) }
        .sorted()
    return labels
}

@Composable
private fun initialLocationTitle(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.DENIED -> stringResource(R.string.i18n_location_deniedtitle)
        else -> stringResource(R.string.i18n_location_pendingtitle)
    }
}

@Composable
private fun initialLocationDescription(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.NOT_DETERMINED ->
            stringResource(R.string.i18n_location_idlemessage)
        LocationAuthorizationStatus.DENIED ->
            stringResource(R.string.i18n_location_deniedmessage)
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE ->
            stringResource(R.string.i18n_location_pendingmessage)
    }
}

@OptIn(ExperimentalFoundationApi::class)
@Composable
private fun StationRow(
    feature: GeoJsonFeature,
    distanceText: String?,
    isFavorite: Boolean,
    markerColor: Color,
    onClick: () -> Unit,
    onToggleFavorite: () -> Unit
) {
    val context = LocalContext.current
    var menuExpanded by remember { mutableStateOf(false) }

    Box(modifier = Modifier.fillMaxWidth()) {
        OutlinedCard(
            modifier = Modifier
                .fillMaxWidth()
                .testTag("station-row")
                .combinedClickable(
                    onClick = onClick,
                    onLongClick = { menuExpanded = true }
                ),
            colors = CardDefaults.outlinedCardColors(
                containerColor = stationCardBackground(feature)
            )
        ) {
            Column(modifier = Modifier.padding(horizontal = 18.dp, vertical = 16.dp)) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Row(
                        modifier = Modifier.weight(1f),
                        verticalAlignment = Alignment.CenterVertically,
                        horizontalArrangement = Arrangement.spacedBy(8.dp)
                    ) {
                        if (isFavorite) {
                            Icon(
                                imageVector = Icons.Filled.Star,
                                contentDescription = stringResource(R.string.i18n_info_legendfavorite),
                                tint = Color(0xFFF59E0B),
                                modifier = Modifier.size(24.dp)
                            )
                        } else {
                            Box(
                                modifier = Modifier
                                    .size(18.dp)
                                    .background(markerColor, CircleShape)
                            )
                        }
                        Text(
                            text = feature.properties.operatorName,
                            style = MaterialTheme.typography.titleSmall,
                            maxLines = 1,
                            overflow = TextOverflow.Ellipsis
                        )
                    }
                    if (distanceText != null) {
                        Text(
                            text = distanceText,
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }

                Text(
                    text = "${feature.properties.city} • ${feature.properties.displayedMaxPowerKw.toInt()} kW • ${chargingPointLabel(feature.properties.chargingPointsCount)}",
                    style = MaterialTheme.typography.bodyMedium,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )

                val amenities = feature.properties.topAmenities()
                val occupancy = feature.occupancySummaryLabel
                val priceDisplay = feature.displayPrice.trim()
                val priceColor = priceChipColor()
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
                                containerColor = priceColor.copy(alpha = 0.14f),
                                contentColor = priceColor
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
                                    modifier = Modifier.size(28.dp)
                                )
                                Text(
                                    text = item.count.toString(),
                                    style = MaterialTheme.typography.labelMedium
                                )
                            }
                        }
                    }
                }
            }
        }

        DropdownMenu(
            expanded = menuExpanded,
            onDismissRequest = { menuExpanded = false }
        ) {
            DropdownMenuItem(
                text = { Text("Google") },
                leadingIcon = { Icon(Icons.Outlined.NearMe, contentDescription = null) },
                onClick = {
                    menuExpanded = false
                    openStationNavigation(context, feature, google = true)
                }
            )
            DropdownMenuItem(
                text = { Text("Maps") },
                leadingIcon = { Icon(Icons.Outlined.NearMe, contentDescription = null) },
                onClick = {
                    menuExpanded = false
                    openStationNavigation(context, feature, google = false)
                }
            )
            DropdownMenuItem(
                text = {
                    Text(
                        stringResource(
                            if (isFavorite) {
                                R.string.i18n_aria_removefavorite
                            } else {
                                R.string.i18n_aria_savefavorite
                            }
                        )
                    )
                },
                leadingIcon = {
                    Icon(
                        imageVector = if (isFavorite) Icons.Filled.Star else Icons.Outlined.Star,
                        contentDescription = null,
                        tint = if (isFavorite) Color(0xFFF59E0B) else MaterialTheme.colorScheme.onSurfaceVariant
                    )
                },
                onClick = {
                    menuExpanded = false
                    onToggleFavorite()
                }
            )
        }
    }
}

@Composable
private fun stationCardBackground(feature: GeoJsonFeature): Color {
    val isDarkMode = isSystemInDarkTheme()
    return when (feature.stationCardState) {
        StationCardState.OUT_OF_ORDER -> if (isDarkMode) Color(0xFF3B121C) else Color(0xFFFFF1F2)
        StationCardState.OCCUPIED -> if (isDarkMode) Color(0xFF26323D) else Color(0xFFE2E8F0)
        StationCardState.ONE_FREE_LEFT -> if (isDarkMode) Color(0xFF332B12) else Color(0xFFFFFBEB)
        StationCardState.OFTEN_BROKEN -> if (isDarkMode) Color(0xFF36161F) else Color(0xFFFFF7F8)
        StationCardState.OFTEN_OCCUPIED -> if (isDarkMode) Color(0xFF0F1E27) else Color(0xFFF8FAFC)
        StationCardState.UNKNOWN -> if (isDarkMode) Color(0xFF16212B) else Color(0xFFF1F5F9)
        StationCardState.DEFAULT -> MaterialTheme.colorScheme.surface
    }
}

@Composable
private fun occupancyColor(feature: GeoJsonFeature): Color {
    val isDarkMode = isSystemInDarkTheme()
    return when (feature.availabilityStatus) {
        AvailabilityStatus.FREE -> if (isDarkMode) Color(0xFF5EEAD4) else Color(0xFF0F766E)
        AvailabilityStatus.OCCUPIED -> if (isDarkMode) Color(0xFFFBBF24) else Color(0xFFB45309)
        AvailabilityStatus.OUT_OF_ORDER -> if (isDarkMode) Color(0xFFF87171) else Color(0xFFB91C1C)
        AvailabilityStatus.UNKNOWN -> MaterialTheme.colorScheme.onSurfaceVariant
    }
}

@Composable
private fun priceChipColor(): Color =
    if (isSystemInDarkTheme()) Color(0xFF86EFAC) else Color(0xFF15803D)

private fun openStationNavigation(context: Context, feature: GeoJsonFeature, google: Boolean) {
    val url = if (google) {
        "https://www.google.com/maps/dir/?api=1&destination=${feature.latitude},${feature.longitude}"
    } else {
        "geo:0,0?q=${feature.latitude},${feature.longitude}(${Uri.encode(feature.properties.operatorName)})"
    }
    val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
    runCatching {
        context.startActivity(intent)
    }
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
                style = MaterialTheme.typography.labelMedium,
                color = contentColor
            )
        }
        Text(
            text = text,
            style = MaterialTheme.typography.labelMedium,
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
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}
