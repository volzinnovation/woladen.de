package de.woladen.android.ui

import android.content.Context
import android.location.Geocoder
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.MyLocation
import androidx.compose.material.icons.outlined.Delete
import androidx.compose.material.icons.outlined.FilterList
import androidx.compose.material.icons.outlined.NearMe
import androidx.compose.material.icons.outlined.Star
import androidx.compose.material.icons.filled.Star
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.unit.dp
import de.woladen.android.R
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.RouteEndpoint
import de.woladen.android.model.RouteStationMetadata
import de.woladen.android.model.RouteSummary
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.displayPrice
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.store.normalizeCategoryLabel
import de.woladen.android.ui.components.RoutePreviewMapView
import de.woladen.android.viewmodel.AppViewModel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.Locale
import kotlin.math.roundToInt

@Composable
fun RouteTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoritesStore: FavoritesStore,
    onShowFilter: () -> Unit
) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    var originText by rememberSaveable { mutableStateOf("") }
    var destinationText by rememberSaveable { mutableStateOf("") }
    var originEndpoint by remember { mutableStateOf<RouteEndpoint?>(null) }
    var destinationEndpoint by remember { mutableStateOf<RouteEndpoint?>(null) }
    var isResolving by rememberSaveable { mutableStateOf(false) }
    var statusMessage by rememberSaveable { mutableStateOf("") }
    var statusIsError by rememberSaveable { mutableStateOf(false) }

    val currentLocationLabel = stringResource(R.string.i18n_route_currentlocation)
    val routeOriginFallback = stringResource(R.string.i18n_route_origin)
    val routeDestinationFallback = stringResource(R.string.i18n_route_destination)

    fun endpointIsDistinct(endpoint: RouteEndpoint, forOrigin: Boolean): Boolean {
        val other = if (forOrigin) destinationEndpoint else originEndpoint
        other ?: return true
        return haversineDistanceMeters(endpoint.lat, endpoint.lon, other.lat, other.lon) >= 25.0
    }

    fun setEndpoint(endpoint: RouteEndpoint, forOrigin: Boolean) {
        if (!endpointIsDistinct(endpoint, forOrigin)) {
            statusMessage = context.getString(R.string.i18n_route_sameendpoint)
            statusIsError = true
            return
        }
        if (forOrigin) {
            originEndpoint = endpoint
            originText = endpoint.label
        } else {
            destinationEndpoint = endpoint
            destinationText = endpoint.label
        }
        statusMessage = ""
        statusIsError = false
    }

    fun useCurrentLocation(forOrigin: Boolean) {
        locationService.activate()
        val location = locationService.currentLocation
        if (location == null) {
            statusMessage = context.getString(R.string.i18n_route_locationunavailable)
            statusIsError = true
            return
        }
        val endpoint = RouteEndpoint(
            lat = location.latitude,
            lon = location.longitude,
            label = currentLocationLabel
        )
        setEndpoint(endpoint, forOrigin)
    }

    fun resolveTypedEndpoint(forOrigin: Boolean) {
        if (isResolving || viewModel.isLoadingRoute) return
        scope.launch {
            isResolving = true
            val endpoint = resolveEndpoint(
                context = context,
                existing = if (forOrigin) originEndpoint else destinationEndpoint,
                text = if (forOrigin) originText else destinationText
            )
            isResolving = false
            if (endpoint == null) {
                statusMessage = context.getString(R.string.i18n_search_noresults)
                statusIsError = true
                return@launch
            }
            setEndpoint(endpoint, forOrigin)
        }
    }

    fun submitRoute() {
        if (isResolving || viewModel.isLoadingRoute) return
        scope.launch {
            isResolving = true
            statusMessage = context.getString(R.string.i18n_route_resolving)
            statusIsError = false

            val origin = resolveEndpoint(context, originEndpoint, originText)
            val destination = resolveEndpoint(context, destinationEndpoint, destinationText)

            isResolving = false
            if (origin == null || destination == null) {
                statusMessage = context.getString(R.string.i18n_route_missingendpoints)
                statusIsError = true
                return@launch
            }
            if (haversineDistanceMeters(origin.lat, origin.lon, destination.lat, destination.lon) < 25.0) {
                statusMessage = context.getString(R.string.i18n_route_sameendpoint)
                statusIsError = true
                return@launch
            }

            originEndpoint = origin
            destinationEndpoint = destination
            statusMessage = ""
            statusIsError = false
            viewModel.searchRoute(origin, destination)
        }
    }

    Column(modifier = Modifier.fillMaxSize()) {
        RouteHeader()
        LazyColumn(
            modifier = Modifier.fillMaxSize(),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            item {
                Column(
                    modifier = Modifier
                        .widthIn(max = 880.dp)
                        .fillMaxWidth()
                        .padding(14.dp),
                    verticalArrangement = Arrangement.spacedBy(14.dp)
                ) {
                    Surface(
                        color = MaterialTheme.colorScheme.surfaceVariant,
                        shape = RoundedCornerShape(8.dp)
                    ) {
                        Column(
                            modifier = Modifier.padding(14.dp),
                            verticalArrangement = Arrangement.spacedBy(12.dp)
                        ) {
                            RouteEndpointRow(
                                label = stringResource(R.string.i18n_route_origin),
                                placeholder = stringResource(R.string.i18n_route_originplaceholder),
                                value = originText,
                                onValueChange = {
                                    originText = it
                                    originEndpoint = null
                                    statusMessage = ""
                                },
                                onUseCurrentLocation = { useCurrentLocation(forOrigin = true) },
                                onSearch = { resolveTypedEndpoint(forOrigin = true) }
                            )

                            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                Spacer(modifier = Modifier.widthIn(min = 48.dp, max = 48.dp))
                                OutlinedButton(
                                    onClick = {
                                        val oldOriginText = originText
                                        val oldOriginEndpoint = originEndpoint
                                        originText = destinationText
                                        originEndpoint = destinationEndpoint
                                        destinationText = oldOriginText
                                        destinationEndpoint = oldOriginEndpoint
                                        statusMessage = ""
                                    }
                                ) {
                                    Text(stringResource(R.string.i18n_route_swap))
                                }
                                HorizontalDivider(modifier = Modifier.weight(1f).align(Alignment.CenterVertically))
                            }

                            RouteEndpointRow(
                                label = stringResource(R.string.i18n_route_destination),
                                placeholder = stringResource(R.string.i18n_route_destinationplaceholder),
                                value = destinationText,
                                onValueChange = {
                                    destinationText = it
                                    destinationEndpoint = null
                                    statusMessage = ""
                                },
                                onUseCurrentLocation = { useCurrentLocation(forOrigin = false) },
                                onSearch = { resolveTypedEndpoint(forOrigin = false) }
                            )

                            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                Button(
                                    onClick = ::submitRoute,
                                    enabled = !isResolving && !viewModel.isLoadingRoute,
                                    modifier = Modifier.fillMaxWidth()
                                ) {
                                    if (isResolving || viewModel.isLoadingRoute) {
                                        CircularProgressIndicator()
                                    } else {
                                        Text(stringResource(R.string.i18n_route_submit))
                                    }
                                }
                            }
                        }
                    }

                    RouteStatus(
                        isLoading = viewModel.isLoadingRoute,
                        statusMessage = statusMessage,
                        statusIsError = statusIsError,
                        routeError = viewModel.routeError
                    )

                    if (viewModel.routeSummary == null && !viewModel.isLoadingRoute) {
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.Center
                        ) {
                            RouteFilterButton(
                                activeFilterCount = viewModel.routeFilterActiveCount(),
                                onShowFilter = onShowFilter
                            )
                        }
                    }

                    viewModel.routeSummary?.let { summary ->
                        RouteSummaryRow(summary = summary, stationCount = viewModel.routeDisplayFeatures().size)
                    }

                    if (viewModel.routeSummary != null && viewModel.routeFiltersRequireRecalculation()) {
                        Surface(
                            color = Color(0xFFF59E0B).copy(alpha = 0.12f),
                            shape = RoundedCornerShape(8.dp)
                        ) {
                            Row(
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .padding(12.dp),
                                horizontalArrangement = Arrangement.spacedBy(10.dp),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                Text(
                                    text = stringResource(R.string.i18n_route_filterchanged),
                                    style = MaterialTheme.typography.bodyMedium,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                    modifier = Modifier.weight(1f)
                                )
                                OutlinedButton(onClick = ::submitRoute) {
                                    Text(stringResource(R.string.i18n_route_recalculate))
                                }
                            }
                        }
                    }

                    val features = viewModel.routeDisplayFeatures()
                    if (viewModel.routeSummary != null && !viewModel.isLoadingRoute && features.isNotEmpty()) {
                        RouteActions(
                            features = features,
                            activeFilterCount = viewModel.routeFilterActiveCount(),
                            onShowFilter = onShowFilter,
                            onAddFavorites = {
                                val stationIds = features.map { it.properties.stationId }.filter { it.isNotBlank() }
                                val category = routeFavoriteCategoryLabel(
                                    originEndpoint?.label ?: originText,
                                    destinationEndpoint?.label ?: destinationText,
                                    routeOriginFallback,
                                    routeDestinationFallback,
                                    context
                                )
                                favoritesStore.addRouteFavorites(stationIds, category)
                                statusMessage = context.getString(R.string.i18n_route_favoritesadded)
                                    .replace("{count}", stationIds.size.toString())
                                    .replace("{category}", category)
                                statusIsError = false
                            },
                            onClearRoute = {
                                viewModel.clearRoute()
                                statusMessage = ""
                            }
                        )
                    }

                    viewModel.routeSummary?.let { summary ->
                        if (!viewModel.isLoadingRoute && (summary.geometry.coordinates.size > 1 || features.isNotEmpty())) {
                            RoutePreviewMapView(
                                routeCoordinates = summary.geometry.coordinates,
                                features = features,
                                favoriteStationIds = favoritesStore.favorites,
                                markerTint = viewModel::markerTint,
                                onFeatureTap = { feature -> viewModel.selectFeature(feature) },
                                modifier = Modifier
                                    .fillMaxWidth()
                                    .height(220.dp)
                                    .clip(RoundedCornerShape(8.dp))
                            )
                        }
                    }

                    when {
                        viewModel.isLoadingRoute -> Unit
                        viewModel.routeSummary == null && !viewModel.isLoadingRoute -> {
                            EmptyRouteState(text = stringResource(R.string.i18n_route_empty))
                        }
                        features.isEmpty() && !viewModel.isLoadingRoute -> {
                            EmptyRouteState(text = stringResource(R.string.i18n_route_nofilteredresults))
                        }
                    }
                }
            }

            items(viewModel.routeDisplayFeatures(), key = { it.id }) { feature ->
                RouteStationRow(
                    feature = feature,
                    isFavorite = favoritesStore.isFavorite(feature.properties.stationId),
                    onClick = { viewModel.selectFeature(feature) },
                    modifier = Modifier
                        .widthIn(max = 880.dp)
                        .fillMaxWidth()
                        .padding(horizontal = 14.dp, vertical = 5.dp)
                )
            }

            item {
                Spacer(modifier = Modifier.padding(bottom = 18.dp))
            }
        }
    }
}

@Composable
private fun RouteHeader() {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .background(MaterialTheme.colorScheme.background)
            .padding(horizontal = 16.dp, vertical = 8.dp),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Row(
            modifier = Modifier.weight(1f),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Icon(Icons.Outlined.NearMe, contentDescription = null, tint = MaterialTheme.colorScheme.primary)
            Text(
                text = stringResource(R.string.i18n_route_title),
                style = MaterialTheme.typography.titleMedium,
                color = MaterialTheme.colorScheme.primary
            )
        }
    }
    HorizontalDivider()
}

@Composable
private fun RouteEndpointRow(
    label: String,
    placeholder: String,
    value: String,
    onValueChange: (String) -> Unit,
    onUseCurrentLocation: () -> Unit,
    onSearch: () -> Unit
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.Top
    ) {
        IconButton(
            onClick = onUseCurrentLocation,
            modifier = Modifier
                .padding(top = 8.dp)
                .background(
                    color = MaterialTheme.colorScheme.surfaceVariant,
                    shape = CircleShape
                )
        ) {
            Icon(Icons.Filled.MyLocation, contentDescription = stringResource(R.string.i18n_route_usecurrent))
        }

        OutlinedTextField(
            value = value,
            onValueChange = onValueChange,
            label = { Text(label) },
            placeholder = { Text(placeholder) },
            singleLine = true,
            keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
            keyboardActions = KeyboardActions(onSearch = { onSearch() }),
            modifier = Modifier.weight(1f)
        )
    }
}

@Composable
private fun RouteStatus(
    isLoading: Boolean,
    statusMessage: String,
    statusIsError: Boolean,
    routeError: String?
) {
    val text = when {
        isLoading -> stringResource(R.string.i18n_route_loading)
        statusMessage.isNotBlank() -> statusMessage
        !routeError.isNullOrBlank() -> routeError
        else -> ""
    }
    if (text.isBlank()) return
    val color = when {
        statusIsError || !routeError.isNullOrBlank() -> MaterialTheme.colorScheme.error
        isLoading -> MaterialTheme.colorScheme.onSurfaceVariant
        else -> MaterialTheme.colorScheme.primary
    }
    if (isLoading) {
        Row(
            horizontalArrangement = Arrangement.spacedBy(8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            CircularProgressIndicator(
                modifier = Modifier.size(18.dp),
                strokeWidth = 2.dp,
                color = color
            )
            Text(
                text = text,
                style = MaterialTheme.typography.labelLarge,
                color = color
            )
        }
    } else {
        Text(
            text = text,
            style = MaterialTheme.typography.labelLarge,
            color = color
        )
    }
}

@Composable
private fun RouteSummaryRow(summary: RouteSummary, stationCount: Int) {
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        RouteSummaryStat(
            label = stringResource(R.string.i18n_route_summarydistancekm),
            value = formatRouteDistanceKilometers(summary.distanceM),
            modifier = Modifier.weight(1f)
        )
        RouteSummaryStat(
            label = stringResource(R.string.i18n_route_summaryduration),
            value = formatRouteClockDuration(summary.durationS),
            modifier = Modifier.weight(1f)
        )
        RouteSummaryStat(
            label = stringResource(R.string.i18n_route_summarystations),
            value = stationCount.toString(),
            modifier = Modifier.weight(1f)
        )
    }
}

@Composable
private fun RouteSummaryStat(label: String, value: String, modifier: Modifier = Modifier) {
    Surface(
        modifier = modifier,
        color = MaterialTheme.colorScheme.surfaceVariant,
        shape = RoundedCornerShape(8.dp)
    ) {
        Column(
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 10.dp),
            horizontalAlignment = Alignment.CenterHorizontally
        ) {
            Text(
                text = label,
                style = MaterialTheme.typography.labelSmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                textAlign = TextAlign.Center,
                modifier = Modifier.fillMaxWidth()
            )
            Text(
                text = value,
                style = MaterialTheme.typography.titleMedium,
                textAlign = TextAlign.Center,
                modifier = Modifier.fillMaxWidth()
            )
        }
    }
}

@Composable
private fun RouteFilterButton(
    activeFilterCount: Int,
    onShowFilter: () -> Unit
) {
    OutlinedButton(
        onClick = onShowFilter,
        colors = routeActionButtonColors()
    ) {
        Icon(Icons.Outlined.FilterList, contentDescription = null)
        Text(stringResource(R.string.i18n_filters_title))
        if (activeFilterCount > 0) {
            Text(" $activeFilterCount")
        }
    }
}

@Composable
private fun RouteActions(
    features: List<GeoJsonFeature>,
    activeFilterCount: Int,
    onShowFilter: () -> Unit,
    onAddFavorites: () -> Unit,
    onClearRoute: () -> Unit
) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        OutlinedButton(
            onClick = onAddFavorites,
            colors = routeActionButtonColors()
        ) {
            Icon(Icons.Filled.Star, contentDescription = null)
            Text(
                stringResource(R.string.i18n_route_addallfavoritesshort)
                    .replace("{count}", features.size.toString())
            )
        }
        Spacer(modifier = Modifier.weight(1f))
        RouteFilterButton(activeFilterCount = activeFilterCount, onShowFilter = onShowFilter)
        Spacer(modifier = Modifier.weight(1f))
        OutlinedButton(
            onClick = onClearRoute,
            colors = routeActionButtonColors()
        ) {
            Icon(
                Icons.Outlined.Delete,
                contentDescription = stringResource(R.string.i18n_route_removeroute)
            )
        }
    }
}

@Composable
private fun routeActionButtonColors() = ButtonDefaults.outlinedButtonColors(
    containerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.12f),
    contentColor = MaterialTheme.colorScheme.primary
)

@Composable
private fun RouteStationRow(
    feature: GeoJsonFeature,
    isFavorite: Boolean,
    onClick: () -> Unit,
    modifier: Modifier = Modifier
) {
    Surface(
        modifier = modifier
            .clip(RoundedCornerShape(8.dp))
            .clickable(onClick = onClick),
        color = routeStationBackground(feature),
        shape = RoundedCornerShape(8.dp),
        tonalElevation = 1.dp
    ) {
        Column(
            modifier = Modifier.padding(14.dp),
            verticalArrangement = Arrangement.spacedBy(7.dp)
        ) {
            Row(verticalAlignment = Alignment.Top, horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Icon(
                    imageVector = if (isFavorite) Icons.Filled.Star else Icons.Outlined.NearMe,
                    contentDescription = null,
                    tint = if (isFavorite) Color(0xFFF59E0B) else MaterialTheme.colorScheme.primary
                )
                Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(3.dp)) {
                    Text(
                        text = feature.properties.operatorName,
                        style = MaterialTheme.typography.titleMedium,
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis
                    )
                    Text(
                        text = "${feature.properties.city} • ${feature.properties.displayedMaxPowerKw.toInt()} kW • ${chargingPointLabel(feature.properties.chargingPointsCount)}",
                        style = MaterialTheme.typography.bodyMedium,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                    routeLine(feature.routeMetadata)?.let {
                        Text(
                            text = it,
                            style = MaterialTheme.typography.labelMedium,
                            color = MaterialTheme.colorScheme.primary
                        )
                    }
                }
            }

            val occupancy = feature.occupancySummaryLabel
            val priceDisplay = feature.displayPrice
            if (occupancy != null || priceDisplay.isNotBlank()) {
                Row(
                    modifier = Modifier.horizontalScroll(rememberScrollState()),
                    horizontalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    occupancy?.let {
                        RouteChip(
                            text = it,
                            containerColor = availabilityColor(feature.availabilityStatus).copy(alpha = 0.16f),
                            contentColor = availabilityColor(feature.availabilityStatus)
                        )
                    }
                    if (priceDisplay.isNotBlank()) {
                        RouteChip(
                            text = priceDisplay,
                            prefix = "€",
                            containerColor = routePriceColor().copy(alpha = 0.14f),
                            contentColor = routePriceColor()
                        )
                    }
                }
            }
        }
    }
}

@Composable
private fun RouteChip(text: String, prefix: String? = null, containerColor: Color, contentColor: Color) {
    Row(
        modifier = Modifier
            .background(containerColor, RoundedCornerShape(12.dp))
            .padding(horizontal = 8.dp, vertical = 4.dp),
        horizontalArrangement = Arrangement.spacedBy(4.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        if (!prefix.isNullOrBlank()) {
            Text(prefix, style = MaterialTheme.typography.labelSmall, color = contentColor)
        }
        Text(text, style = MaterialTheme.typography.labelSmall, color = contentColor)
    }
}

@Composable
private fun EmptyRouteState(text: String) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(24.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Icon(Icons.Outlined.NearMe, contentDescription = null, tint = MaterialTheme.colorScheme.onSurfaceVariant)
        Text(text, style = MaterialTheme.typography.bodyLarge, color = MaterialTheme.colorScheme.onSurfaceVariant)
    }
}

private suspend fun resolveEndpoint(
    context: Context,
    existing: RouteEndpoint?,
    text: String
): RouteEndpoint? {
    existing?.let { return it }
    val query = text.trim()
    if (query.length < 2) return null
    return withContext(Dispatchers.IO) {
        @Suppress("DEPRECATION")
        runCatching {
            Geocoder(context, Locale.getDefault())
                .getFromLocationName(query, 1)
                ?.firstOrNull()
                ?.let { address ->
                    RouteEndpoint(
                        lat = address.latitude,
                        lon = address.longitude,
                        label = address.featureName?.takeIf { it.isNotBlank() } ?: query
                    )
                }
        }.getOrNull()
    }
}

private fun routeFavoriteCategoryLabel(
    origin: String,
    destination: String,
    originFallback: String,
    destinationFallback: String,
    context: Context
): String {
    val label = context.getString(R.string.i18n_route_favoritecategory)
        .replace("{origin}", compactEndpointLabel(origin, originFallback))
        .replace("{destination}", compactEndpointLabel(destination, destinationFallback))
    return normalizeCategoryLabel(label)
}

private fun compactEndpointLabel(value: String, fallback: String): String {
    val text = value.replace("\\s+".toRegex(), " ").trim()
    return text.split(",").map { it.trim() }.firstOrNull { it.isNotBlank() } ?: fallback
}

@Composable
private fun routeLine(route: RouteStationMetadata?): String? {
    route ?: return null
    val parts = mutableListOf<String>()
    if (route.driveDistanceToRouteM > 0) {
        parts += stringResource(R.string.i18n_route_cardaccess)
            .replace("{distance}", formatRouteDistance(route.driveDistanceToRouteM))
    }
    if (route.routePositionM > 0) {
        parts += stringResource(R.string.i18n_route_cardposition)
            .replace("{distance}", formatRouteDistance(route.routePositionM))
    }
    return parts.takeIf { it.isNotEmpty() }?.joinToString(" • ")
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

private fun formatRouteDistance(meters: Int): String {
    return when {
        meters >= 10_000 -> "${(meters / 1000.0).roundToInt()} km"
        meters >= 1000 -> "%.1f km".format(Locale.getDefault(), meters / 1000.0)
        else -> "$meters m"
    }
}

private fun formatRouteDistanceKilometers(meters: Int): String {
    val kilometers = meters / 1000.0
    return if (meters >= 10_000) {
        kilometers.roundToInt().toString()
    } else {
        "%.1f".format(Locale.getDefault(), kilometers)
    }
}

private fun formatRouteClockDuration(seconds: Int): String {
    if (seconds <= 0) return "00:00"
    val minutes = maxOf(1, (seconds / 60.0).roundToInt())
    return "%02d:%02d".format(Locale.getDefault(), minutes / 60, minutes % 60)
}

@Composable
private fun formatRouteDuration(seconds: Int): String {
    if (seconds <= 0) return ""
    val minutes = maxOf(1, (seconds / 60.0).roundToInt())
    val hours = minutes / 60
    val remainder = minutes % 60
    return when {
        hours > 0 && remainder > 0 -> stringResource(R.string.i18n_route_durationhoursminutes)
            .replace("{hours}", hours.toString())
            .replace("{minutes}", remainder.toString())
        hours > 0 -> stringResource(R.string.i18n_route_durationhours)
            .replace("{hours}", hours.toString())
        else -> stringResource(R.string.i18n_route_durationminutes)
            .replace("{minutes}", minutes.toString())
    }
}

@Composable
private fun routeStationBackground(feature: GeoJsonFeature): Color {
    val isDark = isSystemInDarkTheme()
    return when (feature.availabilityStatus) {
        AvailabilityStatus.OUT_OF_ORDER -> if (isDark) Color(0xFF3B121C) else Color(0xFFFFF1F2)
        AvailabilityStatus.OCCUPIED -> if (isDark) Color(0xFF26323D) else Color(0xFFE2E8F0)
        AvailabilityStatus.UNKNOWN -> if (isDark) Color(0xFF16212B) else Color(0xFFF1F5F9)
        AvailabilityStatus.FREE -> MaterialTheme.colorScheme.surface
    }
}

@Composable
private fun availabilityColor(status: AvailabilityStatus): Color {
    val isDark = isSystemInDarkTheme()
    return when (status) {
        AvailabilityStatus.FREE -> if (isDark) Color(0xFF5EEAD4) else Color(0xFF0F766E)
        AvailabilityStatus.OCCUPIED -> if (isDark) Color(0xFFFBBF24) else Color(0xFFB45309)
        AvailabilityStatus.OUT_OF_ORDER -> if (isDark) Color(0xFFF87171) else Color(0xFFB91C1C)
        AvailabilityStatus.UNKNOWN -> MaterialTheme.colorScheme.onSurfaceVariant
    }
}

@Composable
private fun routePriceColor(): Color =
    if (isSystemInDarkTheme()) Color(0xFF86EFAC) else Color(0xFF15803D)

private fun haversineDistanceMeters(
    latitudeA: Double,
    longitudeA: Double,
    latitudeB: Double,
    longitudeB: Double
): Double {
    val latRadA = Math.toRadians(latitudeA)
    val latRadB = Math.toRadians(latitudeB)
    val dLat = latRadB - latRadA
    val dLon = Math.toRadians(longitudeB - longitudeA)
    val a = kotlin.math.sin(dLat / 2) * kotlin.math.sin(dLat / 2) +
        kotlin.math.cos(latRadA) * kotlin.math.cos(latRadB) *
        kotlin.math.sin(dLon / 2) * kotlin.math.sin(dLon / 2)
    val c = 2 * kotlin.math.atan2(kotlin.math.sqrt(a), kotlin.math.sqrt(1 - a))
    return 6_371_000.0 * c
}
