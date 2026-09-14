package de.woladen.android.ui

import android.location.Location
import android.content.Context
import androidx.compose.foundation.border
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
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
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
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
import androidx.compose.ui.platform.LocalFocusManager
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.text.input.ImeAction
import androidx.compose.ui.unit.dp
import de.woladen.android.R
import de.woladen.android.BuildConfig
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.GeocodeResult
import de.woladen.android.model.RouteEndpoint
import de.woladen.android.model.RouteStationMetadata
import de.woladen.android.model.RouteSummary
import de.woladen.android.model.RoutePlan
import de.woladen.android.model.TripEtaEstimator
import de.woladen.android.model.TripStationSnapshot
import de.woladen.android.model.VehicleEnergySettings
import de.woladen.android.model.VehicleProfile
import de.woladen.android.model.ProviderPreferenceMode
import de.woladen.android.model.WoladenMode
import de.woladen.android.model.projectRoutePositionM
import de.woladen.android.model.StationCardState
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.displayPrice
import de.woladen.android.model.occupancySummaryLabel
import de.woladen.android.model.stationCardState
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.app.WoladenApplication
import de.woladen.android.store.normalizeCategoryLabel
import de.woladen.android.ui.components.RoutePreviewMapView
import de.woladen.android.ui.components.markerColorForKey
import de.woladen.android.viewmodel.AppViewModel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.util.Locale
import kotlin.math.roundToInt

private const val ROUTE_SUGGESTION_DEBOUNCE_MS = 350L
private const val ROUTE_SUGGESTION_LIMIT = 5
private const val ROUTE_PROGRESS_DURATION_MS = 60_000L
private const val ROUTE_PROGRESS_INTERVAL_MS = 250L
private const val ROUTE_PROGRESS_MAX = 0.95f

@Composable
fun RouteTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    favoritesStore: FavoritesStore,
    onShowFilter: () -> Unit
) {
    val context = LocalContext.current
    val focusManager = LocalFocusManager.current
    val scope = rememberCoroutineScope()
    val tripStore = remember(context) { (context.applicationContext as WoladenApplication).tripStore }
    var originText by rememberSaveable { mutableStateOf("") }
    var destinationText by rememberSaveable { mutableStateOf("") }
    var originEndpoint by remember { mutableStateOf<RouteEndpoint?>(null) }
    var destinationEndpoint by remember { mutableStateOf<RouteEndpoint?>(null) }
    var originSuggestions by remember { mutableStateOf<List<RoutePlaceSuggestion>>(emptyList()) }
    var destinationSuggestions by remember { mutableStateOf<List<RoutePlaceSuggestion>>(emptyList()) }
    var originSuggestionMessage by rememberSaveable { mutableStateOf("") }
    var destinationSuggestionMessage by rememberSaveable { mutableStateOf("") }
    var isResolving by rememberSaveable { mutableStateOf(false) }
    var statusMessage by rememberSaveable { mutableStateOf("") }
    var statusIsError by rememberSaveable { mutableStateOf(false) }
    var initialSocText by rememberSaveable { mutableStateOf(tripStore.vehicleSettings.targetSocPercent.toInt().toString()) }
    var currentPlanId by rememberSaveable { mutableStateOf<String?>(null) }

    val currentLocationLabel = stringResource(R.string.i18n_route_currentlocation)
    val routeOriginFallback = stringResource(R.string.i18n_route_origin)
    val routeDestinationFallback = stringResource(R.string.i18n_route_destination)
    val routeDisplayFeatures = viewModel.routeDisplayFeatures(locationService.currentLocation)

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
            originSuggestions = emptyList()
            originSuggestionMessage = ""
        } else {
            destinationEndpoint = endpoint
            destinationText = endpoint.label
            destinationSuggestions = emptyList()
            destinationSuggestionMessage = ""
        }
        statusMessage = ""
        statusIsError = false
        focusManager.clearFocus(force = true)
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
        val existingSuggestions = if (forOrigin) originSuggestions else destinationSuggestions
        if (existingSuggestions.isNotEmpty()) {
            setEndpoint(existingSuggestions.first().endpoint, forOrigin)
            return
        }
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

    LaunchedEffect(originText, originEndpoint) {
        val query = originText.trim()
        if (originEndpoint != null || query.length < 2) {
            originSuggestions = emptyList()
            originSuggestionMessage = ""
            return@LaunchedEffect
        }
        originSuggestionMessage = context.getString(R.string.i18n_search_searching)
        delay(ROUTE_SUGGESTION_DEBOUNCE_MS)
        val suggestions = lookupRouteSuggestions(context, query)
        originSuggestions = suggestions
        originSuggestionMessage = if (suggestions.isEmpty()) {
            context.getString(R.string.i18n_search_noresults)
        } else {
            ""
        }
    }

    LaunchedEffect(destinationText, destinationEndpoint) {
        val query = destinationText.trim()
        if (destinationEndpoint != null || query.length < 2) {
            destinationSuggestions = emptyList()
            destinationSuggestionMessage = ""
            return@LaunchedEffect
        }
        destinationSuggestionMessage = context.getString(R.string.i18n_search_searching)
        delay(ROUTE_SUGGESTION_DEBOUNCE_MS)
        val suggestions = lookupRouteSuggestions(context, query)
        destinationSuggestions = suggestions
        destinationSuggestionMessage = if (suggestions.isEmpty()) {
            context.getString(R.string.i18n_search_noresults)
        } else {
            ""
        }
    }

    LaunchedEffect(tripStore.activePlanId, tripStore.mode) {
        val activePlan = tripStore.activePlan
        if (activePlan != null) currentPlanId = activePlan.id
        if (activePlan != null && !activePlan.isStationTargetTrip && tripStore.mode == WoladenMode.TRIP && viewModel.routeSummary == null) {
            originEndpoint = activePlan.route.origin
            destinationEndpoint = activePlan.route.destination
            originText = activePlan.route.origin.label
            destinationText = activePlan.route.destination.label
            initialSocText = activePlan.route.initialSocPercent.toInt().toString()
            viewModel.searchRoute(activePlan.route.origin, activePlan.route.destination)
        }
    }

    val currentPlan = currentPlanId?.let { id -> tripStore.plans.firstOrNull { it.id == id } }

    Column(modifier = Modifier.fillMaxSize().testTag("route-root")) {
        RouteHeader(mode = tripStore.mode, hasActiveTrip = tripStore.activePlan != null, onToggleMode = tripStore::toggleMode)
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
                                suggestions = originSuggestions,
                                suggestionMessage = originSuggestionMessage,
                                onSuggestionSelected = { suggestion ->
                                    setEndpoint(suggestion.endpoint, forOrigin = true)
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
                                        originSuggestions = emptyList()
                                        destinationSuggestions = emptyList()
                                        originSuggestionMessage = ""
                                        destinationSuggestionMessage = ""
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
                                suggestions = destinationSuggestions,
                                suggestionMessage = destinationSuggestionMessage,
                                onSuggestionSelected = { suggestion ->
                                    setEndpoint(suggestion.endpoint, forOrigin = false)
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

                    if (viewModel.routeSummary == null && tripStore.mode == WoladenMode.TRIP && tripStore.activePlan?.isStationTargetTrip == true) {
                        ActiveTripCard(
                            plan = tripStore.activePlan!!,
                            currentLocation = locationService.currentLocation,
                            onCompleteStop = tripStore::completeNextStop,
                            onSkipStop = tripStore::skipNextStop,
                            onEndTrip = { tripStore.endTrip() }
                        )
                    }

                    if (viewModel.routeSummary == null && !viewModel.isLoadingRoute) {
                        SavedTripPlans(
                            plans = tripStore.sortedPlans,
                            activePlanId = tripStore.activePlanId,
                            onLoad = { plan ->
                                currentPlanId = plan.id
                                originEndpoint = plan.route.origin
                                destinationEndpoint = plan.route.destination
                                originText = plan.route.origin.label
                                destinationText = plan.route.destination.label
                                initialSocText = plan.route.initialSocPercent.toInt().toString()
                                statusMessage = ""
                                if (!plan.isStationTargetTrip) {
                                    viewModel.searchRoute(plan.route.origin, plan.route.destination)
                                }
                            },
                            onDelete = tripStore::delete,
                            onStartTrip = { plan ->
                                currentPlanId = plan.id
                                tripStore.activate(plan.id)
                            }
                        )
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
                        RouteSummaryRow(summary = summary, stationCount = routeDisplayFeatures.size)
                        if (tripStore.mode == WoladenMode.TRIP && tripStore.activePlan != null) {
                            ActiveTripCard(
                                plan = tripStore.activePlan!!,
                                currentLocation = locationService.currentLocation,
                                onCompleteStop = tripStore::completeNextStop,
                                onSkipStop = tripStore::skipNextStop,
                                onEndTrip = { tripStore.endTrip() }
                            )
                        }
                        TripEnergyCard(
                            summary = summary,
                            stations = routeDisplayFeatures,
                            initialSocText = initialSocText,
                            onInitialSocChange = { value -> initialSocText = value.filter(Char::isDigit).take(3) },
                            settings = tripStore.vehicleSettings,
                            profiles = tripStore.vehicleProfiles,
                            selectedProfileId = tripStore.selectedVehicleProfileId,
                            onSelectProfile = tripStore::selectVehicleProfile,
                            onAddProfile = tripStore::addVehicleProfile,
                            onRenameProfile = tripStore::renameVehicleProfile,
                            onDeleteProfile = tripStore::deleteVehicleProfile,
                            providerMode = tripStore.providerMode,
                            selectedProviderNames = tripStore.selectedProviderNames,
                            onProviderPreferencesChanged = tripStore::updateProviderPreferences,
                            plan = currentPlan,
                            onSelectStop = { planId, stationId -> tripStore.selectStop(planId, stationId) },
                            onSettingsChanged = tripStore::updateVehicleSettings
                        )
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            OutlinedButton(
                                onClick = {
                                    val origin = originEndpoint
                                    val destination = destinationEndpoint
                                    if (origin != null && destination != null) {
                                        val initialSoc = initialSocText.toDoubleOrNull()?.coerceIn(1.0, 100.0) ?: 80.0
                                        val savedPlan = tripStore.saveCalculatedRoute(
                                            origin = origin,
                                            destination = destination,
                                            summary = summary,
                                            features = routeDisplayFeatures,
                                            filter = viewModel.filterState,
                                            initialSocPercent = initialSoc,
                                            existingPlanId = currentPlanId
                                        )
                                        currentPlanId = savedPlan.id
                                        statusMessage = context.getString(R.string.i18n_route_plansaved)
                                        statusIsError = false
                                    }
                                },
                                modifier = Modifier.weight(1f)
                            ) {
                                Text(stringResource(R.string.i18n_route_saveplan))
                            }
                            if (tripStore.activePlan != null && tripStore.mode == WoladenMode.TRIP) {
                                OutlinedButton(
                                    onClick = tripStore::leaveTripMode,
                                    modifier = Modifier.weight(1f)
                                ) {
                                    Text(stringResource(R.string.i18n_route_endtrip))
                                }
                            }
                        }
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

                    val features = routeDisplayFeatures
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

            items(routeDisplayFeatures, key = { it.id }) { feature ->
                RouteStationRow(
                    feature = feature,
                    isFavorite = favoritesStore.isFavorite(feature.properties.stationId),
                    markerColor = Color(markerColorForKey(viewModel.markerTint(feature))),
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
private fun SavedTripPlans(
    plans: List<RoutePlan>,
    activePlanId: String?,
    onLoad: (RoutePlan) -> Unit,
    onDelete: (String) -> Unit,
    onStartTrip: (RoutePlan) -> Unit
) {
    if (plans.isEmpty()) return
    Column(
        modifier = Modifier.fillMaxWidth(),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(stringResource(R.string.i18n_route_savedplans), style = MaterialTheme.typography.titleMedium)
        plans.take(5).forEach { plan ->
            Surface(
                modifier = Modifier.fillMaxWidth(),
                color = MaterialTheme.colorScheme.surfaceVariant,
                shape = RoundedCornerShape(10.dp)
            ) {
                Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(7.dp)) {
                    Row(verticalAlignment = Alignment.CenterVertically) {
                        Column(modifier = Modifier.weight(1f)) {
                            Text(plan.name, style = MaterialTheme.typography.titleSmall, maxLines = 1, overflow = TextOverflow.Ellipsis)
                            Text(
                                text = "${formatRouteDistanceKilometers(plan.route.distanceM)} · ${plan.rawStations.size} stations · ${plan.route.initialSocPercent.toInt()}% SOC",
                                style = MaterialTheme.typography.bodySmall,
                                color = MaterialTheme.colorScheme.onSurfaceVariant
                            )
                        }
                        IconButton(onClick = { onDelete(plan.id) }) {
                            Icon(Icons.Outlined.Delete, contentDescription = stringResource(R.string.i18n_route_deleteplan))
                        }
                    }
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedButton(onClick = { onLoad(plan) }, modifier = Modifier.weight(1f)) {
                            Text(stringResource(R.string.i18n_route_loadplan))
                        }
                        if (activePlanId != plan.id) {
                            Button(onClick = { onStartTrip(plan) }, modifier = Modifier.weight(1f)) {
                                Text(stringResource(R.string.i18n_route_starttrip))
                            }
                        } else {
                            Text(
                                stringResource(R.string.i18n_route_active),
                                style = MaterialTheme.typography.labelLarge,
                                color = MaterialTheme.colorScheme.primary,
                                modifier = Modifier.align(Alignment.CenterVertically)
                            )
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun TripEnergyCard(
    summary: RouteSummary,
    stations: List<GeoJsonFeature>,
    initialSocText: String,
    onInitialSocChange: (String) -> Unit,
    settings: VehicleEnergySettings,
    profiles: List<VehicleProfile>,
    selectedProfileId: String,
    onSelectProfile: (String) -> Boolean,
    onAddProfile: (String, VehicleEnergySettings) -> VehicleProfile,
    onRenameProfile: (String, String) -> Boolean,
    onDeleteProfile: (String) -> Boolean,
    providerMode: ProviderPreferenceMode,
    selectedProviderNames: List<String>,
    onProviderPreferencesChanged: (ProviderPreferenceMode, List<String>) -> Unit,
    plan: RoutePlan?,
    onSelectStop: (String, String) -> Boolean,
    onSettingsChanged: (VehicleEnergySettings) -> Unit
) {
    val initialSoc = initialSocText.toDoubleOrNull()?.coerceIn(1.0, 100.0) ?: 80.0
    val range = settings.usableRangeKm(initialSoc)
    val windows = de.woladen.android.model.EnergyRoutePlanner.build(
        routeDistanceM = summary.distanceM,
        stations = stations.map(de.woladen.android.model.TripStationSnapshot.Companion::fromFeature),
        settings = settings,
        initialSocPercent = initialSoc,
        providerMode = providerMode,
        selectedProviderNames = selectedProviderNames
    )
    Surface(
        modifier = Modifier.fillMaxWidth(),
        color = MaterialTheme.colorScheme.primary.copy(alpha = 0.08f),
        shape = RoundedCornerShape(10.dp)
    ) {
        Column(
            modifier = Modifier.padding(12.dp),
            verticalArrangement = Arrangement.spacedBy(10.dp)
        ) {
            Row(horizontalArrangement = Arrangement.spacedBy(12.dp), verticalAlignment = Alignment.CenterVertically) {
                Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(3.dp)) {
                    Text(stringResource(R.string.i18n_route_energytitle), style = MaterialTheme.typography.titleSmall)
                    Text(
                        text = "${stringResource(R.string.i18n_route_usable_range)}: ${formatRouteDistanceKilometers((range * 1000.0).toInt())} · ${windows.size} ${stringResource(R.string.i18n_route_chargingwindows)}",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                OutlinedTextField(
                    value = initialSocText,
                    onValueChange = onInitialSocChange,
                    label = { Text(stringResource(R.string.i18n_route_initialsoc)) },
                    suffix = { Text("%") },
                    singleLine = true,
                    modifier = Modifier.widthIn(min = 92.dp, max = 112.dp)
                )
            }
            if (plan != null && plan.windows.isNotEmpty()) {
                Text("Charging stops", style = MaterialTheme.typography.labelLarge)
                plan.windows.forEach { window ->
                    val selected = window.selectedStationId
                    Column(verticalArrangement = Arrangement.spacedBy(5.dp)) {
                        Text(
                            "Window ${window.index + 1} · ${formatRouteDistanceKilometers(window.endPositionM)}",
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                        Row(
                            modifier = Modifier.horizontalScroll(rememberScrollState()),
                            horizontalArrangement = Arrangement.spacedBy(6.dp)
                        ) {
                            window.candidateStationIds.take(5).forEach { stationId ->
                                val station = plan.station(stationId)
                                OutlinedButton(
                                    onClick = { onSelectStop(plan.id, stationId) },
                                    colors = if (stationId == selected) ButtonDefaults.outlinedButtonColors(
                                        containerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.15f),
                                        contentColor = MaterialTheme.colorScheme.primary
                                    ) else ButtonDefaults.outlinedButtonColors(),
                                    modifier = Modifier.heightIn(min = 36.dp)
                                ) {
                                    Text(
                                        station?.stationName?.ifBlank { station.operatorName } ?: stationId,
                                        maxLines = 1,
                                        overflow = TextOverflow.Ellipsis
                                    )
                                }
                            }
                        }
                    }
                }
            }
            VehicleSettingsEditor(
                settings = settings,
                profiles = profiles,
                selectedProfileId = selectedProfileId,
                onSelectProfile = onSelectProfile,
                onAddProfile = onAddProfile,
                onRenameProfile = onRenameProfile,
                onDeleteProfile = onDeleteProfile,
                onSettingsChanged = onSettingsChanged
            )
            ProviderPreferencesEditor(
                availableProviders = stations.map { it.properties.operatorName }.filter { it.isNotBlank() }.distinct().sorted(),
                mode = providerMode,
                selectedProviders = selectedProviderNames,
                onChanged = onProviderPreferencesChanged
            )
        }
    }
}

@Composable
private fun VehicleSettingsEditor(
    settings: VehicleEnergySettings,
    profiles: List<VehicleProfile>,
    selectedProfileId: String,
    onSelectProfile: (String) -> Boolean,
    onAddProfile: (String, VehicleEnergySettings) -> VehicleProfile,
    onRenameProfile: (String, String) -> Boolean,
    onDeleteProfile: (String) -> Boolean,
    onSettingsChanged: (VehicleEnergySettings) -> Unit
) {
    var batteryText by remember(settings.batteryCapacityKWh) { mutableStateOf(settings.batteryCapacityKWh.toString()) }
    var consumptionText by remember(settings.consumptionKWhPer100Km) { mutableStateOf(settings.consumptionKWhPer100Km.toString()) }
    var reserveText by remember(settings.reserveSocPercent) { mutableStateOf(settings.reserveSocPercent.toString()) }
    var targetText by remember(settings.targetSocPercent) { mutableStateOf(settings.targetSocPercent.toString()) }
    var powerText by remember(settings.averageChargingPowerKw) { mutableStateOf(settings.averageChargingPowerKw.toString()) }
    var profileName by remember(selectedProfileId, profiles) { mutableStateOf(profiles.firstOrNull { it.id == selectedProfileId }?.name.orEmpty()) }
    var profileMenuOpen by remember { mutableStateOf(false) }
    Column(verticalArrangement = Arrangement.spacedBy(7.dp)) {
        Text("Vehicle profile", style = MaterialTheme.typography.labelLarge)
        Row(
            modifier = Modifier.horizontalScroll(rememberScrollState()),
            horizontalArrangement = Arrangement.spacedBy(7.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Box {
                OutlinedButton(onClick = { profileMenuOpen = true }) {
                    Text(profiles.firstOrNull { it.id == selectedProfileId }?.name ?: "Vehicle")
                }
                androidx.compose.material3.DropdownMenu(expanded = profileMenuOpen, onDismissRequest = { profileMenuOpen = false }) {
                    profiles.forEach { profile ->
                        androidx.compose.material3.DropdownMenuItem(
                            text = { Text(profile.name) },
                            onClick = { profileMenuOpen = false; onSelectProfile(profile.id) }
                        )
                    }
                }
            }
            OutlinedTextField(
                value = profileName,
                onValueChange = { profileName = it.take(32) },
                label = { Text("Name") },
                singleLine = true,
                modifier = Modifier.widthIn(min = 140.dp, max = 220.dp)
            )
            OutlinedButton(onClick = { onRenameProfile(selectedProfileId, profileName) }) { Text("Rename") }
            OutlinedButton(onClick = { onAddProfile(profileName, settings) }) { Text("Add") }
            if (profiles.size > 1) {
                IconButton(onClick = { onDeleteProfile(selectedProfileId) }) {
                    Icon(Icons.Outlined.Delete, contentDescription = "Delete vehicle profile")
                }
            }
        }
        Row(horizontalArrangement = Arrangement.spacedBy(7.dp)) {
            OutlinedTextField(batteryText, { batteryText = it.filter { c -> c.isDigit() || c == '.' }.take(6) }, label = { Text("Battery kWh") }, singleLine = true, modifier = Modifier.weight(1f))
            OutlinedTextField(consumptionText, { consumptionText = it.filter { c -> c.isDigit() || c == '.' }.take(6) }, label = { Text("Consumption") }, singleLine = true, modifier = Modifier.weight(1f))
        }
        Row(horizontalArrangement = Arrangement.spacedBy(7.dp)) {
            OutlinedTextField(reserveText, { reserveText = it.filter { c -> c.isDigit() || c == '.' }.take(5) }, label = { Text("Reserve %") }, singleLine = true, modifier = Modifier.weight(1f))
            OutlinedTextField(targetText, { targetText = it.filter { c -> c.isDigit() || c == '.' }.take(5) }, label = { Text("Target %") }, singleLine = true, modifier = Modifier.weight(1f))
            OutlinedTextField(powerText, { powerText = it.filter { c -> c.isDigit() || c == '.' }.take(6) }, label = { Text("Charge kW") }, singleLine = true, modifier = Modifier.weight(1f))
        }
        OutlinedButton(
            onClick = {
                onSettingsChanged(
                    settings.copy(
                        batteryCapacityKWh = batteryText.toDoubleOrNull() ?: settings.batteryCapacityKWh,
                        consumptionKWhPer100Km = consumptionText.toDoubleOrNull() ?: settings.consumptionKWhPer100Km,
                        reserveSocPercent = reserveText.toDoubleOrNull() ?: settings.reserveSocPercent,
                        targetSocPercent = targetText.toDoubleOrNull() ?: settings.targetSocPercent,
                        averageChargingPowerKw = powerText.toDoubleOrNull() ?: settings.averageChargingPowerKw
                    )
                )
            },
            modifier = Modifier.fillMaxWidth()
        ) { Text("Save vehicle profile") }
    }
}

@Composable
private fun ProviderPreferencesEditor(
    availableProviders: List<String>,
    mode: ProviderPreferenceMode,
    selectedProviders: List<String>,
    onChanged: (ProviderPreferenceMode, List<String>) -> Unit
) {
    if (availableProviders.isEmpty()) return
    Column(verticalArrangement = Arrangement.spacedBy(7.dp)) {
        Text("Provider preferences", style = MaterialTheme.typography.labelLarge)
        Row(horizontalArrangement = Arrangement.spacedBy(7.dp)) {
            listOf(ProviderPreferenceMode.PREFER to "Prefer", ProviderPreferenceMode.ONLY to "Only").forEach { (candidate, label) ->
                OutlinedButton(
                    onClick = { onChanged(candidate, selectedProviders) },
                    colors = if (candidate == mode) ButtonDefaults.outlinedButtonColors(
                        containerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.15f),
                        contentColor = MaterialTheme.colorScheme.primary
                    ) else ButtonDefaults.outlinedButtonColors()
                ) { Text(label) }
            }
        }
        Row(modifier = Modifier.horizontalScroll(rememberScrollState()), horizontalArrangement = Arrangement.spacedBy(6.dp)) {
            availableProviders.forEach { provider ->
                val selected = provider in selectedProviders
                OutlinedButton(
                    onClick = {
                        val next = if (selected) selectedProviders - provider else selectedProviders + provider
                        onChanged(mode, next)
                    },
                    colors = if (selected) ButtonDefaults.outlinedButtonColors(
                        containerColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.15f),
                        contentColor = MaterialTheme.colorScheme.primary
                    ) else ButtonDefaults.outlinedButtonColors(),
                    modifier = Modifier.heightIn(min = 34.dp)
                ) { Text(provider, maxLines = 1, overflow = TextOverflow.Ellipsis) }
            }
        }
        if (mode == ProviderPreferenceMode.ONLY && selectedProviders.isEmpty()) {
            Text("Select at least one provider for Only mode.", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.error)
        }
    }
}

@Composable
private fun ActiveTripCard(
    plan: RoutePlan,
    currentLocation: Location?,
    onCompleteStop: () -> Boolean,
    onSkipStop: () -> Boolean,
    onEndTrip: () -> Unit
) {
    val context = LocalContext.current
    val application = context.applicationContext as WoladenApplication
    val routePosition = currentLocation?.let { projectRoutePositionM(plan.route, it.latitude, it.longitude) } ?: 0
    var nowEpochMs by remember(plan.id, routePosition) { mutableStateOf(System.currentTimeMillis()) }
    var trafficEta by remember(plan.id, routePosition) { mutableStateOf<de.woladen.android.model.TrafficEtaResult?>(null) }
    LaunchedEffect(plan.id, routePosition) {
        while (true) {
            nowEpochMs = System.currentTimeMillis()
            if (BuildConfig.GOOGLE_TRAFFIC_ETA_ENABLED && !plan.isStationTargetTrip) {
                val current = currentLocation?.let { RouteEndpoint(it.latitude, it.longitude, "Current location") } ?: plan.route.origin
                val next = plan.nextStop?.let { RouteEndpoint(it.latitude, it.longitude, it.stationName) }
                trafficEta = runCatching {
                    application.liveApiClient.trafficEta(current, plan.route.destination, next)
                }.getOrNull()
            }
            delay(30_000L)
        }
    }
    val baseEta = TripEtaEstimator.estimate(plan, routePosition, nowEpochMs)
    val eta = trafficEta?.let { TripEtaEstimator.applyTraffic(baseEta, plan, it, nowEpochMs) } ?: baseEta
    val nextStop = plan.nextStop
    Surface(
        modifier = Modifier.fillMaxWidth(),
        color = MaterialTheme.colorScheme.primary.copy(alpha = 0.10f),
        shape = RoundedCornerShape(10.dp)
    ) {
        Column(modifier = Modifier.padding(12.dp), verticalArrangement = Arrangement.spacedBy(8.dp)) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text("Active trip", style = MaterialTheme.typography.titleSmall, modifier = Modifier.weight(1f))
                Text("${(eta.progress * 100.0).roundToInt()}%", style = MaterialTheme.typography.labelLarge, color = MaterialTheme.colorScheme.primary)
            }
            LinearProgressIndicator(
                progress = { eta.progress.toFloat().coerceIn(0f, 1f) },
                modifier = Modifier.fillMaxWidth().height(7.dp).clip(RoundedCornerShape(999.dp))
            )
            Text(
                text = listOfNotNull(
                    nextStop?.let { "Next: ${it.stationName.ifBlank { it.operatorName }}" },
                    "ETA ${formatRouteClockTime(eta.destinationArrivalEpochMs)}",
                    if (plan.isStationTargetTrip) "Arrival SOC unavailable" else "Arrival SOC ${eta.projectedArrivalSocPercent.toInt()}%"
                ).joinToString(" · "),
                style = MaterialTheme.typography.bodyMedium
            )
            if (eta.trafficAdjusted) {
                Text("Traffic-aware ETA", style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.primary)
            }
            Row(horizontalArrangement = Arrangement.spacedBy(7.dp)) {
                if (nextStop != null) {
                    OutlinedButton(onClick = { onCompleteStop() }, modifier = Modifier.weight(1f)) { Text("Complete stop") }
                    OutlinedButton(onClick = { onSkipStop() }, modifier = Modifier.weight(1f)) { Text("Skip") }
                }
                OutlinedButton(onClick = onEndTrip, modifier = Modifier.weight(1f)) { Text("End trip") }
            }
        }
    }
}

@Composable
private fun RouteHeader(mode: WoladenMode, hasActiveTrip: Boolean, onToggleMode: () -> Unit) {
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
            OutlinedButton(onClick = onToggleMode, enabled = hasActiveTrip || mode == WoladenMode.PLAN) {
                Text(
                    if (mode == WoladenMode.TRIP) {
                        stringResource(R.string.i18n_route_tripmode)
                    } else {
                        stringResource(R.string.i18n_route_planmode)
                    }
                )
            }
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
    suggestions: List<RoutePlaceSuggestion>,
    suggestionMessage: String,
    onSuggestionSelected: (RoutePlaceSuggestion) -> Unit,
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

        Column(
            modifier = Modifier.weight(1f),
            verticalArrangement = Arrangement.spacedBy(6.dp)
        ) {
            OutlinedTextField(
                value = value,
                onValueChange = onValueChange,
                label = { Text(label) },
                placeholder = { Text(placeholder) },
                singleLine = true,
                keyboardOptions = KeyboardOptions(imeAction = ImeAction.Search),
                keyboardActions = KeyboardActions(onSearch = { onSearch() }),
                modifier = Modifier.fillMaxWidth()
            )
            RouteSuggestionList(
                suggestions = suggestions,
                message = suggestionMessage,
                onSuggestionSelected = onSuggestionSelected
            )
        }
    }
}

@Composable
private fun RouteSuggestionList(
    suggestions: List<RoutePlaceSuggestion>,
    message: String,
    onSuggestionSelected: (RoutePlaceSuggestion) -> Unit
) {
    if (suggestions.isEmpty() && message.isBlank()) return

    Surface(
        modifier = Modifier
            .fillMaxWidth()
            .heightIn(max = 236.dp)
            .border(1.dp, MaterialTheme.colorScheme.outline.copy(alpha = 0.55f), RoundedCornerShape(8.dp)),
        color = MaterialTheme.colorScheme.surface,
        shape = RoundedCornerShape(8.dp),
        tonalElevation = 2.dp,
        shadowElevation = 1.dp
    ) {
        if (suggestions.isEmpty()) {
            Text(
                text = message,
                style = MaterialTheme.typography.bodyMedium,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.padding(horizontal = 12.dp, vertical = 10.dp)
            )
        } else {
            Column {
                suggestions.forEachIndexed { index, suggestion ->
                    RouteSuggestionRow(
                        suggestion = suggestion,
                        onClick = { onSuggestionSelected(suggestion) }
                    )
                    if (index < suggestions.lastIndex) {
                        HorizontalDivider(color = MaterialTheme.colorScheme.outline.copy(alpha = 0.35f))
                    }
                }
            }
        }
    }
}

@Composable
private fun RouteSuggestionRow(
    suggestion: RoutePlaceSuggestion,
    onClick: () -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(horizontal = 12.dp, vertical = 9.dp),
        verticalArrangement = Arrangement.spacedBy(2.dp)
    ) {
        Text(
            text = suggestion.title,
            style = MaterialTheme.typography.labelLarge,
            maxLines = 1,
            overflow = TextOverflow.Ellipsis
        )
        if (suggestion.meta.isNotBlank()) {
            Text(
                text = suggestion.meta,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
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
        RouteLoadingProgress()
    } else {
        Text(
            text = text,
            style = MaterialTheme.typography.labelLarge,
            color = color
        )
    }
}

@Composable
private fun RouteLoadingProgress() {
    var progress by remember { mutableStateOf(0f) }
    LaunchedEffect(Unit) {
        val startedAt = System.currentTimeMillis()
        while (true) {
            val elapsedMs = System.currentTimeMillis() - startedAt
            val ratio = (elapsedMs.toFloat() / ROUTE_PROGRESS_DURATION_MS).coerceIn(0f, 1f)
            val eased = 1f - ((1f - ratio) * (1f - ratio))
            progress = (ROUTE_PROGRESS_MAX * eased).coerceAtMost(ROUTE_PROGRESS_MAX)
            delay(ROUTE_PROGRESS_INTERVAL_MS)
        }
    }
    val isHoldingProgress = progress >= ROUTE_PROGRESS_MAX
    val message = stringResource(
        if (isHoldingProgress) R.string.i18n_route_loadingstill else R.string.i18n_route_loading
    )
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 14.dp, horizontal = 4.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        Text(
            text = message,
            style = MaterialTheme.typography.titleMedium,
            color = MaterialTheme.colorScheme.onSurface,
            textAlign = TextAlign.Center
        )
        LinearProgressIndicator(
            progress = { progress },
            modifier = Modifier
                .fillMaxWidth(0.84f)
                .height(8.dp)
                .clip(RoundedCornerShape(999.dp)),
            color = MaterialTheme.colorScheme.primary,
            trackColor = MaterialTheme.colorScheme.primary.copy(alpha = 0.14f)
        )
        Text(
            text = "${(progress * 100).roundToInt()}%",
            style = MaterialTheme.typography.labelLarge,
            color = MaterialTheme.colorScheme.onSurfaceVariant
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
            label = stringResource(R.string.i18n_route_summarystationsshort),
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
    markerColor: Color,
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
                            .padding(top = 4.dp)
                            .size(18.dp)
                            .background(markerColor, CircleShape)
                    )
                }
                Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(3.dp)) {
                    Text(
                        text = feature.properties.stationName.ifBlank { feature.properties.operatorName },
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

private data class RoutePlaceSuggestion(
    val endpoint: RouteEndpoint,
    val title: String,
    val meta: String
)

private suspend fun resolveEndpoint(
    context: Context,
    existing: RouteEndpoint?,
    text: String
): RouteEndpoint? {
    existing?.let { return it }
    val query = text.trim()
    if (query.length < 2) return null
    return lookupRouteSuggestions(context, query, limit = 1).firstOrNull()?.endpoint
}

private suspend fun lookupRouteSuggestions(
    context: Context,
    query: String,
    limit: Int = ROUTE_SUGGESTION_LIMIT
): List<RoutePlaceSuggestion> {
    val normalizedQuery = query.trim()
    if (normalizedQuery.length < 2 || limit <= 0) return emptyList()
    val application = context.applicationContext as? WoladenApplication ?: return emptyList()
    return runCatching {
        application.liveApiClient.geocodeAutocomplete(normalizedQuery, limit = limit)
            .map(::routeSuggestionFromGeocode)
            .distinctBy { suggestion ->
                "${suggestion.title}:${(suggestion.endpoint.lat * 10_000).roundToInt()}:${(suggestion.endpoint.lon * 10_000).roundToInt()}"
            }
    }.getOrDefault(emptyList())
}

private fun routeSuggestionFromGeocode(result: GeocodeResult): RoutePlaceSuggestion {
    val title = result.name.ifBlank { result.label }
    val meta = listOf(result.locality, result.region, result.postalCode, result.country)
        .filter { it.isNotBlank() }
        .filterNot { it.equals(title, ignoreCase = true) }
        .distinct()
        .joinToString(" · ")
    return RoutePlaceSuggestion(
        endpoint = RouteEndpoint(lat = result.lat, lon = result.lon, label = result.label),
        title = title,
        meta = meta
    )
}

private fun firstNonBlank(vararg values: String?): String? =
    values.firstOrNull { !it.isNullOrBlank() }?.trim()

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

private fun formatRouteClockTime(epochMillis: Long): String {
    return java.text.SimpleDateFormat("HH:mm", Locale.getDefault()).format(java.util.Date(epochMillis))
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
    return when (feature.stationCardState) {
        StationCardState.OUT_OF_ORDER -> if (isDark) Color(0xFF3B121C) else Color(0xFFFFF1F2)
        StationCardState.OCCUPIED -> if (isDark) Color(0xFF26323D) else Color(0xFFE2E8F0)
        StationCardState.ONE_FREE_LEFT -> if (isDark) Color(0xFF332B12) else Color(0xFFFFFBEB)
        StationCardState.OFTEN_BROKEN -> if (isDark) Color(0xFF36161F) else Color(0xFFFFF7F8)
        StationCardState.OFTEN_OCCUPIED -> if (isDark) Color(0xFF0F1E27) else Color(0xFFF8FAFC)
        StationCardState.UNKNOWN -> MaterialTheme.colorScheme.surface
        StationCardState.DEFAULT -> MaterialTheme.colorScheme.surface
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
