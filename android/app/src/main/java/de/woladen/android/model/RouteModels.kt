package de.woladen.android.model

import kotlin.math.roundToInt

data class RouteEndpoint(
    val lat: Double,
    val lon: Double,
    val label: String
)

data class RouteFilterPayload(
    val operator: String,
    val minPowerKw: Int,
    val minAmenitiesTotal: Int,
    val selectedAmenities: List<String>,
    val amenityNameQuery: String,
    val availableOnly: Boolean,
    val currentlyOpenOnly: Boolean
) {
    companion object {
        fun from(filterState: FilterState): RouteFilterPayload {
            return RouteFilterPayload(
                operator = filterState.normalizedOperatorNames.singleOrNull().orEmpty(),
                minPowerKw = filterState.minPowerKw.coerceAtLeast(0.0).toInt(),
                minAmenitiesTotal = filterState.minAmenityCount.coerceAtLeast(0.0).roundToInt(),
                selectedAmenities = filterState.selectedAmenities
                    .filter { it.matches("^amenity_[a-z0-9_]+$".toRegex()) }
                    .sorted(),
                amenityNameQuery = filterState.amenityNameQuery.trim(),
                availableOnly = false,
                currentlyOpenOnly = filterState.currentlyOpenOnly
            )
        }
    }
}

data class RouteGeometry(
    val type: String,
    val coordinates: List<List<Double>>
)

data class RouteSummary(
    val source: String,
    val profile: String,
    val distanceM: Int,
    val durationS: Int,
    val geometry: RouteGeometry
)

data class RouteNearestPoint(
    val lat: Double,
    val lon: Double
)

data class RouteStationMetadata(
    val driveDistanceToRouteM: Int,
    val routeDetourM: Int,
    val straightLineDistanceToRouteM: Int,
    val routePositionM: Int,
    val nearestRoutePoint: RouteNearestPoint?
)

data class RouteStationCandidate(
    val station: CatalogStation,
    val route: RouteStationMetadata
)

data class RouteChargerResponse(
    val route: RouteSummary,
    val stations: List<RouteStationCandidate>,
    val source: String
)
