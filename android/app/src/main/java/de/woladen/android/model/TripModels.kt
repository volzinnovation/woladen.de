package de.woladen.android.model

import kotlin.math.max
import kotlin.math.min
import kotlin.math.cos
import kotlin.math.pow
import kotlin.math.sqrt
import kotlin.math.roundToInt

/**
 * The small, platform independent part of the iPhone trip model. Keeping the
 * calculations here makes saved plans useful to the phone UI and Android Auto
 * without coupling either surface to Compose.
 */
data class VehicleEnergySettings(
    val batteryCapacityKWh: Double = 75.0,
    val consumptionKWhPer100Km: Double = 18.0,
    val reserveSocPercent: Double = 10.0,
    val targetSocPercent: Double = 80.0,
    val averageChargingPowerKw: Double = 120.0,
    val earlyWindowKm: Double = 60.0,
    val maximumDetourMinutes: Double = 15.0
) {
    val normalized: VehicleEnergySettings
        get() {
            val reserve = reserveSocPercent.coerceIn(5.0, 40.0)
            return copy(
                batteryCapacityKWh = batteryCapacityKWh.coerceIn(10.0, 200.0),
                consumptionKWhPer100Km = consumptionKWhPer100Km.coerceIn(5.0, 50.0),
                reserveSocPercent = reserve,
                targetSocPercent = targetSocPercent.coerceIn(min(90.0, reserve + 5.0), 90.0),
                averageChargingPowerKw = averageChargingPowerKw.coerceIn(3.0, 400.0),
                earlyWindowKm = earlyWindowKm.coerceIn(10.0, 120.0),
                maximumDetourMinutes = maximumDetourMinutes.coerceIn(5.0, 30.0)
            )
        }

    fun usableRangeKm(departureSocPercent: Double): Double {
        val value = normalized
        val usablePercent = max(0.0, departureSocPercent.coerceIn(0.0, 100.0) - value.reserveSocPercent)
        return value.batteryCapacityKWh * usablePercent / value.consumptionKWhPer100Km
    }

    fun projectedArrivalSoc(departureSocPercent: Double, distanceKm: Double): Double {
        val value = normalized
        val consumed = max(0.0, distanceKm) / 100.0 * value.consumptionKWhPer100Km
        return (departureSocPercent - consumed / value.batteryCapacityKWh * 100.0).coerceIn(0.0, 100.0)
    }

    fun estimatedChargeMinutes(arrivalSocPercent: Double): Double {
        val value = normalized
        val missingKWh = max(0.0, value.targetSocPercent - arrivalSocPercent) * value.batteryCapacityKWh / 100.0
        return missingKWh / value.averageChargingPowerKw * 60.0
    }
}

data class VehicleProfile(
    val id: String,
    val name: String,
    val settings: VehicleEnergySettings = VehicleEnergySettings()
)

/** Provider handling for route planning, matching the iPhone's prefer/only modes. */
enum class ProviderPreferenceMode { PREFER, ONLY }

enum class WoladenMode { PLAN, TRIP }

data class TripRouteSnapshot(
    val origin: RouteEndpoint,
    val destination: RouteEndpoint,
    val distanceM: Int,
    val durationS: Int,
    val geometryCoordinates: List<List<Double>>,
    val calculatedAtEpochMs: Long,
    val filter: RouteFilterPayload,
    val initialSocPercent: Double
) {
    val summary: RouteSummary
        get() = RouteSummary(
            source = "saved-plan",
            profile = "driving-car",
            distanceM = distanceM,
            durationS = durationS,
            geometry = RouteGeometry("LineString", geometryCoordinates)
        )
}

data class TripStationSnapshot(
    val stationId: String,
    val countryCode: String,
    val stationName: String,
    val operatorName: String,
    val city: String,
    val address: String,
    val latitude: Double,
    val longitude: Double,
    val maxPowerKw: Double,
    val chargingPointsCount: Int,
    val routePositionM: Int,
    val routeDetourM: Int,
    val availabilityStatus: AvailabilityStatus,
    val availableEvses: Int,
    val totalEvses: Int,
    val classification: String,
    val reliabilityPercent: Double?,
    val lastUnavailableAt: String?,
    val providerCanonicalId: String?,
    val priceDisplay: String,
    val oftenBroken: Boolean,
    val oftenOccupied: Boolean,
    val operatorGroupIds: Set<String> = emptySet()
) {
    companion object {
        fun fromFeature(feature: GeoJsonFeature): TripStationSnapshot {
            val properties = feature.properties
            val counts = feature.availabilityCounts
            return TripStationSnapshot(
                stationId = properties.stationId,
                countryCode = properties.countryCode,
                stationName = properties.stationName,
                operatorName = properties.operatorName,
                city = properties.city,
                address = properties.address,
                latitude = feature.latitude,
                longitude = feature.longitude,
                maxPowerKw = properties.displayedMaxPowerKw,
                chargingPointsCount = properties.chargingPointsCount,
                routePositionM = feature.routeMetadata?.routePositionM ?: 0,
                routeDetourM = feature.routeMetadata?.routeDetourM ?: 0,
                availabilityStatus = feature.availabilityStatus,
                availableEvses = counts.available,
                totalEvses = counts.total,
                classification = properties.effectiveStationClassification,
                reliabilityPercent = properties.reliabilityPercent,
                lastUnavailableAt = properties.lastUnavailableAt,
                providerCanonicalId = properties.providerCanonicalId,
                priceDisplay = feature.displayPrice,
                oftenBroken = feature.isOftenBrokenFromDailyAnalysis,
                oftenOccupied = feature.isOftenOccupiedFromDailyAnalysis,
                operatorGroupIds = properties.operatorGroupIds
            )
        }
    }
}

enum class TripStopState { PLANNED, COMPLETED, SKIPPED, REJECTED }

data class TripStopSelection(
    val stationId: String,
    val state: TripStopState = TripStopState.PLANNED,
    val selectedAtEpochMs: Long = System.currentTimeMillis()
)

data class ChargingWindow(
    val index: Int,
    val startPositionM: Int,
    val endPositionM: Int,
    val departurePositionM: Int,
    val departureSocPercent: Double,
    val candidateStationIds: List<String>,
    val projectedArrivalSocByStationId: Map<String, Double>,
    val selectedStationId: String? = null
)

enum class RoutePlanState { DRAFT, ACTIVE, COMPLETED }

data class RoutePlan(
    val id: String,
    val name: String,
    val route: TripRouteSnapshot,
    val vehicleSettings: VehicleEnergySettings,
    val rawStations: List<TripStationSnapshot>,
    val windows: List<ChargingWindow>,
    val stopSelections: List<TripStopSelection>,
    val state: RoutePlanState,
    val createdAtEpochMs: Long,
    val updatedAtEpochMs: Long,
    val providerMode: ProviderPreferenceMode = ProviderPreferenceMode.PREFER,
    val selectedProviderNames: List<String> = emptyList(),
    val stationTargetId: String? = null
) {
    val isStationTargetTrip: Boolean
        get() = !stationTargetId.isNullOrBlank()
    val selectedStopIds: List<String>
        get() = stopSelections
            .filter { it.state == TripStopState.PLANNED }
            .map { it.stationId }
            .sortedBy { station(it)?.routePositionM ?: Int.MAX_VALUE }

    val nextStop: TripStationSnapshot?
        get() = selectedStopIds.firstNotNullOfOrNull(::station)

    val isReadyForTrip: Boolean
        get() = windows.all { it.selectedStationId != null } || windows.isEmpty()

    fun station(stationId: String): TripStationSnapshot? = rawStations.firstOrNull { it.stationId == stationId }

    val completedStopIds: List<String>
        get() = stopSelections.filter { it.state == TripStopState.COMPLETED }.map { it.stationId }

    val rejectedStopIds: List<String>
        get() = stopSelections
            .filter { it.state == TripStopState.SKIPPED || it.state == TripStopState.REJECTED }
            .map { it.stationId }

    /** Select the first viable station in every energy window when starting a plan. */
    fun withDefaultSelections(nowEpochMs: Long = System.currentTimeMillis()): RoutePlan {
        val selections = stopSelections.toMutableList()
        val selectedWindows = windows.map { window ->
            val selected = window.selectedStationId ?: window.candidateStationIds.firstOrNull()
            if (selected != null && selections.none { it.stationId == selected }) {
                selections += TripStopSelection(selected, TripStopState.PLANNED, nowEpochMs)
            }
            window.copy(selectedStationId = selected)
        }
        return copy(windows = selectedWindows, stopSelections = selections)
    }
}

/** ETA data shared by the phone trip card and Android Auto. */
data class TripEtaState(
    val currentRoutePositionM: Int,
    val progress: Double,
    val nextStopArrivalEpochMs: Long?,
    val destinationArrivalEpochMs: Long,
    val nextStopTravelTimeS: Int?,
    val totalTravelTimeS: Int,
    val projectedArrivalSocPercent: Double,
    val updatedAtEpochMs: Long,
    val trafficAdjusted: Boolean = false
)

data class TrafficEtaResult(
    val destinationDurationS: Int,
    val destinationStaticDurationS: Int,
    val nextStopDurationS: Int?,
    val nextStopStaticDurationS: Int?,
    val trafficDelayS: Int,
    val updatedAtEpochS: Long
)

/** Base ETA equivalent to the iPhone estimate when live traffic is unavailable. */
object TripEtaEstimator {
    fun estimate(
        plan: RoutePlan,
        currentRoutePositionM: Int,
        nowEpochMs: Long = System.currentTimeMillis()
    ): TripEtaState {
        val routeDistance = plan.route.distanceM.coerceAtLeast(0)
        val current = currentRoutePositionM.coerceIn(0, routeDistance)
        val settings = plan.vehicleSettings.normalized
        val selectedStops = plan.selectedStopIds
            .mapNotNull(plan::station)
            .filter { it.routePositionM >= current }
            .sortedBy { it.routePositionM }
        val secondsPerMeter = if (routeDistance > 0) plan.route.durationS.toDouble() / routeDistance else 0.0
        val remainingDriveSeconds = ((routeDistance - current) * secondsPerMeter).roundToIntSafe()
        var cursor = current
        var soc = settings.projectedArrivalSoc(plan.route.initialSocPercent, current / 1000.0)
        var chargingSeconds = 0
        var nextStopTravelSeconds: Int? = null
        var nextStopArrival: Long? = null
        selectedStops.forEachIndexed { index, station ->
            val segmentM = (station.routePositionM - cursor).coerceAtLeast(0)
            val segmentSeconds = (segmentM * secondsPerMeter).roundToIntSafe()
            if (index == 0) {
                nextStopTravelSeconds = segmentSeconds
                nextStopArrival = nowEpochMs + segmentSeconds * 1_000L
            }
            soc = settings.projectedArrivalSoc(soc, segmentM / 1000.0)
            val chargeMinutes = settings.estimatedChargeMinutes(soc)
            chargingSeconds += (chargeMinutes * 60.0).roundToIntSafe()
            soc = settings.targetSocPercent
            cursor = station.routePositionM
        }
        val finalSegmentM = (routeDistance - cursor).coerceAtLeast(0)
        val destinationSoc = settings.projectedArrivalSoc(soc, finalSegmentM / 1000.0)
        val totalSeconds = (remainingDriveSeconds + chargingSeconds).coerceAtLeast(0)
        return TripEtaState(
            currentRoutePositionM = current,
            progress = if (routeDistance == 0) 1.0 else current.toDouble() / routeDistance,
            nextStopArrivalEpochMs = nextStopArrival,
            destinationArrivalEpochMs = nowEpochMs + totalSeconds * 1_000L,
            nextStopTravelTimeS = nextStopTravelSeconds,
            totalTravelTimeS = totalSeconds,
            projectedArrivalSocPercent = destinationSoc,
            updatedAtEpochMs = nowEpochMs
        )
    }

    fun applyTraffic(base: TripEtaState, plan: RoutePlan, traffic: TrafficEtaResult, nowEpochMs: Long): TripEtaState {
        val routeDistance = plan.route.distanceM.coerceAtLeast(0)
        val secondsPerMeter = if (routeDistance > 0) plan.route.durationS.toDouble() / routeDistance else 0.0
        val baseDriveSeconds = ((routeDistance - base.currentRoutePositionM) * secondsPerMeter).roundToIntSafe()
        val chargingSeconds = (base.totalTravelTimeS - baseDriveSeconds).coerceAtLeast(0)
        val totalSeconds = traffic.destinationDurationS.coerceAtLeast(0) + chargingSeconds
        return base.copy(
            nextStopArrivalEpochMs = traffic.nextStopDurationS?.let { nowEpochMs + it.coerceAtLeast(0) * 1_000L },
            destinationArrivalEpochMs = nowEpochMs + totalSeconds * 1_000L,
            nextStopTravelTimeS = traffic.nextStopDurationS,
            totalTravelTimeS = totalSeconds,
            updatedAtEpochMs = nowEpochMs,
            trafficAdjusted = true
        )
    }
}

/** Return the nearest route position for a GPS fix using the saved polyline. */
fun projectRoutePositionM(route: TripRouteSnapshot, latitude: Double, longitude: Double): Int {
    val coordinates = route.geometryCoordinates
    if (coordinates.size < 2) return 0
    var accumulated = 0.0
    var bestDistance = Double.POSITIVE_INFINITY
    var bestPosition = 0.0
    for (index in 0 until coordinates.lastIndex) {
        val start = coordinates[index]
        val end = coordinates[index + 1]
        if (start.size < 2 || end.size < 2) continue
        val startLon = start[0]
        val startLat = start[1]
        val endLon = end[0]
        val endLat = end[1]
        val meanLat = Math.toRadians((startLat + endLat + latitude) / 3.0)
        val scaleX = 111_320.0 * cos(meanLat)
        val scaleY = 110_540.0
        val dx = (endLon - startLon) * scaleX
        val dy = (endLat - startLat) * scaleY
        val px = (longitude - startLon) * scaleX
        val py = (latitude - startLat) * scaleY
        val segmentLength = sqrt(dx.pow(2) + dy.pow(2))
        val fraction = if (segmentLength == 0.0) 0.0 else ((px * dx + py * dy) / segmentLength.pow(2)).coerceIn(0.0, 1.0)
        val nearestX = dx * fraction
        val nearestY = dy * fraction
        val distance = sqrt((px - nearestX).pow(2) + (py - nearestY).pow(2))
        if (distance < bestDistance) {
            bestDistance = distance
            bestPosition = accumulated + segmentLength * fraction
        }
        accumulated += segmentLength
    }
    if (accumulated <= 0.0) return 0
    return (bestPosition * route.distanceM.coerceAtLeast(0) / accumulated).roundToIntSafe()
}

private fun Double.roundToIntSafe(): Int =
    coerceIn(Int.MIN_VALUE.toDouble(), Int.MAX_VALUE.toDouble()).roundToInt()

object EnergyRoutePlanner {
    fun build(
        routeDistanceM: Int,
        stations: List<TripStationSnapshot>,
        settings: VehicleEnergySettings,
        initialSocPercent: Double,
        providerMode: ProviderPreferenceMode = ProviderPreferenceMode.PREFER,
        selectedProviderNames: List<String> = emptyList()
    ): List<ChargingWindow> {
        val value = settings.normalized
        val total = routeDistanceM.coerceAtLeast(0)
        val selectedProviders = selectedProviderNames.map(::normalizeProvider).filter { it.isNotBlank() }.toSet()
        val providerFiltered = when {
            providerMode != ProviderPreferenceMode.ONLY || selectedProviders.isEmpty() -> stations
            else -> stations.filter { station ->
                val groups = station.operatorGroupIds.map(::normalizeProvider)
                normalizeProvider(station.operatorName) in selectedProviders || groups.any { it in selectedProviders }
            }
        }
        val sorted = providerFiltered.sortedWith(
            compareBy<TripStationSnapshot> { station ->
                if (providerMode == ProviderPreferenceMode.PREFER && selectedProviders.isNotEmpty() &&
                    (normalizeProvider(station.operatorName) in selectedProviders || station.operatorGroupIds.any { normalizeProvider(it) in selectedProviders })
                ) 0 else 1
            }.thenBy { it.routePositionM }
        )
        val windows = mutableListOf<ChargingWindow>()
        var departurePosition = 0
        var departureSoc = initialSocPercent.coerceIn(0.0, 100.0)
        var index = 0
        repeat(12) {
            val remaining = total - departurePosition
            if (remaining <= 0) return@repeat
            val reachableM = (value.usableRangeKm(departureSoc) * 1000.0).toInt()
            if (reachableM >= remaining) return@repeat
            val end = min(total, departurePosition + reachableM)
            val candidateStations = sorted.filter { station ->
                station.routePositionM > departurePosition &&
                    station.routePositionM <= end &&
                    station.routePositionM + station.routeDetourM <= end + (value.earlyWindowKm * 1000.0).toInt()
            }
            if (candidateStations.isEmpty()) return@repeat
            val projected = candidateStations.associate { station ->
                station.stationId to value.projectedArrivalSoc(
                    departureSoc,
                    (station.routePositionM - departurePosition).coerceAtLeast(0) / 1000.0
                )
            }
            windows += ChargingWindow(
                index = index++,
                startPositionM = departurePosition,
                endPositionM = end,
                departurePositionM = candidateStations.first().routePositionM,
                departureSocPercent = departureSoc,
                candidateStationIds = candidateStations.map { it.stationId },
                projectedArrivalSocByStationId = projected
            )
            departurePosition = candidateStations.first().routePositionM
            departureSoc = value.targetSocPercent
        }
        return windows
    }

    private fun normalizeProvider(value: String): String = value.trim().lowercase().replace(Regex("\\s+"), " ")
}
