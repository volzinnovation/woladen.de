package de.woladen.android.store

import android.content.Context
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.EnergyRoutePlanner
import de.woladen.android.model.FilterState
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.RouteEndpoint
import de.woladen.android.model.RouteFilterPayload
import de.woladen.android.model.RoutePlan
import de.woladen.android.model.RoutePlanState
import de.woladen.android.model.RouteSummary
import de.woladen.android.model.TripRouteSnapshot
import de.woladen.android.model.TripStationSnapshot
import de.woladen.android.model.TripStopSelection
import de.woladen.android.model.TripStopState
import de.woladen.android.model.VehicleEnergySettings
import de.woladen.android.model.WoladenMode
import de.woladen.android.widget.WoladenTripWidgetProvider
import org.json.JSONArray
import org.json.JSONObject
import java.util.UUID

private const val TRIP_PREFERENCES = "woladen.trip"
private const val PLANS_KEY = "plans.v1"
private const val ACTIVE_PLAN_KEY = "activePlanId.v1"
private const val MODE_KEY = "mode.v1"
private const val VEHICLE_KEY = "vehicle.v1"
private const val MAX_SAVED_PLANS = 20

/** Persistent phone-side trip state shared by the route tab and car surfaces. */
class TripStore(context: Context) {
    private val appContext = context.applicationContext
    private val preferences = context.getSharedPreferences(TRIP_PREFERENCES, Context.MODE_PRIVATE)

    var plans: List<RoutePlan> by mutableStateOf(loadPlans())
        private set

    var activePlanId: String? by mutableStateOf(preferences.getString(ACTIVE_PLAN_KEY, null))
        private set

    var mode: WoladenMode by mutableStateOf(
        preferences.getString(MODE_KEY, null)?.let { raw ->
            runCatching { WoladenMode.valueOf(raw) }.getOrNull()
        } ?: WoladenMode.PLAN
    )
        private set

    var vehicleSettings: VehicleEnergySettings by mutableStateOf(loadVehicleSettings())
        private set

    val activePlan: RoutePlan?
        get() = activePlanId?.let { id -> plans.firstOrNull { it.id == id } }

    val sortedPlans: List<RoutePlan>
        get() = plans.sortedByDescending { it.updatedAtEpochMs }

    fun saveCalculatedRoute(
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        summary: RouteSummary,
        features: List<GeoJsonFeature>,
        filter: FilterState,
        initialSocPercent: Double,
        existingPlanId: String? = null
    ): RoutePlan {
        val existing = existingPlanId?.let { id -> plans.firstOrNull { it.id == id } }
        val now = System.currentTimeMillis()
        val route = TripRouteSnapshot(
            origin = origin,
            destination = destination,
            distanceM = summary.distanceM,
            durationS = summary.durationS,
            geometryCoordinates = summary.geometry.coordinates,
            calculatedAtEpochMs = now,
            filter = RouteFilterPayload.from(filter),
            initialSocPercent = initialSocPercent.coerceIn(1.0, 100.0)
        )
        val snapshots = features
            .map(TripStationSnapshot.Companion::fromFeature)
            .sortedBy { it.routePositionM }
        val windows = EnergyRoutePlanner.build(
            routeDistanceM = route.distanceM,
            stations = snapshots,
            settings = vehicleSettings,
            initialSocPercent = route.initialSocPercent
        )
        val existingSelections = existing?.stopSelections.orEmpty()
        val selectedByWindow = windows.map { window ->
            window.copy(
                selectedStationId = existingSelections
                    .firstOrNull { it.state == TripStopState.PLANNED && it.stationId in window.candidateStationIds }
                    ?.stationId
            )
        }
        val plan = RoutePlan(
            id = existing?.id ?: UUID.randomUUID().toString(),
            name = "${origin.label} → ${destination.label}",
            route = route,
            vehicleSettings = vehicleSettings.normalized,
            rawStations = snapshots,
            windows = selectedByWindow,
            stopSelections = existingSelections,
            state = existing?.state ?: RoutePlanState.DRAFT,
            createdAtEpochMs = existing?.createdAtEpochMs ?: now,
            updatedAtEpochMs = now
        )
        upsert(plan)
        return plan
    }

    fun updateVehicleSettings(settings: VehicleEnergySettings) {
        vehicleSettings = settings.normalized
        preferences.edit().putString(VEHICLE_KEY, TripJson.vehicleToJson(vehicleSettings).toString()).apply()
        activePlan?.let { plan ->
            val windows = EnergyRoutePlanner.build(
                routeDistanceM = plan.route.distanceM,
                stations = plan.rawStations,
                settings = vehicleSettings,
                initialSocPercent = plan.route.initialSocPercent
            ).map { window ->
                val selected = plan.stopSelections.firstOrNull {
                    it.state == TripStopState.PLANNED && it.stationId in window.candidateStationIds
                }?.stationId
                window.copy(selectedStationId = selected)
            }
            upsert(plan.copy(vehicleSettings = vehicleSettings, windows = windows, updatedAtEpochMs = System.currentTimeMillis()))
        }
    }

    fun activate(planId: String): Boolean {
        val selected = plans.firstOrNull { it.id == planId } ?: return false
        val prepared = selected.withDefaultSelections()
        plans = plans.map { plan ->
            when {
                plan.id == selected.id -> prepared.copy(state = RoutePlanState.ACTIVE, updatedAtEpochMs = System.currentTimeMillis())
                plan.state == RoutePlanState.ACTIVE -> plan.copy(state = RoutePlanState.DRAFT)
                else -> plan
            }
        }
        activePlanId = selected.id
        mode = WoladenMode.TRIP
        persist()
        return true
    }

    fun leaveTripMode() {
        mode = WoladenMode.PLAN
        persist()
    }

    fun toggleMode() {
        if (mode == WoladenMode.TRIP) leaveTripMode() else activePlanId?.let { activate(it) } ?: run { mode = WoladenMode.PLAN }
    }

    fun markStop(stationId: String, state: TripStopState = TripStopState.PLANNED) {
        activePlanId?.let { updateStop(it, stationId, state) }
    }

    fun selectStop(planId: String, stationId: String): Boolean {
        val plan = plans.firstOrNull { it.id == planId } ?: return false
        val window = plan.windows.firstOrNull { stationId in it.candidateStationIds } ?: return false
        val now = System.currentTimeMillis()
        val selections = plan.stopSelections
            .filterNot { existing ->
                existing.stationId == stationId ||
                    (existing.state == TripStopState.PLANNED && existing.stationId in window.candidateStationIds)
            }
            .toMutableList()
        selections += TripStopSelection(stationId, TripStopState.PLANNED, now)
        val windows = plan.windows.map { current ->
            if (current.index == window.index) current.copy(selectedStationId = stationId) else current
        }
        upsert(plan.copy(windows = windows, stopSelections = selections, updatedAtEpochMs = now))
        return true
    }

    fun replaceStop(planId: String, oldStationId: String, newStationId: String): Boolean {
        val plan = plans.firstOrNull { it.id == planId } ?: return false
        val window = plan.windows.firstOrNull { oldStationId in it.candidateStationIds } ?: return false
        if (newStationId !in window.candidateStationIds) return false
        return selectStop(planId, newStationId)
    }

    fun completeNextStop(): Boolean = updateNextStop(TripStopState.COMPLETED)

    fun skipNextStop(): Boolean = updateNextStop(TripStopState.SKIPPED)

    fun endTrip(markCompleted: Boolean = false) {
        val id = activePlanId
        if (id != null) {
            val nextState = if (markCompleted) RoutePlanState.COMPLETED else RoutePlanState.DRAFT
            plans = plans.map { current ->
                if (current.id == id) current.copy(state = nextState, updatedAtEpochMs = System.currentTimeMillis()) else current
            }
        }
        activePlanId = null
        mode = WoladenMode.PLAN
        persist()
    }

    fun delete(planId: String) {
        plans = plans.filterNot { it.id == planId }
        if (activePlanId == planId) {
            activePlanId = null
            mode = WoladenMode.PLAN
        }
        persist()
    }

    private fun upsert(plan: RoutePlan) {
        val next = plans.filterNot { it.id == plan.id }.toMutableList()
        next += plan
        plans = next.sortedByDescending { it.updatedAtEpochMs }.take(MAX_SAVED_PLANS)
        persist()
    }

    private fun updateStop(planId: String, stationId: String, state: TripStopState): Boolean {
        val plan = plans.firstOrNull { it.id == planId } ?: return false
        if (plan.station(stationId) == null) return false
        val next = plan.copy(
            windows = plan.windows.map { window ->
                if (stationId in window.candidateStationIds && state == TripStopState.PLANNED) {
                    window.copy(selectedStationId = stationId)
                } else {
                    window
                }
            },
            stopSelections = plan.stopSelections.filterNot { it.stationId == stationId } +
                TripStopSelection(stationId = stationId, state = state),
            updatedAtEpochMs = System.currentTimeMillis()
        )
        upsert(next)
        return true
    }

    private fun updateNextStop(state: TripStopState): Boolean {
        val station = activePlan?.nextStop ?: return false
        return activePlanId?.let { updateStop(it, station.stationId, state) } ?: false
    }

    private fun persist() {
        preferences.edit()
            .putString(PLANS_KEY, TripJson.encodePlans(plans))
            .putString(ACTIVE_PLAN_KEY, activePlanId)
            .putString(MODE_KEY, mode.name)
            .apply()
        WoladenTripWidgetProvider.refresh(appContext)
    }

    private fun loadPlans(): List<RoutePlan> {
        return preferences.getString(PLANS_KEY, null)?.let(TripJson::decodePlans).orEmpty()
    }

    private fun loadVehicleSettings(): VehicleEnergySettings {
        return preferences.getString(VEHICLE_KEY, null)?.let { raw ->
            runCatching { TripJson.vehicleFromJson(JSONObject(raw)) }.getOrNull()
        }?.normalized ?: VehicleEnergySettings()
    }
}

internal object TripJson {
    fun encodePlans(plans: List<RoutePlan>): String = JSONArray().apply { plans.forEach { put(planToJson(it)) } }.toString()

    fun decodePlans(raw: String): List<RoutePlan> = runCatching {
        val values = JSONArray(raw)
        buildList {
            for (index in 0 until values.length()) {
                planFromJson(values.optJSONObject(index) ?: continue)?.let(::add)
            }
        }
    }.getOrDefault(emptyList())

    fun vehicleToJson(settings: VehicleEnergySettings): JSONObject = JSONObject().apply {
        put("battery_kwh", settings.batteryCapacityKWh)
        put("consumption_kwh_per_100_km", settings.consumptionKWhPer100Km)
        put("reserve_soc", settings.reserveSocPercent)
        put("target_soc", settings.targetSocPercent)
        put("average_power_kw", settings.averageChargingPowerKw)
        put("early_window_km", settings.earlyWindowKm)
        put("maximum_detour_minutes", settings.maximumDetourMinutes)
    }

    fun vehicleFromJson(json: JSONObject): VehicleEnergySettings = VehicleEnergySettings(
        batteryCapacityKWh = json.optDouble("battery_kwh", 75.0),
        consumptionKWhPer100Km = json.optDouble("consumption_kwh_per_100_km", 18.0),
        reserveSocPercent = json.optDouble("reserve_soc", 10.0),
        targetSocPercent = json.optDouble("target_soc", 80.0),
        averageChargingPowerKw = json.optDouble("average_power_kw", 120.0),
        earlyWindowKm = json.optDouble("early_window_km", 60.0),
        maximumDetourMinutes = json.optDouble("maximum_detour_minutes", 15.0)
    )

    private fun planToJson(plan: RoutePlan): JSONObject = JSONObject().apply {
        put("id", plan.id)
        put("name", plan.name)
        put("created_at", plan.createdAtEpochMs)
        put("updated_at", plan.updatedAtEpochMs)
        put("state", plan.state.name)
        put("route", routeToJson(plan.route))
        put("vehicle", vehicleToJson(plan.vehicleSettings))
        put("stations", JSONArray().apply { plan.rawStations.forEach { put(stationToJson(it)) } })
        put("windows", JSONArray().apply { plan.windows.forEach { put(windowToJson(it)) } })
        put("stops", JSONArray().apply { plan.stopSelections.forEach { put(stopToJson(it)) } })
    }

    private fun planFromJson(json: JSONObject): RoutePlan? {
        val route = routeFromJson(json.optJSONObject("route") ?: return null)
        return RoutePlan(
            id = json.optString("id").takeIf { it.isNotBlank() } ?: return null,
            name = json.optString("name").ifBlank { "Saved route" },
            route = route,
            vehicleSettings = vehicleFromJson(json.optJSONObject("vehicle") ?: JSONObject()),
            rawStations = decodeArray(json.optJSONArray("stations"), ::stationFromJson),
            windows = decodeArray(json.optJSONArray("windows"), ::windowFromJson),
            stopSelections = decodeArray(json.optJSONArray("stops"), ::stopFromJson),
            state = runCatching { RoutePlanState.valueOf(json.optString("state")) }.getOrDefault(RoutePlanState.DRAFT),
            createdAtEpochMs = json.optLong("created_at", System.currentTimeMillis()),
            updatedAtEpochMs = json.optLong("updated_at", System.currentTimeMillis())
        )
    }

    private fun routeToJson(route: TripRouteSnapshot): JSONObject = JSONObject().apply {
        put("origin", endpointToJson(route.origin))
        put("destination", endpointToJson(route.destination))
        put("distance_m", route.distanceM)
        put("duration_s", route.durationS)
        put("calculated_at", route.calculatedAtEpochMs)
        put("initial_soc", route.initialSocPercent)
        put("geometry", JSONArray().apply { route.geometryCoordinates.forEach { put(JSONArray(it)) } })
        put("filter", filterToJson(route.filter))
    }

    private fun routeFromJson(json: JSONObject): TripRouteSnapshot = TripRouteSnapshot(
        origin = endpointFromJson(json.optJSONObject("origin") ?: JSONObject()),
        destination = endpointFromJson(json.optJSONObject("destination") ?: JSONObject()),
        distanceM = json.optInt("distance_m", 0).coerceAtLeast(0),
        durationS = json.optInt("duration_s", 0).coerceAtLeast(0),
        geometryCoordinates = decodeCoordinates(json.optJSONArray("geometry")),
        calculatedAtEpochMs = json.optLong("calculated_at", System.currentTimeMillis()),
        filter = filterFromJson(json.optJSONObject("filter") ?: JSONObject()),
        initialSocPercent = json.optDouble("initial_soc", 80.0).coerceIn(1.0, 100.0)
    )

    private fun endpointToJson(endpoint: RouteEndpoint): JSONObject = JSONObject().apply {
        put("lat", endpoint.lat)
        put("lon", endpoint.lon)
        put("label", endpoint.label)
    }

    private fun endpointFromJson(json: JSONObject): RouteEndpoint = RouteEndpoint(
        lat = json.optDouble("lat", 0.0), lon = json.optDouble("lon", 0.0), label = json.optString("label").ifBlank { "Saved location" }
    )

    private fun filterToJson(filter: RouteFilterPayload): JSONObject = JSONObject().apply {
        put("operator", filter.operator)
        put("operator_groups", JSONArray(filter.operatorGroupIds))
        put("min_power_kw", filter.minPowerKw)
        put("min_amenities", filter.minAmenitiesTotal)
        put("amenities", JSONArray(filter.selectedAmenities))
        put("amenity_query", filter.amenityNameQuery)
        put("available_only", filter.availableOnly)
        put("open_only", filter.currentlyOpenOnly)
    }

    private fun filterFromJson(json: JSONObject): RouteFilterPayload = RouteFilterPayload(
        operator = json.optString("operator"),
        operatorGroupIds = strings(json.optJSONArray("operator_groups")),
        minPowerKw = json.optInt("min_power_kw", 50),
        minAmenitiesTotal = json.optInt("min_amenities", 0),
        selectedAmenities = strings(json.optJSONArray("amenities")),
        amenityNameQuery = json.optString("amenity_query"),
        availableOnly = json.optBoolean("available_only", false),
        currentlyOpenOnly = json.optBoolean("open_only", false)
    )

    private fun stationToJson(station: TripStationSnapshot): JSONObject = JSONObject().apply {
        put("station_id", station.stationId)
        put("country_code", station.countryCode)
        put("station_name", station.stationName)
        put("operator", station.operatorName)
        put("city", station.city)
        put("address", station.address)
        put("lat", station.latitude)
        put("lon", station.longitude)
        put("power_kw", station.maxPowerKw)
        put("charging_points", station.chargingPointsCount)
        put("route_position_m", station.routePositionM)
        put("route_detour_m", station.routeDetourM)
        put("availability", station.availabilityStatus.rawValue)
        put("available_evses", station.availableEvses)
        put("total_evses", station.totalEvses)
        put("classification", station.classification)
        station.reliabilityPercent?.let { put("reliability", it) }
        station.lastUnavailableAt?.let { put("last_unavailable", it) }
        station.providerCanonicalId?.let { put("provider_id", it) }
        put("price", station.priceDisplay)
        put("often_broken", station.oftenBroken)
        put("often_occupied", station.oftenOccupied)
    }

    private fun stationFromJson(json: JSONObject): TripStationSnapshot? {
        val id = json.optString("station_id").takeIf { it.isNotBlank() } ?: return null
        return TripStationSnapshot(
            stationId = id,
            countryCode = json.optString("country_code"), stationName = json.optString("station_name"),
            operatorName = json.optString("operator"), city = json.optString("city"), address = json.optString("address"),
            latitude = json.optDouble("lat", 0.0), longitude = json.optDouble("lon", 0.0), maxPowerKw = json.optDouble("power_kw", 0.0),
            chargingPointsCount = json.optInt("charging_points", 0), routePositionM = json.optInt("route_position_m", 0),
            routeDetourM = json.optInt("route_detour_m", 0), availabilityStatus = AvailabilityStatus.fromRaw(json.optString("availability")),
            availableEvses = json.optInt("available_evses", 0), totalEvses = json.optInt("total_evses", 0),
            classification = json.optString("classification"), reliabilityPercent = json.optNullableDouble("reliability"),
            lastUnavailableAt = json.optString("last_unavailable").ifBlank { null }, providerCanonicalId = json.optString("provider_id").ifBlank { null },
            priceDisplay = json.optString("price"), oftenBroken = json.optBoolean("often_broken"), oftenOccupied = json.optBoolean("often_occupied")
        )
    }

    private fun windowToJson(window: de.woladen.android.model.ChargingWindow): JSONObject = JSONObject().apply {
        put("index", window.index); put("start_m", window.startPositionM); put("end_m", window.endPositionM)
        put("departure_m", window.departurePositionM); put("departure_soc", window.departureSocPercent)
        put("candidates", JSONArray(window.candidateStationIds)); put("selected", window.selectedStationId)
        put("projected", JSONObject().apply { window.projectedArrivalSocByStationId.forEach { (id, soc) -> put(id, soc) } })
    }

    private fun windowFromJson(json: JSONObject): de.woladen.android.model.ChargingWindow {
        val projected = mutableMapOf<String, Double>()
        json.optJSONObject("projected")?.let { values ->
            val keys = values.keys(); while (keys.hasNext()) { val key = keys.next(); values.optNullableDouble(key)?.let { projected[key] = it } }
        }
        return de.woladen.android.model.ChargingWindow(
            index = json.optInt("index"), startPositionM = json.optInt("start_m"), endPositionM = json.optInt("end_m"),
            departurePositionM = json.optInt("departure_m"), departureSocPercent = json.optDouble("departure_soc", 0.0),
            candidateStationIds = strings(json.optJSONArray("candidates")), projectedArrivalSocByStationId = projected,
            selectedStationId = json.optString("selected").ifBlank { null }
        )
    }

    private fun stopToJson(stop: TripStopSelection): JSONObject = JSONObject().apply {
        put("station_id", stop.stationId); put("state", stop.state.name); put("selected_at", stop.selectedAtEpochMs)
    }

    private fun stopFromJson(json: JSONObject): TripStopSelection? {
        val id = json.optString("station_id").takeIf { it.isNotBlank() } ?: return null
        return TripStopSelection(
            stationId = id,
            state = runCatching { TripStopState.valueOf(json.optString("state")) }.getOrDefault(TripStopState.PLANNED),
            selectedAtEpochMs = json.optLong("selected_at", System.currentTimeMillis())
        )
    }

    private fun decodeCoordinates(values: JSONArray?): List<List<Double>> {
        if (values == null) return emptyList()
        return buildList {
            for (index in 0 until values.length()) {
                val point = values.optJSONArray(index) ?: continue
                if (point.length() < 2) continue
                add(listOf(point.optDouble(0), point.optDouble(1)))
            }
        }
    }

    private fun strings(values: JSONArray?): List<String> {
        if (values == null) return emptyList()
        return (0 until values.length()).map { values.optString(it) }.filter { it.isNotBlank() }
    }

    private fun <T> decodeArray(values: JSONArray?, decoder: (JSONObject) -> T?): List<T> {
        if (values == null) return emptyList()
        return buildList { for (index in 0 until values.length()) decoder(values.optJSONObject(index) ?: continue)?.let(::add) }
    }
}

private fun JSONObject.optNullableDouble(key: String): Double? {
    if (!has(key) || isNull(key)) return null
    val value = optDouble(key, Double.NaN)
    return value.takeIf { it.isFinite() }
}
