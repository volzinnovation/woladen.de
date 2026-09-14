package de.woladen.android.model

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class TripModelsTest {
    @Test
    fun energySettingsNormalizeLikeIPhoneDefaults() {
        val settings = VehicleEnergySettings(
            batteryCapacityKWh = 2.0,
            consumptionKWhPer100Km = 80.0,
            reserveSocPercent = 45.0,
            targetSocPercent = 2.0,
            averageChargingPowerKw = 500.0
        ).normalized

        assertEquals(10.0, settings.batteryCapacityKWh, 0.001)
        assertEquals(50.0, settings.consumptionKWhPer100Km, 0.001)
        assertEquals(40.0, settings.reserveSocPercent, 0.001)
        assertEquals(45.0, settings.targetSocPercent, 0.001)
        assertEquals(400.0, settings.averageChargingPowerKw, 0.001)
    }

    @Test
    fun projectedArrivalSocAccountsForConsumptionAndReserveRange() {
        val settings = VehicleEnergySettings(
            batteryCapacityKWh = 75.0,
            consumptionKWhPer100Km = 18.0,
            reserveSocPercent = 10.0
        )

        assertEquals(55.0, settings.projectedArrivalSoc(80.0, 104.1667), 0.1)
        assertEquals(291.6, settings.usableRangeKm(80.0), 0.1)
    }

    @Test
    fun energyPlannerCreatesWindowWhenRouteExceedsReachableRange() {
        val station = TripStationSnapshot(
            stationId = "station-1",
            countryCode = "DE",
            stationName = "Stop",
            operatorName = "Operator",
            city = "Berlin",
            address = "",
            latitude = 52.0,
            longitude = 13.0,
            maxPowerKw = 150.0,
            chargingPointsCount = 4,
            routePositionM = 220_000,
            routeDetourM = 0,
            availabilityStatus = AvailabilityStatus.FREE,
            availableEvses = 3,
            totalEvses = 4,
            classification = "gold",
            reliabilityPercent = 98.0,
            lastUnavailableAt = null,
            providerCanonicalId = "provider-1",
            priceDisplay = "",
            oftenBroken = false,
            oftenOccupied = false
        )

        val windows = EnergyRoutePlanner.build(500_000, listOf(station), VehicleEnergySettings(), 80.0)

        assertTrue(windows.isNotEmpty())
        assertEquals("station-1", windows.first().candidateStationIds.first())
    }

    @Test
    fun etaEstimatorIncludesChargingAndReportsProgress() {
        val route = TripRouteSnapshot(
            origin = RouteEndpoint(52.0, 13.0, "Origin"),
            destination = RouteEndpoint(52.0, 14.0, "Destination"),
            distanceM = 100_000,
            durationS = 3_600,
            geometryCoordinates = listOf(listOf(13.0, 52.0), listOf(14.0, 52.0)),
            calculatedAtEpochMs = 0L,
            filter = RouteFilterPayload("", emptyList(), 50, 0, emptyList(), "", false, false),
            initialSocPercent = 80.0
        )
        val station = TripStationSnapshot(
            stationId = "station-1", countryCode = "DE", stationName = "Stop", operatorName = "Operator",
            city = "Berlin", address = "", latitude = 52.0, longitude = 13.4, maxPowerKw = 150.0,
            chargingPointsCount = 4, routePositionM = 40_000, routeDetourM = 0,
            availabilityStatus = AvailabilityStatus.FREE, availableEvses = 3, totalEvses = 4,
            classification = "gold", reliabilityPercent = null, lastUnavailableAt = null,
            providerCanonicalId = null, priceDisplay = "", oftenBroken = false, oftenOccupied = false
        )
        val plan = RoutePlan(
            id = "plan", name = "Trip", route = route, vehicleSettings = VehicleEnergySettings(),
            rawStations = listOf(station),
            windows = listOf(ChargingWindow(0, 0, 80_000, 40_000, 80.0, listOf("station-1"), mapOf("station-1" to 70.0), "station-1")),
            stopSelections = listOf(TripStopSelection("station-1")), state = RoutePlanState.ACTIVE,
            createdAtEpochMs = 0L, updatedAtEpochMs = 0L
        )

        val eta = TripEtaEstimator.estimate(plan, currentRoutePositionM = 20_000, nowEpochMs = 1_000_000L)

        assertEquals(0.2, eta.progress, 0.001)
        assertEquals(20_000, eta.currentRoutePositionM)
        assertTrue(eta.nextStopArrivalEpochMs!! > 1_000_000L)
        assertTrue(eta.destinationArrivalEpochMs > 1_000_000L + 2_000_000L)
        assertTrue(eta.projectedArrivalSocPercent in 0.0..100.0)
    }

    @Test
    fun routeProjectionMapsGpsFixToRouteDistance() {
        val route = TripRouteSnapshot(
            origin = RouteEndpoint(52.0, 13.0, "Origin"),
            destination = RouteEndpoint(52.0, 14.0, "Destination"),
            distanceM = 100_000, durationS = 3_600,
            geometryCoordinates = listOf(listOf(13.0, 52.0), listOf(14.0, 52.0)),
            calculatedAtEpochMs = 0L,
            filter = RouteFilterPayload("", emptyList(), 50, 0, emptyList(), "", false, false),
            initialSocPercent = 80.0
        )
        val position = projectRoutePositionM(route, latitude = 52.0, longitude = 13.5)
        assertTrue(position in 45_000..55_000)
    }

    @Test
    fun startingPlanSelectsOneStationPerChargingWindow() {
        val route = TripRouteSnapshot(
            origin = RouteEndpoint(52.0, 13.0, "Origin"),
            destination = RouteEndpoint(52.0, 14.0, "Destination"),
            distanceM = 100_000, durationS = 3_600,
            geometryCoordinates = listOf(listOf(13.0, 52.0), listOf(14.0, 52.0)),
            calculatedAtEpochMs = 0L,
            filter = RouteFilterPayload("", emptyList(), 50, 0, emptyList(), "", false, false),
            initialSocPercent = 80.0
        )
        val plan = RoutePlan(
            id = "plan", name = "Trip", route = route, vehicleSettings = VehicleEnergySettings(),
            rawStations = emptyList(),
            windows = listOf(ChargingWindow(0, 0, 80_000, 40_000, 80.0, listOf("first", "second"), mapOf("first" to 65.0, "second" to 68.0))),
            stopSelections = emptyList(), state = RoutePlanState.DRAFT,
            createdAtEpochMs = 0L, updatedAtEpochMs = 0L
        )
        val prepared = plan.withDefaultSelections(nowEpochMs = 123L)
        assertEquals("first", prepared.windows.single().selectedStationId)
        assertEquals(listOf("first"), prepared.stopSelections.map { it.stationId })
    }

}
