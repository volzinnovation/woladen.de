import Foundation
import MapKit
import CoreLocation

enum TripETAService {
    static func baseEstimate(plan: RoutePlan, currentRoutePositionM: Int, now: Date = Date()) -> TripETAState {
        let remainingFraction = plan.route.distanceM > 0
            ? Double(max(0, plan.route.distanceM - currentRoutePositionM)) / Double(plan.route.distanceM)
            : 0
        let remainingDriveTime = max(0, Double(plan.route.durationS) * remainingFraction)
        let remainingStops = plan.selectedStopIDs.compactMap(plan.station)
        let chargingTime = remainingStops.reduce(0.0) {
            $0 + estimatedChargingDuration(at: $1, plan: plan)
        }

        var nextStopTravelTime: TimeInterval?
        var nextStopArrival: Date?
        if let next = plan.nextStop {
            let distanceToStop = max(0, next.routePositionM - currentRoutePositionM)
            let fractionToStop = plan.route.distanceM > 0
                ? Double(distanceToStop) / Double(plan.route.distanceM)
                : 0
            let travelTime = max(0, Double(plan.route.durationS) * fractionToStop)
            nextStopTravelTime = travelTime
            nextStopArrival = now.addingTimeInterval(travelTime)
        }

        return TripETAState(
            nextStopArrival: nextStopArrival,
            destinationArrival: now.addingTimeInterval(remainingDriveTime + chargingTime),
            nextStopTravelTime: nextStopTravelTime,
            totalTravelTime: remainingDriveTime + chargingTime,
            updatedAt: now,
            isStale: false,
            nextStopUsesTraffic: false,
            destinationUsesTraffic: false,
            nextStopIsLoading: plan.nextStop != nil,
            destinationIsLoading: true
        )
    }

    static func calculateNextStop(from currentLocation: CLLocation, plan: RoutePlan) async throws -> TripETAState? {
        guard let next = plan.nextStop else { return nil }
        let response = try await eta(
            from: currentLocation.coordinate,
            to: next.coordinate,
            departureDate: Date()
        )
        let travelTime = max(0, response.expectedTravelTime)
        return TripETAState(
            nextStopArrival: Date().addingTimeInterval(travelTime),
            destinationArrival: nil,
            nextStopTravelTime: travelTime,
            totalTravelTime: nil,
            updatedAt: Date(),
            isStale: false,
            nextStopUsesTraffic: true,
            destinationUsesTraffic: false,
            nextStopIsLoading: false,
            destinationIsLoading: true
        )
    }

    static func calculateDestination(from currentLocation: CLLocation, plan: RoutePlan) async throws -> TripETAState {
        try await calculate(from: currentLocation, plan: plan)
    }

    static func calculate(from currentLocation: CLLocation, plan: RoutePlan) async throws -> TripETAState {
        let remainingStops = plan.selectedStopIDs.compactMap(plan.station)
        let destination = CLLocationCoordinate2D(
            latitude: plan.route.destination.lat,
            longitude: plan.route.destination.lon
        )
        let waypoints = remainingStops.map(\.coordinate) + [destination]
        guard !waypoints.isEmpty else { return .unavailable }

        var source = currentLocation.coordinate
        var departureDate = Date()
        var totalTravelTime: TimeInterval = 0
        var nextStopTravelTime: TimeInterval?
        var nextStopArrival: Date?

        for (index, destination) in waypoints.enumerated() {
            let response = try await eta(from: source, to: destination, departureDate: departureDate)
            let travelTime = max(0, response.expectedTravelTime)
            totalTravelTime += travelTime
            departureDate = departureDate.addingTimeInterval(travelTime)
            if index == 0, !remainingStops.isEmpty {
                nextStopTravelTime = travelTime
                nextStopArrival = departureDate
            }
            if index < remainingStops.count {
                let chargingTime = estimatedChargingDuration(
                    at: remainingStops[index],
                    plan: plan
                )
                totalTravelTime += chargingTime
                departureDate = departureDate.addingTimeInterval(chargingTime)
            }
            source = destination
        }

        return TripETAState(
            nextStopArrival: nextStopArrival,
            destinationArrival: Date().addingTimeInterval(totalTravelTime),
            nextStopTravelTime: nextStopTravelTime,
            totalTravelTime: totalTravelTime,
            updatedAt: Date(),
            isStale: false,
            nextStopUsesTraffic: nextStopArrival != nil,
            destinationUsesTraffic: true,
            nextStopIsLoading: false,
            destinationIsLoading: false
        )
    }

    static func estimatedChargingDuration(
        at station: TripStationSnapshot,
        plan: RoutePlan
    ) -> TimeInterval {
        let settings = plan.vehicleSettings.normalized
        let arrivalSOC = plan.projectedArrivalSOC(for: station.stationID) ?? settings.reserveSOCPercent
        let chargePercent = max(0, settings.targetSOCPercent - arrivalSOC)
        let energyKWh = settings.batteryCapacityKWh * chargePercent / 100
        guard settings.averageChargingPowerKW > 0 else { return 0 }
        return energyKWh / settings.averageChargingPowerKW * 3600
    }

    private static func eta(
        from source: CLLocationCoordinate2D,
        to destination: CLLocationCoordinate2D,
        departureDate: Date
    ) async throws -> MKDirections.ETAResponse {
        let request = MKDirections.Request()
        request.source = MKMapItem(placemark: MKPlacemark(coordinate: source))
        request.destination = MKMapItem(placemark: MKPlacemark(coordinate: destination))
        request.transportType = .automobile
        request.departureDate = departureDate
        return try await MKDirections(request: request).calculateETA()
    }
}
