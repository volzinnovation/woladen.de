import Foundation
import CoreLocation

enum EnergyRoutePlanner {
    static func build(
        routeDistanceM: Int,
        stations: [TripStationSnapshot],
        selectedStationIDs: [String],
        initialSOCPercent: Double,
        settings rawSettings: VehicleEnergySettings,
        providerMode: ProviderPackageMode,
        selectedProviderNames: [String]
    ) -> EnergyPlanResult {
        let settings = rawSettings.normalized
        let routeDistanceM = max(0, routeDistanceM)
        let providerKeys = Set(selectedProviderNames.map(normalizedProviderKey).filter { !$0.isEmpty })
        let allStations = stations
            .filter { $0.routePositionM > 0 && $0.routePositionM < routeDistanceM }
            .filter { station in
                providerMode != .only || providerKeys.isEmpty || providerKeys.contains(normalizedProviderKey(station.operatorName))
            }
            .sorted { $0.routePositionM < $1.routePositionM }

        var windows: [ChargingWindow] = []
        var departurePositionM = 0
        var departureSOC = initialSOCPercent.clamped(to: 1...100)
        var remainingSelections = selectedStationIDs
        var validSelectedIDs = Set<String>()
        var firstUncoveredPositionM: Int?

        for index in 0..<20 {
            let reachableDistanceM = Int((settings.usableRangeKM(departureSOCPercent: departureSOC) * 1000).rounded())
            let hardLimitM = min(routeDistanceM, departurePositionM + max(0, reachableDistanceM))
            if routeDistanceM <= hardLimitM {
                break
            }

            let startPositionM = max(
                departurePositionM + 1,
                hardLimitM - Int((settings.earlyWindowKM * 1000).rounded())
            )
            let candidates = allStations.filter {
                $0.routePositionM >= startPositionM && $0.routePositionM <= hardLimitM
            }
            let candidateIDs = candidates.map(\.stationID)
            let selectedID = remainingSelections.first { candidateIDs.contains($0) }
            if let selectedID {
                remainingSelections.removeAll { $0 == selectedID }
                validSelectedIDs.insert(selectedID)
            }

            var arrivals: [String: Double] = [:]
            for candidate in candidates {
                let legDistanceKM = Double(candidate.routePositionM - departurePositionM) / 1000
                arrivals[candidate.stationID] = settings.projectedArrivalSOC(
                    departureSOCPercent: departureSOC,
                    distanceKM: legDistanceKM
                )
            }

            windows.append(
                ChargingWindow(
                    index: index,
                    startPositionM: startPositionM,
                    endPositionM: hardLimitM,
                    departurePositionM: departurePositionM,
                    departureSOCPercent: departureSOC,
                    candidateStationIDs: candidateIDs,
                    projectedArrivalSOCByStationID: arrivals,
                    selectedStationID: selectedID
                )
            )

            guard let anchor = selectedID.flatMap({ id in candidates.first { $0.stationID == id } }) ?? candidates.last else {
                firstUncoveredPositionM = hardLimitM
                break
            }
            guard anchor.routePositionM > departurePositionM else {
                firstUncoveredPositionM = hardLimitM
                break
            }
            departurePositionM = anchor.routePositionM
            departureSOC = settings.targetSOCPercent
        }

        let invalidated = selectedStationIDs.filter { !validSelectedIDs.contains($0) }
        let feasible = firstUncoveredPositionM == nil && windows.allSatisfy { !$0.candidateStationIDs.isEmpty }
        return EnergyPlanResult(
            windows: windows,
            isFeasible: feasible,
            firstUncoveredPositionM: firstUncoveredPositionM,
            invalidatedStationIDs: invalidated
        )
    }

    static func providerCoverage(for plan: RoutePlan) -> [ProviderCoverage] {
        let selectedIDs = Set(plan.selectedStopIDs)
        var providers: [String: (name: String, stationIDs: Set<String>, windowIDs: Set<Int>, selected: Int)] = [:]

        for window in plan.windows {
            for stationID in window.candidateStationIDs {
                guard let station = plan.station(stationID) else { continue }
                let key = normalizedProviderKey(station.operatorName)
                guard !key.isEmpty else { continue }
                var entry = providers[key] ?? (providerPackageDisplayName(station.operatorName), [], [], 0)
                entry.stationIDs.insert(stationID)
                entry.windowIDs.insert(window.index)
                if selectedIDs.contains(stationID) {
                    entry.selected += 1
                }
                providers[key] = entry
            }
        }

        return providers.values.map { entry in
            ProviderCoverage(
                providerName: entry.name,
                stationCount: entry.stationIDs.count,
                coveredWindowCount: entry.windowIDs.count,
                totalWindowCount: plan.windows.count,
                selectedStopCount: entry.selected
            )
        }.sorted { lhs, rhs in
            if lhs.coveredWindowCount != rhs.coveredWindowCount {
                return lhs.coveredWindowCount > rhs.coveredWindowCount
            }
            if lhs.stationCount != rhs.stationCount {
                return lhs.stationCount > rhs.stationCount
            }
            return lhs.providerName.localizedCaseInsensitiveCompare(rhs.providerName) == .orderedAscending
        }
    }

    static func substituteStationIDs(
        in plan: RoutePlan,
        replacing stationID: String,
        currentRoutePositionM: Int
    ) -> (earlier: [String], detour: [String]) {
        guard let failed = plan.station(stationID), let window = plan.window(containing: stationID) else {
            return ([], [])
        }
        let excluded = Set(plan.selectedStopIDs)
            .union(plan.completedStopIDs)
            .union(plan.rejectedStopIDs)
            .union([stationID])
        let candidates = window.candidateStationIDs.compactMap(plan.station).filter {
            !excluded.contains($0.stationID) && $0.routePositionM > currentRoutePositionM
        }
        let earlier = candidates
            .filter { $0.routePositionM <= failed.routePositionM }
            .sorted { lhs, rhs in
                if lhs.routePositionM != rhs.routePositionM { return lhs.routePositionM > rhs.routePositionM }
                return lhs.routeDetourM < rhs.routeDetourM
            }
            .map(\.stationID)
        let detour = candidates
            .filter { $0.routePositionM > failed.routePositionM }
            .sorted { lhs, rhs in
                if lhs.routeDetourM != rhs.routeDetourM { return lhs.routeDetourM < rhs.routeDetourM }
                return lhs.routePositionM < rhs.routePositionM
            }
            .map(\.stationID)
        return (earlier, detour)
    }
}

enum RouteProgressEstimator {
    static func positionM(
        for coordinate: CLLocationCoordinate2D,
        routeCoordinates: [[Double]],
        routeDistanceM: Int
    ) -> Int {
        let points = routeCoordinates.compactMap { raw -> CLLocationCoordinate2D? in
            guard raw.count >= 2 else { return nil }
            return CLLocationCoordinate2D(latitude: raw[1], longitude: raw[0])
        }
        guard !points.isEmpty, routeDistanceM > 0 else { return 0 }

        var cumulative: [CLLocationDistance] = [0]
        var total: CLLocationDistance = 0
        for index in 1..<points.count {
            total += CLLocation(latitude: points[index - 1].latitude, longitude: points[index - 1].longitude)
                .distance(from: CLLocation(latitude: points[index].latitude, longitude: points[index].longitude))
            cumulative.append(total)
        }
        guard total > 0 else { return 0 }

        let location = CLLocation(latitude: coordinate.latitude, longitude: coordinate.longitude)
        var bestIndex = 0
        var bestDistance = CLLocationDistance.greatestFiniteMagnitude
        for (index, point) in points.enumerated() {
            let distance = location.distance(from: CLLocation(latitude: point.latitude, longitude: point.longitude))
            if distance < bestDistance {
                bestDistance = distance
                bestIndex = index
            }
        }
        return Int((cumulative[bestIndex] / total * Double(routeDistanceM)).rounded())
    }
}

private extension Double {
    func clamped(to range: ClosedRange<Double>) -> Double {
        min(max(self, range.lowerBound), range.upperBound)
    }
}
