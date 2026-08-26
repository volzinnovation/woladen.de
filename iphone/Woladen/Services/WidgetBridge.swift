import Foundation
import CoreLocation
import WidgetKit

extension FilterState {
    var widgetFilter: WoladenWidgetFilter {
        WoladenWidgetFilter(
            selectedOperatorNames: selectedOperatorNames,
            minPowerKW: minPowerKW,
            minAmenityCount: minAmenityCount,
            selectedAmenities: selectedAmenities,
            amenityNameQuery: amenityNameQuery,
            availableOnly: availableOnly,
            currentlyOpenOnly: currentlyOpenOnly
        )
    }
}

@MainActor
enum WoladenWidgetBridge {
    static func publish(
        viewModel: AppViewModel,
        tripStore: TripStore,
        location: CLLocation?
    ) {
        let mode: WoladenWidgetMode = tripStore.mode == .trip ? .trip : .plan
        WoladenWidgetStateStore.saveMode(mode)
        WoladenWidgetStateStore.saveFilter(viewModel.filterState.widgetFilter)

        if mode == .trip {
            publishTrip(tripStore)
        } else {
            publishPlanning(viewModel: viewModel, location: location)
        }
        WidgetCenter.shared.reloadTimelines(ofKind: WoladenShared.widgetKind)
    }

    private static func publishTrip(_ tripStore: TripStore) {
        guard let station = tripStore.activePlan?.nextStop else {
            WoladenWidgetStateStore.saveTrip(.empty(mode: .trip, state: .noTripTarget))
            return
        }

        let live = tripStore.liveSummary(for: station.stationID)
        let status = live?.availabilityStatus.rawValue ?? station.availabilityStatus.rawValue
        let updatedAt = woladenParseISO8601(live?.sourceObservedAt ?? "")
            ?? woladenParseISO8601(live?.fetchedAt ?? "")
            ?? woladenParseISO8601(station.lastUpdated)
        let price = live?.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let snapshot = WoladenWidgetStation(
            stationID: station.stationID,
            operatorName: station.operatorName,
            stationName: station.stationName ?? station.operatorName,
            city: station.city,
            address: station.address,
            latitude: station.latitude,
            longitude: station.longitude,
            maxPowerKW: station.maxPowerKW,
            chargingPointCount: station.chargingPointsCount,
            availabilityStatus: status,
            availableEVSEs: live?.availableEVSEs ?? station.availableEVSEs,
            totalEVSEs: live?.totalEVSEs ?? station.totalEVSEs,
            priceDisplay: price,
            sourceUpdatedAt: updatedAt,
            distanceM: Double(max(0, station.routePositionM - tripStore.currentRoutePositionM)),
            eta: tripStore.eta.nextStopArrival
        )
        WoladenWidgetStateStore.saveTrip(
            WoladenWidgetContent(
                mode: .trip,
                state: .station,
                station: snapshot,
                generatedAt: Date(),
                locationIsStale: false
            )
        )
    }

    private static func publishPlanning(viewModel: AppViewModel, location: CLLocation?) {
        guard let location else {
            let existing = WoladenWidgetStateStore.loadPlanning()
            if let existing, existing.station != nil {
                WoladenWidgetStateStore.savePlanning(
                    WoladenWidgetContent(
                        mode: .plan,
                        state: existing.state,
                        station: existing.station,
                        generatedAt: existing.generatedAt,
                        locationIsStale: true
                    )
                )
            } else {
                WoladenWidgetStateStore.savePlanning(.empty(mode: .plan, state: .locationUnavailable))
            }
            return
        }

        guard let feature = viewModel.allFeatures
            .filter({ $0.properties.matches(viewModel.filterState) })
            .min(by: { lhs, rhs in
                let lhsDistance = location.distance(
                    from: CLLocation(latitude: lhs.coordinate.latitude, longitude: lhs.coordinate.longitude)
                )
                let rhsDistance = location.distance(
                    from: CLLocation(latitude: rhs.coordinate.latitude, longitude: rhs.coordinate.longitude)
                )
                if lhsDistance == rhsDistance {
                    return lhs.properties.stationID < rhs.properties.stationID
                }
                return lhsDistance < rhsDistance
            }) else {
            WoladenWidgetStateStore.savePlanning(.empty(mode: .plan, state: .noMatch))
            return
        }

        let counts = feature.availabilityCounts
        let stationLocation = CLLocation(
            latitude: feature.coordinate.latitude,
            longitude: feature.coordinate.longitude
        )
        let sourceUpdatedAt = woladenParseISO8601(feature.liveSummary?.sourceObservedAt ?? "")
            ?? woladenParseISO8601(feature.liveSummary?.fetchedAt ?? "")
            ?? woladenParseISO8601(feature.properties.occupancyLastUpdated)
        let snapshot = WoladenWidgetStation(
            stationID: feature.properties.stationID,
            operatorName: feature.properties.operatorName,
            stationName: feature.properties.stationName.isEmpty
                ? feature.properties.operatorName
                : feature.properties.stationName,
            city: feature.properties.city,
            address: feature.properties.address,
            latitude: feature.coordinate.latitude,
            longitude: feature.coordinate.longitude,
            maxPowerKW: feature.properties.displayedMaxPowerKW,
            chargingPointCount: feature.properties.chargingPointsCount,
            availabilityStatus: feature.availabilityStatus.rawValue,
            availableEVSEs: counts.available,
            totalEVSEs: counts.total,
            priceDisplay: feature.displayPrice,
            sourceUpdatedAt: sourceUpdatedAt,
            distanceM: location.distance(from: stationLocation),
            eta: nil
        )
        WoladenWidgetStateStore.savePlanning(
            WoladenWidgetContent(
                mode: .plan,
                state: .station,
                station: snapshot,
                generatedAt: Date(),
                locationIsStale: false
            )
        )
    }
}
