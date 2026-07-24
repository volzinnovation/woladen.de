import UIKit
import CarPlay
import MapKit

@MainActor
final class CarPlaySceneDelegate: UIResponder, CPTemplateApplicationSceneDelegate {
    private weak var interfaceController: CPInterfaceController?
    private var refreshTimer: Timer?

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didConnect interfaceController: CPInterfaceController,
        to window: CPWindow
    ) {
        self.interfaceController = interfaceController
        updateRootTemplate(animated: false)
        refreshTimer = Timer.scheduledTimer(withTimeInterval: 60, repeats: true) { [weak self] _ in
            Task { @MainActor in
                await TripStore.shared.refreshLive(force: true)
                self?.updateRootTemplate(animated: false)
            }
        }
        Task {
            await TripStore.shared.refreshLive(force: true)
            updateRootTemplate(animated: false)
        }
    }

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didDisconnectInterfaceController interfaceController: CPInterfaceController,
        from window: CPWindow
    ) {
        refreshTimer?.invalidate()
        refreshTimer = nil
        self.interfaceController = nil
    }

    private func updateRootTemplate(animated: Bool) {
        guard let interfaceController else { return }
        interfaceController.setRootTemplate(makeRootTemplate(), animated: animated, completion: nil)
    }

    private func makeRootTemplate() -> CPTemplate {
        guard let plan = TripStore.shared.activePlan else {
            let item = CPListItem(
                text: String(localized: "trip.empty.title", defaultValue: "No active trip"),
                detailText: String(localized: "trip.carplay.prepare", defaultValue: "Plan the route and select charging stops on iPhone.")
            )
            return CPListTemplate(
                title: "Woladen",
                sections: [CPListSection(items: [item], header: nil, sectionIndexTitle: nil)]
            )
        }

        var points: [CPPointOfInterest] = []
        if let next = plan.nextStop {
            points.append(makeChargingPoint(next, plan: plan))
        }
        if !plan.isStationTargetTrip {
            points.append(makeDestinationPoint(plan))
        }

        return CPPointOfInterestTemplate(
            title: String(localized: "trip.active.title", defaultValue: "Active trip"),
            pointsOfInterest: points,
            selectedIndex: points.isEmpty ? NSNotFound : 0
        )
    }

    private func makeChargingPoint(_ station: TripStationSnapshot, plan: RoutePlan) -> CPPointOfInterest {
        let mapItem = mapItem(coordinate: station.coordinate, name: station.operatorName)
        let live = TripStore.shared.liveSummary(for: station.stationID)
        let status = live?.availabilityStatus ?? station.availabilityStatus
        let available = live?.availableEVSEs ?? station.availableEVSEs
        let total = live?.totalEVSEs ?? station.totalEVSEs
        let availability = total > 0 ? "\(status.label) · \(available)/\(total)" : status.label
        let position = max(0, station.routePositionM - TripStore.shared.currentRoutePositionM)

        let point = CPPointOfInterest(
            location: mapItem,
            title: station.operatorName,
            subtitle: "\(station.city) · \(Int(station.maxPowerKW.rounded())) kW",
            summary: "\(availability) · \(formatRouteDistance(position))",
            detailTitle: String(localized: "trip.nextStop", defaultValue: "Next charging stop"),
            detailSubtitle: station.operatorName,
            detailSummary: availability,
            pinImage: UIImage(systemName: "bolt.fill"),
            selectedPinImage: UIImage(systemName: "bolt.circle.fill")
        )
        point.primaryButton = CPTextButton(
            title: String(localized: "trip.navigateApple", defaultValue: "Navigate"),
            textStyle: .confirm
        ) { _ in
            mapItem.openInMaps(launchOptions: [MKLaunchOptionsDirectionsModeKey: MKLaunchOptionsDirectionsModeDriving])
        }
        point.secondaryButton = CPTextButton(
            title: String(localized: "trip.substitute", defaultValue: "Substitute"),
            textStyle: .normal
        ) { [weak self] _ in
            self?.showSubstitutes()
        }
        return point
    }

    private func makeDestinationPoint(_ plan: RoutePlan) -> CPPointOfInterest {
        let item = mapItem(coordinate: plan.route.destination.coordinate, name: plan.route.destination.label)
        let etaText: String
        if let eta = TripStore.shared.eta.destinationArrival {
            let formattedETA = Calendar.current.isDateInToday(eta)
                ? eta.formatted(date: .omitted, time: .shortened)
                : eta.formatted(.dateTime.weekday(.abbreviated).hour().minute())
            etaText = String(localized: "trip.destinationETA", defaultValue: "ETA {time}")
                .replacingOccurrences(of: "{time}", with: formattedETA)
        } else {
            etaText = String(localized: "trip.eta.calculating", defaultValue: "Traffic ETA calculating…")
        }
        let point = CPPointOfInterest(
            location: item,
            title: plan.route.destination.label,
            subtitle: String(localized: "trip.destination", defaultValue: "Final destination"),
            summary: etaText,
            detailTitle: plan.route.destination.label,
            detailSubtitle: String(localized: "trip.destination", defaultValue: "Final destination"),
            detailSummary: etaText,
            pinImage: UIImage(systemName: "flag.checkered"),
            selectedPinImage: UIImage(systemName: "flag.checkered.circle.fill")
        )
        point.primaryButton = CPTextButton(
            title: String(localized: "trip.navigateApple", defaultValue: "Navigate"),
            textStyle: .confirm
        ) { _ in
            item.openInMaps(launchOptions: [MKLaunchOptionsDirectionsModeKey: MKLaunchOptionsDirectionsModeDriving])
        }
        return point
    }

    private func showSubstitutes() {
        guard let interfaceController else { return }
        let choices = TripStore.shared.substitutesForNextStop()
        var sections: [CPListSection] = []

        if let next = TripStore.shared.activePlan?.nextStop,
           (TripStore.shared.liveSummary(for: next.stationID)?.availabilityStatus ?? next.availabilityStatus) == .occupied {
            let wait = CPListItem(
                text: String(localized: "trip.substitute.wait", defaultValue: "Keep this stop and wait"),
                detailText: next.operatorName
            )
            wait.handler = { [weak self] _, completion in
                self?.interfaceController?.popTemplate(animated: true, completion: nil)
                completion()
            }
            sections.append(
                CPListSection(
                    items: [wait],
                    header: String(localized: "trip.substitute.current", defaultValue: "Current stop"),
                    sectionIndexTitle: nil
                )
            )
        }

        if !choices.earlier.isEmpty {
            sections.append(
                CPListSection(
                    items: choices.earlier.prefix(6).map(substituteListItem),
                    header: String(localized: "trip.substitute.earlier", defaultValue: "Earlier stations ahead"),
                    sectionIndexTitle: nil
                )
            )
        }
        if !choices.detour.isEmpty {
            sections.append(
                CPListSection(
                    items: choices.detour.prefix(6).map(substituteListItem),
                    header: String(localized: "trip.substitute.detour", defaultValue: "Detour options"),
                    sectionIndexTitle: nil
                )
            )
        }

        if sections.isEmpty {
            let unavailable = CPListItem(
                text: String(localized: "trip.substitute.none", defaultValue: "No safe substitute found"),
                detailText: String(localized: "trip.substitute.noneHelp", defaultValue: "Keep the current stop or change settings while parked.")
            )
            sections = [CPListSection(items: [unavailable], header: nil, sectionIndexTitle: nil)]
        }

        let template = CPListTemplate(
            title: String(localized: "trip.substitute.title", defaultValue: "Substitute charger"),
            sections: sections
        )
        interfaceController.pushTemplate(template, animated: true, completion: nil)
    }

    private func substituteListItem(_ station: TripStationSnapshot) -> CPListItem {
        let live = TripStore.shared.liveSummary(for: station.stationID)
        let status = live?.availabilityStatus ?? station.availabilityStatus
        let item = CPListItem(
            text: station.operatorName,
            detailText: "\(station.city) · \(status.label) · km \(station.routePositionM / 1000)"
        )
        item.handler = { [weak self] _, completion in
            TripStore.shared.replaceNextStop(with: station.stationID)
            self?.updateRootTemplate(animated: true)
            completion()
        }
        return item
    }

    private func mapItem(coordinate: CLLocationCoordinate2D, name: String) -> MKMapItem {
        let item = MKMapItem(placemark: MKPlacemark(coordinate: coordinate))
        item.name = name
        return item
    }
}
