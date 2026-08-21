import SwiftUI
import MapKit

struct TripView: View {
    @EnvironmentObject private var tripStore: TripStore
    @EnvironmentObject private var locationService: LocationService
    @EnvironmentObject private var viewModel: AppViewModel

    @State private var showingSettings = false
    @State private var showingSubstitutes = false
    @State private var expandedAmenityStationIDs: Set<String> = []

    var body: some View {
        Group {
            if let plan = tripStore.activePlan {
                activeTrip(plan)
            } else {
                noActiveTrip
            }
        }
        .background(Color(.systemBackground))
        .sheet(isPresented: $showingSettings) {
            TripSettingsView(planID: tripStore.activePlanID, initial: tripStore.preferences)
                .environmentObject(tripStore)
        }
        .sheet(isPresented: $showingSubstitutes) {
            SubstituteStationView()
                .environmentObject(tripStore)
        }
        .task {
            await tripStore.refreshLive(force: true)
            if let location = locationService.currentLocation {
                tripStore.refreshETA(from: location, force: true)
            }
        }
    }

    private func activeTrip(_ plan: RoutePlan) -> some View {
        VStack(spacing: 0) {
            tripHeader(plan)

            ScrollView {
                Group {
                    switch tripStore.preferences.visualStyle {
                    case .commandCenter:
                        commandCenter(plan)
                    case .routeProgression:
                        routeProgression(plan)
                    case .mapGlance:
                        mapGlance(plan)
                    }
                }
                .padding(14)
                .frame(maxWidth: 920)
                .frame(maxWidth: .infinity)
            }
        }
    }

    private func tripHeader(_ plan: RoutePlan) -> some View {
        HStack(spacing: 12) {
            VStack(alignment: .leading, spacing: 2) {
                Text(String(localized: "trip.active.title", defaultValue: "Active trip"))
                    .font(.title3.weight(.bold))
                    .foregroundStyle(woladenBrandColor)
                Text(
                    plan.isStationTargetTrip
                        ? String(localized: "trip.station.singleTarget", defaultValue: "Station target")
                        : plan.route.destination.label
                )
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer()
            if tripStore.isLikelyDriving {
                Label(String(localized: "trip.driving", defaultValue: "Driving"), systemImage: "car.fill")
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.white)
                    .padding(.horizontal, 10)
                    .padding(.vertical, 6)
                    .background(woladenBrandColor, in: Capsule())
            }
            Button {
                showingSettings = true
            } label: {
                Image(systemName: "slider.horizontal.3")
                    .frame(width: 44, height: 44)
            }
            .buttonStyle(.bordered)
            .disabled(tripStore.isLikelyDriving)
            .accessibilityLabel(Text(String(localized: "trip.settings.title", defaultValue: "Trip settings")))
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .background(Color(.systemBackground))
        .overlay(alignment: .bottom) { Divider() }
    }

    private func commandCenter(_ plan: RoutePlan) -> some View {
        VStack(spacing: 14) {
            if let next = plan.nextStop {
                nextStopCard(next, plan: plan, prominent: true)
            }
            if !plan.isStationTargetTrip {
                destinationCard(plan, prominent: false)
                remainingStopsStrip(plan)
            }
            tripManagementActions
        }
    }

    private func routeProgression(_ plan: RoutePlan) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            if let next = plan.nextStop {
                nextStopCard(next, plan: plan, prominent: false)
            }

            VStack(alignment: .leading, spacing: 0) {
                Text(String(localized: "trip.routeProgress", defaultValue: "Route progression"))
                    .font(.headline)
                    .padding(.bottom, 12)

                ForEach(Array(plan.selectedStopIDs.enumerated()), id: \.element) { index, stationID in
                    if let station = plan.station(stationID) {
                        progressionRow(
                            number: index + 1,
                            title: station.operatorName,
                            subtitle: stationSubtitle(station),
                            classification: station.classification,
                            isNext: index == 0,
                            isLast: false
                        )
                    }
                }

                if !plan.isStationTargetTrip {
                    progressionRow(
                        number: nil,
                        title: plan.route.destination.label,
                        subtitle: destinationETAText,
                        classification: nil,
                        isNext: plan.nextStop == nil,
                        isLast: true
                    )
                }
            }
            .padding(16)
            .background(StationVisualStyle.controlSurface, in: RoundedRectangle(cornerRadius: 16, style: .continuous))

            if !plan.isStationTargetTrip {
                destinationCard(plan, prominent: false)
            }
            tripManagementActions
        }
    }

    private func mapGlance(_ plan: RoutePlan) -> some View {
        VStack(spacing: 14) {
            if let next = plan.nextStop {
                nextStopCard(next, plan: plan, prominent: false)
            }
            TripGlanceMap(plan: plan, isDriving: tripStore.isLikelyDriving)
            if !plan.isStationTargetTrip {
                destinationCard(plan, prominent: false)
            }
            tripManagementActions
        }
    }

    private func nextStopCard(_ station: TripStationSnapshot, plan: RoutePlan, prominent: Bool) -> some View {
        let live = tripStore.liveSummary(for: station.stationID)
        let status = live?.availabilityStatus ?? station.availabilityStatus
        let available = live?.availableEVSEs ?? station.availableEVSEs
        let total = live?.totalEVSEs ?? station.totalEVSEs
        let colors = stationLiveCardColors(status: status, available: available, total: total)

        return HStack(spacing: 0) {
            StationClassificationRail(
                classification: station.classification,
                width: prominent ? 18 : 14
            )

            VStack(alignment: .leading, spacing: prominent ? 16 : 12) {
                HStack(alignment: .top, spacing: 12) {
                    VStack(alignment: .leading, spacing: 5) {
                        Text(String(localized: "trip.nextStop", defaultValue: "NEXT CHARGING STOP"))
                            .font(.caption.weight(.bold))
                            .foregroundStyle(woladenBrandColor)
                        Text(station.operatorName)
                            .font(prominent ? .largeTitle.weight(.bold) : .title2.weight(.bold))
                            .lineLimit(2)
                        Text("\(station.classification.title) · \(stationSubtitle(station))")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                    }
                    Spacer(minLength: 8)
                    liveBadge(status: status, available: available, total: total)
                }

                HStack(spacing: 10) {
                    etaMetric(
                        title: String(localized: "trip.eta", defaultValue: "ETA"),
                        value: nextStopETAText
                    )
                    etaMetric(
                        title: String(localized: "trip.distance", defaultValue: "Distance"),
                        value: remainingDistanceText(to: station.routePositionM)
                    )
                    if status == .occupied {
                        etaMetric(
                            title: String(localized: "trip.liveStatus", defaultValue: "Status"),
                            value: status.label
                        )
                    } else if let soc = plan.projectedArrivalSOC(for: station.stationID) {
                        etaMetric(
                            title: String(localized: "trip.arrivalCharge", defaultValue: "Arrival"),
                            value: "\(Int(soc.rounded()))%"
                        )
                    }
                }

                stationDetailsDisclosure(station)

                Button {
                    openPreferredNavigation(to: station.coordinate, name: station.operatorName)
                } label: {
                    Label(
                        String(localized: "detail.startNavigation", defaultValue: "Start navigation"),
                        systemImage: "arrow.triangle.turn.up.right.diamond.fill"
                    )
                        .font(.headline)
                        .frame(maxWidth: .infinity, minHeight: 56)
                }
                .buttonStyle(.borderedProminent)
                .tint(woladenBrandColor)

                HStack(spacing: 10) {
                    Button {
                        showingSubstitutes = true
                    } label: {
                        Label(String(localized: "trip.substitute", defaultValue: "Find substitute"), systemImage: "arrow.triangle.2.circlepath")
                            .frame(maxWidth: .infinity, minHeight: 48)
                    }
                    .buttonStyle(.bordered)

                    Button {
                        tripStore.completeNextStop()
                    } label: {
                        Label(String(localized: "trip.stopComplete", defaultValue: "Stop complete"), systemImage: "checkmark")
                            .frame(maxWidth: .infinity, minHeight: 48)
                    }
                    .buttonStyle(.bordered)
                }

                Text(liveFreshnessText)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .center)
            }
            .padding(prominent ? 20 : 16)
        }
        .background(colors.background)
        .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(colors.border, lineWidth: 1)
        }
    }

    private func destinationCard(_ plan: RoutePlan, prominent: Bool) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(String(localized: "trip.destination", defaultValue: "FINAL DESTINATION"))
                        .font(.caption.weight(.bold))
                        .foregroundStyle(.secondary)
                    Text(plan.route.destination.label)
                        .font(prominent ? .title2.weight(.bold) : .headline)
                        .lineLimit(2)
                    Text(destinationETAText)
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(woladenBrandColor)
                }
                Spacer(minLength: 12)
                Image(systemName: "flag.checkered")
                    .font(.title2.weight(.semibold))
                    .foregroundStyle(woladenBrandColor)
                    .frame(width: 50, height: 50)
                    .background(woladenBrandColor.opacity(0.12), in: Circle())
            }

            Button {
                openPreferredNavigation(to: plan.route.destination.coordinate, name: plan.route.destination.label)
            } label: {
                Label(
                    String(localized: "detail.startNavigation", defaultValue: "Start navigation"),
                    systemImage: "map.fill"
                )
                    .frame(maxWidth: .infinity, minHeight: 48)
            }
            .buttonStyle(.bordered)
        }
        .padding(16)
        .background(StationVisualStyle.controlSurface, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
    }

    @ViewBuilder
    private func remainingStopsStrip(_ plan: RoutePlan) -> some View {
        if plan.selectedStopIDs.count > 1 {
            VStack(alignment: .leading, spacing: 8) {
                Text(String(localized: "trip.afterNext", defaultValue: "After the next stop"))
                    .font(.headline)
                ForEach(Array(plan.selectedStopIDs.dropFirst().enumerated()), id: \.element) { index, stationID in
                    if let station = plan.station(stationID) {
                        HStack(spacing: 0) {
                            StationClassificationRail(classification: station.classification, width: 8)
                            HStack {
                                Text("\(index + 2)")
                                    .font(.caption.weight(.bold))
                                    .foregroundStyle(.white)
                                    .frame(width: 26, height: 26)
                                    .background(woladenBrandColor, in: Circle())
                                Text(station.operatorName)
                                    .font(.subheadline.weight(.semibold))
                                Spacer()
                                Text("km \(station.routePositionM / 1000)")
                                    .font(.caption.monospacedDigit())
                                    .foregroundStyle(.secondary)
                            }
                            .padding(.vertical, 6)
                            .padding(.horizontal, 10)
                        }
                        .background(Color(.systemBackground), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                        .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                    }
                }
            }
            .padding(16)
            .background(StationVisualStyle.controlSurface, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        }
    }

    private func progressionRow(
        number: Int?,
        title: String,
        subtitle: String,
        classification: StationClassification?,
        isNext: Bool,
        isLast: Bool
    ) -> some View {
        HStack(alignment: .top, spacing: 12) {
            if let classification {
                StationClassificationRail(classification: classification, width: 8)
            }
            VStack(spacing: 0) {
                Group {
                    if let number {
                        Text("\(number)")
                            .font(.caption.weight(.bold))
                    } else {
                        Image(systemName: "flag.checkered")
                            .font(.caption.weight(.bold))
                    }
                }
                .foregroundStyle(isNext ? Color.white : woladenBrandColor)
                .frame(width: 30, height: 30)
                .background(isNext ? woladenBrandColor : Color(.systemBackground), in: Circle())
                .overlay(Circle().stroke(woladenBrandColor, lineWidth: 2))

                if !isLast {
                    Rectangle()
                        .fill(woladenBrandColor.opacity(0.35))
                        .frame(width: 2, height: 42)
                }
            }
            VStack(alignment: .leading, spacing: 3) {
                Text(title)
                    .font(.subheadline.weight(isNext ? .bold : .semibold))
                Text(subtitle)
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(.top, 3)
            Spacer()
            if isNext {
                Text(String(localized: "trip.nextShort", defaultValue: "NEXT"))
                    .font(.caption2.weight(.bold))
                    .foregroundStyle(woladenBrandColor)
            }
        }
    }

    private var tripManagementActions: some View {
        Button(role: .destructive) {
            // Ending the active trip is intentionally protected by the long-press gesture below.
        } label: {
            Text(String(localized: "trip.end.action", defaultValue: "End trip"))
                .frame(maxWidth: .infinity, minHeight: 48)
        }
        .buttonStyle(.bordered)
        .simultaneousGesture(
            LongPressGesture(minimumDuration: 0.7)
                .onEnded { _ in
                    tripStore.endTrip()
                }
        )
        .accessibilityHint(Text(String(localized: "trip.end.longPressHint", defaultValue: "Press and hold to end the active trip.")))
    }

    private var noActiveTrip: some View {
        VStack(spacing: 18) {
            ContentUnavailableView(
                String(localized: "trip.empty.title", defaultValue: "No active trip"),
                systemImage: "car.side",
                description: Text(String(localized: "trip.empty.help", defaultValue: "Calculate a route, select one charger in every charging window, then start the trip."))
            )

            if !tripStore.sortedPlans.isEmpty {
                VStack(alignment: .leading, spacing: 10) {
                    Text(String(localized: "trip.empty.readyRoutes", defaultValue: "Ready routes"))
                        .font(.headline)
                    ForEach(tripStore.sortedPlans.filter(\.isReadyForTrip)) { plan in
                        Button {
                            tripStore.requestPlanEditing(plan.id)
                            viewModel.selectedTab = .route
                        } label: {
                            HStack {
                                VStack(alignment: .leading, spacing: 3) {
                                    Text(plan.name)
                                        .font(.subheadline.weight(.semibold))
                                        .lineLimit(1)
                                    Text("\(plan.selectedStopIDs.count) \(String(localized: "trip.routes.stops", defaultValue: "stops"))")
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                Image(systemName: "arrow.triangle.2.circlepath")
                            }
                            .frame(maxWidth: .infinity, minHeight: 54)
                        }
                        .buttonStyle(.bordered)
                        .tint(woladenBrandColor)
                    }
                }
                .frame(maxWidth: 640)
            }

            Button {
                tripStore.requestPlanEditing()
                viewModel.selectedTab = .route
            } label: {
                Label(String(localized: "trip.empty.plan", defaultValue: "Open route planning"), systemImage: "map")
                    .frame(minHeight: 48)
            }
            .buttonStyle(.bordered)
        }
        .padding(24)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private func stationDetailsDisclosure(_ station: TripStationSnapshot) -> some View {
        let amenities = amenitySnapshots(for: station)
        let total = max(station.amenitiesTotal ?? amenities.count, amenities.count)

        return DisclosureGroup(isExpanded: amenityExpansionBinding(for: station.stationID)) {
            VStack(alignment: .leading, spacing: 10) {
                if amenities.isEmpty {
                    Text(String(localized: "amenity.noDetails"))
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                } else {
                    ForEach(amenities) { amenity in
                        HStack(alignment: .top, spacing: 10) {
                            Image(systemName: AmenityCatalog.symbol(for: "amenity_\(amenity.category)"))
                                .frame(width: 24)
                                .foregroundStyle(woladenBrandColor)

                            VStack(alignment: .leading, spacing: 3) {
                                Text(amenity.name ?? AmenityCatalog.label(for: "amenity_\(amenity.category)"))
                                    .font(.subheadline.weight(.semibold))
                                Text(amenityMeta(amenity))
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer(minLength: 0)
                        }
                    }
                }

                if let feature = viewModel.feature(forStationID: station.stationID) {
                    Button {
                        viewModel.selectFeature(feature)
                    } label: {
                        Label(String(localized: "trip.station.openDetails", defaultValue: "Open station details"), systemImage: "info.circle")
                            .frame(maxWidth: .infinity, minHeight: 44)
                    }
                    .buttonStyle(.bordered)
                    .disabled(tripStore.isLikelyDriving)
                }
            }
            .padding(.top, 10)
        } label: {
            Label {
                Text(
                    String(localized: "trip.station.detailsAndAmenities", defaultValue: "Station details & amenities")
                        + (total > 0 ? " · \(total)" : "")
                )
                .font(.subheadline.weight(.semibold))
            } icon: {
                Image(systemName: "fork.knife")
                    .foregroundStyle(woladenBrandColor)
            }
        }
        .padding(12)
        .background(Color(.systemBackground).opacity(0.78), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
        .disabled(tripStore.isLikelyDriving)
    }

    private func amenityExpansionBinding(for stationID: String) -> Binding<Bool> {
        Binding(
            get: { expandedAmenityStationIDs.contains(stationID) },
            set: { isExpanded in
                if isExpanded {
                    expandedAmenityStationIDs.insert(stationID)
                } else {
                    expandedAmenityStationIDs.remove(stationID)
                }
            }
        )
    }

    private func amenitySnapshots(for station: TripStationSnapshot) -> [TripAmenitySnapshot] {
        if let amenities = station.amenities, !amenities.isEmpty {
            return amenities
        }
        return viewModel.feature(forStationID: station.stationID)?
            .properties
            .amenityExamples
            .map(TripAmenitySnapshot.init(example:)) ?? []
    }

    private func amenityMeta(_ amenity: TripAmenitySnapshot) -> String {
        var values: [String] = []
        if let openingHours = amenity.openingHours, !openingHours.isEmpty {
            values.append("\(String(localized: "amenity.hours")): \(openingHours)")
        } else {
            values.append(String(localized: "amenity.unknownHours"))
        }
        if let distanceM = amenity.distanceM {
            values.append("\(Int(distanceM.rounded())) m")
        }
        return values.joined(separator: " · ")
    }

    private func etaMetric(title: String, value: String) -> some View {
        VStack(spacing: 3) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.headline.monospacedDigit())
                .lineLimit(1)
                .minimumScaleFactor(0.75)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 10)
        .background(Color(.systemBackground).opacity(0.8), in: RoundedRectangle(cornerRadius: 10, style: .continuous))
    }

    private func liveBadge(status: AvailabilityStatus, available: Int, total: Int) -> some View {
        VStack(spacing: 2) {
            Text(status.label)
                .font(.caption.weight(.bold))
            if total > 0 {
                Text("\(available)/\(total)")
                    .font(.caption.monospacedDigit())
            }
        }
        .foregroundStyle(statusColor(status))
        .padding(.horizontal, 10)
        .padding(.vertical, 7)
        .background(statusColor(status).opacity(0.12), in: Capsule())
    }

    private func stationSubtitle(_ station: TripStationSnapshot) -> String {
        "\(station.city) · \(Int(station.maxPowerKW.rounded())) kW"
    }

    private func remainingDistanceText(to routePositionM: Int) -> String {
        formatRouteDistance(max(0, routePositionM - tripStore.currentRoutePositionM))
    }

    private var nextStopETAText: String {
        guard let date = tripStore.eta.nextStopArrival else {
            return tripStore.eta.nextStopIsLoading
                ? String(localized: "trip.eta.calculatingShort", defaultValue: "Calculating…")
                : String(localized: "trip.eta.unavailable", defaultValue: "—")
        }
        let source = tripStore.eta.nextStopUsesTraffic
            ? String(localized: "trip.eta.traffic", defaultValue: "traffic")
            : String(localized: "trip.eta.base", defaultValue: "estimate")
        return "\(tripDateText(date)) · \(source)"
    }

    private var destinationETAText: String {
        guard let date = tripStore.eta.destinationArrival else {
            return tripStore.eta.destinationIsLoading
                ? String(localized: "trip.eta.calculating", defaultValue: "Traffic ETA calculating…")
                : String(localized: "trip.eta.unavailable", defaultValue: "ETA unavailable")
        }
        let source = tripStore.eta.destinationUsesTraffic
            ? String(localized: "trip.eta.withTraffic", defaultValue: "with traffic")
            : String(localized: "trip.eta.baseEstimate", defaultValue: "base estimate")
        return String(localized: "trip.destinationETAWithSource", defaultValue: "ETA {time} · {source}")
            .replacingOccurrences(of: "{time}", with: tripDateText(date))
            .replacingOccurrences(of: "{source}", with: source)
    }

    private var liveFreshnessText: String {
        guard let updated = tripStore.liveUpdatedAt else {
            return String(localized: "trip.live.static", defaultValue: "Live status unavailable · showing last known station data")
        }
        return String(localized: "trip.live.updated", defaultValue: "Live status updated {time}")
            .replacingOccurrences(of: "{time}", with: updated.formatted(date: .omitted, time: .shortened))
    }

    private func statusColor(_ status: AvailabilityStatus) -> Color {
        switch status {
        case .free: return .teal
        case .occupied: return .orange
        case .outOfOrder: return .red
        case .unknown: return .secondary
        }
    }

    private func tripDateText(_ date: Date) -> String {
        if Calendar.current.isDateInToday(date) {
            return date.formatted(date: .omitted, time: .shortened)
        }
        return date.formatted(.dateTime.weekday(.abbreviated).hour().minute())
    }

    private func openAppleMaps(to coordinate: CLLocationCoordinate2D, name: String) {
        let placemark = MKPlacemark(coordinate: coordinate)
        let item = MKMapItem(placemark: placemark)
        item.name = name
        item.openInMaps(launchOptions: [MKLaunchOptionsDirectionsModeKey: MKLaunchOptionsDirectionsModeDriving])
    }

    private func openPreferredNavigation(to coordinate: CLLocationCoordinate2D, name: String) {
        switch tripStore.preferences.preferredNavigationApp {
        case .appleMaps:
            openAppleMaps(to: coordinate, name: name)
        case .googleMaps:
            openGoogleMaps(to: coordinate)
        }
    }

    private func openGoogleMaps(to coordinate: CLLocationCoordinate2D) {
        let raw = "https://www.google.com/maps/dir/?api=1&destination=\(coordinate.latitude),\(coordinate.longitude)&travelmode=driving"
        guard let url = URL(string: raw) else { return }
        UIApplication.shared.open(url)
    }
}

private struct SubstituteStationView: View {
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var tripStore: TripStore

    var body: some View {
        NavigationStack {
            List {
                if let next = tripStore.activePlan?.nextStop {
                    Section(String(localized: "trip.substitute.current", defaultValue: "Current stop")) {
                        stationRow(next, action: nil)

                        if currentStatus(next) == .occupied {
                            Button {
                                dismiss()
                            } label: {
                                Label(String(localized: "trip.substitute.wait", defaultValue: "Keep this stop and wait"), systemImage: "clock.fill")
                                    .frame(minHeight: 48)
                            }
                        }
                    }
                }

                if !substitutes.earlier.isEmpty {
                    Section {
                        ForEach(substitutes.earlier) { station in
                            stationRow(station) { replace(with: station) }
                        }
                    } header: {
                        Text(String(localized: "trip.substitute.earlier", defaultValue: "Earlier stations ahead"))
                    } footer: {
                        Text(String(localized: "trip.substitute.earlierHelp", defaultValue: "Reachable stations before the current planned stop, without turning back."))
                    }
                }

                if !substitutes.detour.isEmpty {
                    Section {
                        ForEach(substitutes.detour) { station in
                            stationRow(station) { replace(with: station) }
                        }
                    } header: {
                        Text(String(localized: "trip.substitute.detour", defaultValue: "Detour options"))
                    } footer: {
                        Text(String(localized: "trip.substitute.detourHelp", defaultValue: "Other reachable chargers within your maximum detour setting."))
                    }
                }

                if substitutes.earlier.isEmpty && substitutes.detour.isEmpty {
                    ContentUnavailableView(
                        String(localized: "trip.substitute.none", defaultValue: "No safe substitute found"),
                        systemImage: "bolt.slash",
                        description: Text(String(localized: "trip.substitute.noneHelp", defaultValue: "Keep the current stop or safely stop before changing route settings."))
                    )
                }
            }
            .navigationTitle(String(localized: "trip.substitute.title", defaultValue: "Substitute charger"))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(String(localized: "common.done", defaultValue: "Done")) { dismiss() }
                }
            }
        }
    }

    private var substitutes: (earlier: [TripStationSnapshot], detour: [TripStationSnapshot]) {
        tripStore.substitutesForNextStop()
    }

    private func stationRow(_ station: TripStationSnapshot, action: (() -> Void)?) -> some View {
        let status = currentStatus(station)
        let live = tripStore.liveSummary(for: station.stationID)
        let available = live?.availableEVSEs ?? station.availableEVSEs
        let total = live?.totalEVSEs ?? station.totalEVSEs
        let colors = stationLiveCardColors(status: status, available: available, total: total)
        return Button {
            action?()
        } label: {
            HStack(spacing: 0) {
                StationClassificationRail(classification: station.classification, width: 10)
                HStack(spacing: 12) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(station.operatorName)
                            .font(.headline)
                            .foregroundStyle(.primary)
                        Text("\(station.classification.title) · \(station.city) · km \(station.routePositionM / 1000) · \(Int(station.maxPowerKW.rounded())) kW")
                            .font(.subheadline)
                            .foregroundStyle(.secondary)
                        Text(status.label)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(statusColor(status))
                    }
                    Spacer()
                    if action != nil {
                        Image(systemName: "arrow.right.circle.fill")
                            .font(.title2)
                            .foregroundStyle(woladenBrandColor)
                    }
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
            }
            .frame(minHeight: 58)
            .background(colors.background)
            .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .stroke(colors.border, lineWidth: 1)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .disabled(action == nil)
    }

    private func replace(with station: TripStationSnapshot) {
        tripStore.replaceNextStop(with: station.stationID)
        dismiss()
    }

    private func currentStatus(_ station: TripStationSnapshot) -> AvailabilityStatus {
        tripStore.liveSummary(for: station.stationID)?.availabilityStatus ?? station.availabilityStatus
    }

    private func statusColor(_ status: AvailabilityStatus) -> Color {
        switch status {
        case .free: return .teal
        case .occupied: return .orange
        case .outOfOrder: return .red
        case .unknown: return .secondary
        }
    }
}

private struct TripGlanceMap: View {
    let plan: RoutePlan
    let isDriving: Bool
    @State private var position: MapCameraPosition = .automatic

    var body: some View {
        Map(position: $position, interactionModes: isDriving ? [] : [.pan, .zoom]) {
            if routeCoordinates.count > 1 {
                MapPolyline(coordinates: routeCoordinates)
                    .stroke(woladenBrandColor, style: StrokeStyle(lineWidth: 6, lineCap: .round, lineJoin: .round))
            }

            ForEach(Array(plan.selectedStopIDs.enumerated()), id: \.element) { index, stationID in
                if let station = plan.station(stationID) {
                    Annotation("", coordinate: station.coordinate) {
                        Text("\(index + 1)")
                            .font(.caption.weight(.bold))
                            .foregroundStyle(.white)
                            .frame(width: 32, height: 32)
                            .background(woladenBrandColor, in: Circle())
                            .overlay(Circle().stroke(Color.white, lineWidth: 2))
                    }
                }
            }

            if !plan.isStationTargetTrip {
                Annotation("", coordinate: plan.route.destination.coordinate) {
                    Image(systemName: "flag.checkered.circle.fill")
                        .font(.system(size: 34))
                        .foregroundStyle(woladenBrandColor)
                        .background(Color.white, in: Circle())
                }
            }
        }
        .frame(height: 300)
        .clipShape(RoundedRectangle(cornerRadius: 18, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: 18, style: .continuous)
                .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
        }
        .onAppear { fitRoute() }
    }

    private var routeCoordinates: [CLLocationCoordinate2D] {
        plan.route.geometryCoordinates.compactMap { raw in
            guard raw.count >= 2 else { return nil }
            return CLLocationCoordinate2D(latitude: raw[1], longitude: raw[0])
        }
    }

    private func fitRoute() {
        guard !routeCoordinates.isEmpty else { return }
        var rect = MKMapRect.null
        for coordinate in routeCoordinates {
            let point = MKMapPoint(coordinate)
            let pointRect = MKMapRect(x: point.x, y: point.y, width: 1, height: 1)
            rect = rect.isNull ? pointRect : rect.union(pointRect)
        }
        if !rect.isNull {
            position = .rect(rect.insetBy(dx: -max(rect.width * 0.12, 25_000), dy: -max(rect.height * 0.12, 25_000)))
        }
    }
}
