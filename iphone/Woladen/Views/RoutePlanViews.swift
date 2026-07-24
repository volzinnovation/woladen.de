import SwiftUI
import MapKit

struct SavedRoutePlansView: View {
    @EnvironmentObject private var tripStore: TripStore

    let currentPlanID: UUID?
    let onEdit: (RoutePlan) -> Void
    let onNew: () -> Void

    @State private var pendingDeletion: RoutePlan?

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(String(localized: "trip.routes.title", defaultValue: "Your routes"))
                .font(.title2.weight(.bold))

            if tripStore.sortedPlans.isEmpty {
                ContentUnavailableView(
                    String(localized: "trip.routes.empty", defaultValue: "No saved routes"),
                    systemImage: "map",
                    description: Text(String(localized: "trip.routes.emptyHelp", defaultValue: "Save route preferences here and recalculate the charging stops for every departure."))
                )
                .frame(maxWidth: .infinity)
                .padding(.vertical, 20)
            } else {
                ForEach(tripStore.sortedPlans) { plan in
                    HStack(spacing: 10) {
                        Button {
                            onEdit(plan)
                        } label: {
                            VStack(alignment: .leading, spacing: 4) {
                                HStack(spacing: 6) {
                                    if plan.state == .active {
                                        Text(String(localized: "trip.routes.active", defaultValue: "ACTIVE"))
                                            .font(.caption2.weight(.bold))
                                            .foregroundStyle(.white)
                                            .padding(.horizontal, 6)
                                            .padding(.vertical, 3)
                                            .background(woladenBrandColor, in: Capsule())
                                    }
                                    Text(plan.name)
                                        .font(.subheadline.weight(.semibold))
                                        .lineLimit(1)
                                }
                                Text("\(plan.route.origin.label) → \(plan.route.destination.label)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                                Text(routeMeta(plan))
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)

                        if plan.isReadyForTrip, plan.state != .active {
                            Button {
                                onEdit(plan)
                            } label: {
                                Image(systemName: "arrow.triangle.2.circlepath")
                                    .frame(width: 34, height: 34)
                            }
                            .buttonStyle(.borderedProminent)
                            .tint(woladenBrandColor)
                            .accessibilityLabel(Text(String(localized: "trip.routes.reviewDeparture", defaultValue: "Review start and battery level")))
                        }

                        Button(role: .destructive) {
                            pendingDeletion = plan
                        } label: {
                            Image(systemName: "trash")
                                .frame(width: 34, height: 34)
                        }
                        .buttonStyle(.bordered)
                        .accessibilityLabel(Text(String(localized: "trip.routes.delete", defaultValue: "Delete route")))
                    }
                    .padding(12)
                    .background(
                        plan.id == currentPlanID ? StationVisualStyle.selectedControlSurface : StationVisualStyle.controlSurface,
                        in: RoundedRectangle(cornerRadius: 10, style: .continuous)
                    )
                    .overlay {
                        RoundedRectangle(cornerRadius: 10, style: .continuous)
                            .stroke(
                                plan.id == currentPlanID ? StationVisualStyle.selectedControlBorder : StationVisualStyle.controlBorder,
                                lineWidth: 1
                            )
                    }
                }
            }

            Button {
                onNew()
            } label: {
                Label(String(localized: "trip.routes.newFull", defaultValue: "New route"), systemImage: "plus")
                    .font(.headline)
                    .frame(maxWidth: .infinity, minHeight: 56)
            }
            .buttonStyle(.borderedProminent)
            .tint(woladenBrandColor)
        }
        .confirmationDialog(
            String(localized: "trip.routes.deleteConfirm", defaultValue: "Delete this saved route?"),
            isPresented: Binding(
                get: { pendingDeletion != nil },
                set: { if !$0 { pendingDeletion = nil } }
            ),
            titleVisibility: .visible
        ) {
            Button(String(localized: "trip.routes.delete", defaultValue: "Delete route"), role: .destructive) {
                if let pendingDeletion {
                    tripStore.deletePlan(id: pendingDeletion.id)
                }
                pendingDeletion = nil
            }
            Button(String(localized: "common.cancel", defaultValue: "Cancel"), role: .cancel) {
                    pendingDeletion = nil
            }
        }
    }

    private func routeMeta(_ plan: RoutePlan) -> String {
        let selected = plan.selectedStopIDs.count
        let stopLabel = selected == 1
            ? String(localized: "trip.routes.stop", defaultValue: "stop")
            : String(localized: "trip.routes.stops", defaultValue: "stops")
        let providerLabel = plan.providerCoverage.count == 1
            ? String(localized: "trip.routes.provider", defaultValue: "provider")
            : String(localized: "trip.routes.providers", defaultValue: "providers")
        return "\(formatRouteDistance(plan.route.distanceM)) · \(selected) \(stopLabel) · \(plan.providerCoverage.count) \(providerLabel)"
    }
}

struct RoutePlanEditorView: View {
    @EnvironmentObject private var tripStore: TripStore

    let planID: UUID
    let onOpenStation: (String) -> Void

    @State private var showingSettings = false

    var body: some View {
        if let plan = tripStore.plan(id: planID) {
            VStack(alignment: .leading, spacing: 14) {
                planSummary(plan)
                RoutePlanMapView(plan: plan)
                providerSection(plan)
                invalidatedSelectionWarning(plan)
                selectedStopsSection(plan)
                chargingWindows(plan)
                startTripButton(plan)
            }
            .sheet(isPresented: $showingSettings) {
                TripSettingsView(planID: plan.id, initial: tripStore.preferences)
                    .environmentObject(tripStore)
            }
        }
    }

    private func planSummary(_ plan: RoutePlan) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 3) {
                    Text(plan.name)
                        .font(.headline)
                    Text(
                        String(localized: "trip.plan.calculated", defaultValue: "Calculated with {soc}% initial charge")
                            .replacingOccurrences(of: "{soc}", with: "\(Int(plan.route.initialSOCPercent.rounded()))")
                    )
                    .font(.caption)
                    .foregroundStyle(.secondary)
                }
                Spacer(minLength: 12)
                Button {
                    showingSettings = true
                } label: {
                    Image(systemName: "slider.horizontal.3")
                        .frame(width: 40, height: 40)
                }
                .buttonStyle(.bordered)
                .accessibilityLabel(Text(String(localized: "trip.settings.title", defaultValue: "Trip settings")))
            }

            HStack(spacing: 8) {
                summaryPill(
                    "\(plan.selectedStopIDs.count)",
                    label: String(localized: "trip.plan.selected", defaultValue: "selected stops")
                )
                summaryPill(
                    "\(plan.windows.flatMap(\.candidateStationIDs).count)",
                    label: String(localized: "trip.plan.candidates", defaultValue: "candidates")
                )
                summaryPill(
                    "\(plan.providerCoverage.count)",
                    label: String(localized: "trip.plan.providers", defaultValue: "providers")
                )
            }
        }
        .padding(14)
        .background(StationVisualStyle.controlSurface, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
    }

    private func summaryPill(_ value: String, label: String) -> some View {
        VStack(spacing: 2) {
            Text(value)
                .font(.headline.monospacedDigit())
            Text(label)
                .font(.caption2)
                .foregroundStyle(.secondary)
                .lineLimit(1)
                .minimumScaleFactor(0.75)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 8)
        .background(Color(.systemBackground), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
    }

    private func providerSection(_ plan: RoutePlan) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                Text(String(localized: "trip.providers.title", defaultValue: "Charging packages"))
                    .font(.headline)
                Spacer()
                Picker(
                    String(localized: "trip.settings.packageMode", defaultValue: "Charging packages"),
                    selection: providerModeBinding(plan)
                ) {
                    ForEach(ProviderPackageMode.allCases) { mode in
                        Text(mode.title).tag(mode)
                    }
                }
                .pickerStyle(.menu)
            }

            if plan.providerCoverage.isEmpty {
                Text(String(localized: "trip.providers.none", defaultValue: "No provider covers the current charging windows."))
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
            } else {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 8) {
                        ForEach(plan.providerCoverage) { coverage in
                            let selected = plan.selectedProviderNames.contains {
                                normalizedProviderKey($0) == normalizedProviderKey(coverage.providerName)
                            }
                            Button {
                                tripStore.toggleProvider(coverage.providerName, planID: plan.id)
                            } label: {
                                VStack(alignment: .leading, spacing: 3) {
                                    HStack(spacing: 5) {
                                        Text(coverage.providerName)
                                            .font(.subheadline.weight(.semibold))
                                        if selected {
                                            Image(systemName: "checkmark.circle.fill")
                                        }
                                    }
                                    Text("\(coverage.stationCount) \(providerStationLabel(coverage.stationCount)) · \(coverage.coveredWindowCount)/\(coverage.totalWindowCount) \(String(localized: "trip.providers.windowsShort", defaultValue: "windows"))")
                                        .font(.caption)
                                }
                                .foregroundStyle(selected ? Color.white : Color.primary)
                                .padding(.horizontal, 12)
                                .padding(.vertical, 9)
                                .background(
                                    selected ? woladenBrandColor : StationVisualStyle.controlSurface,
                                    in: RoundedRectangle(cornerRadius: 10, style: .continuous)
                                )
                                .overlay {
                                    RoundedRectangle(cornerRadius: 10, style: .continuous)
                                        .stroke(selected ? Color.clear : StationVisualStyle.controlBorder, lineWidth: 1)
                                }
                            }
                            .buttonStyle(.plain)
                            .accessibilityAddTraits(selected ? .isSelected : [])
                        }
                    }
                }
            }
        }
    }

    @ViewBuilder
    private func invalidatedSelectionWarning(_ plan: RoutePlan) -> some View {
        if !plan.invalidatedStationIDs.isEmpty {
            Label {
                Text(String(localized: "trip.plan.invalidated", defaultValue: "Some previously selected stops no longer match the reachable charging windows. Please select replacements."))
            } icon: {
                Image(systemName: "exclamationmark.triangle.fill")
            }
            .font(.subheadline)
            .foregroundStyle(Color.orange)
            .padding(12)
            .background(Color.orange.opacity(0.12), in: RoundedRectangle(cornerRadius: 10, style: .continuous))
        }
    }

    @ViewBuilder
    private func selectedStopsSection(_ plan: RoutePlan) -> some View {
        if !plan.selectedStopIDs.isEmpty {
            VStack(alignment: .leading, spacing: 8) {
                Text(String(localized: "trip.plan.selectedStops", defaultValue: "Planned charging stops"))
                    .font(.headline)
                ForEach(Array(plan.selectedStopIDs.enumerated()), id: \.element) { index, stationID in
                    if let station = plan.station(stationID) {
                        compactSelectedRow(station, index: index + 1, plan: plan)
                    }
                }
            }
        }
    }

    private func compactSelectedRow(_ station: TripStationSnapshot, index: Int, plan: RoutePlan) -> some View {
        HStack(spacing: 10) {
            Text("\(index)")
                .font(.subheadline.weight(.bold))
                .foregroundStyle(.white)
                .frame(width: 28, height: 28)
                .background(woladenBrandColor, in: Circle())
            VStack(alignment: .leading, spacing: 2) {
                Text(station.operatorName)
                    .font(.subheadline.weight(.semibold))
                Text("\(station.city) · \(Int(station.maxPowerKW.rounded())) kW · km \(Int((Double(station.routePositionM) / 1000).rounded()))")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            Button {
                tripStore.toggleStop(stationID: station.stationID, planID: plan.id)
            } label: {
                Image(systemName: "minus.circle.fill")
                    .font(.title3)
                    .foregroundStyle(Color.red)
                    .frame(width: 44, height: 44)
            }
            .buttonStyle(.plain)
            .disabled(tripStore.isLikelyDriving)
            .accessibilityLabel(Text(String(localized: "trip.plan.deselect", defaultValue: "Deselect charging stop")))
        }
        .padding(10)
        .background(StationVisualStyle.selectedControlSurface, in: RoundedRectangle(cornerRadius: 10, style: .continuous))
    }

    private func chargingWindows(_ plan: RoutePlan) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            if plan.windows.isEmpty {
                ContentUnavailableView(
                    String(localized: "trip.plan.noWindow", defaultValue: "No charging stop needed"),
                    systemImage: "battery.100percent",
                    description: Text(String(localized: "trip.plan.destinationReachable", defaultValue: "The destination is estimated to be reachable above your reserve."))
                )
            } else {
                ForEach(plan.visibleWindows) { window in
                    windowSection(window, plan: plan)
                }
                if plan.visibleWindows.count < plan.windows.count {
                    Label(
                        String(localized: "trip.plan.sequentialHelp", defaultValue: "Choose this stop to calculate the next charging window."),
                        systemImage: "arrow.down.circle"
                    )
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .center)
                    .padding(.vertical, 6)
                }
            }
        }
    }

    private func windowSection(_ window: ChargingWindow, plan: RoutePlan) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(
                        String(localized: "trip.plan.window", defaultValue: "Charging window {index}")
                            .replacingOccurrences(of: "{index}", with: "\(window.index + 1)")
                    )
                    .font(.headline)
                    Text("km \(window.startPositionM / 1000)–\(window.endPositionM / 1000) · \(window.candidateStationIDs.count) \(String(localized: "trip.plan.matches", defaultValue: "matches"))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                if window.selectedStationID != nil {
                    Label(String(localized: "trip.plan.selectedShort", defaultValue: "Selected"), systemImage: "checkmark.circle.fill")
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(woladenBrandColor)
                }
            }

            if window.candidateStationIDs.isEmpty {
                Label(
                    String(localized: "trip.plan.noCandidates", defaultValue: "No reachable charger matches these settings."),
                    systemImage: "exclamationmark.triangle.fill"
                )
                .font(.subheadline)
                .foregroundStyle(Color.red)
            } else {
                ForEach(sortedCandidateIDs(window, plan: plan), id: \.self) { stationID in
                    if let station = plan.station(stationID) {
                        candidateRow(station, window: window, plan: plan)
                    }
                }
            }
        }
        .padding(14)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
    }

    private func candidateRow(_ station: TripStationSnapshot, window: ChargingWindow, plan: RoutePlan) -> some View {
        let isSelected = window.selectedStationID == station.stationID
        let arrivalSOC = window.projectedArrivalSOCByStationID[station.stationID]
        return HStack(spacing: 0) {
            StationClassificationRail(classification: station.classification)

            Button {
                onOpenStation(station.stationID)
            } label: {
                VStack(alignment: .leading, spacing: 4) {
                    HStack(spacing: 6) {
                        Text(station.operatorName)
                            .font(.subheadline.weight(.semibold))
                            .foregroundStyle(.primary)
                        availabilityBadge(station)
                    }
                    Text("\(station.classification.title) · \(station.city)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text("\(Int(station.maxPowerKW.rounded())) kW · \(station.chargingPointsCount) \(String(localized: "trip.station.pads", defaultValue: "charging points")) · \(formatRouteDistance(station.routeDetourM)) \(String(localized: "trip.station.detour", defaultValue: "detour"))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    if let reliability = station.reliabilityPercent {
                        Text(
                            String(localized: "trip.station.reliability", defaultValue: "Reliability {value}%")
                                .replacingOccurrences(of: "{value}", with: "\(Int(reliability.rounded()))")
                        )
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(woladenBrandColor)
                    }
                    if let lastUnavailable = station.lastUnavailableAt, !lastUnavailable.isEmpty {
                        Text(
                            String(localized: "trip.station.lastUnavailable", defaultValue: "Last unavailable: {date}")
                                .replacingOccurrences(of: "{date}", with: lastUnavailable)
                        )
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                    }
                    if let arrivalSOC {
                        Text(
                            String(localized: "trip.plan.arrivalSOC", defaultValue: "Estimated arrival {soc}%")
                                .replacingOccurrences(of: "{soc}", with: "\(Int(arrivalSOC.rounded()))")
                        )
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(woladenBrandColor)
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .padding(.horizontal, 10)

            VStack(spacing: 4) {
                Text("km \(Int((Double(station.routePositionM) / 1000).rounded()))")
                    .font(.caption.weight(.semibold).monospacedDigit())
                    .foregroundStyle(.secondary)

                Button {
                    _ = tripStore.activateFocusedStation(stationID: station.stationID, planID: plan.id)
                } label: {
                    Image(systemName: "play.fill")
                        .font(.headline)
                        .frame(width: 48, height: 44)
                }
                .buttonStyle(.borderedProminent)
                .tint(woladenBrandColor)
                .accessibilityLabel(
                    Text(
                        String(localized: "trip.station.driveTarget", defaultValue: "Use {station} as driving target")
                            .replacingOccurrences(of: "{station}", with: station.operatorName)
                    )
                )

                Button {
                    tripStore.toggleStop(stationID: station.stationID, planID: plan.id)
                } label: {
                    Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                        .font(.system(size: 25, weight: .semibold))
                        .foregroundStyle(isSelected ? woladenBrandColor : StationVisualStyle.mutedForeground)
                        .frame(width: 48, height: 44)
                }
                .buttonStyle(.plain)
                .disabled(tripStore.isLikelyDriving)
                .accessibilityLabel(
                    Text(
                        isSelected
                            ? String(localized: "trip.plan.deselect", defaultValue: "Deselect charging stop")
                            : String(localized: "trip.plan.select", defaultValue: "Select charging stop")
                    )
                )
            }
            .padding(.trailing, 8)
            .padding(.vertical, 8)
        }
        .clipShape(RoundedRectangle(cornerRadius: 10, style: .continuous))
        .background(
            isSelected ? StationVisualStyle.selectedControlSurface : Color(.systemBackground),
            in: RoundedRectangle(cornerRadius: 10, style: .continuous)
        )
        .overlay {
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .stroke(isSelected ? StationVisualStyle.selectedControlBorder : StationVisualStyle.controlBorder, lineWidth: 1)
        }
    }

    private func availabilityBadge(_ station: TripStationSnapshot) -> some View {
        Text(station.availabilityStatus.label)
            .font(.caption2.weight(.semibold))
            .foregroundStyle(statusColor(station.availabilityStatus))
            .padding(.horizontal, 6)
            .padding(.vertical, 3)
            .background(statusColor(station.availabilityStatus).opacity(0.12), in: Capsule())
    }

    private func startTripButton(_ plan: RoutePlan) -> some View {
        VStack(spacing: 8) {
            if !plan.isReadyForTrip {
                Text(String(localized: "trip.plan.selectEach", defaultValue: "Select one reachable charger in every charging window."))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
            }
            Button {
                _ = tripStore.activate(planID: plan.id)
            } label: {
                Label(
                    (plan.selectedStopIDs.isEmpty
                        ? String(localized: "trip.plan.startDirect", defaultValue: "Start direct trip")
                        : plan.selectedStopIDs.count == 1
                            ? String(localized: "trip.plan.startOne", defaultValue: "Start trip with {count} stop")
                            : String(localized: "trip.plan.start", defaultValue: "Start trip with {count} stops"))
                        .replacingOccurrences(of: "{count}", with: "\(plan.selectedStopIDs.count)"),
                    systemImage: "car.fill"
                )
                .font(.headline)
                .frame(maxWidth: .infinity, minHeight: 56)
            }
            .buttonStyle(.borderedProminent)
            .tint(woladenBrandColor)
            .disabled(!plan.isReadyForTrip || tripStore.isLikelyDriving)
        }
    }

    private func providerModeBinding(_ plan: RoutePlan) -> Binding<ProviderPackageMode> {
        Binding(
            get: { plan.providerMode },
            set: { next in
                tripStore.setProviderSelection(
                    names: plan.selectedProviderNames,
                    mode: next,
                    planID: plan.id
                )
            }
        )
    }

    private func sortedCandidateIDs(_ window: ChargingWindow, plan: RoutePlan) -> [String] {
        let preferred = Set(plan.selectedProviderNames.map(normalizedProviderKey))
        return window.candidateStationIDs.sorted { lhsID, rhsID in
            guard let lhs = plan.station(lhsID), let rhs = plan.station(rhsID) else { return lhsID < rhsID }
            let lhsPreferred = preferred.contains(normalizedProviderKey(lhs.operatorName))
            let rhsPreferred = preferred.contains(normalizedProviderKey(rhs.operatorName))
            if lhsPreferred != rhsPreferred { return lhsPreferred }
            if lhs.availabilityStatus != rhs.availabilityStatus {
                return statusRank(lhs.availabilityStatus) < statusRank(rhs.availabilityStatus)
            }
            if lhs.routeDetourM != rhs.routeDetourM { return lhs.routeDetourM < rhs.routeDetourM }
            return lhs.routePositionM > rhs.routePositionM
        }
    }

    private func statusRank(_ status: AvailabilityStatus) -> Int {
        switch status {
        case .free: return 0
        case .unknown: return 1
        case .occupied: return 2
        case .outOfOrder: return 3
        }
    }

    private func providerStationLabel(_ count: Int) -> String {
        count == 1
            ? String(localized: "trip.providers.station", defaultValue: "station")
            : String(localized: "trip.providers.stations", defaultValue: "stations")
    }

    private func statusColor(_ status: AvailabilityStatus) -> Color {
        switch status {
        case .free: return Color.teal
        case .occupied: return Color.orange
        case .outOfOrder: return Color.red
        case .unknown: return Color.secondary
        }
    }
}

private struct RoutePlanMapView: View {
    let plan: RoutePlan
    @State private var position: MapCameraPosition = .automatic

    private var visibleStationIDs: Set<String> {
        Set(plan.windows.flatMap(\.candidateStationIDs)).union(plan.selectedStopIDs)
    }

    var body: some View {
        Map(position: $position, interactionModes: [.pan, .zoom]) {
            let coordinates = routeCoordinates
            if coordinates.count > 1 {
                MapPolyline(coordinates: coordinates)
                    .stroke(woladenBrandColor, style: StrokeStyle(lineWidth: 5, lineCap: .round, lineJoin: .round))
            }

            ForEach(plan.rawStations.filter { visibleStationIDs.contains($0.stationID) }) { station in
                Annotation("", coordinate: station.coordinate) {
                    marker(station)
                }
            }
        }
        .frame(height: 250)
        .clipShape(RoundedRectangle(cornerRadius: 12, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
        }
        .onAppear { fitRoute() }
        .onChange(of: plan.updatedAt) { _, _ in fitRoute() }
    }

    private var routeCoordinates: [CLLocationCoordinate2D] {
        plan.route.geometryCoordinates.compactMap { raw in
            guard raw.count >= 2 else { return nil }
            return CLLocationCoordinate2D(latitude: raw[1], longitude: raw[0])
        }
    }

    @ViewBuilder
    private func marker(_ station: TripStationSnapshot) -> some View {
        if let index = plan.selectedStopIDs.firstIndex(of: station.stationID) {
            Text("\(index + 1)")
                .font(.caption2.weight(.bold))
                .foregroundStyle(.white)
                .frame(width: 28, height: 28)
                .background(woladenBrandColor, in: Circle())
                .overlay(Circle().stroke(Color.white, lineWidth: 2))
                .shadow(radius: 2, y: 1)
        } else {
            Circle()
                .fill(Color(.systemBackground))
                .frame(width: 18, height: 18)
                .overlay(Circle().stroke(woladenBrandColor, lineWidth: 3))
                .shadow(radius: 2, y: 1)
        }
    }

    private func fitRoute() {
        let coordinates = routeCoordinates
        guard !coordinates.isEmpty else { return }
        var rect = MKMapRect.null
        for coordinate in coordinates {
            let point = MKMapPoint(coordinate)
            let pointRect = MKMapRect(x: point.x, y: point.y, width: 1, height: 1)
            rect = rect.isNull ? pointRect : rect.union(pointRect)
        }
        if !rect.isNull {
            let insetX = max(rect.width * 0.1, 25_000)
            let insetY = max(rect.height * 0.1, 25_000)
            position = .rect(rect.insetBy(dx: -insetX, dy: -insetY))
        }
    }
}
