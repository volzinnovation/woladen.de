import SwiftUI

struct TripSettingsView: View {
    @Environment(\.dismiss) private var dismiss
    @EnvironmentObject private var tripStore: TripStore

    let planID: UUID?
    @State private var draft: TripPreferences

    init(planID: UUID? = nil, initial: TripPreferences) {
        self.planID = planID
        _draft = State(initialValue: initial)
    }

    var body: some View {
        NavigationStack {
            Form {
                layoutSection
                navigationSection
                vehicleSection
                energyWindowSection
                providerSection
                drivingSection
                carPlaySection
            }
            .navigationTitle(String(localized: "trip.settings.appTitle", defaultValue: "App settings"))
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(String(localized: "common.cancel", defaultValue: "Cancel")) {
                        dismiss()
                    }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button(String(localized: "common.done", defaultValue: "Done")) {
                        tripStore.updatePreferences(draft, recalculatePlanID: planID)
                        dismiss()
                    }
                    .fontWeight(.semibold)
                }
            }
        }
    }

    private var layoutSection: some View {
        Section {
            ForEach(TripVisualStyle.allCases) { style in
                Button {
                    draft.visualStyle = style
                } label: {
                    HStack(spacing: 12) {
                        Image(systemName: layoutIcon(style))
                            .font(.title3.weight(.semibold))
                            .foregroundStyle(woladenBrandColor)
                            .frame(width: 28)
                        VStack(alignment: .leading, spacing: 2) {
                            Text(style.title)
                                .foregroundStyle(.primary)
                            Text(layoutDescription(style))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Spacer()
                        if draft.visualStyle == style {
                            Image(systemName: "checkmark.circle.fill")
                                .foregroundStyle(woladenBrandColor)
                        }
                    }
                    .contentShape(Rectangle())
                }
                .buttonStyle(.plain)
                .accessibilityAddTraits(draft.visualStyle == style ? .isSelected : [])
            }
        } header: {
            Text(String(localized: "trip.settings.layout", defaultValue: "Trip layout"))
        } footer: {
            Text(String(localized: "trip.settings.layoutHelp", defaultValue: "All layouts use the same route, live data and safety rules."))
        }
    }

    private var vehicleSection: some View {
        Section(String(localized: "trip.settings.vehicle", defaultValue: "Vehicle & energy")) {
            Picker(
                String(localized: "trip.settings.activeVehicle", defaultValue: "Active vehicle"),
                selection: activeVehicleIDBinding
            ) {
                ForEach(vehicleProfiles) { profile in
                    Text(profile.name).tag(profile.id)
                }
            }

            HStack {
                Button {
                    addVehicleProfile()
                } label: {
                    Label(String(localized: "trip.settings.addVehicle", defaultValue: "Add vehicle"), systemImage: "plus")
                }
                Spacer()
                Button(role: .destructive) {
                    removeActiveVehicleProfile()
                } label: {
                    Label(String(localized: "trip.settings.removeVehicle", defaultValue: "Remove"), systemImage: "trash")
                }
                .disabled(vehicleProfiles.count <= 1)
            }

            TextField(
                String(localized: "trip.settings.vehicleName", defaultValue: "Vehicle name"),
                text: activeVehicleNameBinding
            )

            sliderRow(
                title: String(localized: "trip.settings.battery", defaultValue: "Usable battery"),
                value: activeVehicleValueBinding(\.batteryCapacityKWh),
                range: 10...200,
                step: 1,
                valueText: "\(Int(activeVehicleSettings.batteryCapacityKWh.rounded())) kWh"
            )
            sliderRow(
                title: String(localized: "trip.settings.consumption", defaultValue: "Expected consumption"),
                value: activeVehicleValueBinding(\.consumptionKWhPer100KM),
                range: 5...50,
                step: 0.5,
                valueText: String(format: "%.1f kWh/100 km", activeVehicleSettings.consumptionKWhPer100KM)
            )
            sliderRow(
                title: String(localized: "trip.settings.chargingPower", defaultValue: "Average charging power"),
                value: activeVehicleValueBinding(\.averageChargingPowerKW),
                range: 3...400,
                step: 1,
                valueText: "\(Int(activeVehicleSettings.averageChargingPowerKW.rounded())) kW"
            )
        }
    }

    private var energyWindowSection: some View {
        Section {
            sliderRow(
                title: String(localized: "trip.settings.reserve", defaultValue: "Preferred arrival charge"),
                value: activeVehicleValueBinding(\.reserveSOCPercent),
                range: 5...40,
                step: 1,
                valueText: "\(Int(activeVehicleSettings.reserveSOCPercent.rounded()))%"
            )
            sliderRow(
                title: String(localized: "trip.settings.target", defaultValue: "Target charge after a stop"),
                value: targetSOCBinding,
                range: 10...90,
                step: 1,
                valueText: "\(Int(activeVehicleSettings.targetSOCPercent.rounded()))%"
            )
            sliderRow(
                title: String(localized: "trip.settings.window", defaultValue: "Show candidates this much earlier"),
                value: activeVehicleValueBinding(\.earlyWindowKM),
                range: 10...120,
                step: 5,
                valueText: "\(Int(activeVehicleSettings.earlyWindowKM.rounded())) km"
            )
            sliderRow(
                title: String(localized: "trip.settings.detour", defaultValue: "Maximum substitute detour"),
                value: activeVehicleValueBinding(\.maximumDetourMinutes),
                range: 5...30,
                step: 1,
                valueText: "\(Int(activeVehicleSettings.maximumDetourMinutes.rounded())) min"
            )
        } header: {
            Text(String(localized: "trip.settings.energyWindow", defaultValue: "Charging window"))
        } footer: {
            Text(String(localized: "trip.settings.estimateHelp", defaultValue: "These values estimate reachability; they are not live vehicle telemetry."))
        }
    }

    private var providerSection: some View {
        Section {
            Picker(
                String(localized: "trip.settings.packageMode", defaultValue: "Charging packages"),
                selection: $draft.providerMode
            ) {
                ForEach(ProviderPackageMode.allCases) { mode in
                    Text(mode.title).tag(mode)
                }
            }
            .pickerStyle(.segmented)

            if providerOptions.isEmpty {
                Text(String(localized: "trip.providers.afterRoute", defaultValue: "Calculate a route to compare provider coverage."))
                    .foregroundStyle(.secondary)
            } else {
                ForEach(providerOptions, id: \.self) { provider in
                    Toggle(isOn: providerBinding(provider)) {
                        VStack(alignment: .leading, spacing: 2) {
                            Text(provider)
                            if let coverage = providerCoverage(provider) {
                                Text("\(coverage.stationCount) \(coverage.stationCount == 1 ? String(localized: "trip.providers.station", defaultValue: "station") : String(localized: "trip.providers.stations", defaultValue: "stations")) · \(coverage.coveredWindowCount)/\(coverage.totalWindowCount) \(String(localized: "trip.providers.windows", defaultValue: "charging windows"))")
                                    .font(.caption)
                                    .foregroundStyle(coverage.coversEveryWindow ? woladenBrandColor : .secondary)
                            }
                        }
                    }
                    .tint(woladenBrandColor)
                }
            }
        } header: {
            Text(String(localized: "trip.settings.providers", defaultValue: "Providers & packages"))
        } footer: {
            Text(
                draft.providerMode == .prefer
                    ? String(localized: "trip.providers.preferHelp", defaultValue: "Preferred providers rank first; other chargers remain available as fallbacks.")
                    : String(localized: "trip.providers.onlyHelp", defaultValue: "Only selected providers are eligible. Uncovered charging windows make the route infeasible.")
            )
        }
    }

    private var navigationSection: some View {
        Section {
            Toggle(isOn: googleMapsBinding) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(String(localized: "trip.settings.navigationApp", defaultValue: "Use Google Maps for navigation"))
                    Text(
                        String(localized: "trip.settings.navigationSelected", defaultValue: "{app} is used for the navigation button.")
                            .replacingOccurrences(of: "{app}", with: draft.preferredNavigationApp.title)
                    )
                    .font(.caption)
                    .foregroundStyle(.secondary)
                }
            }
            .tint(woladenBrandColor)
        } header: {
            Text(String(localized: "trip.settings.navigation", defaultValue: "Navigation"))
        } footer: {
            Text(String(localized: "trip.settings.navigationHelp", defaultValue: "Choose the navigation app used by the single navigation button. Woladen uses MapKit for in-app traffic ETA."))
        }
    }

    private var drivingSection: some View {
        Section {
            Toggle(
                String(localized: "trip.settings.suggestMode", defaultValue: "Suggest Fahrt mode when movement is detected"),
                isOn: suggestModeBinding
            )
            .tint(woladenBrandColor)
            sliderRow(
                title: String(localized: "trip.settings.roadSpeed", defaultValue: "Road-speed threshold"),
                value: $draft.roadSpeedThresholdKPH,
                range: 10...40,
                step: 1,
                valueText: "\(Int(draft.roadSpeedThresholdKPH.rounded())) km/h"
            )
        } header: {
            Text(String(localized: "trip.settings.driving", defaultValue: "Driving"))
        } footer: {
            Text(String(localized: "trip.settings.suggestHelp", defaultValue: "Speed can trigger a suggestion, but Woladen never changes mode without confirmation."))
        }
    }

    private var carPlaySection: some View {
        Section(String(localized: "trip.settings.carplay", defaultValue: "CarPlay")) {
            Label {
                Text(String(localized: "trip.settings.carplayGate", defaultValue: "The CarPlay interface becomes distributable after Apple grants the EV-charging entitlement."))
                    .font(.footnote)
            } icon: {
                Image(systemName: "car.side")
                    .foregroundStyle(woladenBrandColor)
            }
        }
    }

    private func sliderRow(
        title: String,
        value: Binding<Double>,
        range: ClosedRange<Double>,
        step: Double,
        valueText: String
    ) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                Text(title)
                Spacer(minLength: 12)
                Text(valueText)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(woladenBrandColor)
                    .monospacedDigit()
            }
            Slider(value: value, in: range, step: step)
                .tint(woladenBrandColor)
        }
        .padding(.vertical, 4)
    }

    private var targetSOCBinding: Binding<Double> {
        Binding(
            get: { max(activeVehicleSettings.targetSOCPercent, activeVehicleSettings.reserveSOCPercent + 5) },
            set: { next in
                var settings = activeVehicleSettings
                settings.targetSOCPercent = max(next, settings.reserveSOCPercent + 5)
                draft.updateActiveVehicleSettings(settings)
            }
        )
    }

    private var vehicleProfiles: [VehicleProfile] {
        draft.normalized.vehicleProfiles ?? []
    }

    private var activeVehicleSettings: VehicleEnergySettings {
        draft.activeVehicleSettings
    }

    private var activeVehicleIDBinding: Binding<UUID> {
        Binding(
            get: { draft.normalized.selectedVehicleProfileID ?? vehicleProfiles[0].id },
            set: { next in
                draft.selectedVehicleProfileID = next
                draft.vehicle = draft.normalized.activeVehicleSettings
            }
        )
    }

    private var activeVehicleNameBinding: Binding<String> {
        Binding(
            get: { draft.activeVehicleProfile.name },
            set: { next in
                var profiles = vehicleProfiles
                guard let index = profiles.firstIndex(where: { $0.id == draft.normalized.selectedVehicleProfileID }) else { return }
                profiles[index].name = next
                draft.vehicleProfiles = profiles
            }
        )
    }

    private func activeVehicleValueBinding(_ keyPath: WritableKeyPath<VehicleEnergySettings, Double>) -> Binding<Double> {
        Binding(
            get: { activeVehicleSettings[keyPath: keyPath] },
            set: { next in
                var settings = activeVehicleSettings
                settings[keyPath: keyPath] = next
                draft.updateActiveVehicleSettings(settings)
            }
        )
    }

    private var googleMapsBinding: Binding<Bool> {
        Binding(
            get: { draft.preferredNavigationApp == .googleMaps },
            set: { draft.navigationApp = $0 ? .googleMaps : .appleMaps }
        )
    }

    private var suggestModeBinding: Binding<Bool> {
        Binding(
            get: { draft.shouldSuggestTripMode },
            set: { draft.suggestTripMode = $0 }
        )
    }

    private func addVehicleProfile() {
        var profiles = vehicleProfiles
        let profile = VehicleProfile(
            name: String(localized: "trip.vehicle.numberedName", defaultValue: "Vehicle {number}")
                .replacingOccurrences(of: "{number}", with: "\(profiles.count + 1)"),
            settings: activeVehicleSettings
        )
        profiles.append(profile)
        draft.vehicleProfiles = profiles
        draft.selectedVehicleProfileID = profile.id
        draft.vehicle = profile.settings
    }

    private func removeActiveVehicleProfile() {
        var profiles = vehicleProfiles
        guard profiles.count > 1,
              let activeID = draft.normalized.selectedVehicleProfileID else { return }
        profiles.removeAll { $0.id == activeID }
        draft.vehicleProfiles = profiles
        draft.selectedVehicleProfileID = profiles[0].id
        draft.vehicle = profiles[0].settings
    }

    private var providerOptions: [String] {
        let plans: [RoutePlan]
        if let plan = tripStore.plan(id: planID) {
            plans = [plan]
        } else if let active = tripStore.activePlan {
            plans = [active]
        } else {
            plans = tripStore.sortedPlans
        }
        return Array(Set(plans.flatMap { $0.providerCoverage.map(\.providerName) }))
            .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    private func providerCoverage(_ provider: String) -> ProviderCoverage? {
        let plan = tripStore.plan(id: planID) ?? tripStore.activePlan
        return plan?.providerCoverage.first { normalizedProviderKey($0.providerName) == normalizedProviderKey(provider) }
    }

    private func providerBinding(_ provider: String) -> Binding<Bool> {
        Binding(
            get: {
                draft.selectedProviderNames.contains {
                    normalizedProviderKey($0) == normalizedProviderKey(provider)
                }
            },
            set: { selected in
                draft.selectedProviderNames.removeAll {
                    normalizedProviderKey($0) == normalizedProviderKey(provider)
                }
                if selected {
                    draft.selectedProviderNames.append(provider)
                }
            }
        )
    }

    private func layoutIcon(_ style: TripVisualStyle) -> String {
        switch style {
        case .commandCenter: return "rectangle.tophalf.inset.filled"
        case .routeProgression: return "point.topleft.down.curvedto.point.bottomright.up"
        case .mapGlance: return "map"
        }
    }

    private func layoutDescription(_ style: TripVisualStyle) -> String {
        switch style {
        case .commandCenter:
            return String(localized: "trip.layout.commandHelp", defaultValue: "Largest next-stop actions")
        case .routeProgression:
            return String(localized: "trip.layout.progressionHelp", defaultValue: "All selected stops stay visible")
        case .mapGlance:
            return String(localized: "trip.layout.mapHelp", defaultValue: "Route context above the next stop")
        }
    }
}
