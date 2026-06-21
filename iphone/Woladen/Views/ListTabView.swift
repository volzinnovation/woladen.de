import SwiftUI
import CoreLocation
import UIKit

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)

enum StationVisualStyle {
    static let amenityGold = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)
    static let amenitySilver = Color(red: 148.0 / 255.0, green: 163.0 / 255.0, blue: 184.0 / 255.0)
    static let amenityBronze = Color(red: 180.0 / 255.0, green: 83.0 / 255.0, blue: 9.0 / 255.0)
    static let amenityGrey = Color(red: 100.0 / 255.0, green: 116.0 / 255.0, blue: 139.0 / 255.0)

    static let cardOutOfOrder = Color(red: 1.0, green: 241.0 / 255.0, blue: 242.0 / 255.0)
    static let cardOccupied = Color(red: 241.0 / 255.0, green: 245.0 / 255.0, blue: 249.0 / 255.0)
    static let cardOneFreeLeft = Color(red: 1.0, green: 251.0 / 255.0, blue: 235.0 / 255.0)
    static let cardOftenBroken = Color(red: 1.0, green: 247.0 / 255.0, blue: 248.0 / 255.0)
    static let cardOftenOccupied = Color(red: 248.0 / 255.0, green: 250.0 / 255.0, blue: 252.0 / 255.0)
    static let cardDefault = Color(.secondarySystemBackground)

    static let borderOutOfOrder = Color(red: 220.0 / 255.0, green: 38.0 / 255.0, blue: 38.0 / 255.0).opacity(0.28)
    static let borderOccupied = Color(red: 100.0 / 255.0, green: 116.0 / 255.0, blue: 139.0 / 255.0).opacity(0.28)
    static let borderOneFreeLeft = Color(red: 217.0 / 255.0, green: 119.0 / 255.0, blue: 6.0 / 255.0).opacity(0.28)
    static let borderOftenBroken = Color(red: 244.0 / 255.0, green: 63.0 / 255.0, blue: 94.0 / 255.0).opacity(0.18)
    static let borderOftenOccupied = Color(red: 100.0 / 255.0, green: 116.0 / 255.0, blue: 139.0 / 255.0).opacity(0.16)
    static let borderDefault = Color(.separator).opacity(0.35)

    static let markerOutOfOrder = Color(red: 239.0 / 255.0, green: 68.0 / 255.0, blue: 68.0 / 255.0)
    static let markerFullyOccupied = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)
}

extension GeoJSONFeature {
    var hasAvailabilitySummary: Bool {
        availabilityCounts.total > 0
    }

    var isStationOutOfOrder: Bool {
        hasAvailabilitySummary && availabilityStatus == .outOfOrder
    }

    var isStationFullyOccupied: Bool {
        hasAvailabilitySummary && availabilityStatus == .occupied
    }

    var isStationOneFreeLeft: Bool {
        let counts = availabilityCounts
        return hasAvailabilitySummary && counts.total > 1 && counts.available == 1
    }

    var stationCardBackground: Color {
        if isStationOutOfOrder { return StationVisualStyle.cardOutOfOrder }
        if isStationFullyOccupied { return StationVisualStyle.cardOccupied }
        if isStationOneFreeLeft { return StationVisualStyle.cardOneFreeLeft }
        return StationVisualStyle.cardDefault
    }

    var stationCardBorder: Color {
        if isStationOutOfOrder { return StationVisualStyle.borderOutOfOrder }
        if isStationFullyOccupied { return StationVisualStyle.borderOccupied }
        if isStationOneFreeLeft { return StationVisualStyle.borderOneFreeLeft }
        return StationVisualStyle.borderDefault
    }
}

struct ListTabView: View {
    @Environment(\.scenePhase) private var scenePhase
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService
    @EnvironmentObject private var favoritesStore: FavoritesStore

    @Binding var showingFilter: Bool

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Group {
                if let error = viewModel.loadError {
                    ContentUnavailableView(String(localized: "errors.dataLoad"), systemImage: "exclamationmark.triangle", description: Text(error))
                } else if viewModel.isAwaitingFirstLocationFix {
                    LocationAccessInstructionView(
                        authorizationStatus: locationService.authorizationStatus,
                        lastError: locationService.lastError,
                        retry: requestLocationAccess
                    )
                } else if viewModel.isLoading && viewModel.allFeatures.isEmpty {
                    ProgressView(String(localized: "list.loading"))
                } else if viewModel.discoveredFeatures.isEmpty {
                    ContentUnavailableView(String(localized: "list.empty"), systemImage: "bolt.slash")
                } else {
                    ScrollView {
                        LazyVGrid(
                            columns: [GridItem(.adaptive(minimum: 320, maximum: 520), spacing: 10, alignment: .top)],
                            alignment: .leading,
                            spacing: 10
                        ) {
                            ForEach(viewModel.discoveredFeatures) { feature in
                                Button {
                                    viewModel.selectFeature(feature)
                                } label: {
                                    StationRowView(
                                        feature: feature,
                                        distanceText: viewModel.distanceText(from: locationService.currentLocation, to: feature.coordinate),
                                        markerColor: color(for: viewModel.markerTint(for: feature)),
                                        isFavorite: favoritesStore.isFavorite(feature.properties.stationID)
                                    )
                                    .padding(14)
                                    .background(feature.stationCardBackground, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                                    .overlay {
                                        RoundedRectangle(cornerRadius: 8, style: .continuous)
                                            .stroke(feature.stationCardBorder, lineWidth: 1)
                                    }
                                }
                                .buttonStyle(.plain)
                            }
                        }
                        .padding(.horizontal, 14)
                        .padding(.top, 58)
                        .padding(.bottom, 12)
                    }
                }
            }
                Button {
                    showingFilter = true
                } label: {
                    Image(systemName: "line.3.horizontal.decrease.circle")
                        .font(.title2)
                        .padding(8)
                        .background(Color(.secondarySystemBackground), in: Circle())
                }
                .padding(.trailing, 14)
                .padding(.top, 10)
                .accessibilityLabel(Text(String(localized: "aria.filterOpen")))
            }
        .onAppear(perform: reloadForActiveLocation)
        .onChange(of: scenePhase) { _, newValue in
            guard newValue == .active else { return }
            reloadForActiveLocation()
        }
    }

    private func color(for key: String) -> Color {
        switch key {
        case "gold": return StationVisualStyle.amenityGold
        case "silver": return StationVisualStyle.amenitySilver
        case "bronze": return StationVisualStyle.amenityBronze
        default: return StationVisualStyle.amenityGrey
        }
    }

    private func reloadForActiveLocation() {
        if viewModel.allFeatures.isEmpty, viewModel.loadError == nil {
            if locationService.currentLocation == nil {
                locationService.activate()
            }
            viewModel.loadIfNeeded(userLocation: locationService.currentLocation)
            return
        }
        viewModel.reloadListForCurrentLocation(locationService.currentLocation)
    }

    private func requestLocationAccess() {
        locationService.requestAuthorization()
        viewModel.loadIfNeeded(userLocation: locationService.currentLocation)
    }
}

struct LocationAccessInstructionView: View {
    let authorizationStatus: CLAuthorizationStatus
    let lastError: String?
    let retry: () -> Void

    var body: some View {
        VStack(spacing: 18) {
            Image(systemName: iconName)
                .font(.system(size: 42, weight: .semibold))
                .foregroundStyle(Color.accentColor)
                .accessibilityHidden(true)

            VStack(spacing: 8) {
                Text(title)
                    .font(.title3.weight(.semibold))
                    .multilineTextAlignment(.center)
                Text(message)
                    .font(.body)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
            }
            .frame(maxWidth: 420)

            Button {
                primaryAction()
            } label: {
                Label(primaryActionTitle, systemImage: primaryActionIcon)
                    .frame(minWidth: 180)
            }
            .buttonStyle(.borderedProminent)

            if let lastError, !lastError.isEmpty, isAuthorized {
                Text(lastError)
                    .font(.footnote)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .frame(maxWidth: 420)
            }
        }
        .padding(.horizontal, 28)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var title: String {
        switch authorizationStatus {
        case .denied, .restricted:
            return String(localized: "location.deniedTitle")
        case .authorizedWhenInUse, .authorizedAlways:
            return String(localized: "location.pendingTitle")
        case .notDetermined:
            return String(localized: "location.idleTitle")
        @unknown default:
            return String(localized: "location.unknownMessage")
        }
    }

    private var message: String {
        switch authorizationStatus {
        case .denied, .restricted:
            return String(localized: "location.settingsMessage")
        case .authorizedWhenInUse, .authorizedAlways:
            return String(localized: "location.pendingMessage")
        case .notDetermined:
            return String(localized: "location.idleMessage")
        @unknown default:
            return String(localized: "location.unavailableMessage")
        }
    }

    private var iconName: String {
        switch authorizationStatus {
        case .denied, .restricted:
            return "gearshape.fill"
        default:
            return "location.magnifyingglass"
        }
    }

    private var primaryActionTitle: String {
        switch authorizationStatus {
        case .denied, .restricted:
            return String(localized: "location.openSettings")
        case .notDetermined:
            return String(localized: "location.idleAction")
        default:
            return String(localized: "location.retry")
        }
    }

    private var primaryActionIcon: String {
        switch authorizationStatus {
        case .denied, .restricted:
            return "gearshape"
        default:
            return "location.fill"
        }
    }

    private var isAuthorized: Bool {
        authorizationStatus == .authorizedWhenInUse || authorizationStatus == .authorizedAlways
    }

    private func primaryAction() {
        switch authorizationStatus {
        case .denied, .restricted:
            openAppSettings()
        default:
            retry()
        }
    }

    private func openAppSettings() {
        guard let url = URL(string: UIApplication.openSettingsURLString) else { return }
        UIApplication.shared.open(url)
    }
}

private struct StationRowView: View {
    let feature: GeoJSONFeature
    let distanceText: String?
    let markerColor: Color
    let isFavorite: Bool

    var body: some View {
        let topAmenities = feature.properties.topAmenities()
        let occupancy = feature.occupancySummaryLabel
        let priceDisplay = feature.displayPrice

        VStack(alignment: .leading, spacing: 4) {
            HStack(alignment: .firstTextBaseline) {
                HStack(spacing: 6) {
                    if isFavorite {
                        Image(systemName: "star.fill")
                            .font(.system(size: 12, weight: .semibold))
                            .foregroundStyle(favoriteStarColor)
                            .frame(width: 12, height: 12)
                            .accessibilityHidden(true)
                    } else {
                        Circle()
                            .fill(markerColor)
                            .frame(width: 12, height: 12)
                    }
                    Text(feature.properties.operatorName)
                        .font(.headline.weight(.semibold))
                        .lineLimit(1)
                }
                Spacer()
                if let distanceText {
                    Text(distanceText)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                }
            }

            Text("\(feature.properties.city) • \(Int(feature.properties.displayedMaxPowerKW.rounded())) kW • \(chargingPointLabel(feature.properties.chargingPointsCount))")
                .font(.subheadline)
                .foregroundStyle(.secondary)

            if occupancy != nil || !priceDisplay.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 6) {
                        if let occupancy {
                            Label(occupancy, systemImage: "dot.radiowaves.left.and.right")
                                .font(.caption)
                                .lineLimit(1)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 3)
                                .background(occupancyBackgroundColor.opacity(0.16))
                                .foregroundStyle(occupancyBackgroundColor)
                                .clipShape(Capsule())
                        }

                        if !priceDisplay.isEmpty {
                            Label(priceDisplay, systemImage: "eurosign")
                                .font(.caption)
                                .lineLimit(1)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 3)
                                .background(Color.green.opacity(0.12))
                                .foregroundStyle(Color.green)
                                .clipShape(Capsule())
                        }
                    }
                }
            }

            if !topAmenities.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 6) {
                        ForEach(topAmenities, id: \.key) { item in
                            Label("\(item.count)", systemImage: AmenityCatalog.symbol(for: item.key))
                                .font(.caption)
                                .lineLimit(1)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 3)
                                .background(Color(.secondarySystemBackground))
                                .clipShape(Capsule())
                        }
                    }
                }
            }
        }
        .padding(.vertical, 4)
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var occupancyBackgroundColor: Color {
        switch feature.availabilityStatus {
        case .free:
            return Color.teal
        case .occupied:
            return Color.orange
        case .outOfOrder:
            return Color.red
        case .unknown:
            return Color.secondary
        }
    }

    private func chargingPointLabel(_ count: Int) -> String {
        let template = count == 1
            ? String(localized: "station.chargingPointOne")
            : String(localized: "station.chargingPointMany")
        return template
            .replacingOccurrences(of: "{count}", with: "\(count)")
    }
}
