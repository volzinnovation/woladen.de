import SwiftUI
import CoreLocation
import UIKit

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)

private func woladenUIColor(_ red: CGFloat, _ green: CGFloat, _ blue: CGFloat, _ alpha: CGFloat = 1.0) -> UIColor {
    UIColor(red: red / 255.0, green: green / 255.0, blue: blue / 255.0, alpha: alpha)
}

private func adaptiveColor(light: UIColor, dark: UIColor) -> Color {
    Color(uiColor: UIColor { traits in
        traits.userInterfaceStyle == .dark ? dark : light
    })
}

let woladenBrandColor = adaptiveColor(light: woladenUIColor(15, 118, 110), dark: woladenUIColor(94, 234, 212))

private let officialWoladenAppIconImage: UIImage? = {
    let bundle = Bundle.main
    let resourceNames = [
        "AppIcon60x60@3x",
        "AppIcon60x60@2x",
        "AppIcon60x60",
        "AppIcon76x76@2x",
        "AppIcon76x76"
    ]
    for name in resourceNames {
        if let url = bundle.url(forResource: name, withExtension: "png"),
           let image = UIImage(contentsOfFile: url.path) {
            return image
        }
    }
    return nil
}()

enum StationVisualStyle {
    static let amenityGold = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)
    static let amenitySilver = Color(red: 148.0 / 255.0, green: 163.0 / 255.0, blue: 184.0 / 255.0)
    static let amenityBronze = Color(red: 180.0 / 255.0, green: 83.0 / 255.0, blue: 9.0 / 255.0)
    static let amenityGrey = Color(red: 14.0 / 255.0, green: 165.0 / 255.0, blue: 233.0 / 255.0)

    static let cardOutOfOrder = adaptiveColor(light: woladenUIColor(255, 241, 242), dark: woladenUIColor(59, 18, 28))
    static let cardOccupied = adaptiveColor(light: woladenUIColor(226, 232, 240), dark: woladenUIColor(38, 50, 61))
    static let cardOneFreeLeft = adaptiveColor(light: woladenUIColor(255, 251, 235), dark: woladenUIColor(51, 43, 18))
    static let cardOftenBroken = adaptiveColor(light: woladenUIColor(255, 247, 248), dark: woladenUIColor(54, 22, 31))
    static let cardOftenOccupied = adaptiveColor(light: woladenUIColor(248, 250, 252), dark: woladenUIColor(15, 30, 39))
    static let cardUnknown = adaptiveColor(light: woladenUIColor(241, 245, 249), dark: woladenUIColor(22, 33, 43))
    static let cardDefault = adaptiveColor(light: .systemBackground, dark: .secondarySystemBackground)

    static let borderOutOfOrder = adaptiveColor(light: woladenUIColor(220, 38, 38, 0.28), dark: woladenUIColor(248, 113, 113, 0.46))
    static let borderOccupied = adaptiveColor(light: woladenUIColor(100, 116, 139, 0.38), dark: woladenUIColor(148, 163, 184, 0.46))
    static let borderOneFreeLeft = adaptiveColor(light: woladenUIColor(217, 119, 6, 0.28), dark: woladenUIColor(251, 191, 36, 0.42))
    static let borderOftenBroken = adaptiveColor(light: woladenUIColor(244, 63, 94, 0.18), dark: woladenUIColor(251, 113, 133, 0.36))
    static let borderOftenOccupied = adaptiveColor(light: woladenUIColor(100, 116, 139, 0.16), dark: woladenUIColor(148, 163, 184, 0.28))
    static let borderUnknown = adaptiveColor(light: woladenUIColor(100, 116, 139, 0.24), dark: woladenUIColor(148, 163, 184, 0.34))
    static let borderDefault = Color(.separator).opacity(0.35)

    static let controlSurface = adaptiveColor(light: woladenUIColor(241, 245, 249), dark: woladenUIColor(22, 33, 43))
    static let inputSurface = adaptiveColor(light: woladenUIColor(248, 250, 252), dark: woladenUIColor(15, 30, 39))
    static let controlBorder = adaptiveColor(light: woladenUIColor(226, 232, 240), dark: woladenUIColor(51, 65, 85))
    static let selectedControlSurface = adaptiveColor(light: woladenUIColor(15, 118, 110, 0.12), dark: woladenUIColor(94, 234, 212, 0.18))
    static let selectedControlBorder = adaptiveColor(light: woladenUIColor(15, 118, 110, 0.38), dark: woladenUIColor(94, 234, 212, 0.46))
    static let mutedForeground = adaptiveColor(light: woladenUIColor(100, 116, 139), dark: woladenUIColor(148, 163, 184))

    static let markerOutOfOrder = Color(red: 239.0 / 255.0, green: 68.0 / 255.0, blue: 68.0 / 255.0)
    static let markerFullyOccupied = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)
}

extension StationClassification {
    var railColor: Color {
        switch self {
        case .gold: return StationVisualStyle.amenityGold
        case .silver: return StationVisualStyle.amenitySilver
        case .bronze: return StationVisualStyle.amenityBronze
        case .unclassified: return StationVisualStyle.amenityGrey
        }
    }
}

struct StationClassificationRail: View {
    let classification: StationClassification
    var width: CGFloat = 26

    var body: some View {
        Rectangle()
            .fill(classification.railColor)
            .frame(width: width)
            .frame(maxHeight: .infinity)
            .accessibilityLabel(Text(classification.title))
    }
}

func stationLiveCardColors(
    status: AvailabilityStatus,
    available: Int,
    total: Int
) -> (background: Color, border: Color) {
    if status == .outOfOrder {
        return (StationVisualStyle.cardOutOfOrder, StationVisualStyle.borderOutOfOrder)
    }
    if status == .occupied {
        return (StationVisualStyle.cardOccupied, StationVisualStyle.borderOccupied)
    }
    if total > 1, available == 1 {
        return (StationVisualStyle.cardOneFreeLeft, StationVisualStyle.borderOneFreeLeft)
    }
    if status == .unknown {
        return (StationVisualStyle.cardDefault, StationVisualStyle.borderDefault)
    }
    return (StationVisualStyle.cardDefault, StationVisualStyle.borderDefault)
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

    var isStationAvailabilityUnknown: Bool {
        !hasAvailabilitySummary || availabilityStatus == .unknown
    }

    var stationCardBackground: Color {
        switch stationCardState {
        case .outOfOrder:
            return StationVisualStyle.cardOutOfOrder
        case .occupied:
            return StationVisualStyle.cardOccupied
        case .oneFreeLeft:
            return StationVisualStyle.cardOneFreeLeft
        case .oftenBroken:
            return StationVisualStyle.cardOftenBroken
        case .oftenOccupied:
            return StationVisualStyle.cardOftenOccupied
        case .unknown:
            return StationVisualStyle.cardDefault
        case .default:
            return StationVisualStyle.cardDefault
        }
    }

    var stationCardBorder: Color {
        switch stationCardState {
        case .outOfOrder:
            return StationVisualStyle.borderOutOfOrder
        case .occupied:
            return StationVisualStyle.borderOccupied
        case .oneFreeLeft:
            return StationVisualStyle.borderOneFreeLeft
        case .oftenBroken:
            return StationVisualStyle.borderOftenBroken
        case .oftenOccupied:
            return StationVisualStyle.borderOftenOccupied
        case .unknown:
            return StationVisualStyle.borderDefault
        case .default:
            return StationVisualStyle.borderDefault
        }
    }
}

struct ListTabView: View {
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.openURL) private var openURL
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @EnvironmentObject private var tripStore: TripStore

    @Binding var showingFilter: Bool

    var body: some View {
        VStack(spacing: 0) {
            listHeader

            if !activeFilterLabels.isEmpty {
                activeFilterSummary
            }

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
                                HStack(spacing: 0) {
                                    StationClassificationRail(classification: feature.stationClassification)

                                    Button {
                                        viewModel.selectFeature(feature)
                                    } label: {
                                        StationRowView(
                                            feature: feature,
                                            isFavorite: favoritesStore.isFavorite(feature.properties.stationID)
                                        )
                                        .padding(14)
                                    }
                                    .buttonStyle(.plain)
                                    .frame(maxWidth: .infinity, alignment: .leading)

                                    VStack(alignment: .trailing, spacing: 6) {
                                        if let distanceText = viewModel.distanceText(
                                            from: locationService.currentLocation,
                                            to: feature.coordinate
                                        ) {
                                            Text(distanceText)
                                                .font(.subheadline)
                                                .foregroundStyle(.secondary)
                                                .lineLimit(1)
                                        }

                                        if !feature.displayPrice.isEmpty {
                                            Text(feature.displayPrice)
                                                .font(.caption)
                                                .lineLimit(1)
                                                .padding(.horizontal, 8)
                                                .padding(.vertical, 3)
                                                .background(Color.green.opacity(0.12))
                                                .foregroundStyle(Color.green)
                                                .clipShape(Capsule())
                                        }

                                        Button {
                                            _ = tripStore.activateStationTarget(
                                                feature: feature,
                                                alternatives: viewModel.allFeatures.filter {
                                                    $0.properties.matches(viewModel.filterState)
                                                },
                                                from: locationService.currentLocation
                                            )
                                        } label: {
                                            Image(systemName: "play.fill")
                                                .font(.headline.weight(.bold))
                                                .foregroundStyle(.white)
                                                .frame(width: 44, height: 44)
                                                .background(woladenBrandColor, in: Circle())
                                        }
                                        .buttonStyle(.plain)
                                        .accessibilityLabel(
                                            Text(
                                                String(localized: "trip.station.driveTarget", defaultValue: "Use {station} as driving target")
                                                    .replacingOccurrences(of: "{station}", with: feature.properties.operatorName)
                                            )
                                        )
                                    }
                                    .frame(minWidth: 72, maxWidth: 94, alignment: .trailing)
                                    .padding(.vertical, 10)
                                    .padding(.trailing, 10)
                                }
                                .background(feature.stationCardBackground)
                                .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                                .overlay {
                                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                                        .stroke(feature.stationCardBorder, lineWidth: 1)
                                }
                                .contextMenu {
                                    stationContextMenu(for: feature)
                                }
                            }
                        }
                        .padding(.horizontal, 14)
                        .padding(.top, 8)
                        .padding(.bottom, 12)
                    }
                    .refreshable {
                        await refreshListFromPull()
                    }
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .onAppear(perform: reloadForActiveLocation)
        .onChange(of: scenePhase) { _, newValue in
            guard newValue == .active else { return }
            reloadForActiveLocation()
        }
    }

    private var listHeader: some View {
        HStack(alignment: .center, spacing: 12) {
            HStack(alignment: .center, spacing: 10) {
                OfficialWoladenAppIconView()

                Text("woladen")
                    .font(.title3.weight(.semibold))
                    .foregroundStyle(woladenBrandColor)
                    .lineLimit(1)
            }
            .frame(maxWidth: .infinity, alignment: .leading)

            Button {
                showingFilter = true
            } label: {
                HStack(spacing: 6) {
                    Image(systemName: "line.3.horizontal.decrease.circle")
                    Text(String(localized: "filters.title"))
                }
                .font(.subheadline.weight(.semibold))
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
                .background(StationVisualStyle.controlSurface, in: Capsule())
                .overlay(alignment: .topTrailing) {
                    filterCountBadge
                }
            }
            .buttonStyle(.plain)
            .accessibilityLabel(Text(String(localized: "aria.filterOpen")))
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 6)
        .background(Color(.systemBackground))
        .overlay(alignment: .bottom) {
            Divider()
        }
    }

    private var activeFilterSummary: some View {
        HStack(spacing: 8) {
            Image(systemName: "line.3.horizontal.decrease.circle")
                .accessibilityHidden(true)
            Text(activeFilterSummaryText)
                .font(.subheadline.weight(.semibold))
                .lineLimit(1)
                .truncationMode(.tail)
            Spacer()
            if hasClearableFilters {
                Button {
                    viewModel.filterState = viewModel.filterState.clearableState
                    viewModel.reloadCatalogForCurrentContext(userLocation: locationService.currentLocation)
                } label: {
                    Image(systemName: "xmark")
                        .font(.footnote.weight(.bold))
                        .frame(width: 34, height: 34)
                        .background(woladenBrandColor.opacity(0.12), in: Circle())
                        .overlay {
                            Circle()
                                .stroke(woladenBrandColor.opacity(0.18), lineWidth: 1)
                        }
                }
                .buttonStyle(.plain)
                .accessibilityLabel(Text(String(localized: "filters.reset")))
            }
        }
        .foregroundStyle(woladenBrandColor)
        .padding(.horizontal, 14)
        .padding(.vertical, 6)
        .background(woladenBrandColor.opacity(0.06))
        .overlay {
            RoundedRectangle(cornerRadius: 10, style: .continuous)
                .stroke(woladenBrandColor.opacity(0.14), lineWidth: 1)
                .padding(.horizontal, 14)
                .padding(.vertical, 4)
        }
        .overlay(alignment: .bottom) {
            Divider()
        }
    }

    private var activeFilterLabels: [String] {
        viewModel.filterState.activeDisplayLabels
    }

    private var activeFilterSummaryText: String {
        viewModel.filterState.activeDisplaySummary
    }

    private var hasClearableFilters: Bool {
        let filter = viewModel.filterState
        return !filter.selectedOperatorNames.isEmpty
            || !filter.amenityNameQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            || filter.availableOnly
            || filter.currentlyOpenOnly
            || !filter.selectedAmenities.isEmpty
            || filter.routeMaxDistanceFromLocationKM != nil
            || Int(filter.minPowerKW.rounded()) != 50
            || Int(filter.minAmenityCount.rounded()) != 0
    }

    @ViewBuilder
    private var filterCountBadge: some View {
        if viewModel.filterState.activeCount > 0 {
            Text("\(viewModel.filterState.activeCount)")
                .font(.caption2.weight(.bold))
                .foregroundStyle(.white)
                .monospacedDigit()
                .frame(minWidth: 18, minHeight: 18)
                .padding(.horizontal, 2)
                .background(woladenBrandColor, in: Capsule())
                .offset(x: 7, y: -7)
                .accessibilityHidden(true)
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

    @MainActor
    private func refreshListFromPull() async {
        locationService.activate()
        if let currentLocation = locationService.currentLocation {
            viewModel.reloadCatalogForCurrentContext(userLocation: currentLocation)
        } else {
            requestLocationAccess()
        }
        await Task.yield()
    }

    @ViewBuilder
    private func stationContextMenu(for feature: GeoJSONFeature) -> some View {
        Button {
            favoritesStore.toggle(feature.properties.stationID)
        } label: {
            let isFavorite = favoritesStore.isFavorite(feature.properties.stationID)
            let title = isFavorite
                ? String(localized: "aria.removeFavorite")
                : String(localized: "aria.saveFavorite")
            Label(
                title,
                systemImage: isFavorite ? "star.slash" : "star"
            )
        }

        Button {
            openNavigationLink(feature)
        } label: {
            Label(
                String(localized: "detail.startNavigation", defaultValue: "Start navigation"),
                systemImage: "location.north.line.fill"
            )
        }
    }

    private func openNavigationLink(_ feature: GeoJSONFeature) {
        let lat = feature.coordinate.latitude
        let lon = feature.coordinate.longitude
        let urlString = tripStore.preferences.preferredNavigationApp == .googleMaps
            ? "https://www.google.com/maps/dir/?api=1&destination=\(lat),\(lon)"
            : "http://maps.apple.com/?daddr=\(lat),\(lon)"
        guard let url = URL(string: urlString) else { return }
        openURL(url)
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
    let isFavorite: Bool

    var body: some View {
        let topAmenities = feature.properties.topAmenities()
        let occupancy = feature.occupancySummaryLabel

        VStack(alignment: .leading, spacing: 4) {
            HStack(alignment: .firstTextBaseline, spacing: 6) {
                if isFavorite {
                    Image(systemName: "star.fill")
                        .font(.system(size: 12, weight: .semibold))
                        .foregroundStyle(favoriteStarColor)
                        .frame(width: 12, height: 12)
                        .accessibilityHidden(true)
                }
                Text(feature.properties.operatorName)
                    .font(.headline.weight(.semibold))
                    .lineLimit(1)
                Spacer()
            }

            Text(feature.properties.city)
                .font(.subheadline)
                .foregroundStyle(.secondary)

            Text("\(feature.properties.chargingPointsCount) × \(Int(feature.properties.displayedMaxPowerKW.rounded())) kW")
                .font(.subheadline)
                .foregroundStyle(.secondary)

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

            if !topAmenities.isEmpty {
                HStack(spacing: 6) {
                    Text("\(feature.properties.amenitiesTotal)")
                        .font(.caption.weight(.semibold))
                    ForEach(topAmenities, id: \.key) { item in
                        Image(systemName: AmenityCatalog.symbol(for: item.key))
                            .font(.caption)
                            .accessibilityLabel("\(item.count)")
                    }
                }
                .foregroundStyle(.secondary)
                .padding(.horizontal, 8)
                .padding(.vertical, 3)
                .background(StationVisualStyle.controlSurface)
                .clipShape(Capsule())
                .accessibilityElement(children: .combine)
                .accessibilityLabel("\(feature.properties.amenitiesTotal) amenities")
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

struct WoladenBrandIntroView: View {
    let showProductMessage: Bool

    var body: some View {
        VStack(spacing: showProductMessage ? 6 : 0) {
            HStack(alignment: .center, spacing: 10) {
                OfficialWoladenAppIconView()

                Text("woladen")
                    .font(.title3.weight(.semibold))
                    .foregroundStyle(woladenBrandColor)
                    .lineLimit(1)
            }
            .frame(maxWidth: .infinity, alignment: .center)

            if showProductMessage {
                Text(String(localized: "seo.productMessage"))
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .lineLimit(3)
                    .frame(maxWidth: 760)
            }
        }
        .padding(.horizontal, 18)
        .padding(.vertical, showProductMessage ? 10 : 6)
        .frame(maxWidth: .infinity)
        .background(Color(.systemBackground))
        .overlay(alignment: .bottom) {
            Divider()
        }
    }
}

private struct OfficialWoladenAppIconView: View {
    var body: some View {
        Group {
            if let icon = officialWoladenAppIconImage {
                Image(uiImage: icon)
                    .resizable()
                    .renderingMode(.original)
                    .interpolation(.high)
                    .scaledToFill()
            } else {
                Color.clear
            }
        }
        .frame(width: 34, height: 34)
        .clipShape(RoundedRectangle(cornerRadius: 9, style: .continuous))
        .shadow(color: Color.black.opacity(0.10), radius: 5, x: 0, y: 2)
        .accessibilityHidden(true)
    }
}
