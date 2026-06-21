import SwiftUI

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)

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
                    ContentUnavailableView(
                        initialLocationTitle,
                        systemImage: "location.magnifyingglass",
                        description: Text(initialLocationDescription)
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
                                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
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
                .accessibilityLabel(Text("aria.filterOpen"))
            }
        .onAppear(perform: reloadForActiveLocation)
        .onChange(of: scenePhase) { _, newValue in
            guard newValue == .active else { return }
            reloadForActiveLocation()
        }
    }

    private func color(for key: String) -> Color {
        switch key {
        case "gold": return Color.yellow
        case "silver": return Color.gray
        case "bronze": return Color.brown
        default: return Color.secondary
        }
    }

    private var initialLocationTitle: String {
        switch locationService.authorizationStatus {
        case .denied, .restricted:
            return String(localized: "location.deniedTitle")
        default:
            return String(localized: "location.pendingTitle")
        }
    }

    private var initialLocationDescription: String {
        switch locationService.authorizationStatus {
        case .notDetermined:
            return String(localized: "location.idleMessage")
        case .denied, .restricted:
            return String(localized: "location.deniedMessage")
        case .authorizedWhenInUse, .authorizedAlways:
            return String(localized: "location.pendingMessage")
        @unknown default:
            return String(localized: "location.pendingMessage")
        }
    }

    private func reloadForActiveLocation() {
        viewModel.reloadListForCurrentLocation(locationService.currentLocation)
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
