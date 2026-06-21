import SwiftUI

struct FavoritesTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @EnvironmentObject private var locationService: LocationService

    var body: some View {
        let items = viewModel.favoritesFeatures(favoritesStore.favorites, userLocation: locationService.currentLocation)

        Group {
            if items.isEmpty {
                if favoritesStore.favorites.isEmpty {
                    ContentUnavailableView(
                        String(localized: "favorites.empty"),
                        systemImage: "star",
                        description: Text(String(localized: "favorites.emptyHelp"))
                    )
                } else {
                    ContentUnavailableView(String(localized: "favorites.loading"), systemImage: "star")
                }
            } else {
                List(items) { feature in
                    HStack(spacing: 10) {
                        Button {
                            viewModel.selectFeature(feature)
                        } label: {
                            VStack(alignment: .leading, spacing: 6) {
                                Text(feature.properties.operatorName)
                                    .font(.title3.weight(.semibold))
                                Text(feature.properties.city)
                                    .font(.body)
                                    .foregroundStyle(.secondary)
                                Text("\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW max • \(chargingPointLabel(feature.properties.chargingPointsCount))")
                                    .font(.subheadline)
                                    .foregroundStyle(.secondary)
                                if let occupancy = feature.occupancySummaryLabel ?? nil, !occupancy.isEmpty {
                                    Label(occupancy, systemImage: "dot.radiowaves.left.and.right")
                                        .font(.caption)
                                        .padding(.horizontal, 8)
                                        .padding(.vertical, 3)
                                        .background(favoriteOccupancyColor(for: feature).opacity(0.16))
                                        .foregroundStyle(favoriteOccupancyColor(for: feature))
                                        .clipShape(Capsule())
                                } else if !feature.displayPrice.isEmpty {
                                    Label(feature.displayPrice, systemImage: "eurosign")
                                        .font(.caption)
                                        .padding(.horizontal, 8)
                                        .padding(.vertical, 3)
                                        .background(Color.green.opacity(0.12))
                                        .foregroundStyle(Color.green)
                                        .clipShape(Capsule())
                                }
                            }
                            .frame(maxWidth: .infinity, alignment: .leading)
                        }
                        .buttonStyle(.plain)

                        Button(role: .destructive) {
                            favoritesStore.remove(feature.properties.stationID)
                        } label: {
                            Image(systemName: "trash")
                                .font(.headline)
                        }
                        .accessibilityLabel(Text("aria.removeFavorite"))
                    }
                    .padding(.vertical, 2)
                    .listRowBackground(feature.stationCardBackground)
                }
                .listStyle(.plain)
            }
        }
        .task(id: favoritesStore.favorites.sorted().joined(separator: "|")) {
            await viewModel.refreshFavoriteStaticDetails(favoritesStore.favorites)
            while !Task.isCancelled {
                await viewModel.refreshFavoritesLiveSummaries(favoritesStore.favorites, force: true)
                try? await Task.sleep(nanoseconds: 15_000_000_000)
            }
        }
    }

    private func favoriteOccupancyColor(for feature: GeoJSONFeature) -> Color {
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
