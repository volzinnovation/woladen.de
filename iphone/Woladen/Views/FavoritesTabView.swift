import SwiftUI

struct FavoritesTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @EnvironmentObject private var locationService: LocationService

    var body: some View {
        let items = viewModel.favoritesFeatures(favoritesStore.favorites, userLocation: locationService.currentLocation)

        Group {
            if items.isEmpty {
                ContentUnavailableView("Keine Favoriten", systemImage: "star")
            } else {
                List(items) { feature in
                    HStack(spacing: 10) {
                        Button {
                            viewModel.selectedFeature = feature
                        } label: {
                            VStack(alignment: .leading, spacing: 6) {
                                Text(feature.properties.operatorName)
                                    .font(.headline)
                                Text(feature.properties.city)
                                    .foregroundStyle(.secondary)
                                Text("\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW max • \(feature.properties.chargingPointsCount) Ladepunkte")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
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
                    }
                    .padding(.vertical, 2)
                }
                .listStyle(.plain)
            }
        }
    }
}
