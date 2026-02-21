import SwiftUI

struct ListTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @Binding var showingFilter: Bool

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Group {
                if let error = viewModel.loadError {
                    ContentUnavailableView("Fehler beim Laden", systemImage: "exclamationmark.triangle", description: Text(error))
                } else if viewModel.isLoading && viewModel.allFeatures.isEmpty {
                    ProgressView("Lade Ladepunkte...")
                } else if viewModel.discoveredFeatures.isEmpty {
                    ContentUnavailableView("Keine Ladepunkte", systemImage: "bolt.slash")
                } else {
                    List(viewModel.discoveredFeatures) { feature in
                        Button {
                            viewModel.selectedFeature = feature
                        } label: {
                            StationRowView(
                                feature: feature,
                                distanceText: viewModel.distanceText(from: locationService.currentLocation, to: feature.coordinate),
                                markerColor: color(for: viewModel.markerTint(for: feature))
                            )
                        }
                        .buttonStyle(.plain)
                    }
                    .listStyle(.plain)
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
}

private struct StationRowView: View {
    let feature: GeoJSONFeature
    let distanceText: String?
    let markerColor: Color

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(alignment: .firstTextBaseline) {
                HStack(spacing: 6) {
                    Circle()
                        .fill(markerColor)
                        .frame(width: 10, height: 10)
                    Text(feature.properties.operatorName)
                        .font(.subheadline.weight(.semibold))
                        .lineLimit(1)
                }
                Spacer()
                if let distanceText {
                    Text(distanceText)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Text("\(feature.properties.city) • \(Int(feature.properties.displayedMaxPowerKW.rounded())) kW • \(feature.properties.chargingPointsCount) Ladepunkte")
                .font(.caption)
                .foregroundStyle(.secondary)

            if !feature.properties.topAmenities().isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 6) {
                        ForEach(feature.properties.topAmenities(), id: \.key) { item in
                            Label("\(item.count)", systemImage: AmenityCatalog.symbol(for: item.key))
                                .font(.caption2)
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
    }
}
