import SwiftUI

struct ListTabView: View {
    @Environment(\.scenePhase) private var scenePhase
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @Binding var showingFilter: Bool

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Group {
                if let error = viewModel.loadError {
                    ContentUnavailableView("Fehler beim Laden", systemImage: "exclamationmark.triangle", description: Text(error))
                } else if viewModel.isAwaitingFirstLocationFix {
                    ContentUnavailableView(
                        initialLocationTitle,
                        systemImage: "location.magnifyingglass",
                        description: Text(initialLocationDescription)
                    )
                } else if viewModel.isLoading && viewModel.allFeatures.isEmpty {
                    ProgressView("Lade Ladepunkte...")
                } else if viewModel.discoveredFeatures.isEmpty {
                    ContentUnavailableView("Keine Ladepunkte", systemImage: "bolt.slash")
                } else {
                    List(viewModel.discoveredFeatures) { feature in
                        Button {
                            viewModel.selectFeature(feature)
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
            return "Standortfreigabe benötigt"
        default:
            return "Warte auf ersten GPS-Fix"
        }
    }

    private var initialLocationDescription: String {
        switch locationService.authorizationStatus {
        case .notDetermined:
            return "Nahe Ladepunkte werden geladen, sobald dein Standort freigegeben ist."
        case .denied, .restricted:
            return "Aktiviere den Standortzugriff, damit die Liste nahe Ladepunkte laden kann."
        case .authorizedWhenInUse, .authorizedAlways:
            return "Die Liste lädt Ladepunkte, sobald der erste Standort bestimmt wurde."
        @unknown default:
            return "Die Liste lädt Ladepunkte, sobald der erste Standort bestimmt wurde."
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

    var body: some View {
        let topAmenities = feature.properties.topAmenities()
        let occupancy = feature.occupancySummaryLabel
        let priceDisplay = feature.displayPrice

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

            if occupancy != nil || !priceDisplay.isEmpty {
                ScrollView(.horizontal, showsIndicators: false) {
                    HStack(spacing: 6) {
                        if let occupancy {
                            Label(occupancy, systemImage: "dot.radiowaves.left.and.right")
                                .font(.caption2)
                                .lineLimit(1)
                                .padding(.horizontal, 8)
                                .padding(.vertical, 3)
                                .background(occupancyBackgroundColor.opacity(0.16))
                                .foregroundStyle(occupancyBackgroundColor)
                                .clipShape(Capsule())
                        }

                        if !priceDisplay.isEmpty {
                            Label(priceDisplay, systemImage: "eurosign")
                                .font(.caption2)
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
}
