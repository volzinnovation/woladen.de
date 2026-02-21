import SwiftUI
import MapKit

struct StationDetailView: View {
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @Environment(\.dismiss) private var dismiss
    @Environment(\.openURL) private var openURL

    let feature: GeoJSONFeature

    @State private var cameraPosition: MapCameraPosition

    init(feature: GeoJSONFeature) {
        self.feature = feature
        _cameraPosition = State(initialValue: .region(
            MKCoordinateRegion(
                center: feature.coordinate,
                span: MKCoordinateSpan(latitudeDelta: 0.01, longitudeDelta: 0.01)
            )
        ))
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 0) {
                mapSection
                VStack(alignment: .leading, spacing: 14) {
                    headerSection
                    amenitySection
                }
                .padding(.horizontal)
                .padding(.top, 12)
                .padding(.bottom, 20)
            }
        }
        .background(Color(.systemBackground))
        .onAppear(perform: updateRegionToFit)
    }

    private var mapSection: some View {
        Map(position: $cameraPosition) {
            ForEach(mapItems) { item in
                Annotation("", coordinate: item.coordinate) {
                    if item.isStation {
                        Circle()
                            .fill(Color.teal)
                            .frame(width: 16, height: 16)
                            .overlay(Circle().stroke(Color.white, lineWidth: 1.5))
                    } else {
                        Image(systemName: item.symbol)
                            .font(.caption2)
                            .padding(6)
                            .background(Color.clear, in: Circle())
                            .shadow(radius: 1)
                    }
                }
            }
        }
        .frame(height: 260)
        .overlay(alignment: .topLeading) {
            Button {
                dismiss()
            } label: {
                Label("", systemImage: "chevron.backward")
                    .font(.subheadline.weight(.semibold))
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                    .background(Color(.systemBackground).opacity(0.9), in: Capsule())
            }
            .padding(.leading, 12)
            .padding(.top, 12)
        }
    }

    private var headerSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Text(feature.properties.operatorName)
                    .font(.title3.bold())
                Spacer()
                Button {
                    favoritesStore.toggle(feature.properties.stationID)
                } label: {
                    Image(systemName: favoritesStore.isFavorite(feature.properties.stationID) ? "star.fill" : "star")
                        .font(.title3)
                }
            }

            Text("\(feature.properties.address), \(feature.properties.postcode) \(feature.properties.city)")
                .font(.subheadline)
                .foregroundStyle(.secondary)

            HStack {
                Label("\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW max / \(feature.properties.chargingPointsCount) Ladepunkte", systemImage: "bolt.fill")
                Spacer()
                Label("\(feature.properties.amenitiesTotal) Amenities", systemImage: "storefront")
            }
            .font(.footnote)
            .foregroundStyle(.secondary)

            HStack(spacing: 10) {
                Button("Google Navi") { openNavigationLink(google: true) }
                    .buttonStyle(.borderedProminent)
                Button("Apple Navi") { openNavigationLink(google: false) }
                    .buttonStyle(.bordered)
            }
        }
    }

    private var amenitySection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("In der Nahe")
                .font(.headline)

            if feature.properties.amenityExamples.isEmpty {
                Text("Keine Details verfugbar.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(feature.properties.amenityExamples) { item in
                    amenityRow(for: item)
                }
            }
        }
    }

    private func amenityRow(for item: AmenityExample) -> some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: AmenityCatalog.symbol(for: "amenity_\(item.category)"))
                .frame(width: 24)
                .foregroundStyle(Color.accentColor)

            VStack(alignment: .leading, spacing: 4) {
                Text(item.name ?? AmenityCatalog.label(for: "amenity_\(item.category)"))
                    .font(.subheadline)
                Text(meta(for: item))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
    }

    private var mapItems: [MapPoint] {
        var items: [MapPoint] = [
            .init(id: "station", coordinate: feature.coordinate, symbol: "bolt.fill", isStation: true)
        ]
        for (idx, example) in feature.properties.amenityExamples.enumerated() {
            guard let coordinate = example.coordinate else { continue }
            items.append(
                .init(
                    id: "amenity-\(idx)",
                    coordinate: coordinate,
                    symbol: AmenityCatalog.symbol(for: "amenity_\(example.category)"),
                    isStation: false
                )
            )
        }
        return items
    }

    private func updateRegionToFit() {
        let coordinates = mapItems.map(\.coordinate)
        guard let first = coordinates.first else { return }
        var minLat = first.latitude
        var maxLat = first.latitude
        var minLon = first.longitude
        var maxLon = first.longitude

        for coordinate in coordinates.dropFirst() {
            minLat = min(minLat, coordinate.latitude)
            maxLat = max(maxLat, coordinate.latitude)
            minLon = min(minLon, coordinate.longitude)
            maxLon = max(maxLon, coordinate.longitude)
        }

        let latDelta = max(0.01, (maxLat - minLat) * 1.6)
        let lonDelta = max(0.01, (maxLon - minLon) * 1.6)
        let region = MKCoordinateRegion(
            center: CLLocationCoordinate2D(
                latitude: (minLat + maxLat) / 2,
                longitude: (minLon + maxLon) / 2
            ),
            span: MKCoordinateSpan(latitudeDelta: latDelta, longitudeDelta: lonDelta)
        )
        cameraPosition = .region(region)
    }

    private func meta(for example: AmenityExample) -> String {
        var parts: [String] = []
        if let distance = example.distanceM {
            parts.append("~\(Int(distance.rounded())) m")
        }
        if let opening = example.openingHours, !opening.isEmpty {
            parts.append(opening)
        }
        return parts.isEmpty ? "" : parts.joined(separator: " • ")
    }

    private func openNavigationLink(google: Bool) {
        let lat = feature.coordinate.latitude
        let lon = feature.coordinate.longitude
        let urlString = google
            ? "https://www.google.com/maps/dir/?api=1&destination=\(lat),\(lon)"
            : "http://maps.apple.com/?daddr=\(lat),\(lon)"
        guard let url = URL(string: urlString) else { return }
        openURL(url)
    }
}

private struct MapPoint: Identifiable {
    let id: String
    let coordinate: CLLocationCoordinate2D
    let symbol: String
    let isStation: Bool
}
