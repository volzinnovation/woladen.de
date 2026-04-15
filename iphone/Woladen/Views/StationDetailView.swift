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
                    staticDetailsSection
                    sourceFooterSection
                }
                .padding(.horizontal)
                .padding(.top, 12)
                .padding(.bottom, 20)
            }
        }
        .background(Color(.systemBackground))
        .onAppear(perform: updateRegionToFit)
    }

    private var amenityCountLabel: String {
        feature.properties.amenitiesTotal == 1 ? "Angebot vor Ort" : "Angebote vor Ort"
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
        let occupancy = feature.properties.occupancySummaryLabel

        return VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .top, spacing: 12) {
                Text(feature.properties.operatorName)
                    .font(.title3.bold())
                    .lineLimit(2)
                    .layoutPriority(1)
                Spacer(minLength: 0)
                Button {
                    favoritesStore.toggle(feature.properties.stationID)
                } label: {
                    Image(systemName: favoritesStore.isFavorite(feature.properties.stationID) ? "star.fill" : "star")
                        .font(.title3)
                        .frame(width: 42, height: 42)
                        .background(Color(.secondarySystemBackground), in: Circle())
                }
            }

            if feature.properties.hasPrimaryDetailHighlights {
                HStack(spacing: 8) {
                    if !feature.properties.priceDisplay.isEmpty {
                        detailChip(text: feature.properties.priceDisplay, systemImage: "eurosign")
                    }
                    if !feature.properties.openingHoursDisplay.isEmpty {
                        detailChip(text: feature.properties.openingHoursDisplay, systemImage: "clock")
                    }
                }
            }

            Text("\(feature.properties.address), \(feature.properties.postcode) \(feature.properties.city)")
                .font(.subheadline)
                .foregroundStyle(.secondary)
                .lineLimit(2)
                .fixedSize(horizontal: false, vertical: true)

            HStack(alignment: .top, spacing: 10) {
                detailStatCard(
                    text: "\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW max / \(feature.properties.chargingPointsCount) Ladepunkte",
                    systemImage: "bolt.fill"
                )
                if let occupancy {
                    detailStatCard(
                        text: occupancy,
                        systemImage: "dot.radiowaves.left.and.right"
                    )
                }
            }

            HStack(spacing: 6) {
                Button {
                    openNavigationLink(google: true)
                } label: {
                    actionButtonLabel("Google", systemImage: "location.north.line.fill")
                }
                .buttonStyle(.borderedProminent)
                .frame(maxWidth: .infinity, minHeight: 50)
                Button {
                    openNavigationLink(google: false)
                } label: {
                    actionButtonLabel("Apple", systemImage: "location.north.line.fill")
                }
                .buttonStyle(.bordered)
                .frame(maxWidth: .infinity, minHeight: 50)
                if !feature.properties.helpdeskPhone.isEmpty {
                    Button {
                        openHelpdeskPhone()
                    } label: {
                        actionButtonLabel("Hilfe", systemImage: "phone.fill")
                    }
                    .buttonStyle(.bordered)
                    .frame(maxWidth: .infinity, minHeight: 50)
                }
            }
            .font(.subheadline.weight(.semibold))
        }
    }

    private var amenitySection: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("In der Nähe: \(feature.properties.amenitiesTotal) \(amenityCountLabel)")
                .font(.headline)

            if feature.properties.amenityExamples.isEmpty {
                Text("Keine Details verfügbar.")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(feature.properties.amenityExamples) { item in
                    amenityRow(for: item)
                }
            }
        }
    }

    @ViewBuilder
    private var staticDetailsSection: some View {
        let rows = feature.properties.staticDetailRows
        let source = feature.properties.detailSourceLabel
        if !rows.isEmpty || source != nil {
            VStack(alignment: .leading, spacing: 10) {
                Text("Details")
                    .font(.headline)

                ForEach(rows) { row in
                    HStack(alignment: .top, spacing: 10) {
                        Text(row.label)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .frame(width: 88, alignment: .leading)
                        Text(row.value)
                            .font(.subheadline)
                        Spacer()
                    }
                    .padding(.vertical, 8)
                    .padding(.horizontal, 12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12))
                }

                if let source {
                    Text(source)
                        .font(.caption)
                        .foregroundStyle(.secondary)
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

    private func openHelpdeskPhone() {
        let digits = feature.properties.helpdeskPhone.filter { "+0123456789".contains($0) }
        guard let url = URL(string: "tel:\(digits)") else { return }
        openURL(url)
    }

    private func detailChip(text: String, systemImage: String) -> some View {
        Label(text, systemImage: systemImage)
            .font(.footnote.weight(.semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background(Color.teal.opacity(0.12), in: Capsule())
            .foregroundStyle(Color.teal)
    }

    @ViewBuilder
    private var sourceFooterSection: some View {
        if let occupancySource = feature.properties.occupancySourceLabel, !occupancySource.isEmpty {
            Text(occupancySource)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func detailStatCard(text: String, systemImage: String) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Label(text, systemImage: systemImage)
                .font(.footnote.weight(.semibold))
                .foregroundStyle(Color.primary)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12))
    }

    private func actionButtonLabel(_ text: String, systemImage: String) -> some View {
        Label(text, systemImage: systemImage)
            .font(.subheadline.weight(.semibold))
            .lineLimit(1)
            .minimumScaleFactor(0.85)
    }
}

private struct MapPoint: Identifiable {
    let id: String
    let coordinate: CLLocationCoordinate2D
    let symbol: String
    let isStation: Bool
}
