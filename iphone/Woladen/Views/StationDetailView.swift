import SwiftUI
import MapKit

struct StationDetailView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @Environment(\.dismiss) private var dismiss
    @Environment(\.openURL) private var openURL

    let stationID: String

    @State private var cameraPosition: MapCameraPosition = .region(
        MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: 51.1657, longitude: 10.4515),
            span: MKCoordinateSpan(latitudeDelta: 7.5, longitudeDelta: 7.5)
        )
    )

    private var feature: GeoJSONFeature? {
        viewModel.feature(forStationID: stationID) ?? viewModel.selectedFeature
    }

    var body: some View {
        Group {
            if let feature {
                ScrollView {
                    VStack(alignment: .leading, spacing: 0) {
                        mapSection(feature)
                        VStack(alignment: .leading, spacing: 14) {
                            headerSection(feature)
                            amenitySection(feature)
                            liveSection(feature)
                            staticDetailsSection(feature)
                            sourceFooterSection(feature)
                        }
                        .padding(.horizontal)
                        .padding(.top, 12)
                        .padding(.bottom, 20)
                    }
                }
            } else {
                ContentUnavailableView("Ladepunkt nicht gefunden", systemImage: "bolt.slash")
            }
        }
        .background(Color(.systemBackground))
        .onAppear(perform: updateRegionToFit)
        .onChange(of: feature?.id) { _, _ in
            updateRegionToFit()
        }
    }

    private func amenityCountLabel(for feature: GeoJSONFeature) -> String {
        feature.properties.amenitiesTotal == 1 ? "Angebot vor Ort" : "Angebote vor Ort"
    }

    private func mapSection(_ feature: GeoJSONFeature) -> some View {
        Map(position: $cameraPosition) {
            ForEach(mapItems(for: feature)) { item in
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
                viewModel.clearSelectedFeature()
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

    private func headerSection(_ feature: GeoJSONFeature) -> some View {
        let occupancy = feature.occupancySummaryLabel

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

            if feature.hasPrimaryDetailHighlights {
                HStack(spacing: 8) {
                    if !feature.displayPrice.isEmpty {
                        detailChip(text: feature.displayPrice, systemImage: "eurosign")
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
                        systemImage: "dot.radiowaves.left.and.right",
                        tint: statusColor(for: feature.availabilityStatus)
                    )
                }
            }

            HStack(spacing: 6) {
                Button {
                    openNavigationLink(feature, google: true)
                } label: {
                    actionButtonLabel("Google", systemImage: "location.north.line.fill")
                }
                .buttonStyle(.borderedProminent)
                .frame(maxWidth: .infinity, minHeight: 50)

                Button {
                    openNavigationLink(feature, google: false)
                } label: {
                    actionButtonLabel("Apple", systemImage: "location.north.line.fill")
                }
                .buttonStyle(.bordered)
                .frame(maxWidth: .infinity, minHeight: 50)

                if !feature.properties.helpdeskPhone.isEmpty {
                    Button {
                        openHelpdeskPhone(feature)
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

    @ViewBuilder
    private func liveSection(_ feature: GeoJSONFeature) -> some View {
        let rows = feature.liveEVSERows
        if !rows.isEmpty {
            VStack(alignment: .leading, spacing: 10) {
                Text(liveSectionTitle(for: feature))
                    .font(.headline)

                ForEach(rows) { row in
                    VStack(alignment: .leading, spacing: 8) {
                        HStack(alignment: .top, spacing: 10) {
                            Text(row.title)
                                .font(.subheadline.weight(.semibold))
                            Spacer()
                            statusPill(status: row.status)
                        }

                        HStack(alignment: .top, spacing: 10) {
                            Text(row.meta)
                                .font(.caption)
                                .foregroundStyle(.secondary)
                            Spacer(minLength: 0)
                            if !row.price.isEmpty {
                                Text(row.price)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(Color.green)
                            }
                        }

                        if !row.notes.isEmpty {
                            VStack(alignment: .leading, spacing: 6) {
                                ForEach(row.notes) { note in
                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(note.label)
                                            .font(.caption.weight(.semibold))
                                            .foregroundStyle(.secondary)
                                        Text(note.value)
                                            .font(.caption)
                                    }
                                }
                            }
                        }
                    }
                    .padding(.vertical, 10)
                    .padding(.horizontal, 12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12))
                }
            }
        }
    }

    private func amenitySection(_ feature: GeoJSONFeature) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("\(feature.properties.amenitiesTotal) \(amenityCountLabel(for: feature))")
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

    private func liveSectionTitle(for feature: GeoJSONFeature) -> String {
        guard let provider = compactLiveProvider(from: feature.occupancySourceLabel) else {
            return "Live"
        }
        if provider == "lokale API" {
            return "Live von lokaler API"
        }
        return "Live von \(provider)"
    }

    private func compactLiveProvider(from sourceLabel: String?) -> String? {
        guard let sourceLabel else { return nil }
        let candidate = sourceLabel
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .components(separatedBy: " • ")
            .first?
            .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !candidate.isEmpty else { return nil }
        if candidate.hasPrefix("Live via ") {
            let provider = String(candidate.dropFirst("Live via ".count))
                .trimmingCharacters(in: .whitespacesAndNewlines)
            if provider == "lokaler API" {
                return "lokale API"
            }
            return provider.isEmpty ? nil : provider
        }
        if candidate.hasPrefix("Live-Stand") || candidate.hasPrefix("Stand ") {
            return nil
        }
        return candidate
    }

    @ViewBuilder
    private func staticDetailsSection(_ feature: GeoJSONFeature) -> some View {
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

    private func mapItems(for feature: GeoJSONFeature) -> [MapPoint] {
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
        guard let feature else { return }
        let coordinates = mapItems(for: feature).map(\.coordinate)
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

    private func openNavigationLink(_ feature: GeoJSONFeature, google: Bool) {
        let lat = feature.coordinate.latitude
        let lon = feature.coordinate.longitude
        let urlString = google
            ? "https://www.google.com/maps/dir/?api=1&destination=\(lat),\(lon)"
            : "http://maps.apple.com/?daddr=\(lat),\(lon)"
        guard let url = URL(string: urlString) else { return }
        openURL(url)
    }

    private func openHelpdeskPhone(_ feature: GeoJSONFeature) {
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
    private func sourceFooterSection(_ feature: GeoJSONFeature) -> some View {
        if feature.liveEVSERows.isEmpty, let occupancySource = feature.occupancySourceLabel, !occupancySource.isEmpty {
            Text(occupancySource)
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func detailStatCard(text: String, systemImage: String, tint: Color = .primary) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Label(text, systemImage: systemImage)
                .font(.footnote.weight(.semibold))
                .foregroundStyle(tint)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(.vertical, 10)
        .padding(.horizontal, 12)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12))
    }

    private func actionButtonLabel(_ title: String, systemImage: String) -> some View {
        Label(title, systemImage: systemImage)
            .frame(maxWidth: .infinity)
    }

    private func statusPill(status: AvailabilityStatus) -> some View {
        Text(status.label)
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 6)
            .background(statusColor(for: status).opacity(0.16), in: Capsule())
            .foregroundStyle(statusColor(for: status))
    }

    private func statusColor(for status: AvailabilityStatus) -> Color {
        switch status {
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

private struct MapPoint: Identifiable {
    let id: String
    let coordinate: CLLocationCoordinate2D
    let symbol: String
    let isStation: Bool
}
