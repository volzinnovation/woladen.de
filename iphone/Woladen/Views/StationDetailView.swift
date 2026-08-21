import SwiftUI
import MapKit

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)

struct StationDetailView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @EnvironmentObject private var tripStore: TripStore
    @EnvironmentObject private var locationService: LocationService
    @Environment(\.dismiss) private var dismiss
    @Environment(\.openURL) private var openURL

    let stationID: String
    let prefersWideLayout: Bool

    init(stationID: String, prefersWideLayout: Bool = false) {
        self.stationID = stationID
        self.prefersWideLayout = prefersWideLayout
    }

    @State private var cameraPosition: MapCameraPosition = .region(
        MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: 51.1657, longitude: 10.4515),
            span: MKCoordinateSpan(latitudeDelta: 7.5, longitudeDelta: 7.5)
        )
    )
    @State private var favoriteCategoryInput = ""

    private var feature: GeoJSONFeature? {
        viewModel.feature(forStationID: stationID) ?? viewModel.selectedFeature
    }

    var body: some View {
        Group {
            if let feature {
                if prefersWideLayout {
                    wideDetail(feature)
                } else {
                    compactDetail(feature)
                }
            } else {
                ContentUnavailableView(String(localized: "errors.catalogTitle"), systemImage: "bolt.slash")
            }
        }
        .background(Color(.systemBackground))
        .onAppear(perform: updateRegionToFit)
        .onChange(of: feature?.id) { _, _ in
            updateRegionToFit()
        }
        .onChange(of: feature?.properties.amenityExamples.count) { _, _ in
            updateRegionToFit()
        }
    }

    private func amenityCountLabel(for feature: GeoJSONFeature) -> String {
        let count = feature.properties.amenitiesTotal
        let template = count == 1
            ? String(localized: "amenity.one")
            : String(localized: "amenity.many")
        return template.replacingOccurrences(of: "{count}", with: "\(count)")
    }

    private func compactDetail(_ feature: GeoJSONFeature) -> some View {
        HStack(spacing: 0) {
            StationClassificationRail(
                classification: feature.stationClassification,
                width: 26
            )

            ScrollView {
                VStack(alignment: .leading, spacing: 0) {
                    mapSection(feature, showsBackButton: true)
                    detailContent(feature)
                        .padding(.horizontal)
                        .padding(.top, 12)
                        .padding(.bottom, 20)
                }
            }
            .background(feature.stationCardBackground)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func wideDetail(_ feature: GeoJSONFeature) -> some View {
        HStack(spacing: 0) {
            StationClassificationRail(
                classification: feature.stationClassification,
                width: 26
            )

            mapContent(feature)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(StationVisualStyle.controlSurface)

            Divider()

            ScrollView {
                detailContent(feature)
                    .padding(24)
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .background(feature.stationCardBackground)
        }
        .overlay(alignment: .topTrailing) {
            Button {
                viewModel.clearSelectedFeature()
                dismiss()
            } label: {
                Image(systemName: "xmark")
                    .font(.headline.weight(.semibold))
                    .foregroundStyle(Color(red: 30.0 / 255.0, green: 41.0 / 255.0, blue: 59.0 / 255.0))
                    .frame(width: 42, height: 42)
                    .background(Color(.systemBackground).opacity(0.94), in: Circle())
                    .shadow(color: Color.black.opacity(0.12), radius: 10, x: 0, y: 4)
            }
            .buttonStyle(.plain)
            .padding(14)
            .accessibilityLabel(Text(String(localized: "aria.closeDetail")))
        }
    }

    private func detailContent(_ feature: GeoJSONFeature) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            headerSection(feature)
            favoriteCategorySection(feature)
            amenitySection(feature)
            liveSection(feature)
            staticDetailsSection(feature)
            sourceFooterSection(feature)
        }
    }

    @ViewBuilder
    private func favoriteCategorySection(_ feature: GeoJSONFeature) -> some View {
        let stationID = feature.properties.stationID
        if favoritesStore.isFavorite(stationID) {
            let categories = favoritesStore.categories(for: stationID)
            VStack(alignment: .leading, spacing: 10) {
                HStack {
                    Text(String(localized: "detail.favoriteCategories"))
                        .font(.headline)
                    Spacer()
                    Text(String(localized: "detail.categoryDeviceOnly"))
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }

                if categories.isEmpty {
                    Text(String(localized: "favorites.uncategorized"))
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .padding(.horizontal, 10)
                        .padding(.vertical, 6)
                        .background(Color(.secondarySystemBackground), in: Capsule())
                } else {
                    FlowLayout(spacing: 7) {
                        ForEach(categories, id: \.self) { category in
                            HStack(spacing: 6) {
                                Text(category)
                                Button {
                                    favoritesStore.removeCategory(category, from: stationID)
                                } label: {
                                    Image(systemName: "xmark")
                                        .font(.caption2.weight(.bold))
                                }
                                .buttonStyle(.plain)
                                .accessibilityLabel(
                                    Text(
                                        String(localized: "detail.removeCategory")
                                            .replacingOccurrences(of: "{category}", with: category)
                                    )
                                )
                            }
                            .font(.caption.weight(.semibold))
                            .padding(.horizontal, 10)
                            .padding(.vertical, 6)
                            .background(woladenBrandColor.opacity(0.12), in: Capsule())
                            .foregroundStyle(woladenBrandColor)
                        }
                    }
                }

                HStack(spacing: 8) {
                    TextField(String(localized: "detail.categoryPlaceholder"), text: $favoriteCategoryInput)
                        .submitLabel(.done)
                        .onSubmit {
                            addFavoriteCategory(to: stationID)
                        }
                        .padding(.horizontal, 12)
                        .frame(height: 40)
                        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 8))

                    Button {
                        addFavoriteCategory(to: stationID)
                    } label: {
                        Image(systemName: "plus")
                            .font(.headline.weight(.semibold))
                            .frame(width: 40, height: 40)
                    }
                    .buttonStyle(.bordered)
                    .accessibilityLabel(Text(String(localized: "detail.addCategory")))
                }

                let suggestions = favoritesStore.categorySuggestions(query: favoriteCategoryInput, excluding: categories)
                if !suggestions.isEmpty {
                    FlowLayout(spacing: 7) {
                        ForEach(suggestions, id: \.self) { suggestion in
                            Button(suggestion) {
                                favoriteCategoryInput = suggestion
                                addFavoriteCategory(to: stationID)
                            }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                        }
                    }
                }
            }
        }
    }

    private func addFavoriteCategory(to stationID: String) {
        let category = normalizeCategoryLabel(favoriteCategoryInput)
        guard !category.isEmpty else { return }
        favoritesStore.addCategory(category, to: stationID)
        favoriteCategoryInput = ""
    }

    private func mapSection(_ feature: GeoJSONFeature) -> some View {
        mapSection(feature, showsBackButton: true)
    }

    private func mapContent(_ feature: GeoJSONFeature) -> some View {
        Map(position: $cameraPosition) {
            ForEach(mapItems(for: feature)) { item in
                Annotation("", coordinate: item.coordinate) {
                    if item.isStation {
                        stationMapMarker(for: feature)
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
    }

    private func mapSection(_ feature: GeoJSONFeature, showsBackButton: Bool) -> some View {
        mapContent(feature)
        .frame(height: 260)
        .overlay(alignment: .topLeading) {
            if showsBackButton {
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
                .accessibilityLabel(Text(String(localized: "aria.closeDetail")))
            }
        }
    }

    private func headerSection(_ feature: GeoJSONFeature) -> some View {
        return VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .top, spacing: 12) {
                Button {
                    favoritesStore.toggle(feature.properties.stationID)
                } label: {
                    let isFavorite = favoritesStore.isFavorite(feature.properties.stationID)
                    Image(systemName: isFavorite ? "star.fill" : "star")
                        .font(.title3)
                        .foregroundStyle(isFavorite ? favoriteStarColor : Color.primary)
                        .frame(width: 42, height: 42)
                        .background(Color(.secondarySystemBackground), in: Circle())
                }
                .accessibilityLabel(
                    favoritesStore.isFavorite(feature.properties.stationID)
                    ? Text("aria.removeFavorite")
                    : Text("aria.saveFavorite")
                )
                Text(feature.properties.operatorName)
                    .font(.title2.bold())
                    .lineLimit(2)
                    .layoutPriority(1)
                Spacer(minLength: 0)
            }

            if !feature.properties.openingHoursDisplay.isEmpty {
                HStack(spacing: 8) {
                    detailChip(text: feature.properties.openingHoursDisplay, systemImage: "clock")
                }
            }

            Text("\(feature.properties.address), \(feature.properties.postcode) \(feature.properties.city)")
                .font(.body)
                .foregroundStyle(.secondary)
                .lineLimit(2)
                .fixedSize(horizontal: false, vertical: true)

            Button {
                let alternatives = viewModel.allFeatures.filter {
                    $0.properties.matches(viewModel.filterState)
                }
                _ = tripStore.activateStationTarget(
                    feature: feature,
                    alternatives: alternatives,
                    from: locationService.currentLocation
                )
                viewModel.clearSelectedFeature()
                dismiss()
            } label: {
                Label(
                    String(localized: "trip.station.startTarget", defaultValue: "Use station as Fahrt target"),
                    systemImage: "play.fill"
                )
                .font(.headline)
                .frame(maxWidth: .infinity, minHeight: 54)
            }
            .buttonStyle(.borderedProminent)
            .tint(woladenBrandColor)

            HStack(spacing: 6) {
                Button {
                    openNavigationLink(feature)
                } label: {
                    actionButtonLabel(
                        String(localized: "detail.startNavigation", defaultValue: "Start navigation"),
                        systemImage: "location.north.line.fill"
                    )
                }
                .buttonStyle(.borderedProminent)
                .frame(maxWidth: .infinity, minHeight: 50)

                if !feature.properties.helpdeskPhone.isEmpty {
                    Button {
                        openHelpdeskPhone(feature)
                    } label: {
                        actionButtonLabel(String(localized: "detail.help"), systemImage: "phone.fill")
                    }
                    .buttonStyle(.bordered)
                    .frame(maxWidth: .infinity, minHeight: 50)
                }
            }
            .font(.subheadline.weight(.semibold))

            HStack(alignment: .top, spacing: 10) {
                detailStatCard(
                    lines: chargingPointPowerLines(for: feature),
                    systemImage: "bolt.fill"
                )
                availabilityStatCard(for: feature)
                if !feature.displayPrice.isEmpty {
                    detailStatCard(
                        lines: priceLines(for: feature.displayPrice),
                        systemImage: "eurosign",
                        tint: Color.green
                    )
                }
            }

            if let reliability = feature.properties.reliabilityPercent {
                Label(
                    String(localized: "trip.station.reliability", defaultValue: "Reliability {value}%")
                        .replacingOccurrences(of: "{value}", with: "\(Int(reliability.rounded()))"),
                    systemImage: "checkmark.shield.fill"
                )
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(woladenBrandColor)
            }

            if let lastUnavailable = feature.properties.lastUnavailableAt,
               !lastUnavailable.isEmpty {
                Label(
                    String(localized: "trip.station.lastUnavailable", defaultValue: "Last unavailable: {date}")
                        .replacingOccurrences(of: "{date}", with: lastUnavailable),
                    systemImage: "clock.badge.exclamationmark"
                )
                .font(.footnote)
                .foregroundStyle(.secondary)
            }
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
                                .font(.body.weight(.semibold))
                            Spacer()
                            statusPill(status: row.status)
                        }

                        HStack(alignment: .top, spacing: 10) {
                            Text(row.meta)
                                .font(.footnote)
                                .foregroundStyle(.secondary)
                            Spacer(minLength: 0)
                            if !row.price.isEmpty {
                                Text(row.price)
                                    .font(.footnote.weight(.semibold))
                                    .foregroundStyle(Color.green)
                            }
                        }

                        if !row.notes.isEmpty {
                            VStack(alignment: .leading, spacing: 6) {
                                ForEach(row.notes) { note in
                                    VStack(alignment: .leading, spacing: 2) {
                                        Text(note.label)
                                            .font(.footnote.weight(.semibold))
                                            .foregroundStyle(.secondary)
                                        Text(note.value)
                                            .font(.footnote)
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
            Text(amenityCountLabel(for: feature))
                .font(.headline)

            if feature.properties.amenityExamples.isEmpty {
                Text(String(localized: "amenity.noDetails"))
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
            return String(localized: "station.live")
        }
        return String(localized: "station.liveVia")
            .replacingOccurrences(of: "{source}", with: provider)
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
        if candidate.hasPrefix("Live-Stand")
            || candidate.hasPrefix("Stand ")
            || candidate.hasPrefix("Live seit ")
            || candidate.hasPrefix("Seit ")
        {
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
                Text(String(localized: "station.details"))
                    .font(.headline)

                ForEach(rows) { row in
                    HStack(alignment: .top, spacing: 10) {
                        Text(row.label)
                            .font(.footnote.weight(.semibold))
                            .foregroundStyle(.secondary)
                            .frame(width: 88, alignment: .leading)
                        Text(row.value)
                            .font(.body)
                        Spacer()
                    }
                    .padding(.vertical, 8)
                    .padding(.horizontal, 12)
                    .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12))
                }

                if let source {
                    Text(source)
                        .font(.footnote)
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
                    .font(.body)
                Text(meta(for: item))
                    .font(.footnote)
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

    private func stationMapMarker(for feature: GeoJSONFeature) -> some View {
        let isFavorite = favoritesStore.isFavorite(feature.properties.stationID)
        return Group {
            if isFavorite {
                Image(systemName: "star.fill")
                    .font(.system(size: 16, weight: .bold))
                    .foregroundStyle(favoriteStarColor)
                    .frame(width: 24, height: 24)
                    .background(Color(.systemBackground).opacity(0.92), in: Circle())
            } else {
                Circle()
                    .fill(Color.teal)
                    .frame(width: 16, height: 16)
                    .overlay(Circle().stroke(Color.white, lineWidth: 1.5))
            }
        }
        .accessibilityLabel(isFavorite ? Text("info.legendFavorite") : Text("station.chargingStation"))
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

    private func openNavigationLink(_ feature: GeoJSONFeature) {
        let lat = feature.coordinate.latitude
        let lon = feature.coordinate.longitude
        let urlString = tripStore.preferences.preferredNavigationApp == .googleMaps
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

    private func chargingPointPowerLines(for feature: GeoJSONFeature) -> [(String, Color)] {
        [
            ("\(feature.properties.chargingPointsCount) x", .primary),
            ("\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW", .primary)
        ]
    }

    private func priceLines(for price: String) -> [(String, Color)] {
        let parts = price.split(separator: " ", omittingEmptySubsequences: true)
        guard parts.count > 1 else {
            return [(price, .green), ("€/kWh", .green)]
        }
        let amount = parts.dropLast().joined(separator: " ")
        let unit = String(parts.last!)
        return [
            (amount, .green),
            (unit, .green)
        ]
    }

    private func detailChip(text: String, systemImage: String) -> some View {
        Label(text, systemImage: systemImage)
            .font(.subheadline.weight(.semibold))
            .padding(.horizontal, 10)
            .padding(.vertical, 8)
            .background(Color.teal.opacity(0.12), in: Capsule())
            .foregroundStyle(Color.teal)
    }

    @ViewBuilder
    private func sourceFooterSection(_ feature: GeoJSONFeature) -> some View {
        if feature.liveEVSERows.isEmpty, let occupancySource = feature.occupancySourceLabel, !occupancySource.isEmpty {
            Text(occupancySource)
                .font(.footnote)
                .foregroundStyle(.secondary)
        }
    }

    private func detailStatCard(
        lines: [(String, Color)],
        systemImage: String,
        tint: Color = .primary
    ) -> some View {
        VStack(alignment: .center, spacing: 4) {
            Image(systemName: systemImage)
                .font(.title3.weight(.semibold))
                .foregroundStyle(tint)

            ForEach(Array(lines.enumerated()), id: \.offset) { _, line in
                Text(line.0)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(line.1)
                    .lineLimit(1)
                    .minimumScaleFactor(0.75)
            }
        }
        .multilineTextAlignment(.center)
        .frame(maxWidth: .infinity, minHeight: 88, alignment: .center)
        .padding(.vertical, 6)
        .padding(.horizontal, 12)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 12))
    }

    private func availabilityStatCard(for feature: GeoJSONFeature) -> some View {
        let counts = feature.availabilityCounts
        let lines: [(String, Color)]

        if counts.total > 0 {
            lines = [
                ("\(counts.available)", .teal),
                (String(localized: "availability.free"), .teal)
            ]
        } else {
            lines = [
                ("\(counts.unknown)", .secondary),
                (String(localized: "availability.unknown"), .secondary)
            ]
        }

        return detailStatCard(
            lines: lines,
            systemImage: "dot.radiowaves.left.and.right",
            tint: .secondary
        )
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(Text(availabilityAccessibilityLabel(for: counts)))
    }

    private func availabilityAccessibilityLabel(for counts: AvailabilityCounts) -> String {
        if counts.total > 0 {
            return [
                availabilityLineText("availability.available", count: counts.available),
                availabilityLineText("availability.occupiedCount", count: counts.occupied),
                availabilityLineText("availability.outOfOrderCount", count: counts.outOfOrder)
            ].joined(separator: ", ")
        }
        return availabilityLineText("availability.unknownCount", count: counts.unknown)
    }

    private func availabilityLineText(_ key: String, count: Int) -> String {
        NSLocalizedString(key, comment: "")
            .replacingOccurrences(of: "{count}", with: "\(count)")
    }

    private func actionButtonLabel(_ title: String, systemImage: String) -> some View {
        Label(title, systemImage: systemImage)
            .frame(maxWidth: .infinity)
    }

    private func statusPill(status: AvailabilityStatus) -> some View {
        Text(status.label)
            .font(.footnote.weight(.semibold))
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

private struct FlowLayout: Layout {
    var spacing: CGFloat

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        let maxWidth = proposal.width ?? 320
        var width: CGFloat = 0
        var height: CGFloat = 0
        var rowWidth: CGFloat = 0
        var rowHeight: CGFloat = 0

        for subview in subviews {
            let size = subview.sizeThatFits(.unspecified)
            if rowWidth > 0, rowWidth + spacing + size.width > maxWidth {
                width = max(width, rowWidth)
                height += rowHeight + spacing
                rowWidth = size.width
                rowHeight = size.height
            } else {
                rowWidth += rowWidth == 0 ? size.width : spacing + size.width
                rowHeight = max(rowHeight, size.height)
            }
        }

        width = max(width, rowWidth)
        height += rowHeight
        return CGSize(width: width, height: height)
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        var x = bounds.minX
        var y = bounds.minY
        var rowHeight: CGFloat = 0

        for subview in subviews {
            let size = subview.sizeThatFits(.unspecified)
            if x > bounds.minX, x + size.width > bounds.maxX {
                x = bounds.minX
                y += rowHeight + spacing
                rowHeight = 0
            }
            subview.place(
                at: CGPoint(x: x, y: y),
                proposal: ProposedViewSize(width: size.width, height: size.height)
            )
            x += size.width + spacing
            rowHeight = max(rowHeight, size.height)
        }
    }
}
