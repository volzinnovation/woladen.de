import SwiftUI

struct FavoritesTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var favoritesStore: FavoritesStore
    @EnvironmentObject private var locationService: LocationService
    @EnvironmentObject private var tripStore: TripStore

    @State private var categoryFilter = favoriteFilterAll

    var body: some View {
        let matchingIDs = favoritesStore.favoriteIDs(matching: normalizedCategoryFilter())
        let items = viewModel.favoritesFeatures(matchingIDs, userLocation: locationService.currentLocation)

        VStack(spacing: 0) {
            categoryFilterBar

            Group {
                if items.isEmpty, favoritesStore.favorites.isEmpty, tripStore.sortedPlans.isEmpty {
                    ContentUnavailableView(
                        String(localized: "favorites.empty"),
                        systemImage: "star",
                        description: Text(String(localized: "favorites.emptyHelp"))
                    )
                } else {
                    ScrollView {
                        LazyVStack(alignment: .leading, spacing: 14) {
                            routeTemplatesSection

                            if items.isEmpty, !favoritesStore.favorites.isEmpty {
                                ContentUnavailableView(String(localized: "favorites.loading"), systemImage: "star")
                            }

                            ForEach(favoriteGroups(items)) { group in
                                VStack(alignment: .leading, spacing: 8) {
                                    HStack {
                                        Text(group.label)
                                            .font(.headline)
                                        Spacer()
                                        Text(groupCountLabel(group.features.count))
                                            .font(.caption.weight(.semibold))
                                            .foregroundStyle(.secondary)
                                    }
                                    .padding(.horizontal, 14)

                                    ForEach(group.features) { feature in
                                        FavoriteRow(
                                            feature: feature,
                                            categories: favoritesStore.categories(for: feature.properties.stationID),
                                            onOpen: { viewModel.selectFeature(feature) },
                                            onDrive: {
                                                _ = tripStore.activateStationTarget(
                                                    feature: feature,
                                                    alternatives: viewModel.allFeatures.filter {
                                                        $0.properties.matches(viewModel.filterState)
                                                    },
                                                    from: locationService.currentLocation
                                                )
                                            },
                                            onRemove: { favoritesStore.remove(feature.properties.stationID) }
                                        )
                                        .padding(.horizontal, 14)
                                    }
                                }
                            }
                        }
                        .padding(.vertical, 12)
                    }
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .task(id: favoritesStore.favorites.sorted().joined(separator: "|")) {
            await viewModel.refreshFavoriteStaticDetails(favoritesStore.favorites)
            while !Task.isCancelled {
                await viewModel.refreshFavoritesLiveSummaries(favoritesStore.favorites, force: true)
                try? await Task.sleep(nanoseconds: 15_000_000_000)
            }
        }
    }

    @ViewBuilder
    private var routeTemplatesSection: some View {
        if !tripStore.sortedPlans.isEmpty {
            VStack(alignment: .leading, spacing: 8) {
                Text(String(localized: "trip.routes.favoriteTitle", defaultValue: "Saved routes"))
                    .font(.headline)

                ForEach(tripStore.sortedPlans) { plan in
                    Button {
                        tripStore.requestPlanEditing(plan.id)
                        viewModel.selectedTab = .route
                    } label: {
                        HStack(spacing: 10) {
                            Image(systemName: "point.topleft.down.curvedto.point.bottomright.up")
                                .font(.title3.weight(.semibold))
                                .foregroundStyle(woladenBrandColor)
                                .frame(width: 36, height: 36)
                            VStack(alignment: .leading, spacing: 3) {
                                Text(plan.name)
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(.primary)
                                    .lineLimit(1)
                                Text("\(plan.route.origin.label) → \(plan.route.destination.label)")
                                    .font(.caption)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                            Spacer()
                            Image(systemName: "arrow.triangle.2.circlepath")
                                .foregroundStyle(woladenBrandColor)
                        }
                        .padding(12)
                        .background(StationVisualStyle.controlSurface, in: RoundedRectangle(cornerRadius: 10))
                    }
                    .buttonStyle(.plain)
                }
            }
            .padding(.horizontal, 14)
        }
    }

    private var categoryFilterBar: some View {
        ScrollView(.horizontal, showsIndicators: false) {
            HStack(spacing: 8) {
                ForEach(categoryFilterItems()) { item in
                    Button {
                        categoryFilter = item.id
                    } label: {
                        HStack(spacing: 6) {
                            Text(item.label)
                            Text("\(item.count)")
                                .font(.caption.weight(.bold))
                                .monospacedDigit()
                                .padding(.horizontal, 6)
                                .padding(.vertical, 2)
                                .background(
                                    (categoryFilter == item.id ? Color.white.opacity(0.22) : Color(.tertiarySystemBackground)),
                                    in: Capsule()
                                )
                        }
                        .font(.subheadline.weight(.semibold))
                        .foregroundStyle(categoryFilter == item.id ? woladenBrandColor : StationVisualStyle.mutedForeground)
                        .padding(.horizontal, 10)
                        .padding(.vertical, 8)
                        .background(
                            categoryFilter == item.id ? StationVisualStyle.selectedControlSurface : StationVisualStyle.controlSurface,
                            in: Capsule()
                        )
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel(
                        Text(
                            String(localized: "favorites.categoryFilterAria")
                                .replacingOccurrences(of: "{category}", with: item.label)
                                .replacingOccurrences(of: "{count}", with: "\(item.count)")
                        )
                    )
                }
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 8)
        }
        .background(Color(.systemBackground))
        .overlay(alignment: .bottom) {
            Divider()
        }
    }

    private func categoryFilterItems() -> [FavoriteFilterItem] {
        var items: [FavoriteFilterItem] = [
            .init(id: favoriteFilterAll, label: String(localized: "favorites.all"), count: favoritesStore.favorites.count)
        ]
        for category in favoritesStore.sortedCategories() {
            let key = categoryKey(category)
            items.append(.init(id: key, label: category, count: favoritesStore.count(matching: key)))
        }
        let uncategorizedCount = favoritesStore.count(matching: favoriteCategoryUncategorized)
        if uncategorizedCount > 0 {
            items.append(
                .init(
                    id: favoriteCategoryUncategorized,
                    label: String(localized: "favorites.uncategorized"),
                    count: uncategorizedCount
                )
            )
        }
        return items
    }

    private func normalizedCategoryFilter() -> String {
        let ids = Set(categoryFilterItems().map(\.id))
        if !ids.contains(categoryFilter) {
            DispatchQueue.main.async {
                categoryFilter = favoriteFilterAll
            }
            return favoriteFilterAll
        }
        return categoryFilter
    }

    private func favoriteGroups(_ features: [GeoJSONFeature]) -> [FavoriteFeatureGroup] {
        let filter = normalizedCategoryFilter()
        if filter != favoriteFilterAll {
            return [
                FavoriteFeatureGroup(
                    id: filter,
                    label: categoryFilterItems().first(where: { $0.id == filter })?.label ?? filter,
                    features: features
                )
            ]
        }

        var groups: [FavoriteFeatureGroup] = []
        for category in favoritesStore.sortedCategories() {
            let key = categoryKey(category)
            let categoryFeatures = features.filter { feature in
                favoritesStore.categories(for: feature.properties.stationID).contains { categoryKey($0) == key }
            }
            if !categoryFeatures.isEmpty {
                groups.append(.init(id: key, label: category, features: categoryFeatures))
            }
        }

        let uncategorized = features.filter {
            favoritesStore.categories(for: $0.properties.stationID).isEmpty
        }
        if !uncategorized.isEmpty {
            groups.append(
                .init(
                    id: favoriteCategoryUncategorized,
                    label: String(localized: "favorites.uncategorized"),
                    features: uncategorized
                )
            )
        }
        return groups.isEmpty
            ? [FavoriteFeatureGroup(id: favoriteFilterAll, label: String(localized: "favorites.all"), features: features)]
            : groups
    }

    private func groupCountLabel(_ count: Int) -> String {
        let template = count == 1
            ? String(localized: "favorites.groupCountOne")
            : String(localized: "favorites.groupCountMany")
        return template.replacingOccurrences(of: "{count}", with: "\(count)")
    }

    private struct FavoriteFilterItem: Identifiable {
        let id: String
        let label: String
        let count: Int
    }

    private struct FavoriteFeatureGroup: Identifiable {
        let id: String
        let label: String
        let features: [GeoJSONFeature]
    }

    private struct FavoriteRow: View {
        let feature: GeoJSONFeature
        let categories: [String]
        let onOpen: () -> Void
        let onDrive: () -> Void
        let onRemove: () -> Void

        var body: some View {
            HStack(spacing: 0) {
                StationClassificationRail(classification: feature.stationClassification)

                HStack(spacing: 10) {
                    Button(action: onOpen) {
                        VStack(alignment: .leading, spacing: 6) {
                            Text(feature.properties.operatorName)
                                .font(.title3.weight(.semibold))
                            Text(feature.properties.city)
                                .font(.body)
                                .foregroundStyle(.secondary)
                            Text("\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW max • \(chargingPointLabel(feature.properties.chargingPointsCount))")
                                .font(.subheadline)
                                .foregroundStyle(.secondary)
                            if !categories.isEmpty {
                                Text(categories.joined(separator: " • "))
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(woladenBrandColor)
                                    .lineLimit(1)
                            }
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

                    VStack(spacing: 8) {
                        Button(action: onDrive) {
                            Image(systemName: "play.fill")
                                .font(.headline)
                                .frame(width: 44, height: 44)
                        }
                        .buttonStyle(.borderedProminent)
                        .tint(woladenBrandColor)
                        .accessibilityLabel(Text(String(localized: "trip.station.startTarget", defaultValue: "Use station as Fahrt target")))

                        Button(role: .destructive, action: onRemove) {
                            Image(systemName: "trash")
                                .font(.headline)
                                .frame(width: 44, height: 44)
                        }
                        .buttonStyle(.bordered)
                        .accessibilityLabel(Text("aria.removeFavorite"))
                    }
                }
                .padding(14)
            }
            .background(feature.stationCardBackground)
            .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .stroke(feature.stationCardBorder, lineWidth: 1)
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
}
