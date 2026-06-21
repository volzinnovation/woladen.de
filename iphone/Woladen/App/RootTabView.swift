import SwiftUI

struct RootTabView: View {
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @State private var showingFilter = false

    var body: some View {
        Group {
            if usesWideLayout {
                wideLayout
            } else {
                compactLayout
            }
        }
        .background(Color(.systemBackground))
        .ignoresSafeArea(.keyboard, edges: .bottom)
        .sheet(isPresented: $showingFilter) {
            filterSheet
        }
        .sheet(item: selectedFeatureSheetBinding) { feature in
            StationDetailView(stationID: feature.properties.stationID)
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
        }
    }

    private var usesWideLayout: Bool {
        horizontalSizeClass == .regular
    }

    private var compactLayout: some View {
        currentTab
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .safeAreaInset(edge: .bottom, spacing: 0) {
                tabBar
            }
    }

    private var wideLayout: some View {
        GeometryReader { proxy in
            let detailWidth = min(max(proxy.size.width * 0.36, 380), 520)

            HStack(spacing: 0) {
                sidebar
                Divider()
                currentTab
                    .frame(maxWidth: .infinity, maxHeight: .infinity)

                if let selectedFeature = viewModel.selectedFeature {
                    Divider()
                    StationDetailView(stationID: selectedFeature.properties.stationID)
                        .frame(width: detailWidth)
                        .frame(maxHeight: .infinity)
                }
            }
        }
    }

    private var sidebar: some View {
        VStack(alignment: .leading, spacing: 18) {
            VStack(alignment: .leading, spacing: 4) {
                Text("Woladen")
                    .font(.title2.bold())
                Text("Schnelllader")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(.top, 12)

            VStack(spacing: 6) {
                wideTabButton(.list, title: "Liste", systemImage: "list.bullet")
                wideTabButton(.map, title: "Karte", systemImage: "map")
                wideTabButton(.favorites, title: "Favoriten", systemImage: "star")
                wideTabButton(.info, title: "Info", systemImage: "info.circle")
            }

            Spacer()

            Button {
                showingFilter = true
            } label: {
                Label("Filter", systemImage: "line.3.horizontal.decrease.circle")
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
            .buttonStyle(.bordered)
        }
        .padding(.horizontal, 18)
        .padding(.vertical, 16)
        .frame(width: 220)
        .frame(maxHeight: .infinity, alignment: .top)
        .background(Color(.secondarySystemBackground))
    }

    private var filterSheet: some View {
        FilterSheetView(
            filter: viewModel.filterState,
            operators: viewModel.operators,
            availableAmenityKeys: availableAmenityKeys()
        ) { newFilter in
            viewModel.filterState = newFilter
            viewModel.reloadCatalogForCurrentContext(userLocation: locationService.currentLocation)
        }
        .presentationDetents([.medium, .large])
    }

    @ViewBuilder
    private var currentTab: some View {
        switch viewModel.selectedTab {
        case .list:
            ListTabView(showingFilter: $showingFilter)
        case .map:
            MapTabView(showingFilter: $showingFilter)
        case .favorites:
            FavoritesTabView()
        case .info:
            InfoTabView()
        }
    }

    private var tabBar: some View {
        VStack(spacing: 8) {
            HStack(spacing: 8) {
                tabButton(.list, title: "Liste", systemImage: "list.bullet")
                tabButton(.map, title: "Karte", systemImage: "map")
                tabButton(.favorites, title: "Favoriten", systemImage: "star")
                tabButton(.info, title: "Info", systemImage: "info.circle")
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 8)
            .background(.ultraThinMaterial, in: Capsule())
            .overlay {
                Capsule()
                    .stroke(Color.primary.opacity(0.08), lineWidth: 0.5)
            }

            Capsule()
                .fill(Color.secondary.opacity(0.35))
                .frame(width: 120, height: 5)
        }
        .frame(maxWidth: 380)
        .padding(.horizontal, 16)
        .padding(.top, 6)
        .padding(.bottom, 8)
        .frame(maxWidth: .infinity)
    }

    private func tabButton(_ tab: AppViewModel.AppTab, title: String, systemImage: String) -> some View {
        let isSelected = viewModel.selectedTab == tab
        return Button {
            viewModel.selectedTab = tab
        } label: {
            VStack(spacing: 4) {
                Image(systemName: systemImage)
                    .font(.system(size: 17, weight: .semibold))
                Text(title)
                    .font(.system(size: 11, weight: .semibold))
            }
            .frame(maxWidth: .infinity, minHeight: 54)
            .foregroundStyle(isSelected ? Color.accentColor : Color.secondary)
            .background {
                RoundedRectangle(cornerRadius: 18, style: .continuous)
                    .fill(isSelected ? Color.accentColor.opacity(0.14) : Color.clear)
            }
        }
        .buttonStyle(.plain)
    }

    private func wideTabButton(_ tab: AppViewModel.AppTab, title: String, systemImage: String) -> some View {
        let isSelected = viewModel.selectedTab == tab
        return Button {
            viewModel.selectedTab = tab
        } label: {
            Label(title, systemImage: systemImage)
                .font(.system(size: 15, weight: .semibold))
                .foregroundStyle(isSelected ? Color.accentColor : Color.primary)
                .frame(maxWidth: .infinity, minHeight: 44, alignment: .leading)
                .padding(.horizontal, 12)
                .background {
                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                        .fill(isSelected ? Color.accentColor.opacity(0.14) : Color.clear)
                }
        }
        .buttonStyle(.plain)
    }

    private func availableAmenityKeys() -> [String] {
        var keys = Set<String>()
        for feature in viewModel.allFeatures {
            for (key, count) in feature.properties.amenityCounts where count > 0 {
                keys.insert(key)
            }
        }
        return keys.sorted { AmenityCatalog.label(for: $0) < AmenityCatalog.label(for: $1) }
    }

    private var selectedFeatureBinding: Binding<GeoJSONFeature?> {
        Binding(
            get: { viewModel.selectedFeature },
            set: { feature in
                if let feature {
                    viewModel.selectFeature(feature)
                } else {
                    viewModel.clearSelectedFeature()
                }
            }
        )
    }

    private var selectedFeatureSheetBinding: Binding<GeoJSONFeature?> {
        Binding(
            get: { usesWideLayout ? nil : viewModel.selectedFeature },
            set: { feature in
                if let feature {
                    viewModel.selectFeature(feature)
                } else {
                    viewModel.clearSelectedFeature()
                }
            }
        )
    }
}
