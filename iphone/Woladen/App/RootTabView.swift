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
            ZStack {
                HStack(spacing: 0) {
                    wideNavigationRail
                    Divider()
                    currentTab
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                }

                if shouldShowWideDetail, let selectedFeature = viewModel.selectedFeature {
                    Color.black.opacity(0.4)
                        .ignoresSafeArea()
                        .onTapGesture {
                            viewModel.clearSelectedFeature()
                        }

                    StationDetailView(stationID: selectedFeature.properties.stationID, prefersWideLayout: true)
                        .frame(
                            width: min(1180, max(360, proxy.size.width - 64)),
                            height: min(820, max(520, proxy.size.height - 48))
                        )
                        .clipShape(RoundedRectangle(cornerRadius: 24, style: .continuous))
                        .shadow(color: Color.black.opacity(0.24), radius: 24, x: 0, y: 14)
                }
            }
        }
    }

    private var wideNavigationRail: some View {
        VStack(spacing: 8) {
            VStack(spacing: 8) {
                wideTabButton(.list, title: String(localized: "nav.list"), systemImage: "list.bullet")
                wideTabButton(.map, title: String(localized: "nav.map"), systemImage: "map")
                wideTabButton(.favorites, title: String(localized: "nav.favorites"), systemImage: "star")
                wideTabButton(.info, title: String(localized: "nav.info"), systemImage: "info.circle")
            }
            .padding(.top, 12)

            Spacer()
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 14)
        .frame(width: 108)
        .frame(maxHeight: .infinity, alignment: .top)
        .background(Color(.systemBackground))
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
        VStack(spacing: 0) {
            Divider()
            HStack(spacing: 8) {
                tabButton(.list, title: String(localized: "nav.list"), systemImage: "list.bullet")
                tabButton(.map, title: String(localized: "nav.map"), systemImage: "map")
                tabButton(.favorites, title: String(localized: "nav.favorites"), systemImage: "star")
                tabButton(.info, title: String(localized: "nav.info"), systemImage: "info.circle")
            }
            .padding(.horizontal, 10)
            .padding(.top, 6)
            .padding(.bottom, 8)
        }
        .frame(maxWidth: .infinity)
        .background(Color(.systemBackground))
    }

    private func tabButton(_ tab: AppViewModel.AppTab, title: String, systemImage: String) -> some View {
        let isSelected = viewModel.selectedTab == tab
        return Button {
            viewModel.selectedTab = tab
        } label: {
            VStack(spacing: 4) {
                Image(systemName: systemImage)
                    .font(.system(size: 22, weight: .semibold))
                Text(title)
                    .font(.system(size: 12, weight: .semibold))
            }
            .frame(maxWidth: .infinity, minHeight: 62)
            .foregroundStyle(isSelected ? woladenBrandColor : StationVisualStyle.mutedForeground)
            .background {
                RoundedRectangle(cornerRadius: 14, style: .continuous)
                    .fill(isSelected ? StationVisualStyle.selectedControlSurface : Color.clear)
            }
        }
        .buttonStyle(.plain)
    }

    private func wideTabButton(_ tab: AppViewModel.AppTab, title: String, systemImage: String) -> some View {
        let isSelected = viewModel.selectedTab == tab
        return Button {
            viewModel.selectedTab = tab
        } label: {
            VStack(spacing: 6) {
                Image(systemName: systemImage)
                    .font(.system(size: 24, weight: .semibold))
                Text(title)
                    .font(.system(size: 13, weight: .semibold))
                    .lineLimit(1)
                    .minimumScaleFactor(0.82)
            }
                .foregroundStyle(isSelected ? woladenBrandColor : StationVisualStyle.mutedForeground)
                .frame(width: 78, height: 78)
                .background {
                    RoundedRectangle(cornerRadius: 16, style: .continuous)
                        .fill(isSelected ? StationVisualStyle.selectedControlSurface : Color.clear)
                }
        }
        .buttonStyle(.plain)
        .accessibilityLabel(Text(title))
        .accessibilityAddTraits(isSelected ? .isSelected : [])
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

    private var shouldShowWideDetail: Bool {
        switch viewModel.selectedTab {
        case .list, .map, .favorites:
            return true
        case .info:
            return false
        }
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
