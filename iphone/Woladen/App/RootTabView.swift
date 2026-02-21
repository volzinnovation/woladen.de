import SwiftUI
import UIKit

struct RootTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @State private var showingFilter = false

    var body: some View {
        ZStack(alignment: .bottom) {
            Color(.systemBackground)
                .ignoresSafeArea()

            currentTab
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(Color(.systemBackground))

            tabBar(safeBottom: safeBottomInset)
        }
        .ignoresSafeArea(.keyboard, edges: .bottom)
        .sheet(isPresented: $showingFilter) {
            FilterSheetView(
                filter: viewModel.filterState,
                operators: viewModel.operators,
                availableAmenityKeys: availableAmenityKeys()
            ) { newFilter in
                viewModel.filterState = newFilter
                viewModel.applyFilters(userLocation: locationService.currentLocation)
            }
            .presentationDetents([.medium, .large])
        }
        .sheet(item: $viewModel.selectedFeature) { feature in
            StationDetailView(feature: feature)
                .presentationDetents([.large])
                .presentationDragIndicator(.visible)
        }
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
        }
    }

    private func tabBar(safeBottom: CGFloat) -> some View {
        VStack(spacing: 0) {
            Divider()

            HStack(spacing: 0) {
                tabButton(.list, title: "Liste", systemImage: "list.bullet")
                tabButton(.map, title: "Karte", systemImage: "map")
                tabButton(.favorites, title: "Favoriten", systemImage: "star")
            }
            .padding(.top, 6)
            .padding(.bottom, max(6, safeBottom))
        }
        .frame(maxWidth: .infinity)
        .background(Color(.systemBackground))
        .ignoresSafeArea(.container, edges: .bottom)
    }

    private var safeBottomInset: CGFloat {
        let windows = UIApplication.shared.connectedScenes
            .compactMap { $0 as? UIWindowScene }
            .flatMap(\.windows)

        let bottom = windows.map { $0.safeAreaInsets.bottom }.max() ?? 0
        if bottom > 0 { return bottom }

        // Fallback for cases where window insets are not available yet.
        if UIDevice.current.userInterfaceIdiom == .phone {
            return 34
        }
        return 0
    }

    private func tabButton(_ tab: AppViewModel.AppTab, title: String, systemImage: String) -> some View {
        let isSelected = viewModel.selectedTab == tab
        return Button {
            viewModel.selectedTab = tab
        } label: {
            VStack(spacing: 4) {
                Image(systemName: systemImage)
                    .font(.system(size: 21, weight: .medium))
                Text(title)
                    .font(.caption)
            }
            .frame(maxWidth: .infinity)
            .padding(.vertical, 4)
            .foregroundStyle(isSelected ? Color.accentColor : Color.secondary)
            .background(
                RoundedRectangle(cornerRadius: 12)
                    .fill(isSelected ? Color.accentColor.opacity(0.14) : Color.clear)
                    .padding(.horizontal, 10)
            )
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
}
