import SwiftUI

struct ContentView: View {
    @StateObject private var viewModel = ChargerViewModel()

    var body: some View {
        TabView {
            NavigationStack {
                MapTabView(viewModel: viewModel)
            }
            .tabItem {
                Label("Map", systemImage: "map")
            }

            NavigationStack {
                StationListView(
                    viewModel: viewModel,
                    stations: viewModel.filteredStations,
                    title: "Chargers",
                    emptyText: "No chargers match your current filters."
                )
            }
            .tabItem {
                Label("List", systemImage: "list.bullet")
            }

            NavigationStack {
                StationListView(
                    viewModel: viewModel,
                    stations: viewModel.favoriteStations,
                    title: "Favorites",
                    emptyText: "No favorites saved yet."
                )
            }
            .tabItem {
                Label("Favorites", systemImage: "star")
            }

            NavigationStack {
                AboutTabView(viewModel: viewModel)
            }
            .tabItem {
                Label("Info", systemImage: "info.circle")
            }
        }
        .task {
            if viewModel.allStations.isEmpty {
                await viewModel.load()
            }
        }
        .sheet(isPresented: $viewModel.isFilterSheetPresented) {
            FilterSheetView(viewModel: viewModel)
        }
        .sheet(item: $viewModel.selectedStation) { station in
            StationDetailView(viewModel: viewModel, station: station)
        }
        .overlay {
            if viewModel.isLoading {
                ZStack {
                    Color.black.opacity(0.12).ignoresSafeArea()
                    ProgressView("Loading stations...")
                        .padding(16)
                        .background(.regularMaterial)
                        .clipShape(RoundedRectangle(cornerRadius: 12))
                }
            }
        }
        .alert("Could Not Load Data", isPresented: Binding(
            get: { viewModel.errorMessage != nil },
            set: { if !$0 { viewModel.errorMessage = nil } }
        )) {
            Button("OK", role: .cancel) {}
        } message: {
            Text(viewModel.errorMessage ?? "")
        }
    }
}
