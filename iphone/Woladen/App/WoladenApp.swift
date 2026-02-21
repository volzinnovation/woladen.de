import SwiftUI

@main
struct WoladenApp: App {
    @UIApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var viewModel = AppViewModel()
    @StateObject private var locationService = LocationService()
    @StateObject private var favoritesStore = FavoritesStore()

    var body: some Scene {
        WindowGroup {
            ZStack {
                Color(.systemBackground).ignoresSafeArea()

                RootTabView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            .environmentObject(viewModel)
            .environmentObject(locationService)
            .environmentObject(favoritesStore)
            .task {
                locationService.requestAuthorization()
                locationService.requestSingleLocation()
                viewModel.load(userLocation: locationService.currentLocation)
            }
            .onChange(of: viewModel.allFeatures.count) { _, _ in
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }
            .onChange(of: locationService.currentLocation) { _, newValue in
                viewModel.seedFromInitialUserLocation(newValue)
            }
        }
    }
}
