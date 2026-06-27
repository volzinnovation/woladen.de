import SwiftUI

@main
struct WoladenApp: App {
    @Environment(\.scenePhase) private var scenePhase
    @StateObject private var viewModel = AppViewModel()
    @StateObject private var locationService = LocationService()
    @StateObject private var favoritesStore = FavoritesStore()
    @State private var screenshotReadyWritten = false

    private let screenshotConfig = AppStoreScreenshotConfig.current

    var body: some Scene {
        WindowGroup {
            ZStack {
                Color(.systemBackground).ignoresSafeArea()

                RootTabView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
            .overlay {
                if viewModel.isLoading && viewModel.allFeatures.isEmpty && viewModel.loadError == nil {
                    StartupLoadingView()
                }
            }
            .environmentObject(viewModel)
            .environmentObject(locationService)
            .environmentObject(favoritesStore)
            .task {
                locationService.activate()
                viewModel.loadIfNeeded(userLocation: locationService.currentLocation)
            }
            .onChange(of: scenePhase) { _, newValue in
                if newValue == .active {
                    locationService.activate()
                }
            }
            .onChange(of: viewModel.allFeatures.count) { _, _ in
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }
            .onChange(of: locationService.currentLocation) { _, newValue in
                viewModel.seedFromInitialUserLocation(newValue)
            }
            .task(id: screenshotPreparationKey) {
                await prepareScreenshotIfNeeded()
            }
        }
    }

    private var screenshotPreparationKey: String {
        guard let screenshotConfig else { return "disabled" }
        let locationKey: String
        if let currentLocation = locationService.currentLocation {
            locationKey = "\(currentLocation.coordinate.latitude),\(currentLocation.coordinate.longitude)"
        } else {
            locationKey = "no-location"
        }
        return "\(screenshotConfig.scene.rawValue)|\(viewModel.allFeatures.count)|\(locationKey)|\(screenshotReadyWritten)"
    }

    @MainActor
    private func prepareScreenshotIfNeeded() async {
        guard let screenshotConfig else { return }
        guard !screenshotReadyWritten else { return }
        guard !viewModel.allFeatures.isEmpty else { return }
        guard screenshotConfig.scene == .info || locationService.currentLocation != nil else { return }

        viewModel.clearSelectedFeature()

        switch screenshotConfig.scene {
        case .list:
            viewModel.selectedTab = .list
        case .search:
            viewModel.selectedTab = .list
        case .detail:
            viewModel.selectedTab = .list
            if let feature = screenshotFeature(for: screenshotConfig) {
                viewModel.selectFeature(feature)
            }
        case .map:
            viewModel.selectedTab = .map
        case .route:
            viewModel.selectedTab = .route
            if let routeEndpoints = screenshotConfig.routeEndpoints {
                viewModel.searchRoute(origin: routeEndpoints.origin, destination: routeEndpoints.destination)
                await waitForScreenshotRoute()
            }
        case .favorites:
            viewModel.selectedTab = .favorites
        case .info:
            viewModel.selectedTab = .info
        }

        try? await Task.sleep(nanoseconds: screenshotConfig.renderDelayNanoseconds)
        ScreenshotReadyMarker.writeReadyMarker(named: screenshotConfig.outputName)
        screenshotReadyWritten = true
    }

    @MainActor
    private func screenshotFeature(for config: AppStoreScreenshotConfig) -> GeoJSONFeature? {
        if let stationID = config.stationID,
           let feature = viewModel.feature(forStationID: stationID) {
            return feature
        }
        return viewModel.discoveredFeatures.first ?? viewModel.allFeatures.first
    }

    @MainActor
    private func waitForScreenshotRoute() async {
        let startedAt = Date()
        while viewModel.isLoadingRoute || (viewModel.routeSummary == nil && viewModel.routeError == nil) {
            if Date().timeIntervalSince(startedAt) > 130 {
                return
            }
            try? await Task.sleep(nanoseconds: 250_000_000)
        }
    }
}

private struct StartupLoadingView: View {
    var body: some View {
        VStack(spacing: 14) {
            Image(systemName: "bolt.circle.fill")
                .font(.system(size: 46, weight: .semibold))
                .foregroundStyle(Color.accentColor)
                .accessibilityHidden(true)

            VStack(spacing: 4) {
                Text("Woladen")
                    .font(.title2.weight(.bold))
                Text(String(localized: "list.loading"))
                    .font(.body)
                    .foregroundStyle(.secondary)
            }

            ProgressView()
                .controlSize(.regular)
        }
        .padding(28)
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(.systemBackground))
    }
}

private struct AppStoreScreenshotConfig {
    enum Scene: String {
        case list
        case search
        case detail
        case map
        case route
        case favorites
        case info
    }

    let scene: Scene
    let outputName: String
    let stationID: String?
    let routeEndpoints: AppStoreScreenshotRouteEndpoints?

    var renderDelayNanoseconds: UInt64 {
        switch scene {
        case .list, .search, .favorites, .info:
            return 1_000_000_000
        case .detail:
            return 2_000_000_000
        case .route:
            return 2_500_000_000
        case .map:
            return 3_500_000_000
        }
    }

    static var current: AppStoreScreenshotConfig? {
        let environment = ProcessInfo.processInfo.environment
        guard environment["WOLADEN_SCREENSHOT_MODE"] == "1" else { return nil }
        guard let rawScene = environment["WOLADEN_SCREENSHOT_SCENE"]?.trimmedNonEmpty,
              let scene = Scene(rawValue: rawScene) else {
            return nil
        }

        return AppStoreScreenshotConfig(
            scene: scene,
            outputName: environment["WOLADEN_SCREENSHOT_NAME"]?.trimmedNonEmpty ?? rawScene,
            stationID: environment["WOLADEN_SCREENSHOT_STATION_ID"]?.trimmedNonEmpty,
            routeEndpoints: AppStoreScreenshotRouteEndpoints(environment: environment)
        )
    }
}

struct AppStoreScreenshotRouteEndpoints {
    let origin: RouteEndpoint
    let destination: RouteEndpoint

    init?(environment: [String: String] = ProcessInfo.processInfo.environment) {
        guard environment["WOLADEN_SCREENSHOT_MODE"] == "1",
              environment["WOLADEN_SCREENSHOT_SCENE"] == AppStoreScreenshotConfig.Scene.route.rawValue,
              let origin = Self.endpoint(
                prefix: "WOLADEN_SCREENSHOT_ROUTE_ORIGIN",
                environment: environment
              ),
              let destination = Self.endpoint(
                prefix: "WOLADEN_SCREENSHOT_ROUTE_DESTINATION",
                environment: environment
              ) else {
            return nil
        }

        self.origin = origin
        self.destination = destination
    }

    private static func endpoint(prefix: String, environment: [String: String]) -> RouteEndpoint? {
        guard let label = environment["\(prefix)_LABEL"]?.trimmedNonEmpty,
              let rawLat = environment["\(prefix)_LAT"]?.trimmedNonEmpty,
              let rawLon = environment["\(prefix)_LON"]?.trimmedNonEmpty,
              let lat = Double(rawLat),
              let lon = Double(rawLon) else {
            return nil
        }
        return RouteEndpoint(lat: lat, lon: lon, label: label)
    }
}

private enum ScreenshotReadyMarker {
    private static let directoryName = "app-store-screenshots"

    static func writeReadyMarker(named outputName: String) {
        guard let documentsDirectory = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first else {
            return
        }

        let directoryURL = documentsDirectory.appendingPathComponent(directoryName, isDirectory: true)
        let markerURL = directoryURL.appendingPathComponent("\(outputName).ready")

        do {
            try FileManager.default.createDirectory(
                at: directoryURL,
                withIntermediateDirectories: true,
                attributes: nil
            )
            try Data(Date().description.utf8).write(to: markerURL, options: .atomic)
        } catch {
            print("Failed to write screenshot ready marker: \(error.localizedDescription)")
        }
    }
}

private extension String {
    var trimmedNonEmpty: String? {
        let trimmed = trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }
}
