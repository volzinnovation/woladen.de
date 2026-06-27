import SwiftUI
import MapKit

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)
private let mapGPSRefreshIntervalNanoseconds: UInt64 = 5 * 60 * 1_000_000_000

struct MapTabView: View {
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.openURL) private var openURL
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService
    @EnvironmentObject private var favoritesStore: FavoritesStore

    @Binding var showingFilter: Bool

    @StateObject private var locationSearchCompleter = PlaceSearchCompleter()
    @State private var cameraPosition: MapCameraPosition = .region(
        MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: 51.1657, longitude: 10.4515),
            span: MKCoordinateSpan(latitudeDelta: 7.5, longitudeDelta: 7.5)
        )
    )
    @State private var centerOnNextLocationUpdate = false
    @State private var hasCenteredInitialLocation = false
    @State private var lastQueriedCenter: CLLocationCoordinate2D?
    @State private var locationSearchQuery = ""
    @State private var isSearchingLocation = false
    @State private var locationSearchError: String?
    @FocusState private var isLocationSearchFocused: Bool

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Map(position: $cameraPosition) {
                if let route = viewModel.routeSummary {
                    let coordinates = routePolylineCoordinates(route)
                    if coordinates.count > 1 {
                        MapPolyline(coordinates: coordinates)
                            .stroke(woladenBrandColor, style: StrokeStyle(lineWidth: 5, lineCap: .round, lineJoin: .round))
                    }
                }

                ForEach(mapItems()) { feature in
                    Annotation("", coordinate: feature.coordinate) {
                        Button {
                            viewModel.selectFeature(feature)
                        } label: {
                            marker(for: feature)
                        }
                        .buttonStyle(.plain)
                        .contextMenu {
                            stationContextMenu(for: feature)
                        }
                    }
                }

                if let current = locationService.currentLocation {
                    UserAnnotation()
                    Annotation(String(localized: "aria.locate"), coordinate: current.coordinate) {
                        Circle()
                            .fill(Color.blue)
                            .frame(width: 10, height: 10)
                            .overlay(Circle().stroke(Color.white, lineWidth: 1))
                    }
                }
            }
            .ignoresSafeArea(edges: [.top, .horizontal])
            .onMapCameraChange(frequency: .onEnd) { context in
                guard viewModel.routeSummary == nil else { return }
                guard hasCenteredInitialLocation else { return }
                let center = context.region.center
                guard shouldQuery(for: center) else { return }
                lastQueriedCenter = center
                viewModel.handleMapCenterChange(center)
            }

            mapControlsOverlay

            if viewModel.isLoading && viewModel.allFeatures.isEmpty {
                ProgressView(String(localized: "list.loading"))
                    .padding(12)
                    .background(Color(.systemBackground), in: RoundedRectangle(cornerRadius: 10))
                    .padding(.top, 12)
                    .padding(.leading, 12)
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            }

            if viewModel.isAwaitingFirstLocationFix {
                LocationAccessInstructionView(
                    authorizationStatus: locationService.authorizationStatus,
                    lastError: locationService.lastError,
                    retry: requestCurrentLocation
                )
                .padding(.horizontal, 24)
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
            }
        }
        .onChange(of: locationService.currentLocation) { _, newValue in
            guard let newValue else { return }
            if centerOnNextLocationUpdate || !hasCenteredInitialLocation {
                centerMap(on: newValue)
                hasCenteredInitialLocation = true
            }
        }
        .onAppear(perform: handleActivation)
        .onChange(of: scenePhase) { _, newValue in
            guard newValue == .active else { return }
            handleActivation()
        }
        .onChange(of: routeMapCameraKey) { _, _ in
            if viewModel.routeSummary != nil {
                focusMapOnRoute()
            }
        }
        .task {
            await runGPSRefreshLoop()
        }
    }

    private func mapItems() -> [GeoJSONFeature] {
        if viewModel.routeSummary != nil {
            return viewModel.routeDisplayFeatures()
        }
        return viewModel.discoveredFeatures
    }

    private var mapControlsOverlay: some View {
        VStack(spacing: 8) {
            HStack(spacing: 8) {
                Button {
                    requestCurrentLocation()
                    if let current = locationService.currentLocation {
                        centerMap(on: current)
                    }
                } label: {
                    Image(systemName: "location.fill")
                        .font(.headline)
                        .frame(width: 40, height: 40)
                        .background(Color(.systemBackground), in: Circle())
                }
                .buttonStyle(.plain)
                .accessibilityLabel(Text(String(localized: "aria.locate")))

                TextField(String(localized: "search.placeholder"), text: $locationSearchQuery)
                    .textInputAutocapitalization(.words)
                    .disableAutocorrection(true)
                    .submitLabel(.search)
                    .focused($isLocationSearchFocused)
                    .onChange(of: locationSearchQuery) { _, newValue in
                        locationSearchError = nil
                        locationSearchCompleter.update(
                            query: newValue,
                            region: locationService.currentLocation.map(searchRegion)
                        )
                    }
                    .onSubmit(searchLocation)
                    .font(.subheadline)
                    .padding(.horizontal, 12)
                    .frame(height: 40)
                    .background(Color(.secondarySystemBackground), in: Capsule())
                    .accessibilityLabel(Text(String(localized: "search.label")))

                if isSearchingLocation {
                    ProgressView()
                        .controlSize(.small)
                        .frame(width: 24, height: 40)
                        .accessibilityLabel(Text(String(localized: "search.searching")))
                }

                Button {
                    showingFilter = true
                } label: {
                    Image(systemName: "line.3.horizontal.decrease.circle")
                        .font(.headline)
                        .frame(width: 40, height: 40)
                        .background(Color(.systemBackground), in: Circle())
                        .overlay(alignment: .topTrailing) {
                            filterCountBadge
                        }
                }
                .buttonStyle(.plain)
                .accessibilityLabel(Text(String(localized: "aria.filterOpen")))
            }
            .padding(8)
            .frame(maxWidth: 620)
            .background(.regularMaterial, in: Capsule())
            .overlay {
                Capsule()
                    .stroke(Color(.separator).opacity(0.55), lineWidth: 1)
            }
            .shadow(color: Color.black.opacity(0.12), radius: 10, x: 0, y: 5)

            if isLocationSearchFocused, !locationSearchCompleter.completions.isEmpty {
                PlaceCompletionList(completions: locationSearchCompleter.completions) { completion in
                    selectLocationCompletion(completion)
                }
                .frame(maxWidth: 620)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 14, style: .continuous))
                .overlay {
                    RoundedRectangle(cornerRadius: 14, style: .continuous)
                        .stroke(Color(.separator).opacity(0.55), lineWidth: 1)
                }
                .shadow(color: Color.black.opacity(0.12), radius: 10, x: 0, y: 5)
            }

            if let locationSearchError {
                Text(locationSearchError)
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 7)
                    .background(.regularMaterial, in: Capsule())
            }
        }
        .padding(.horizontal, 16)
        .padding(.top, 12)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
    }

    @ViewBuilder
    private var filterCountBadge: some View {
        if viewModel.filterState.activeCount > 0 {
            Text("\(viewModel.filterState.activeCount)")
                .font(.caption2.weight(.bold))
                .foregroundStyle(.white)
                .monospacedDigit()
                .frame(minWidth: 18, minHeight: 18)
                .padding(.horizontal, 2)
                .background(woladenBrandColor, in: Capsule())
                .offset(x: 5, y: -5)
                .accessibilityHidden(true)
        }
    }

    private func searchLocation() {
        let query = locationSearchQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty, !isSearchingLocation else { return }
        isSearchingLocation = true
        locationSearchError = nil
        locationSearchCompleter.clear()
        isLocationSearchFocused = false

        Task {
            do {
                let request = MKLocalSearch.Request()
                request.naturalLanguageQuery = query
                if let current = locationService.currentLocation {
                    request.region = searchRegion(around: current)
                }
                let response = try await MKLocalSearch(request: request).start()
                guard let item = response.mapItems.first else {
                    locationSearchError = String(localized: "search.noResults")
                    isSearchingLocation = false
                    return
                }
                applyMapSearchItem(item)
            } catch {
                locationSearchError = String(localized: "search.unavailable")
            }
            isSearchingLocation = false
        }
    }

    private func selectLocationCompletion(_ completion: MKLocalSearchCompletion) {
        guard !isSearchingLocation else { return }
        isSearchingLocation = true
        locationSearchError = nil

        Task {
            do {
                let request = MKLocalSearch.Request(completion: completion)
                if let current = locationService.currentLocation {
                    request.region = searchRegion(around: current)
                }
                let response = try await MKLocalSearch(request: request).start()
                guard let item = response.mapItems.first else {
                    locationSearchError = String(localized: "search.noResults")
                    isSearchingLocation = false
                    return
                }
                locationSearchQuery = placeLabel(for: item, fallback: completion.title)
                locationSearchCompleter.clear()
                isLocationSearchFocused = false
                applyMapSearchItem(item)
            } catch {
                locationSearchError = String(localized: "search.unavailable")
            }
            isSearchingLocation = false
        }
    }

    private func applyMapSearchItem(_ item: MKMapItem) {
        let coordinate = item.placemark.coordinate
        cameraPosition = .region(
            MKCoordinateRegion(
                center: coordinate,
                span: MKCoordinateSpan(latitudeDelta: 0.12, longitudeDelta: 0.12)
            )
        )
        lastQueriedCenter = coordinate
        hasCenteredInitialLocation = true
        if viewModel.routeSummary == nil {
            viewModel.handleMapCenterChange(coordinate)
        }
    }

    private func searchRegion(around location: CLLocation) -> MKCoordinateRegion {
        MKCoordinateRegion(
            center: location.coordinate,
            span: MKCoordinateSpan(latitudeDelta: 2.0, longitudeDelta: 2.0)
        )
    }

    private func placeLabel(for item: MKMapItem, fallback: String) -> String {
        let name = item.name?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        if !name.isEmpty {
            return name
        }
        let title = item.placemark.title?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        return title.isEmpty ? fallback : title
    }

    private func centerMap(on location: CLLocation) {
        centerOnNextLocationUpdate = false
        cameraPosition = .region(
            MKCoordinateRegion(
                center: location.coordinate,
                span: MKCoordinateSpan(latitudeDelta: 0.12, longitudeDelta: 0.12)
            )
        )
        lastQueriedCenter = location.coordinate
        if viewModel.routeSummary == nil {
            viewModel.handleMapCenterChange(location.coordinate)
        }
    }

    private func shouldQuery(for center: CLLocationCoordinate2D) -> Bool {
        guard let lastQueriedCenter else { return true }
        let lhs = CLLocation(latitude: lastQueriedCenter.latitude, longitude: lastQueriedCenter.longitude)
        let rhs = CLLocation(latitude: center.latitude, longitude: center.longitude)
        return lhs.distance(from: rhs) > 250
    }

    private func handleActivation() {
        if focusMapOnRoute() {
            return
        }
        if let current = locationService.currentLocation {
            if !hasCenteredInitialLocation {
                centerMap(on: current)
                hasCenteredInitialLocation = true
            } else {
                viewModel.reloadMapForCenter(lastQueriedCenter ?? current.coordinate)
            }
        } else {
            centerOnNextLocationUpdate = true
            locationService.activate()
            viewModel.waitForLocation()
        }
    }

    private func runGPSRefreshLoop() async {
        while !Task.isCancelled {
            try? await Task.sleep(nanoseconds: mapGPSRefreshIntervalNanoseconds)
            guard !Task.isCancelled, scenePhase == .active else { continue }
            guard viewModel.routeSummary == nil else { continue }
            refreshFromGPSPosition()
        }
    }

    private func refreshFromGPSPosition() {
        guard viewModel.routeSummary == nil else { return }
        guard locationService.authorizationStatus == .authorizedWhenInUse ||
                locationService.authorizationStatus == .authorizedAlways else {
            return
        }
        locationService.requestSingleLocation()
        guard let current = locationService.currentLocation else {
            locationService.activate()
            return
        }
        lastQueriedCenter = current.coordinate
        viewModel.refreshMapForUserLocation(current)
    }

    private var routeMapCameraKey: String {
        guard let route = viewModel.routeSummary else { return "" }
        let ids = viewModel.routeDisplayFeatures().map(\.id).joined(separator: ",")
        return "\(route.distanceM):\(route.durationS):\(route.geometry.coordinates.count):\(ids)"
    }

    @discardableResult
    private func focusMapOnRoute() -> Bool {
        guard let route = viewModel.routeSummary,
              let rect = routeMapRect(route: route, features: viewModel.routeDisplayFeatures()) else {
            return false
        }
        cameraPosition = .rect(rect)
        hasCenteredInitialLocation = true
        return true
    }

    private func routePolylineCoordinates(_ route: RouteSummary) -> [CLLocationCoordinate2D] {
        route.geometry.coordinates.compactMap { point in
            guard point.count >= 2 else { return nil }
            let lon = point[0]
            let lat = point[1]
            guard lat.isFinite, lon.isFinite else { return nil }
            return CLLocationCoordinate2D(latitude: lat, longitude: lon)
        }
    }

    private func routeMapRect(route: RouteSummary, features: [GeoJSONFeature]) -> MKMapRect? {
        let coordinates = routePolylineCoordinates(route) + features.map(\.coordinate)
        guard !coordinates.isEmpty else { return nil }

        var rect = MKMapRect.null
        for coordinate in coordinates {
            let point = MKMapPoint(coordinate)
            let pointRect = MKMapRect(x: point.x, y: point.y, width: 1, height: 1)
            rect = rect.isNull ? pointRect : rect.union(pointRect)
        }
        guard !rect.isNull else { return nil }
        let insetX = max(rect.width * 0.12, 30_000)
        let insetY = max(rect.height * 0.12, 30_000)
        return rect.insetBy(dx: -insetX, dy: -insetY)
    }

    private func requestCurrentLocation() {
        centerOnNextLocationUpdate = true
        locationService.requestAuthorization()
        if let current = locationService.currentLocation {
            centerMap(on: current)
            hasCenteredInitialLocation = true
        } else {
            viewModel.waitForLocation()
        }
    }

    private func color(for key: String) -> Color {
        switch key {
        case "gold": return StationVisualStyle.amenityGold
        case "silver": return StationVisualStyle.amenitySilver
        case "bronze": return StationVisualStyle.amenityBronze
        default: return StationVisualStyle.amenityGrey
        }
    }

    private func marker(for feature: GeoJSONFeature) -> some View {
        let isFavorite = favoritesStore.isFavorite(feature.properties.stationID)
        return Group {
            if isFavorite {
                Image(systemName: "star.fill")
                    .font(.system(size: 16, weight: .bold))
                    .foregroundStyle(favoriteStarColor)
                    .frame(width: 24, height: 24)
            } else if feature.isStationOutOfOrder {
                ZStack {
                    Circle()
                        .fill(StationVisualStyle.markerOutOfOrder)
                        .overlay(Circle().stroke(Color.white, lineWidth: 1.5))
                    Image(systemName: "xmark")
                        .font(.system(size: 11, weight: .heavy))
                        .foregroundStyle(Color.white)
                }
                .frame(width: 22, height: 22)
            } else if feature.isStationFullyOccupied {
                Circle()
                    .fill(Color.white)
                    .overlay(Circle().stroke(StationVisualStyle.markerFullyOccupied, lineWidth: 2))
                    .frame(width: 18, height: 18)
            } else {
                Circle()
                    .fill(color(for: viewModel.markerTint(for: feature)))
                    .frame(width: 16, height: 16)
                    .overlay(Circle().stroke(Color.white, lineWidth: 1.5))
            }
        }
        .shadow(color: Color.black.opacity(0.22), radius: 2.5, x: 0, y: 2)
        .accessibilityLabel(markerAccessibilityLabel(for: feature, isFavorite: isFavorite))
    }

    private func markerAccessibilityLabel(for feature: GeoJSONFeature, isFavorite: Bool) -> String {
        let stationLabel = String(localized: "station.chargingStation")
        let favoritePrefix = isFavorite ? "\(String(localized: "info.legendFavorite")): " : ""
        return "\(favoritePrefix)\(stationLabel), \(feature.properties.operatorName), \(feature.properties.city), \(Int(feature.properties.displayedMaxPowerKW.rounded())) kW"
    }

    @ViewBuilder
    private func stationContextMenu(for feature: GeoJSONFeature) -> some View {
        Button {
            favoritesStore.toggle(feature.properties.stationID)
        } label: {
            let isFavorite = favoritesStore.isFavorite(feature.properties.stationID)
            let title = isFavorite
                ? String(localized: "aria.removeFavorite")
                : String(localized: "aria.saveFavorite")
            Label(
                title,
                systemImage: isFavorite ? "star.slash" : "star"
            )
        }

        Button {
            openNavigationLink(feature, google: true)
        } label: {
            Label("Google", systemImage: "location.north.line.fill")
        }

        Button {
            openNavigationLink(feature, google: false)
        } label: {
            Label("Apple", systemImage: "location.north.line.fill")
        }
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
}
