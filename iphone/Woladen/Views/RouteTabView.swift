import SwiftUI
import MapKit
import CoreLocation

private let routeFavoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)
private let routeProgressDuration: TimeInterval = 60.0
private let routeProgressIntervalNanoseconds: UInt64 = 250_000_000
private let routeProgressMaximum = 0.95

private enum RouteEndpointField: Hashable {
    case origin
    case destination
}

struct RouteTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService
    @EnvironmentObject private var favoritesStore: FavoritesStore

    @Binding var showingFilter: Bool

    @StateObject private var endpointSearchCompleter = PlaceSearchCompleter()
    @State private var originText = ""
    @State private var destinationText = ""
    @State private var originEndpoint: RouteEndpoint?
    @State private var destinationEndpoint: RouteEndpoint?
    @State private var isResolvingEndpoints = false
    @State private var activeCompletionField: RouteEndpointField?
    @State private var statusMessage = ""
    @State private var statusIsError = false
    @State private var routeMapCameraPosition: MapCameraPosition = .automatic
    @FocusState private var focusedEndpointField: RouteEndpointField?

    private var routeDisplayFeatures: [GeoJSONFeature] {
        viewModel.routeDisplayFeatures(userLocation: locationService.currentLocation)
    }

    var body: some View {
        VStack(spacing: 0) {
            routeHeader

            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    endpointForm
                    statusView
                    preRouteFilterControl
                    routeSummarySection
                    routeActionsSection
                    routeMapSection
                    recalculationNotice
                    routeResults
                }
                .padding(14)
                .frame(maxWidth: 880, alignment: .topLeading)
                .frame(maxWidth: .infinity, alignment: .top)
            }
            .refreshable {
                if viewModel.routeSummary != nil {
                    await submitRoute()
                }
            }
        }
        .onAppear {
            locationService.activate()
            applyScreenshotRouteDefaultsIfNeeded()
        }
    }

    private var routeHeader: some View {
        HStack(alignment: .center, spacing: 12) {
            Label(String(localized: "route.title"), systemImage: "point.topleft.down.curvedto.point.bottomright.up")
                .font(.title3.weight(.semibold))
                .foregroundStyle(woladenBrandColor)
                .frame(maxWidth: .infinity, alignment: .leading)
        }
        .padding(.horizontal, 16)
        .padding(.vertical, 8)
        .background(Color(.systemBackground))
        .overlay(alignment: .bottom) {
            Divider()
        }
    }

    private var endpointForm: some View {
        VStack(alignment: .leading, spacing: 12) {
            routeEndpointRow(
                field: .origin,
                title: String(localized: "route.origin"),
                placeholder: String(localized: "route.originPlaceholder"),
                text: $originText,
                endpoint: $originEndpoint
            )

            HStack(spacing: 10) {
                Spacer()
                    .frame(width: 40)

                Button {
                    swapEndpoints()
                } label: {
                    Label(String(localized: "route.swap"), systemImage: "arrow.up.arrow.down")
                        .labelStyle(.iconOnly)
                        .font(.headline.weight(.semibold))
                        .frame(width: 38, height: 38)
                        .background(Color(.systemBackground), in: Circle())
                        .overlay {
                            Circle()
                                .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
                        }
                }
                .buttonStyle(.plain)
                .accessibilityLabel(Text(String(localized: "route.swap")))

                Rectangle()
                    .fill(StationVisualStyle.controlBorder)
                    .frame(height: 1)
            }

            routeEndpointRow(
                field: .destination,
                title: String(localized: "route.destination"),
                placeholder: String(localized: "route.destinationPlaceholder"),
                text: $destinationText,
                endpoint: $destinationEndpoint
            )

            Button {
                Task { await submitRoute() }
            } label: {
                if isResolvingEndpoints || viewModel.isLoadingRoute {
                    ProgressView()
                        .controlSize(.small)
                        .frame(maxWidth: .infinity)
                } else {
                    Text(String(localized: "route.submit"))
                        .frame(maxWidth: .infinity)
                }
            }
            .buttonStyle(.borderedProminent)
            .controlSize(.large)
            .disabled(isResolvingEndpoints || viewModel.isLoadingRoute)
        }
        .padding(14)
        .background(StationVisualStyle.controlSurface, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
    }

    private func routeEndpointRow(
        field: RouteEndpointField,
        title: String,
        placeholder: String,
        text: Binding<String>,
        endpoint: Binding<RouteEndpoint?>
    ) -> some View {
        HStack(alignment: .top, spacing: 10) {
            currentLocationButton(for: field)
                .padding(.top, 23)

            VStack(alignment: .leading, spacing: 6) {
                Text(title)
                    .font(.caption.weight(.semibold))
                    .foregroundStyle(.secondary)

                TextField(placeholder, text: text)
                    .textInputAutocapitalization(.words)
                    .disableAutocorrection(true)
                    .submitLabel(.search)
                    .focused($focusedEndpointField, equals: field)
                    .onChange(of: text.wrappedValue) { _, newValue in
                        let trimmed = newValue.trimmingCharacters(in: .whitespacesAndNewlines)
                        if endpoint.wrappedValue?.label != trimmed {
                            endpoint.wrappedValue = nil
                        }
                        statusMessage = ""
                        updateCompletions(for: field, query: trimmed, endpoint: endpoint.wrappedValue)
                    }
                    .onChange(of: focusedEndpointField) { _, newValue in
                        if newValue == field {
                            updateCompletions(
                                for: field,
                                query: text.wrappedValue,
                                endpoint: endpoint.wrappedValue
                            )
                        }
                    }
                    .onSubmit {
                        Task { await resolveTypedEndpoint(for: field) }
                    }
                    .font(.body)
                    .padding(.horizontal, 12)
                    .frame(height: 42)
                    .background(Color(.systemBackground), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
                    .overlay {
                        RoundedRectangle(cornerRadius: 8, style: .continuous)
                            .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
                    }

                completionList(for: field)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func currentLocationButton(for field: RouteEndpointField) -> some View {
        Button {
            useCurrentLocation(for: field)
        } label: {
            Image(systemName: "location.fill")
                .font(.headline)
                .frame(width: 40, height: 40)
                .background(Color(.systemBackground), in: Circle())
        }
        .buttonStyle(.plain)
        .accessibilityLabel(Text(String(localized: "route.useCurrent")))
    }

    @ViewBuilder
    private func completionList(for field: RouteEndpointField) -> some View {
        if activeCompletionField == field,
           focusedEndpointField == field,
           !endpointSearchCompleter.completions.isEmpty {
            PlaceCompletionList(completions: endpointSearchCompleter.completions) { completion in
                Task { await selectEndpointCompletion(completion, for: field) }
            }
            .background(Color(.systemBackground), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 8, style: .continuous)
                    .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
            }
        }
    }

    @ViewBuilder
    private var statusView: some View {
        let routeError = viewModel.routeError
        if viewModel.isLoadingRoute {
            RouteLoadingProgressView()
        } else if !statusMessage.isEmpty {
            Text(statusMessage)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(statusIsError ? Color.red : Color.teal)
        } else if let routeError {
            Text(routeError)
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(Color.red)
        }
    }

    @ViewBuilder
    private var routeSummarySection: some View {
        if let route = viewModel.routeSummary {
            HStack(spacing: 8) {
                routeSummaryStat(label: String(localized: "route.summaryDistanceKm"), value: formatRouteDistanceKilometers(route.distanceM))
                routeSummaryStat(label: String(localized: "route.summaryDuration"), value: formatRouteClockDuration(route.durationS))
                routeSummaryStat(label: String(localized: "route.summaryStations"), value: "\(routeDisplayFeatures.count)")
            }
        }
    }

    private func routeSummaryStat(label: String, value: String) -> some View {
        VStack(alignment: .center, spacing: 2) {
            Text(label)
                .font(.caption)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
            Text(value)
                .font(.headline)
                .monospacedDigit()
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity, alignment: .center)
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(Color(.secondarySystemBackground), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
    }

    @ViewBuilder
    private var preRouteFilterControl: some View {
        if viewModel.routeSummary == nil, !viewModel.isLoadingRoute {
            HStack {
                Spacer(minLength: 0)
                routeFilterButton()
                Spacer(minLength: 0)
            }
        }
    }

    @ViewBuilder
    private var routeActionsSection: some View {
        let features = routeDisplayFeatures
        if viewModel.routeSummary != nil, !viewModel.isLoadingRoute, !features.isEmpty {
            HStack(spacing: 8) {
                Button {
                    addRouteResultsToFavorites(features)
                } label: {
                    Label(
                        String(localized: "route.addAllFavoritesShort")
                            .replacingOccurrences(of: "{count}", with: "\(features.count)"),
                        systemImage: "star.fill"
                    )
                    .lineLimit(1)
                }
                .buttonStyle(.bordered)
                .tint(Color.blue)

                routeFilterButton()

                Button {
                    viewModel.clearRoute()
                    statusMessage = ""
                } label: {
                    Image(systemName: "trash")
                        .frame(width: 30, height: 20)
                }
                .buttonStyle(.bordered)
                .tint(Color.blue)
                .accessibilityLabel(Text(String(localized: "route.removeRoute")))
            }
            .frame(maxWidth: .infinity)
        }
    }

    private func routeFilterButton() -> some View {
        Button {
            showingFilter = true
        } label: {
            HStack(spacing: 6) {
                Image(systemName: "line.3.horizontal.decrease.circle")
                Text(String(localized: "filters.title"))
            }
            .lineLimit(1)
            .font(.subheadline.weight(.semibold))
        }
        .buttonStyle(.bordered)
        .tint(Color.blue)
        .overlay(alignment: .topTrailing) {
            if viewModel.routeFilterActiveCount() > 0 {
                Text("\(viewModel.routeFilterActiveCount())")
                    .font(.caption2.weight(.bold))
                    .foregroundStyle(.white)
                    .monospacedDigit()
                    .frame(minWidth: 18, minHeight: 18)
                    .padding(.horizontal, 2)
                    .background(woladenBrandColor, in: Capsule())
                    .offset(x: 7, y: -7)
                    .accessibilityHidden(true)
            }
        }
        .accessibilityLabel(Text(String(localized: "aria.filterOpen")))
    }

    @ViewBuilder
    private var routeMapSection: some View {
        let features = routeDisplayFeatures
        if let route = viewModel.routeSummary, !viewModel.isLoadingRoute {
            let coordinates = routePolylineCoordinates(route)
            if coordinates.count > 1 || !features.isEmpty {
                let cameraKey = routeMapCameraKey(route: route, features: features)
                Map(position: $routeMapCameraPosition, interactionModes: [.pan, .zoom]) {
                    if coordinates.count > 1 {
                        MapPolyline(coordinates: coordinates)
                            .stroke(woladenBrandColor, style: StrokeStyle(lineWidth: 5, lineCap: .round, lineJoin: .round))
                    }

                    ForEach(features) { feature in
                        Annotation("", coordinate: feature.coordinate) {
                            Button {
                                viewModel.selectFeature(feature)
                            } label: {
                                routeMapMarker(for: feature)
                            }
                            .buttonStyle(.plain)
                        }
                    }
                }
                .frame(height: 220)
                .clipShape(RoundedRectangle(cornerRadius: 8, style: .continuous))
                .overlay {
                    RoundedRectangle(cornerRadius: 8, style: .continuous)
                        .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
                }
                .onAppear {
                    updateRouteMapCamera(route: route, features: features)
                }
                .onChange(of: cameraKey) { _, _ in
                    updateRouteMapCamera(route: route, features: features)
                }
            }
        }
    }

    @ViewBuilder
    private var recalculationNotice: some View {
        if viewModel.routeSummary != nil, viewModel.routeFiltersRequireRecalculation() {
            HStack(spacing: 10) {
                Text(String(localized: "route.filterChanged"))
                    .font(.subheadline)
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
                Button(String(localized: "route.recalculate")) {
                    Task { await submitRoute() }
                }
                .buttonStyle(.bordered)
            }
            .padding(12)
            .background(Color.orange.opacity(0.12), in: RoundedRectangle(cornerRadius: 8, style: .continuous))
        }
    }

    @ViewBuilder
    private var routeResults: some View {
        let features = routeDisplayFeatures
        if viewModel.isLoadingRoute {
            EmptyView()
        } else if viewModel.routeSummary == nil {
            ContentUnavailableView(String(localized: "route.empty"), systemImage: "point.topleft.down.curvedto.point.bottomright.up")
                .frame(maxWidth: .infinity)
                .padding(.top, 24)
        } else if features.isEmpty {
            ContentUnavailableView(String(localized: "route.noFilteredResults"), systemImage: "bolt.slash")
                .frame(maxWidth: .infinity)
                .padding(.top, 24)
        } else {
            LazyVStack(spacing: 10) {
                ForEach(features) { feature in
                    Button {
                        viewModel.selectFeature(feature)
                    } label: {
                        routeRow(feature)
                    }
                    .buttonStyle(.plain)
                }
            }
        }
    }

    private func routeRow(_ feature: GeoJSONFeature) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .top, spacing: 8) {
                Image(systemName: favoritesStore.isFavorite(feature.properties.stationID) ? "star.fill" : "bolt.fill")
                    .foregroundStyle(favoritesStore.isFavorite(feature.properties.stationID) ? routeFavoriteStarColor : woladenBrandColor)
                    .frame(width: 24)
                VStack(alignment: .leading, spacing: 4) {
                    Text(feature.properties.operatorName)
                        .font(.headline)
                        .foregroundStyle(.primary)
                        .lineLimit(2)
                    Text("\(feature.properties.city) • \(Int(feature.properties.displayedMaxPowerKW.rounded())) kW • \(chargingPointLabel(feature.properties.chargingPointsCount))")
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                    if let line = routeLine(feature) {
                        Text(line)
                            .font(.caption.weight(.semibold))
                            .foregroundStyle(woladenBrandColor)
                    }
                }
                Spacer(minLength: 0)
            }

            if let occupancy = feature.occupancySummaryLabel {
                Text(occupancy)
                    .font(.caption.weight(.semibold))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(statusColor(for: feature.availabilityStatus).opacity(0.16), in: Capsule())
                    .foregroundStyle(statusColor(for: feature.availabilityStatus))
            } else if !feature.displayPrice.isEmpty {
                Text(feature.displayPrice)
                    .font(.caption.weight(.semibold))
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(Color.green.opacity(0.14), in: Capsule())
                    .foregroundStyle(Color.green)
            }
        }
        .padding(14)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(feature.stationCardBackground, in: RoundedRectangle(cornerRadius: 8, style: .continuous))
        .overlay {
            RoundedRectangle(cornerRadius: 8, style: .continuous)
                .stroke(feature.stationCardBorder, lineWidth: 1)
        }
    }

    private func routeMapMarker(for feature: GeoJSONFeature) -> some View {
        let isFavorite = favoritesStore.isFavorite(feature.properties.stationID)
        return Group {
            if isFavorite {
                Image(systemName: "star.fill")
                    .font(.system(size: 16, weight: .bold))
                    .foregroundStyle(routeFavoriteStarColor)
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
                    .fill(routeMarkerColor(for: feature))
                    .frame(width: 16, height: 16)
                    .overlay(Circle().stroke(Color.white, lineWidth: 1.5))
            }
        }
        .shadow(color: Color.black.opacity(0.22), radius: 2.5, x: 0, y: 2)
        .accessibilityLabel(Text("\(feature.properties.operatorName), \(feature.properties.city)"))
    }

    private func routeMarkerColor(for feature: GeoJSONFeature) -> Color {
        switch viewModel.markerTint(for: feature) {
        case "gold": return StationVisualStyle.amenityGold
        case "silver": return StationVisualStyle.amenitySilver
        case "bronze": return StationVisualStyle.amenityBronze
        default: return StationVisualStyle.amenityGrey
        }
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

    private func routeMapCameraKey(route: RouteSummary, features: [GeoJSONFeature]) -> String {
        let ids = features.map(\.id).joined(separator: ",")
        return "\(route.distanceM):\(route.durationS):\(route.geometry.coordinates.count):\(ids)"
    }

    private func updateRouteMapCamera(route: RouteSummary, features: [GeoJSONFeature]) {
        guard let rect = routeMapRect(route: route, features: features) else { return }
        routeMapCameraPosition = .rect(rect)
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

    private func applyScreenshotRouteDefaultsIfNeeded() {
        guard let endpoints = AppStoreScreenshotRouteEndpoints() else { return }
        guard originEndpoint == nil, destinationEndpoint == nil else { return }
        originEndpoint = endpoints.origin
        destinationEndpoint = endpoints.destination
        originText = endpoints.origin.label
        destinationText = endpoints.destination.label
    }

    private func submitRoute() async {
        isResolvingEndpoints = true
        statusMessage = String(localized: "route.resolving")
        statusIsError = false
        endpointSearchCompleter.clear()
        focusedEndpointField = nil
        defer { isResolvingEndpoints = false }

        let origin = await resolvedEndpoint(existing: originEndpoint, text: originText)
        let destination = await resolvedEndpoint(existing: destinationEndpoint, text: destinationText)

        guard let origin, let destination else {
            statusMessage = String(localized: "route.missingEndpoints")
            statusIsError = true
            return
        }
        guard CLLocation(latitude: origin.lat, longitude: origin.lon)
            .distance(from: CLLocation(latitude: destination.lat, longitude: destination.lon)) >= 25 else {
            statusMessage = String(localized: "route.sameEndpoint")
            statusIsError = true
            return
        }

        originEndpoint = origin
        destinationEndpoint = destination
        statusMessage = ""
        viewModel.searchRoute(origin: origin, destination: destination)
    }

    private func resolveTypedEndpoint(for field: RouteEndpointField) async {
        let text = textBinding(for: field).wrappedValue
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        isResolvingEndpoints = true
        defer { isResolvingEndpoints = false }
        guard let endpoint = await resolvedEndpoint(existing: endpointBinding(for: field).wrappedValue, text: text) else {
            statusMessage = String(localized: "search.noResults")
            statusIsError = true
            return
        }
        setEndpoint(endpoint, for: field)
    }

    private func resolvedEndpoint(existing: RouteEndpoint?, text: String) async -> RouteEndpoint? {
        if let existing {
            return existing
        }
        let query = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard query.count >= 2 else { return nil }

        do {
            let request = MKLocalSearch.Request()
            request.naturalLanguageQuery = query
            if let current = locationService.currentLocation {
                request.region = searchRegion(around: current)
            }
            let response = try await MKLocalSearch(request: request).start()
            guard let item = response.mapItems.first else { return nil }
            return RouteEndpoint(
                coordinate: item.placemark.coordinate,
                label: placeLabel(for: item, fallback: query)
            )
        } catch {
            return nil
        }
    }

    private func selectEndpointCompletion(_ completion: MKLocalSearchCompletion, for field: RouteEndpointField) async {
        guard !isResolvingEndpoints else { return }
        isResolvingEndpoints = true
        defer { isResolvingEndpoints = false }

        do {
            let request = MKLocalSearch.Request(completion: completion)
            if let current = locationService.currentLocation {
                request.region = searchRegion(around: current)
            }
            let response = try await MKLocalSearch(request: request).start()
            guard let item = response.mapItems.first else {
                statusMessage = String(localized: "search.noResults")
                statusIsError = true
                return
            }
            let endpoint = RouteEndpoint(
                coordinate: item.placemark.coordinate,
                label: placeLabel(for: item, fallback: completion.title)
            )
            setEndpoint(endpoint, for: field)
        } catch {
            statusMessage = String(localized: "search.unavailable")
            statusIsError = true
        }
    }

    private func useCurrentLocation(for field: RouteEndpointField) {
        locationService.requestAuthorization()
        guard let current = locationService.currentLocation else {
            statusMessage = String(localized: "route.locationUnavailable")
            statusIsError = true
            return
        }
        let endpoint = RouteEndpoint(coordinate: current.coordinate, label: String(localized: "route.currentLocation"))
        setEndpoint(endpoint, for: field)
    }

    private func setEndpoint(_ endpoint: RouteEndpoint, for field: RouteEndpointField) {
        guard endpointIsDistinct(endpoint, for: field) else {
            statusMessage = String(localized: "route.sameEndpoint")
            statusIsError = true
            return
        }
        textBinding(for: field).wrappedValue = endpoint.label
        endpointBinding(for: field).wrappedValue = endpoint
        focusedEndpointField = nil
        activeCompletionField = nil
        endpointSearchCompleter.clear()
        statusMessage = ""
        statusIsError = false
    }

    private func endpointIsDistinct(_ endpoint: RouteEndpoint, for field: RouteEndpointField) -> Bool {
        let other = field == .origin ? destinationEndpoint : originEndpoint
        guard let other else { return true }
        let endpointLocation = CLLocation(latitude: endpoint.lat, longitude: endpoint.lon)
        let otherLocation = CLLocation(latitude: other.lat, longitude: other.lon)
        return endpointLocation.distance(from: otherLocation) >= 25
    }

    private func updateCompletions(for field: RouteEndpointField, query: String, endpoint: RouteEndpoint?) {
        guard focusedEndpointField == field else { return }
        guard endpoint?.label != query else {
            endpointSearchCompleter.clear()
            return
        }
        activeCompletionField = field
        endpointSearchCompleter.update(query: query, region: locationService.currentLocation.map(searchRegion))
    }

    private func searchRegion(around location: CLLocation) -> MKCoordinateRegion {
        MKCoordinateRegion(
            center: location.coordinate,
            span: MKCoordinateSpan(latitudeDelta: 3.0, longitudeDelta: 3.0)
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

    private func textBinding(for field: RouteEndpointField) -> Binding<String> {
        field == .origin ? $originText : $destinationText
    }

    private func endpointBinding(for field: RouteEndpointField) -> Binding<RouteEndpoint?> {
        field == .origin ? $originEndpoint : $destinationEndpoint
    }

    private func swapEndpoints() {
        swap(&originText, &destinationText)
        swap(&originEndpoint, &destinationEndpoint)
        focusedEndpointField = nil
        activeCompletionField = nil
        endpointSearchCompleter.clear()
        statusMessage = ""
    }

    private func addRouteResultsToFavorites(_ features: [GeoJSONFeature]) {
        let stationIDs = features.map { $0.properties.stationID }.filter { !$0.isEmpty }
        guard !stationIDs.isEmpty else { return }
        let category = routeFavoriteCategoryLabel()
        favoritesStore.addRouteFavorites(stationIDs: stationIDs, category: category)
        statusMessage = String(localized: "route.favoritesAdded")
            .replacingOccurrences(of: "{count}", with: "\(stationIDs.count)")
            .replacingOccurrences(of: "{category}", with: category)
        statusIsError = false
    }

    private func routeFavoriteCategoryLabel() -> String {
        let origin = compactEndpointLabel(originEndpoint?.label ?? originText, fallback: String(localized: "route.origin"))
        let destination = compactEndpointLabel(destinationEndpoint?.label ?? destinationText, fallback: String(localized: "route.destination"))
        return normalizeCategoryLabel(
            String(localized: "route.favoriteCategory")
                .replacingOccurrences(of: "{origin}", with: origin)
                .replacingOccurrences(of: "{destination}", with: destination)
        )
    }

    private func compactEndpointLabel(_ value: String, fallback: String) -> String {
        let text = value.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        return text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.first { !$0.isEmpty } ?? fallback
    }

    private func routeLine(_ feature: GeoJSONFeature) -> String? {
        guard let route = feature.routeMetadata else { return nil }
        var parts: [String] = []
        if route.driveDistanceToRouteM > 0 {
            parts.append(
                String(localized: "route.cardAccess")
                    .replacingOccurrences(of: "{distance}", with: formatRouteDistance(route.driveDistanceToRouteM))
            )
        }
        if route.routePositionM > 0 {
            parts.append(
                String(localized: "route.cardPosition")
                    .replacingOccurrences(of: "{distance}", with: formatRouteDistance(route.routePositionM))
            )
        }
        return parts.isEmpty ? nil : parts.joined(separator: " • ")
    }

    private func chargingPointLabel(_ count: Int) -> String {
        let template = count == 1
            ? String(localized: "station.chargingPointOne")
            : String(localized: "station.chargingPointMany")
        return template.replacingOccurrences(of: "{count}", with: "\(count)")
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

private struct RouteLoadingProgressView: View {
    @State private var progress = 0.0

    var body: some View {
        let isHoldingProgress = progress >= routeProgressMaximum
        let messageKey: String.LocalizationValue = isHoldingProgress ? "route.loadingStill" : "route.loading"

        VStack(spacing: 10) {
            Text(String(localized: messageKey))
                .font(.headline)
                .multilineTextAlignment(.center)
                .foregroundStyle(.primary)
            ProgressView(value: progress, total: 1)
                .progressViewStyle(.linear)
                .tint(woladenBrandColor)
                .frame(maxWidth: 420)
            Text("\(Int((progress * 100).rounded()))%")
                .font(.subheadline.weight(.semibold))
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity)
        .padding(.vertical, 18)
        .task {
            await runProgress()
        }
    }

    private func runProgress() async {
        let startedAt = Date()
        while !Task.isCancelled {
            let elapsed = Date().timeIntervalSince(startedAt)
            let ratio = min(max(elapsed / routeProgressDuration, 0), 1)
            let eased = 1 - ((1 - ratio) * (1 - ratio))
            let nextProgress = min(routeProgressMaximum, routeProgressMaximum * eased)
            await MainActor.run {
                progress = nextProgress
            }
            try? await Task.sleep(nanoseconds: routeProgressIntervalNanoseconds)
        }
    }
}

func formatRouteDistance(_ meters: Int) -> String {
    if meters >= 10_000 {
        return "\(Int((Double(meters) / 1000.0).rounded())) km"
    }
    if meters >= 1000 {
        return String(format: "%.1f km", Double(meters) / 1000.0)
    }
    return "\(meters) m"
}

func formatRouteDistanceKilometers(_ meters: Int) -> String {
    let kilometers = Double(meters) / 1000.0
    if meters >= 10_000 {
        return "\(Int(kilometers.rounded()))"
    }
    return String(format: "%.1f", kilometers)
}

func formatRouteClockDuration(_ seconds: Int) -> String {
    guard seconds > 0 else { return "00:00" }
    let minutes = max(1, Int((Double(seconds) / 60.0).rounded()))
    return String(format: "%02d:%02d", minutes / 60, minutes % 60)
}

func formatRouteDuration(_ seconds: Int) -> String {
    guard seconds > 0 else { return "" }
    let minutes = max(1, Int((Double(seconds) / 60.0).rounded()))
    let hours = minutes / 60
    let remainder = minutes % 60
    if hours > 0, remainder > 0 {
        return String(localized: "route.durationHoursMinutes")
            .replacingOccurrences(of: "{hours}", with: "\(hours)")
            .replacingOccurrences(of: "{minutes}", with: "\(remainder)")
    }
    if hours > 0 {
        return String(localized: "route.durationHours")
            .replacingOccurrences(of: "{hours}", with: "\(hours)")
    }
    return String(localized: "route.durationMinutes")
        .replacingOccurrences(of: "{minutes}", with: "\(minutes)")
}
