import Foundation
import CoreLocation

@MainActor
final class AppViewModel: ObservableObject {
    enum AppTab: Hashable {
        case list
        case map
        case favorites
        case info
    }

    @Published private(set) var allFeatures: [GeoJSONFeature] = []
    @Published private(set) var discoveredFeatures: [GeoJSONFeature] = []
    @Published private(set) var operators: [OperatorEntry] = []
    @Published var filterState = FilterState()
    @Published var selectedFeature: GeoJSONFeature?
    @Published var selectedTab: AppTab = .list
    @Published private(set) var loadError: String?
    @Published private(set) var isLoading: Bool = false
    @Published private(set) var isAwaitingFirstLocationFix: Bool = false
    @Published private(set) var activeBundleInfo: ActiveDataBundleInfo?

    private let liveAPIClient = LiveAPIClient()
    private let maxVisibleChargers = 20
    private let liveRefreshInterval: TimeInterval = 15

    private var filterPool: [GeoJSONFeature] = []
    private var discoveredByID: [String: GeoJSONFeature] = [:]
    private var discoveredOrder: [String] = []
    private var didSeedFromUserLocation = false

    private var liveSummaryFetchedAtByStationID: [String: Date] = [:]
    private var liveDetailFetchedAtByStationID: [String: Date] = [:]
    private var pendingLiveSummaryStationIDs: Set<String> = []
    private var pendingLiveDetailStationIDs: Set<String> = []
    private var liveSummaryRefreshTask: Task<Void, Never>?
    private var selectedFeatureRefreshTask: Task<Void, Never>?

    init() {
        startLiveSummaryRefreshLoop()
    }

    deinit {
        liveSummaryRefreshTask?.cancel()
        selectedFeatureRefreshTask?.cancel()
    }

    func load(userLocation: CLLocation?) {
        isLoading = true
        loadError = nil

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let result: Result<(features: [GeoJSONFeature], operators: [OperatorEntry], bundle: ActiveDataBundleInfo), Error>
            do {
                result = .success(try ChargerRepository().loadData())
            } catch {
                result = .failure(error)
            }

            DispatchQueue.main.async {
                guard let self else { return }
                self.isLoading = false
                switch result {
                case .success(let loaded):
                    self.resetLiveState()
                    self.allFeatures = loaded.features
                    self.operators = loaded.operators
                    self.activeBundleInfo = loaded.bundle
                    self.loadError = nil
                    self.didSeedFromUserLocation = false
                    self.applyFilters(userLocation: userLocation)
                case .failure(let error):
                    self.loadError = error.localizedDescription
                    self.allFeatures = []
                    self.filterPool = []
                    self.discoveredFeatures = []
                    self.operators = []
                    self.activeBundleInfo = nil
                    self.isAwaitingFirstLocationFix = false
                    self.resetLiveState()
                }
            }
        }
    }

    func reloadDataAfterBundleUpdate(userLocation: CLLocation?) {
        load(userLocation: userLocation)
    }

    func applyFilters(userLocation: CLLocation?) {
        filterPool = allFeatures.filter { feature in
            feature.properties.matches(filterState)
        }
        resetDiscoveredList()
        if let userLocation {
            discoverNearby(
                center: userLocation.coordinate,
                resetHistory: false
            )
        } else {
            didSeedFromUserLocation = false
            isAwaitingFirstLocationFix = !allFeatures.isEmpty
            discoveredFeatures = []
        }
    }

    func handleMapCenterChange(_ center: CLLocationCoordinate2D) {
        discoverNearby(center: center, resetHistory: false)
    }

    func seedFromInitialUserLocation(_ location: CLLocation?) {
        guard let location else { return }
        guard !allFeatures.isEmpty else { return }
        if !didSeedFromUserLocation {
            // Start charger discovery from the first real location fix.
            applyFilters(userLocation: location)
        }
    }

    func reloadListForCurrentLocation(_ location: CLLocation?) {
        guard !allFeatures.isEmpty else { return }
        guard let location else {
            isAwaitingFirstLocationFix = true
            return
        }
        applyFilters(userLocation: location)
    }

    func reloadMapForCenter(_ center: CLLocationCoordinate2D?) {
        guard !allFeatures.isEmpty else { return }
        guard let center else {
            isAwaitingFirstLocationFix = true
            return
        }
        discoverNearby(center: center, resetHistory: false)
    }

    func selectFeature(_ feature: GeoJSONFeature) {
        let stationID = feature.properties.stationID
        selectedFeature = self.feature(forStationID: stationID) ?? feature
        startSelectedFeatureRefresh(for: stationID)
    }

    func clearSelectedFeature() {
        selectedFeature = nil
        selectedFeatureRefreshTask?.cancel()
        selectedFeatureRefreshTask = nil
    }

    func feature(forStationID stationID: String) -> GeoJSONFeature? {
        allFeatures.first(where: { $0.properties.stationID == stationID })
            ?? discoveredFeatures.first(where: { $0.properties.stationID == stationID })
            ?? selectedFeature.flatMap { $0.properties.stationID == stationID ? $0 : nil }
    }

    func favoritesFeatures(_ favorites: Set<String>, userLocation: CLLocation?) -> [GeoJSONFeature] {
        var items = allFeatures.filter { favorites.contains($0.properties.stationID) }
        if let userLocation {
            items.sort { lhs, rhs in
                distance(from: userLocation, to: lhs.coordinate) < distance(from: userLocation, to: rhs.coordinate)
            }
        }
        return items
    }

    func refreshFavoritesLiveSummaries(_ favorites: Set<String>, force: Bool = false) async {
        await requestLiveSummaries(forStationIDs: Array(favorites), force: force)
    }

    func distanceText(from userLocation: CLLocation?, to coordinate: CLLocationCoordinate2D) -> String? {
        guard let userLocation else { return nil }
        let meters = distance(from: userLocation, to: coordinate)
        if meters >= 1000 { return String(format: "%.1f km", meters / 1000) }
        return "\(Int(meters.rounded())) m"
    }

    func markerTint(for feature: GeoJSONFeature) -> String {
        let total = feature.properties.amenitiesTotal
        if total > 10 { return "gold" }
        if total > 5 { return "silver" }
        if total > 0 { return "bronze" }
        return "gray"
    }

    func humanReadableBundleSource() -> String {
        guard let activeBundleInfo else { return "unbekannt" }
        if activeBundleInfo.source == "installed" {
            return "Installiertes Datenbundle (\(activeBundleInfo.manifest.version))"
        }
        return "In der App gebündeltes Baseline-Datenbundle"
    }

    func requestLiveDetailIfNeeded(for stationID: String, force: Bool = false) async {
        let trimmed = stationID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        guard liveAPIClient.isEnabled else { return }
        guard !pendingLiveDetailStationIDs.contains(trimmed) else { return }

        let now = Date()
        if !force,
           let lastFetch = liveDetailFetchedAtByStationID[trimmed],
           now.timeIntervalSince(lastFetch) < liveRefreshInterval {
            return
        }

        pendingLiveDetailStationIDs.insert(trimmed)
        defer {
            pendingLiveDetailStationIDs.remove(trimmed)
        }

        do {
            let detail = try await liveAPIClient.stationDetail(stationID: trimmed)
            liveDetailFetchedAtByStationID[trimmed] = now
            liveSummaryFetchedAtByStationID[trimmed] = now
            applyLiveDetail(detail, stationID: trimmed)
        } catch {
            // Keep offline behavior intact by silently falling back to bundled data.
        }
    }

    private func requestLiveSummaries(forStationIDs stationIDs: [String], force: Bool = false) async {
        guard liveAPIClient.isEnabled else { return }

        let ids = Array(
            Set(
                stationIDs
                    .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                    .filter { !$0.isEmpty }
            )
        )
        guard !ids.isEmpty else { return }

        let now = Date()
        let eligibleIDs = ids.filter { stationID in
            if pendingLiveSummaryStationIDs.contains(stationID) {
                return false
            }
            guard !force else {
                return true
            }
            guard let lastFetch = liveSummaryFetchedAtByStationID[stationID] else {
                return true
            }
            return now.timeIntervalSince(lastFetch) >= liveRefreshInterval
        }
        guard !eligibleIDs.isEmpty else { return }

        pendingLiveSummaryStationIDs.formUnion(eligibleIDs)
        defer {
            eligibleIDs.forEach { pendingLiveSummaryStationIDs.remove($0) }
        }

        do {
            let response = try await liveAPIClient.lookupStations(stationIDs: eligibleIDs)
            let fetchedAt = Date()
            let stationIDs = Set(response.stations.map(\.stationID)).union(response.missingStationIDs)
            stationIDs.forEach { liveSummaryFetchedAtByStationID[$0] = fetchedAt }
            applyLiveSummaries(response.stations, missingStationIDs: response.missingStationIDs)
        } catch {
            // Keep offline behavior intact by silently falling back to bundled data.
        }
    }

    private func startLiveSummaryRefreshLoop() {
        liveSummaryRefreshTask?.cancel()
        liveSummaryRefreshTask = Task { [weak self] in
            guard let self else { return }
            while !Task.isCancelled {
                await self.refreshTrackedLiveSummaries()
                try? await Task.sleep(nanoseconds: UInt64(self.liveRefreshInterval * 1_000_000_000))
            }
        }
    }

    private func refreshTrackedLiveSummaries(force: Bool = false) async {
        await requestLiveSummaries(forStationIDs: trackedStationIDs(), force: force)
    }

    private func trackedStationIDs() -> [String] {
        var ids = Set(discoveredFeatures.map { $0.properties.stationID })
        if let selectedFeature {
            ids.insert(selectedFeature.properties.stationID)
        }
        return Array(ids)
    }

    private func startSelectedFeatureRefresh(for stationID: String) {
        selectedFeatureRefreshTask?.cancel()
        selectedFeatureRefreshTask = Task { [weak self] in
            guard let self else { return }
            await self.requestLiveDetailIfNeeded(for: stationID, force: true)
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: UInt64(self.liveRefreshInterval * 1_000_000_000))
                guard self.selectedFeature?.properties.stationID == stationID else { return }
                await self.requestLiveDetailIfNeeded(for: stationID, force: true)
            }
        }
    }

    private func applyLiveSummaries(_ summaries: [LiveStationSummary], missingStationIDs: [String]) {
        let summaryByStationID = Dictionary(uniqueKeysWithValues: summaries.map { ($0.stationID, $0) })
        let missingIDs = Set(missingStationIDs)
        let affectedStationIDs = Set(summaryByStationID.keys).union(missingIDs)
        guard !affectedStationIDs.isEmpty else { return }

        updateFeatureCollections(for: affectedStationIDs) { feature in
            var updated = feature
            let stationID = feature.properties.stationID
            if let summary = summaryByStationID[stationID] {
                updated.liveSummary = summary
            } else if missingIDs.contains(stationID) {
                updated.liveSummary = nil
            }
            return updated
        }
    }

    private func applyLiveDetail(_ detail: LiveStationDetail, stationID: String) {
        updateFeatureCollections(for: [stationID]) { feature in
            var updated = feature
            updated.liveSummary = detail.station
            updated.liveDetail = detail
            return updated
        }
    }

    private func updateFeatureCollections(for stationIDs: Set<String>, update: (GeoJSONFeature) -> GeoJSONFeature) {
        guard !stationIDs.isEmpty else { return }
        allFeatures = allFeatures.map { feature in
            stationIDs.contains(feature.properties.stationID) ? update(feature) : feature
        }
        filterPool = filterPool.map { feature in
            stationIDs.contains(feature.properties.stationID) ? update(feature) : feature
        }
        discoveredByID = discoveredByID.mapValues { feature in
            stationIDs.contains(feature.properties.stationID) ? update(feature) : feature
        }
        discoveredFeatures = discoveredFeatures.map { feature in
            stationIDs.contains(feature.properties.stationID) ? update(feature) : feature
        }
        if let selectedFeature, stationIDs.contains(selectedFeature.properties.stationID) {
            self.selectedFeature = update(selectedFeature)
        }
    }

    private func updateFeatureCollections(for stationIDs: [String], update: (GeoJSONFeature) -> GeoJSONFeature) {
        updateFeatureCollections(for: Set(stationIDs), update: update)
    }

    private func resetLiveState() {
        liveSummaryFetchedAtByStationID = [:]
        liveDetailFetchedAtByStationID = [:]
        pendingLiveSummaryStationIDs = []
        pendingLiveDetailStationIDs = []
        clearSelectedFeature()
    }

    private func distance(from userLocation: CLLocation, to coordinate: CLLocationCoordinate2D) -> CLLocationDistance {
        let target = CLLocation(latitude: coordinate.latitude, longitude: coordinate.longitude)
        return userLocation.distance(from: target)
    }

    private func distance(from coordinate: CLLocationCoordinate2D, to target: CLLocationCoordinate2D) -> CLLocationDistance {
        let lhs = CLLocation(latitude: coordinate.latitude, longitude: coordinate.longitude)
        let rhs = CLLocation(latitude: target.latitude, longitude: target.longitude)
        return lhs.distance(from: rhs)
    }

    private func resetDiscoveredList() {
        discoveredByID = [:]
        discoveredOrder = []
        discoveredFeatures = []
    }

    private func discoverNearby(center: CLLocationCoordinate2D, resetHistory: Bool) {
        didSeedFromUserLocation = true
        isAwaitingFirstLocationFix = false
        if resetHistory {
            resetDiscoveredList()
        }
        refreshNearby(center: center)
    }

    private func refreshNearby(center: CLLocationCoordinate2D) {
        guard !filterPool.isEmpty else {
            discoveredFeatures = []
            return
        }

        let nearest = filterPool
            .map { feature in
                (feature: feature, distance: distance(from: center, to: feature.coordinate))
            }
            .sorted { lhs, rhs in lhs.distance < rhs.distance }
            .prefix(maxVisibleChargers)
            .map { $0.feature }

        for feature in nearest {
            if discoveredByID[feature.id] == nil {
                discoveredOrder.append(feature.id)
            }
            discoveredByID[feature.id] = feature
        }
        discoveredFeatures = discoveredOrder.compactMap { discoveredByID[$0] }

        Task {
            await requestLiveSummaries(forStationIDs: nearest.map { $0.properties.stationID })
        }
    }
}
