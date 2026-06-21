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
    @Published private(set) var activeCatalogInfo: ActiveCatalogSourceInfo?

    private let liveAPIClient: LiveAPIClient
    private let repository: ChargerRepository
    private let maxVisibleChargers = 20
    private let liveRefreshInterval: TimeInterval = 15
    private let catalogReloadDistanceM: CLLocationDistance = 10_000

    private var filterPool: [GeoJSONFeature] = []
    private var discoveredByID: [String: GeoJSONFeature] = [:]
    private var discoveredOrder: [String] = []
    private var didSeedFromUserLocation = false
    private var currentCatalogCenter = ChargerRepository.defaultCatalogCenter
    private var favoriteDetailFeatures: [String: GeoJSONFeature] = [:]

    private var liveSummaryFetchedAtByStationID: [String: Date] = [:]
    private var liveDetailFetchedAtByStationID: [String: Date] = [:]
    private var pendingLiveSummaryStationIDs: Set<String> = []
    private var pendingLiveDetailStationIDs: Set<String> = []
    private var pendingStaticDetailStationIDs: Set<String> = []
    private var catalogLoadTask: Task<Void, Never>?
    private var liveSummaryRefreshTask: Task<Void, Never>?
    private var selectedFeatureRefreshTask: Task<Void, Never>?

    init(liveAPIClient: LiveAPIClient = LiveAPIClient()) {
        self.liveAPIClient = liveAPIClient
        self.repository = ChargerRepository(liveAPIClient: liveAPIClient)
        startLiveSummaryRefreshLoop()
    }

    deinit {
        catalogLoadTask?.cancel()
        liveSummaryRefreshTask?.cancel()
        selectedFeatureRefreshTask?.cancel()
    }

    func load(userLocation: CLLocation?) {
        resetLiveState()
        favoriteDetailFeatures = [:]
        let center = userLocation?.coordinate ?? currentCatalogCenter
        loadCatalog(center: center, userLocation: userLocation)
    }

    private func loadCatalog(center: CLLocationCoordinate2D, userLocation: CLLocation?) {
        catalogLoadTask?.cancel()
        isLoading = true
        loadError = nil

        catalogLoadTask = Task { [weak self] in
            guard let self else { return }
            let result: Result<ChargerRepositoryLoadResult, Error>
            do {
                result = .success(try await self.repository.loadData(center: center, filterState: self.filterState))
            } catch {
                result = .failure(error)
            }

            guard !Task.isCancelled else { return }
            self.isLoading = false
            switch result {
            case .success(let loaded):
                self.allFeatures = loaded.features
                self.operators = loaded.operators
                self.activeCatalogInfo = loaded.sourceInfo
                self.currentCatalogCenter = loaded.catalogCenter
                self.loadError = nil
                self.didSeedFromUserLocation = userLocation != nil
                self.applyLocalFilters(userLocation: userLocation)
            case .failure(let error):
                self.loadError = error.localizedDescription
                if self.allFeatures.isEmpty {
                    self.filterPool = []
                    self.discoveredFeatures = []
                    self.operators = []
                    self.activeCatalogInfo = nil
                    self.isAwaitingFirstLocationFix = false
                    self.resetLiveState()
                }
            }
        }
    }

    func reloadCatalog(userLocation: CLLocation?) {
        Task { [weak self] in
            guard let self else { return }
            await self.repository.invalidateCache()
            self.load(userLocation: userLocation)
        }
    }

    func applyFilters(userLocation: CLLocation?) {
        loadCatalog(center: userLocation?.coordinate ?? currentCatalogCenter, userLocation: userLocation)
    }

    private func applyLocalFilters(userLocation: CLLocation?) {
        filterPool = allFeatures.filter { feature in
            feature.properties.matches(filterState)
        }
        resetDiscoveredList()
        let center = userLocation?.coordinate ?? currentCatalogCenter
        discoverNearby(
            center: center,
            resetHistory: false,
            seededByUserLocation: userLocation != nil
        )
    }

    func handleMapCenterChange(_ center: CLLocationCoordinate2D) {
        if shouldReloadCatalog(for: center) {
            loadCatalog(center: center, userLocation: nil)
        } else {
            discoverNearby(center: center, resetHistory: false, seededByUserLocation: false)
        }
    }

    func seedFromInitialUserLocation(_ location: CLLocation?) {
        guard let location else { return }
        guard !allFeatures.isEmpty else { return }
        if !didSeedFromUserLocation {
            if shouldReloadCatalog(for: location.coordinate) {
                load(userLocation: location)
            } else {
                applyFilters(userLocation: location)
            }
        }
    }

    func reloadListForCurrentLocation(_ location: CLLocation?) {
        guard !allFeatures.isEmpty else {
            load(userLocation: location)
            return
        }
        guard let location else {
            applyFilters(userLocation: nil)
            return
        }
        if shouldReloadCatalog(for: location.coordinate) {
            load(userLocation: location)
        } else {
            applyFilters(userLocation: location)
        }
    }

    func reloadMapForCenter(_ center: CLLocationCoordinate2D?) {
        guard !allFeatures.isEmpty else {
            loadCatalog(center: center ?? currentCatalogCenter, userLocation: nil)
            return
        }
        guard let center else {
            discoverNearby(center: currentCatalogCenter, resetHistory: false, seededByUserLocation: false)
            return
        }
        if shouldReloadCatalog(for: center) {
            loadCatalog(center: center, userLocation: nil)
        } else {
            discoverNearby(center: center, resetHistory: false, seededByUserLocation: false)
        }
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
            ?? favoriteDetailFeatures[stationID]
            ?? selectedFeature.flatMap { $0.properties.stationID == stationID ? $0 : nil }
    }

    func favoritesFeatures(_ favorites: Set<String>, userLocation: CLLocation?) -> [GeoJSONFeature] {
        var byStationID: [String: GeoJSONFeature] = [:]
        for feature in allFeatures where favorites.contains(feature.properties.stationID) {
            byStationID[feature.properties.stationID] = feature
        }
        for feature in favoriteDetailFeatures.values where favorites.contains(feature.properties.stationID) {
            byStationID[feature.properties.stationID] = feature
        }
        var items = favorites.compactMap { byStationID[$0] }
        if let userLocation {
            items.sort { lhs, rhs in
                distance(from: userLocation, to: lhs.coordinate) < distance(from: userLocation, to: rhs.coordinate)
            }
        } else {
            items.sort { $0.properties.operatorName < $1.properties.operatorName }
        }
        return items
    }

    func refreshFavoritesLiveSummaries(_ favorites: Set<String>, force: Bool = false) async {
        await hydrateFavoriteDetails(favorites)
        await requestLiveSummaries(forStationIDs: Array(favorites), force: force)
    }

    func refreshFavoriteStaticDetails(_ favorites: Set<String>) async {
        for stationID in favorites.sorted() {
            await requestStaticDetailIfNeeded(for: stationID)
        }
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

    func humanReadableCatalogSource() -> String {
        guard let activeCatalogInfo else { return "unbekannt" }
        if activeCatalogInfo.source == "catalog-api" {
            return "Live-EU-Katalog API (\(activeCatalogInfo.manifest.version))"
        }
        return "Live-EU-Katalog API"
    }

    func reloadCatalogForCurrentContext(userLocation: CLLocation?) {
        loadCatalog(center: userLocation?.coordinate ?? currentCatalogCenter, userLocation: userLocation)
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
            // The catalog row remains visible when the live detail endpoint is temporarily unavailable.
        }
    }

    func requestStaticDetailIfNeeded(for stationID: String) async {
        let trimmed = stationID.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else { return }
        guard !pendingStaticDetailStationIDs.contains(trimmed) else { return }

        pendingStaticDetailStationIDs.insert(trimmed)
        defer {
            pendingStaticDetailStationIDs.remove(trimmed)
        }

        do {
            let updated = try await repository.stationDetail(
                stationID: trimmed,
                preserving: feature(forStationID: trimmed)
            )
            upsertFeature(updated)
        } catch {
            // Static catalog detail is an enhancement; keep the selected search row visible.
        }
    }

    private func hydrateFavoriteDetails(_ favorites: Set<String>) async {
        for stationID in favorites {
            let trimmed = stationID.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty else { continue }
            guard feature(forStationID: trimmed) == nil else { continue }
            guard !pendingStaticDetailStationIDs.contains(trimmed) else { continue }

            pendingStaticDetailStationIDs.insert(trimmed)
            defer {
                pendingStaticDetailStationIDs.remove(trimmed)
            }

            do {
                let feature = try await repository.stationDetail(stationID: trimmed, preserving: nil)
                favoriteDetailFeatures[trimmed] = feature
            } catch {
                // Favorites outside the current search area remain hidden until API/cache detail is available.
            }
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
            // Keep the last API/cache state visible when live summaries are temporarily unavailable.
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
            await self.requestStaticDetailIfNeeded(for: stationID)
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

    private func applyStaticDetail(_ detail: CatalogStationDetailResponse, stationID: String) {
        let existing = feature(forStationID: stationID)
        let updated = detail.feature(preserving: existing)
        upsertFeature(updated)
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
        pendingStaticDetailStationIDs = []
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

    private func discoverNearby(
        center: CLLocationCoordinate2D,
        resetHistory: Bool,
        seededByUserLocation: Bool = true
    ) {
        if seededByUserLocation {
            didSeedFromUserLocation = true
        }
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

        let nearestStationIDs = nearest.map { $0.properties.stationID }
        Task { [weak self] in
            await self?.requestLiveSummaries(forStationIDs: nearestStationIDs)
        }
    }

    private func shouldReloadCatalog(for center: CLLocationCoordinate2D) -> Bool {
        distance(from: currentCatalogCenter, to: center) >= catalogReloadDistanceM
    }

    private func upsertFeature(_ feature: GeoJSONFeature) {
        let stationID = feature.properties.stationID
        if let index = allFeatures.firstIndex(where: { $0.properties.stationID == stationID }) {
            allFeatures[index] = feature
        } else {
            allFeatures.append(feature)
        }

        if feature.properties.matches(filterState) {
            if let index = filterPool.firstIndex(where: { $0.properties.stationID == stationID }) {
                filterPool[index] = feature
            } else {
                filterPool.append(feature)
            }
        } else {
            filterPool.removeAll { $0.properties.stationID == stationID }
        }

        if discoveredByID[feature.id] != nil {
            discoveredByID[feature.id] = feature
        }
        discoveredFeatures = discoveredFeatures.map { existing in
            existing.properties.stationID == stationID ? feature : existing
        }
        if let selectedFeature, selectedFeature.properties.stationID == stationID {
            self.selectedFeature = feature
        }
    }
}
