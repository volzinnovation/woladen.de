import Foundation
import CoreLocation

final class ChargerRepository {
    struct SearchResult {
        let features: [GeoJSONFeature]
        let operators: [OperatorEntry]
    }

    private let client: LiveAPIClient
    private let cache = CatalogRepositoryCache()

    init(client: LiveAPIClient = LiveAPIClient()) {
        self.client = client
    }

    convenience init(liveAPIClient: LiveAPIClient) {
        self.init(client: liveAPIClient)
    }

    func loadData(center: CLLocationCoordinate2D, filterState: FilterState) async throws -> ChargerRepositoryLoadResult {
        let result = try await searchCatalog(center: center, filter: filterState)
        return ChargerRepositoryLoadResult(
            features: result.features,
            operators: result.operators,
            sourceInfo: ActiveCatalogSourceInfo.catalogAPI,
            catalogCenter: center
        )
    }

    func invalidateCache() async {
        await cache.removeAll()
    }

    func invalidateInfoSummaryCache() async {
        await cache.removeInfoSummary()
    }

    func infoSummary() async throws -> CatalogInfoSummary {
        if let cached = await cache.infoSummary(maxAge: .infoFresh) {
            return cached
        }

        do {
            let summary = try await client.catalogInfoSummary()
            await cache.storeInfoSummary(summary)
            return summary
        } catch {
            do {
                let openStaticSummary = try await client.webOpenStaticSummary()
                let buildSummary = try? await client.webBuildSummary()
                let summary = CatalogInfoSummary(
                    openStaticSummary: openStaticSummary,
                    buildSummary: buildSummary
                )
                await cache.storeInfoSummary(summary)
                return summary
            } catch {
                if let stale = await cache.infoSummary(maxAge: .infoStale) {
                    return stale
                }
                throw error
            }
        }
    }

    func searchCatalog(
        center: CLLocationCoordinate2D,
        filter: FilterState,
        radiusM: Int = 20_000,
        limit: Int = 100
    ) async throws -> SearchResult {
        let key = CatalogSearchCacheKey(
            latitudeBucket: Int((center.latitude * 10_000).rounded()),
            longitudeBucket: Int((center.longitude * 10_000).rounded()),
            radiusM: radiusM,
            limit: limit,
            minPowerKW: Int((filter.minPowerKW * 10).rounded()),
            operatorNames: filter.selectedOperatorNames.sorted()
        )

        if let cached = await cache.searchResult(for: key, maxAge: .searchFresh) {
            return cached
        }

        do {
            let operatorNames = filter.selectedOperatorNames.sorted()
            let responses: [CatalogSearchResponse]
            if operatorNames.count <= 1 {
                responses = [
                    try await client.searchCatalog(
                        center: center,
                        radiusM: radiusM,
                        limit: limit,
                        minPowerKW: filter.minPowerKW,
                        operatorName: filter.operatorName
                    )
                ]
            } else {
                var fetchedResponses: [CatalogSearchResponse] = []
                for operatorName in operatorNames {
                    fetchedResponses.append(
                        try await client.searchCatalog(
                            center: center,
                            radiusM: radiusM,
                            limit: limit,
                            minPowerKW: filter.minPowerKW,
                            operatorName: operatorName
                        )
                    )
                }
                responses = fetchedResponses
            }
            let features = uniqueFeatures(
                responses.flatMap { response in
                    response.stations.map { $0.feature() }
                }
            )
            let result = SearchResult(features: features, operators: operators(from: features))
            await cache.storeSearchResult(result, for: key)
            return result
        } catch {
            if let stale = await cache.searchResult(for: key, maxAge: .searchStale) {
                return stale
            }
            throw error
        }
    }

    func routeChargers(
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        filter: FilterState
    ) async throws -> RouteChargerLoadResult {
        let response = try await client.routeChargers(
            origin: origin,
            destination: destination,
            filters: RouteFilterPayload(filter: filter)
        )
        let features = response.stations.map { candidate in
            candidate.station.feature(routeMetadata: candidate.route)
        }
        return RouteChargerLoadResult(route: response.route, features: features, source: response.source)
    }

    func stationDetail(
        stationID: String,
        preserving existing: GeoJSONFeature?
    ) async throws -> GeoJSONFeature {
        let trimmed = stationID.trimmingCharacters(in: .whitespacesAndNewlines)
        if let cached = await cache.detailFeature(for: trimmed, maxAge: .detailFresh) {
            return cached.mergingLiveState(from: existing)
        }

        do {
            let response = try await client.catalogStationDetail(stationID: trimmed)
            let feature = response.feature(preserving: existing)
            await cache.storeDetailFeature(feature, for: trimmed)
            return feature
        } catch {
            if let stale = await cache.detailFeature(for: trimmed, maxAge: .detailStale) {
                return stale.mergingLiveState(from: existing)
            }
            throw error
        }
    }

}

struct ChargerRepositoryLoadResult {
    let features: [GeoJSONFeature]
    let operators: [OperatorEntry]
    let sourceInfo: ActiveCatalogSourceInfo
    let catalogCenter: CLLocationCoordinate2D
}

struct RouteChargerLoadResult {
    let route: RouteSummary
    let features: [GeoJSONFeature]
    let source: String
}

private extension TimeInterval {
    static let searchFresh: TimeInterval = 5 * 60
    static let searchStale: TimeInterval = 24 * 60 * 60
    static let detailFresh: TimeInterval = 24 * 60 * 60
    static let detailStale: TimeInterval = 7 * 24 * 60 * 60
    static let infoFresh: TimeInterval = 30 * 60
    static let infoStale: TimeInterval = 7 * 24 * 60 * 60
}

private struct CatalogSearchCacheKey: Hashable {
    let latitudeBucket: Int
    let longitudeBucket: Int
    let radiusM: Int
    let limit: Int
    let minPowerKW: Int
    let operatorNames: [String]
}

private actor CatalogRepositoryCache {
    private struct SearchEntry {
        let storedAt: Date
        let result: ChargerRepository.SearchResult
    }

    private struct DetailEntry {
        let storedAt: Date
        let feature: GeoJSONFeature
    }

    private let maxSearchEntries = 24
    private let maxDetailEntries = 240

    private var searches: [CatalogSearchCacheKey: SearchEntry] = [:]
    private var searchOrder: [CatalogSearchCacheKey] = []
    private var details: [String: DetailEntry] = [:]
    private var detailOrder: [String] = []
    private var infoSummaryEntry: InfoSummaryEntry?

    private struct InfoSummaryEntry {
        let storedAt: Date
        let summary: CatalogInfoSummary
    }

    func searchResult(for key: CatalogSearchCacheKey, maxAge: TimeInterval) -> ChargerRepository.SearchResult? {
        guard let entry = searches[key], Date().timeIntervalSince(entry.storedAt) <= maxAge else {
            return nil
        }
        touchSearch(key)
        return entry.result
    }

    func storeSearchResult(_ result: ChargerRepository.SearchResult, for key: CatalogSearchCacheKey) {
        searches[key] = SearchEntry(storedAt: Date(), result: result)
        touchSearch(key)
        while searchOrder.count > maxSearchEntries, let oldest = searchOrder.first {
            searchOrder.removeFirst()
            searches.removeValue(forKey: oldest)
        }
    }

    func detailFeature(for stationID: String, maxAge: TimeInterval) -> GeoJSONFeature? {
        guard let entry = details[stationID], Date().timeIntervalSince(entry.storedAt) <= maxAge else {
            return nil
        }
        touchDetail(stationID)
        return entry.feature
    }

    func storeDetailFeature(_ feature: GeoJSONFeature, for stationID: String) {
        details[stationID] = DetailEntry(storedAt: Date(), feature: feature)
        touchDetail(stationID)
        while detailOrder.count > maxDetailEntries, let oldest = detailOrder.first {
            detailOrder.removeFirst()
            details.removeValue(forKey: oldest)
        }
    }

    func infoSummary(maxAge: TimeInterval) -> CatalogInfoSummary? {
        guard let entry = infoSummaryEntry, Date().timeIntervalSince(entry.storedAt) <= maxAge else {
            return nil
        }
        return entry.summary
    }

    func storeInfoSummary(_ summary: CatalogInfoSummary) {
        infoSummaryEntry = InfoSummaryEntry(storedAt: Date(), summary: summary)
    }

    func removeInfoSummary() {
        infoSummaryEntry = nil
    }

    func removeAll() {
        searches = [:]
        searchOrder = []
        details = [:]
        detailOrder = []
        infoSummaryEntry = nil
    }

    private func touchSearch(_ key: CatalogSearchCacheKey) {
        searchOrder.removeAll { $0 == key }
        searchOrder.append(key)
    }

    private func touchDetail(_ stationID: String) {
        detailOrder.removeAll { $0 == stationID }
        detailOrder.append(stationID)
    }
}

private extension ActiveCatalogSourceInfo {
    static var catalogAPI: ActiveCatalogSourceInfo {
        ActiveCatalogSourceInfo(
            source: "catalog-api",
            manifest: CatalogSourceManifest(
                version: "live-eu",
                generatedAt: ISO8601DateFormatter().string(from: Date()),
                schema: "/v1/catalog/search+/v1/catalog/stations/{station_id}"
            )
        )
    }
}

private func operators(from features: [GeoJSONFeature]) -> [OperatorEntry] {
    let counts = features.reduce(into: [String: Int]()) { result, feature in
        let name = feature.properties.operatorName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !name.isEmpty else { return }
        result[name, default: 0] += 1
    }
    return counts
        .map { OperatorEntry(name: $0.key, stations: $0.value) }
        .sorted {
            $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
        }
}

private func uniqueFeatures(_ features: [GeoJSONFeature]) -> [GeoJSONFeature] {
    var seenStationIDs = Set<String>()
    var result: [GeoJSONFeature] = []
    for feature in features {
        let stationID = feature.properties.stationID
        guard !stationID.isEmpty else { continue }
        guard seenStationIDs.insert(stationID).inserted else { continue }
        result.append(feature)
    }
    return result
}

private extension GeoJSONFeature {
    func mergingLiveState(from existing: GeoJSONFeature?) -> GeoJSONFeature {
        guard let existing else { return self }
        return GeoJSONFeature(
            id: id,
            geometry: geometry,
            properties: properties,
            liveSummary: liveSummary ?? existing.liveSummary,
            liveDetail: liveDetail ?? existing.liveDetail,
            routeMetadata: routeMetadata ?? existing.routeMetadata
        )
    }
}
