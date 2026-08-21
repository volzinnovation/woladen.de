import Foundation
import CoreLocation

enum LiveAPIError: LocalizedError {
    case invalidBaseURL
    case invalidResponse
    case unexpectedStatusCode(Int)

    var errorDescription: String? {
        switch self {
        case .invalidBaseURL:
            return "Ungültige Live-API-URL"
        case .invalidResponse:
            return "Unerwartete Antwort der Live-API"
        case .unexpectedStatusCode(let statusCode):
            return "Live-API antwortete mit HTTP \(statusCode)"
        }
    }
}

final class LiveAPIClient {
    static let defaultBaseURL = URL(string: "https://live-eu.woladen.de")!
    static let openStaticSummaryPath = "/data/open_static_summary.json"
    static let maxLookupStationIDs = 20
    private static let catalogSearchTimeout: TimeInterval = 30.0
    private static let routeChargerTimeout: TimeInterval = 120.0

    private let baseURL: URL?
    private let session: URLSession
    private let acceptLanguageHeader: String
    private let decoder = JSONDecoder()

    init(
        baseURL: URL? = LiveAPIClient.defaultBaseURL,
        session: URLSession = .shared,
        preferredLanguages: [String] = Locale.preferredLanguages
    ) {
        self.baseURL = baseURL
        self.session = session
        self.acceptLanguageHeader = WoladenLanguagePreference.acceptLanguageHeader(
            preferredLanguages: preferredLanguages
        )
    }

    var isEnabled: Bool {
        baseURL != nil
    }

    func lookupStations(stationIDs: [String]) async throws -> LiveStationLookupResponse {
        let ids = normalizedStationIDs(stationIDs)
        guard !ids.isEmpty else {
            return LiveStationLookupResponse(stations: [], missingStationIDs: [])
        }

        var stations: [LiveStationSummary] = []
        var missingStationIDs: [String] = []

        for batchStart in stride(from: ids.startIndex, to: ids.endIndex, by: Self.maxLookupStationIDs) {
            let batchEnd = min(batchStart + Self.maxLookupStationIDs, ids.endIndex)
            let batch = Array(ids[batchStart..<batchEnd])
            let response = try await lookupStationBatch(stationIDs: batch)
            stations.append(contentsOf: response.stations)
            missingStationIDs.append(contentsOf: response.missingStationIDs)
        }

        return LiveStationLookupResponse(
            stations: stations,
            missingStationIDs: normalizedStationIDs(missingStationIDs)
        )
    }

    private func lookupStationBatch(stationIDs: [String]) async throws -> LiveStationLookupResponse {
        guard let url = endpointURL(path: "/v1/stations/lookup") else {
            throw LiveAPIError.invalidBaseURL
        }

        var request = makeRequest(url: url, method: "POST", timeout: 3.5)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONSerialization.data(withJSONObject: [
            "station_ids": stationIDs
        ])

        return try await send(request)
    }

    func stationDetail(stationID: String) async throws -> LiveStationDetail {
        guard let encoded = encodedPathComponent(stationID),
              let url = endpointURL(path: "/v1/stations/\(encoded)") else {
            throw LiveAPIError.invalidBaseURL
        }

        let request = makeRequest(url: url, method: "GET", timeout: 4.0)
        return try await send(request)
    }

    func searchCatalog(
        center: CLLocationCoordinate2D,
        radiusM: Int,
        limit: Int,
        minPowerKW: Double,
        operatorName: String
    ) async throws -> CatalogSearchResponse {
        var queryItems = [
            URLQueryItem(name: "lat", value: String(center.latitude)),
            URLQueryItem(name: "lon", value: String(center.longitude)),
            URLQueryItem(name: "radius_m", value: String(radiusM)),
            URLQueryItem(name: "limit", value: String(limit)),
            URLQueryItem(name: "mode", value: "travel"),
            URLQueryItem(name: "min_power_kw", value: String(minPowerKW))
        ]
        let trimmedOperator = operatorName.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmedOperator.isEmpty {
            queryItems.append(URLQueryItem(name: "operator", value: trimmedOperator))
        }

        guard let url = endpointURL(path: "/v1/catalog/search", queryItems: queryItems) else {
            throw LiveAPIError.invalidBaseURL
        }

        let request = makeRequest(url: url, method: "GET", timeout: Self.catalogSearchTimeout)
        return try await send(request)
    }

    func catalogStationDetail(stationID: String) async throws -> CatalogStationDetailResponse {
        guard let encoded = encodedPathComponent(stationID),
              let url = endpointURL(path: "/v1/catalog/stations/\(encoded)") else {
            throw LiveAPIError.invalidBaseURL
        }

        let request = makeRequest(url: url, method: "GET", timeout: 5.0)
        return try await send(request)
    }

    func catalogInfoSummary() async throws -> CatalogInfoSummary {
        guard let url = endpointURL(path: Self.openStaticSummaryPath) else {
            throw LiveAPIError.invalidBaseURL
        }

        let request = makeRequest(url: url, method: "GET", timeout: 5.0)
        return try await send(request)
    }

    func routeChargers(
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        filters: RouteFilterPayload
    ) async throws -> RouteChargerResponse {
        guard let url = endpointURL(path: "/v1/routes/chargers") else {
            throw LiveAPIError.invalidBaseURL
        }

        let payload = RouteChargerRequest(
            origin: origin,
            destination: destination,
            filters: filters,
            filterMode: "route_calculation"
        )
        var request = makeRequest(url: url, method: "POST", timeout: Self.routeChargerTimeout)
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(payload)
        return try await send(request)
    }

    private func endpointURL(path: String) -> URL? {
        baseURL?.appending(path: path)
    }

    private func endpointURL(path: String, queryItems: [URLQueryItem]) -> URL? {
        guard let url = endpointURL(path: path),
              var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
            return nil
        }
        components.queryItems = queryItems
        return components.url
    }

    private func makeRequest(url: URL, method: String, timeout: TimeInterval) -> URLRequest {
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.timeoutInterval = timeout
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        request.setValue(acceptLanguageHeader, forHTTPHeaderField: "Accept-Language")
        return request
    }

    private func send<Response: Decodable>(_ request: URLRequest) async throws -> Response {
        let (data, response) = try await session.data(for: request)
        guard let httpResponse = response as? HTTPURLResponse else {
            throw LiveAPIError.invalidResponse
        }
        guard (200..<300).contains(httpResponse.statusCode) else {
            throw LiveAPIError.unexpectedStatusCode(httpResponse.statusCode)
        }
        return try decoder.decode(Response.self, from: data)
    }

    private func normalizedStationIDs(_ stationIDs: [String]) -> [String] {
        var seen: Set<String> = []
        var normalized: [String] = []
        for stationID in stationIDs {
            let trimmed = stationID.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !trimmed.isEmpty, seen.insert(trimmed).inserted else { continue }
            normalized.append(trimmed)
        }
        return normalized
    }

    private func encodedPathComponent(_ value: String) -> String? {
        var allowed = CharacterSet.urlPathAllowed
        allowed.remove("/")
        return value.addingPercentEncoding(withAllowedCharacters: allowed)
    }
}

private struct RouteChargerRequest: Encodable {
    let origin: RouteEndpoint
    let destination: RouteEndpoint
    let filters: RouteFilterPayload
    let filterMode: String

    enum CodingKeys: String, CodingKey {
        case origin
        case destination
        case filters
        case filterMode = "filter_mode"
    }
}

struct WoladenLanguagePreference {
    static let fallbackLanguageCode = NativeI18n.fallbackLanguageCode
    static let supportedLanguageCodes: Set<String> = Set(NativeI18n.supportedLanguageCodes)

    static func acceptLanguageHeader(preferredLanguages: [String]) -> String {
        var accepted: [String] = []
        for language in preferredLanguages {
            guard let code = supportedLanguageCode(for: language), !accepted.contains(code) else {
                continue
            }
            accepted.append(code)
        }
        if !accepted.contains(fallbackLanguageCode) {
            accepted.append(fallbackLanguageCode)
        }
        return accepted.enumerated().map { index, code in
            if index == 0 {
                return code
            }
            return "\(code);q=\(qualityValue(for: index))"
        }.joined(separator: ", ")
    }

    static func supportedLanguageCode(for identifier: String) -> String? {
        let normalized = identifier
            .trimmingCharacters(in: .whitespacesAndNewlines)
            .lowercased()
        guard !normalized.isEmpty else { return nil }

        let primaryCode = normalized
            .split(whereSeparator: { $0 == "-" || $0 == "_" })
            .first
            .map(String.init) ?? normalized
        let aliasedCode = primaryCode == "no" ? "nb" : primaryCode
        return supportedLanguageCodes.contains(aliasedCode) ? aliasedCode : nil
    }

    private static func qualityValue(for index: Int) -> String {
        "0.\(max(1, 10 - index))"
    }
}
