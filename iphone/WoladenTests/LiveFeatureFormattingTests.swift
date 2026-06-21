import Foundation
import XCTest
@testable import Woladen

final class LiveFeatureFormattingTests: XCTestCase {
    func testLiveAPIClientDefaultsToEuropeanLiveAPIBase() {
        XCTAssertEqual(LiveAPIClient.defaultBaseURL.absoluteString, "https://live-eu.woladen.de")
    }

    func testSupportedLanguageResolverKeepsWebLanguageAliases() {
        XCTAssertEqual(WoladenLanguagePreference.supportedLanguageCode(for: "fr-FR"), "fr")
        XCTAssertEqual(WoladenLanguagePreference.supportedLanguageCode(for: "no-NO"), "nb")
        XCTAssertNil(WoladenLanguagePreference.supportedLanguageCode(for: "ja-JP"))
    }

    func testLookupBatchesAtTwentyStationIDsAndSendsAcceptLanguage() async throws {
        let recorder = LiveAPIRequestRecorder()
        LiveAPIMockURLProtocol.requestHandler = { request in
            recorder.append(request)
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            return (response, Data(#"{"stations":[],"missing_station_ids":[]}"#.utf8))
        }
        defer {
            LiveAPIMockURLProtocol.requestHandler = nil
        }

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LiveAPIMockURLProtocol.self]
        let session = URLSession(configuration: configuration)
        let client = LiveAPIClient(
            baseURL: URL(string: "https://api.example.test")!,
            session: session,
            preferredLanguages: ["fr-FR", "de-DE"]
        )
        let ids = (1...45).map { "station-\($0)" }

        _ = try await client.lookupStations(stationIDs: ids)

        let requests = recorder.requests
        XCTAssertEqual(requests.count, 3)
        XCTAssertEqual(requests.map { requestStationIDs($0).count }, [20, 20, 5])
        XCTAssertTrue(requests.allSatisfy { $0.value(forHTTPHeaderField: "Accept-Language") == "fr, de;q=0.9, en;q=0.8" })
        XCTAssertTrue(requests.allSatisfy { $0.httpMethod == "POST" })
        XCTAssertTrue(requests.allSatisfy { $0.url?.path == "/v1/stations/lookup" })
    }

    func testCatalogSearchUsesLiveEUContractParametersAndAcceptLanguage() async throws {
        let recorder = LiveAPIRequestRecorder()
        LiveAPIMockURLProtocol.requestHandler = { request in
            recorder.append(request)
            let response = HTTPURLResponse(
                url: request.url!,
                statusCode: 200,
                httpVersion: nil,
                headerFields: ["Content-Type": "application/json"]
            )!
            return (response, Data(#"{"stations":[{"station_id":"at:test:fast","operator_name":"Fast Operator","latitude":48.2082,"longitude":16.3738,"charger_count":2,"max_power_kw":150,"amenity_category_counts":{"restaurant":1}}]}"#.utf8))
        }
        defer {
            LiveAPIMockURLProtocol.requestHandler = nil
        }

        let configuration = URLSessionConfiguration.ephemeral
        configuration.protocolClasses = [LiveAPIMockURLProtocol.self]
        let session = URLSession(configuration: configuration)
        let client = LiveAPIClient(
            baseURL: URL(string: "https://api.example.test")!,
            session: session,
            preferredLanguages: ["nl-NL", "de-DE"]
        )

        let response = try await client.searchCatalog(
            center: .init(latitude: 48.2082, longitude: 16.3738),
            radiusM: 20_000,
            limit: 100,
            minPowerKW: 50,
            operatorName: "Fast"
        )

        XCTAssertEqual(response.stations.count, 1)
        let request = try XCTUnwrap(recorder.requests.first)
        XCTAssertEqual(request.httpMethod, "GET")
        XCTAssertEqual(request.url?.path, "/v1/catalog/search")
        XCTAssertEqual(request.value(forHTTPHeaderField: "Accept-Language"), "nl, de;q=0.9, en;q=0.8")
        let query = queryItems(request)
        XCTAssertEqual(query["lat"], "48.2082")
        XCTAssertEqual(query["lon"], "16.3738")
        XCTAssertEqual(query["radius_m"], "20000")
        XCTAssertEqual(query["limit"], "100")
        XCTAssertEqual(query["mode"], "travel")
        XCTAssertEqual(query["min_power_kw"], "50.0")
        XCTAssertEqual(query["operator"], "Fast")
    }

    func testCatalogStationDetailConvertsChargersAndAmenitiesToFeature() throws {
        let data = Data(
            #"""
            {
              "station": {
                "station_id": "at:test:fast",
                "operator_name": "Fast Operator",
                "station_name": "Fast Station",
                "address": "Fast Street 1",
                "postal_code": "1010",
                "city": "Wien",
                "latitude": 48.2082,
                "longitude": 16.3738,
                "charger_count": 1,
                "max_power_kw": 150,
                "connector_types": "ccs,type2",
                "price_display": "0.59 EUR/kWh",
                "amenities_total": 1,
                "amenity_category_counts": {"restaurant": 1}
              },
              "chargers": [
                {
                  "charger_id": "at:test:fast:1",
                  "source_evse_id": "EVSE-1",
                  "connector_type": "ccs",
                  "current_type": "dc",
                  "max_power_kw": 150
                }
              ],
              "amenities": {
                "amenities_total": 2,
                "amenity_category_counts": {"restaurant": 1, "toilets": 1},
                "amenity_examples": [
                  {"kind": "restaurant", "name": "Nearby Food", "distance_m": 42, "lat": 48.208, "lon": 16.374}
                ]
              }
            }
            """#.utf8
        )

        let detail = try JSONDecoder().decode(CatalogStationDetailResponse.self, from: data)
        let feature = detail.feature(preserving: nil)

        XCTAssertEqual(feature.properties.stationID, "at:test:fast")
        XCTAssertEqual(feature.properties.operatorName, "Fast Operator")
        XCTAssertEqual(feature.properties.connectorCount, 1)
        XCTAssertEqual(feature.properties.connectorTypesDisplay, "ccs, type2")
        XCTAssertEqual(feature.properties.currentTypesDisplay, "dc")
        XCTAssertEqual(feature.properties.amenitiesTotal, 2)
        XCTAssertEqual(feature.properties.amenityCounts["amenity_restaurant"], 1)
        XCTAssertEqual(feature.properties.amenityCounts["amenity_toilets"], 1)
        XCTAssertEqual(feature.properties.amenityExamples.first?.category, "restaurant")
        XCTAssertEqual(feature.displayPrice, "0.59 EUR/kWh")
    }

    func testFormattedElapsedLiveTimeUsesMinutesForRecentUpdates() {
        let now = ISO8601DateFormatter().date(from: "2026-04-17T15:03:18Z")!
        let formatted = formattedElapsedLiveTime("2026-04-17T14:50:00Z", now: now)
        XCTAssertNotNil(formatted)
        XCTAssertTrue(formatted?.contains("13") == true)
    }

    func testFormattedElapsedLiveTimeUsesRelativeLabelsForFreshUpdates() {
        let now = ISO8601DateFormatter().date(from: "2026-04-17T15:03:18Z")!
        XCTAssertFalse(formattedElapsedLiveTime("2026-04-17T15:03:00Z", now: now)?.isEmpty ?? true)
    }

    func testLiveSummaryOverridesStaticCatalogOccupancyAndPrice() {
        let feature = sampleFeature(
            properties: sampleProperties(
                occupancyTotalEVSEs: 2,
                occupancyAvailableEVSEs: 2,
                priceDisplay: "ab 0,59 €/kWh",
                detailSourceUID: "mobilithek_enbwmobility_static",
                detailSourceName: "EnBWmobility+"
            ),
            liveSummary: LiveStationSummary(
                stationID: "station-1",
                availabilityStatus: .occupied,
                availableEVSEs: 1,
                occupiedEVSEs: 2,
                outOfOrderEVSEs: 0,
                unknownEVSEs: 0,
                totalEVSEs: 3,
                priceDisplay: "ab 0,69 €/kWh",
                priceCurrency: "EUR",
                priceEnergyEURKwhMin: "0.69",
                priceEnergyEURKwhMax: "0.69",
                sourceObservedAt: "2026-04-16T14:10:02Z",
                fetchedAt: "2026-04-16T14:12:16Z",
                ingestedAt: "2026-04-16T14:12:16Z"
            )
        )

        XCTAssertEqual(feature.displayPrice, "ab 0,69 €/kWh")
        XCTAssertEqual(
            feature.occupancySummaryLabel,
            [
                String(localized: "availability.available").replacingOccurrences(of: "{count}", with: "1"),
                String(localized: "availability.occupiedCount").replacingOccurrences(of: "{count}", with: "2")
            ].joined(separator: ", ")
        )
        XCTAssertEqual(feature.availabilityStatus, .occupied)
        XCTAssertEqual(feature.liveEVSERows.count, 1)
        XCTAssertTrue(feature.occupancySourceLabel?.contains("EnBWmobility+") == true)
    }

    func testStaticCatalogValuesRemainWhenNoLiveOverlayExists() {
        let feature = sampleFeature(
            properties: sampleProperties(
                occupancyTotalEVSEs: 4,
                occupancyAvailableEVSEs: 3,
                occupancyOccupiedEVSEs: 1,
                priceDisplay: "ab 0,59 €/kWh"
            )
        )

        XCTAssertEqual(feature.displayPrice, "ab 0,59 €/kWh")
        XCTAssertEqual(
            feature.occupancySummaryLabel,
            [
                String(localized: "availability.available").replacingOccurrences(of: "{count}", with: "3"),
                String(localized: "availability.occupiedCount").replacingOccurrences(of: "{count}", with: "1")
            ].joined(separator: ", ")
        )
        XCTAssertEqual(feature.availabilityStatus, .free)
        XCTAssertTrue(feature.liveEVSERows.isEmpty)
    }

    private func sampleFeature(
        properties: ChargerProperties,
        liveSummary: LiveStationSummary? = nil
    ) -> GeoJSONFeature {
        GeoJSONFeature(
            id: properties.stationID,
            geometry: GeoJSONPointGeometry(type: "Point", coordinates: [13.4, 52.5]),
            properties: properties,
            liveSummary: liveSummary
        )
    }

    private func sampleProperties(
        occupancyTotalEVSEs: Int = 0,
        occupancyAvailableEVSEs: Int = 0,
        occupancyOccupiedEVSEs: Int = 0,
        priceDisplay: String = "",
        detailSourceUID: String = "",
        detailSourceName: String = ""
    ) -> ChargerProperties {
        ChargerProperties(
            stationID: "station-1",
            operatorName: "IONITY",
            status: "In Betrieb",
            maxPowerKW: 150,
            chargingPointsCount: 4,
            maxIndividualPowerKW: 150,
            postcode: "10115",
            city: "Berlin",
            address: "Teststraße 1",
            occupancySourceUID: "",
            occupancySourceName: "",
            occupancyStatus: "",
            occupancyLastUpdated: "",
            occupancyTotalEVSEs: occupancyTotalEVSEs,
            occupancyAvailableEVSEs: occupancyAvailableEVSEs,
            occupancyOccupiedEVSEs: occupancyOccupiedEVSEs,
            occupancyChargingEVSEs: 0,
            occupancyOutOfOrderEVSEs: 0,
            occupancyUnknownEVSEs: 0,
            detailSourceUID: detailSourceUID,
            detailSourceName: detailSourceName,
            detailLastUpdated: "",
            datexSiteID: "",
            datexStationIDs: "",
            datexChargePointIDs: "",
            priceDisplay: priceDisplay,
            priceEnergyEURKwhMin: "",
            priceEnergyEURKwhMax: "",
            priceCurrency: "EUR",
            priceQuality: "",
            openingHoursDisplay: "24/7",
            openingHoursIs24_7: true,
            helpdeskPhone: "",
            paymentMethodsDisplay: "",
            authMethodsDisplay: "",
            connectorTypesDisplay: "",
            currentTypesDisplay: "",
            connectorCount: 0,
            greenEnergy: nil,
            serviceTypesDisplay: "",
            detailsJSON: "",
            amenitiesTotal: 0,
            amenitiesSource: "osm-pbf",
            amenityExamples: [],
            amenityCounts: [:]
        )
    }
}

private final class LiveAPIRequestRecorder {
    private let lock = NSLock()
    private var storedRequests: [URLRequest] = []

    var requests: [URLRequest] {
        lock.lock()
        defer { lock.unlock() }
        return storedRequests
    }

    func append(_ request: URLRequest) {
        lock.lock()
        storedRequests.append(request)
        lock.unlock()
    }
}

private final class LiveAPIMockURLProtocol: URLProtocol {
    static var requestHandler: ((URLRequest) throws -> (HTTPURLResponse, Data))?

    override class func canInit(with request: URLRequest) -> Bool {
        true
    }

    override class func canonicalRequest(for request: URLRequest) -> URLRequest {
        request
    }

    override func startLoading() {
        guard let requestHandler = Self.requestHandler else {
            client?.urlProtocol(self, didFailWithError: LiveAPIMockError.missingHandler)
            return
        }

        do {
            let (response, data) = try requestHandler(request)
            client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
            client?.urlProtocol(self, didLoad: data)
            client?.urlProtocolDidFinishLoading(self)
        } catch {
            client?.urlProtocol(self, didFailWithError: error)
        }
    }

    override func stopLoading() {}
}

private enum LiveAPIMockError: Error {
    case missingHandler
}

private func requestStationIDs(_ request: URLRequest) -> [String] {
    guard let body = requestBodyData(from: request),
          let object = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
          let ids = object["station_ids"] as? [String] else {
        return []
    }
    return ids
}

private func requestBodyData(from request: URLRequest) -> Data? {
    if let body = request.httpBody {
        return body
    }
    guard let stream = request.httpBodyStream else {
        return nil
    }

    stream.open()
    defer { stream.close() }

    var data = Data()
    let bufferSize = 1024
    let buffer = UnsafeMutablePointer<UInt8>.allocate(capacity: bufferSize)
    defer { buffer.deallocate() }

    while true {
        let count = stream.read(buffer, maxLength: bufferSize)
        if count > 0 {
            data.append(buffer, count: count)
        } else if count == 0 {
            break
        } else {
            return nil
        }
    }

    return data
}

private func queryItems(_ request: URLRequest) -> [String: String] {
    guard let url = request.url,
          let components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
        return [:]
    }
    return Dictionary(uniqueKeysWithValues: (components.queryItems ?? []).compactMap { item in
        guard let value = item.value else { return nil }
        return (item.name, value)
    })
}
