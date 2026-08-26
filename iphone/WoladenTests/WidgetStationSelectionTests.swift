import XCTest
import CoreLocation
@testable import Woladen

final class WidgetStationSelectionTests: XCTestCase {
    func testAvailabilityFilterIsAppliedAfterLiveHydration() throws {
        let station = try makeStation(
            stationID: "station-a",
            latitude: 52.52,
            longitude: 13.40,
            availableEVSEs: 0,
            totalEVSEs: 4
        )
        let live = try JSONDecoder().decode(
            WoladenWidgetLiveStation.self,
            from: Data("""
            {
              "station_id": "station-a",
              "availability_status": "available",
              "available_evses": 2,
              "total_evses": 4,
              "price_display": "0.49 EUR/kWh",
              "source_observed_at": "2026-08-25T10:00:00Z",
              "fetched_at": "2026-08-25T10:00:01Z"
            }
            """.utf8)
        )

        let filter = WoladenWidgetFilter(availableOnly: true)
        XCTAssertFalse(station.matches(filter))
        XCTAssertTrue(station.merging(live).matches(filter))
        XCTAssertEqual(station.merging(live).priceDisplay, "0.49 EUR/kWh")
    }

    func testNearestOrderingUsesStationIDAsStableTieBreaker() throws {
        let origin = CLLocation(latitude: 52.52, longitude: 13.40)
        let stations = [
            try makeStation(stationID: "b", latitude: 52.53, longitude: 13.40),
            try makeStation(stationID: "a", latitude: 52.53, longitude: 13.40),
            try makeStation(stationID: "near", latitude: 52.521, longitude: 13.40)
        ]

        let ordered = stations.sorted { lhs, rhs in
            let left = lhs.distance(from: origin)
            let right = rhs.distance(from: origin)
            return left == right ? lhs.stationID < rhs.stationID : left < right
        }

        XCTAssertEqual(ordered.map(\.stationID), ["near", "a", "b"])
    }

    func testWidgetStationUsesEveryPlanningFilterDimension() throws {
        let station = try makeStation(
            stationID: "station-a",
            latitude: 52.52,
            longitude: 13.40
        )
        XCTAssertTrue(station.matches(WoladenWidgetFilter(
            selectedOperatorNames: ["Test Operator"],
            minPowerKW: 150,
            minAmenityCount: 1,
            selectedAmenities: ["cafe"],
            amenityNameQuery: "Cafe",
            availableOnly: true,
            currentlyOpenOnly: true
        )))
        XCTAssertFalse(station.matches(WoladenWidgetFilter(selectedOperatorNames: ["Other"])))
        XCTAssertFalse(station.matches(WoladenWidgetFilter(minPowerKW: 151)))
        XCTAssertFalse(station.matches(WoladenWidgetFilter(minAmenityCount: 2)))
        XCTAssertFalse(station.matches(WoladenWidgetFilter(selectedAmenities: ["restaurant"])))
        XCTAssertFalse(station.matches(WoladenWidgetFilter(amenityNameQuery: "Bakery")))
    }

    func testSnapshotDeepLinkKeepsExactStationIdentity() throws {
        let station = try makeStation(
            stationID: "DE:test/with space",
            latitude: 52.52,
            longitude: 13.40
        ).snapshot(from: CLLocation(latitude: 52.52, longitude: 13.40))

        XCTAssertEqual(station.deepLinkURL?.scheme, "woladen")
        XCTAssertEqual(station.deepLinkURL?.host, "station")
        XCTAssertEqual(station.deepLinkURL?.path.removingPercentEncoding, "/DE:test/with space")
    }

    private func makeStation(
        stationID: String,
        latitude: Double,
        longitude: Double,
        availableEVSEs: Int = 1,
        totalEVSEs: Int = 4
    ) throws -> WoladenWidgetCatalogStation {
        let json: [String: Any] = [
            "station_id": stationID,
            "operator_name": "Test Operator",
            "station_name": "Test Station",
            "city": "Berlin",
            "address": "Teststraße 1",
            "latitude": latitude,
            "longitude": longitude,
            "charger_count": 4,
            "max_power_kw": 150,
            "price_display": "",
            "amenities_total": 1,
            "amenity_category_counts": ["cafe": 1],
            "amenity_examples": [["category": "cafe", "name": "Café", "opening_hours": "24/7"]],
            "availability_status": availableEVSEs > 0 ? "available" : "occupied",
            "available_evses": availableEVSEs,
            "total_evses": totalEVSEs,
            "source_observed_at": "",
            "fetched_at": ""
        ]
        return try JSONDecoder().decode(
            WoladenWidgetCatalogStation.self,
            from: JSONSerialization.data(withJSONObject: json)
        )
    }
}
