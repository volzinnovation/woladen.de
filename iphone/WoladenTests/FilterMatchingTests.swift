import XCTest
@testable import Woladen

final class FilterMatchingTests: XCTestCase {
    func testAmenityNameQueryMatchesIgnoringCasePunctuationAndDiacritics() {
        let properties = sampleProperties(
            amenityExamples: [
                AmenityExample(
                    category: "fast_food",
                    name: "McDonald's Café",
                    openingHours: nil,
                    distanceM: 42,
                    lat: nil,
                    lon: nil
                )
            ]
        )

        XCTAssertTrue(properties.matchesAmenityNameQuery("mcdonalds"))
        XCTAssertTrue(properties.matchesAmenityNameQuery("cafe"))
        XCTAssertFalse(properties.matchesAmenityNameQuery("burger king"))
    }

    func testFilterStateMatchesAllConfiguredFiltersIncludingAmenityName() {
        let properties = sampleProperties(
            operatorName: "EnBW",
            maxPowerKW: 300,
            amenityExamples: [
                AmenityExample(
                    category: "fast_food",
                    name: "McDonald's",
                    openingHours: nil,
                    distanceM: 25,
                    lat: nil,
                    lon: nil
                )
            ],
            amenityCounts: ["amenity_fast_food": 2]
        )

        let matching = FilterState(
            operatorName: "EnBW",
            minPowerKW: 150,
            selectedAmenities: ["amenity_fast_food"],
            amenityNameQuery: "McDonald"
        )
        let nonMatching = FilterState(
            operatorName: "EnBW",
            minPowerKW: 150,
            selectedAmenities: ["amenity_fast_food"],
            amenityNameQuery: "Subway"
        )

        XCTAssertTrue(properties.matches(matching))
        XCTAssertFalse(properties.matches(nonMatching))
    }

    func testActiveCountIncludesAmenityNameQuery() {
        let filters = FilterState(
            selectedOperatorNames: ["IONITY", "EnBW"],
            minPowerKW: 150,
            selectedAmenities: ["amenity_restaurant", "amenity_toilets"],
            amenityNameQuery: "McDonald"
        )

        XCTAssertEqual(filters.activeCount, 6)
    }

    func testFilterStateMatchesAnySelectedOperator() {
        let ionity = sampleProperties(operatorName: "IONITY")
        let enbw = sampleProperties(operatorName: "EnBW")
        let other = sampleProperties(operatorName: "Other")
        let filter = FilterState(selectedOperatorNames: ["IONITY", "EnBW"])

        XCTAssertTrue(ionity.matches(filter))
        XCTAssertTrue(enbw.matches(filter))
        XCTAssertFalse(other.matches(filter))
    }

    func testAvailableOnlyRequiresKnownFreeChargingPoint() {
        let available = sampleProperties(occupancyTotalEVSEs: 4, occupancyAvailableEVSEs: 1)
        let occupied = sampleProperties(occupancyTotalEVSEs: 4, occupancyAvailableEVSEs: 0, occupancyOccupiedEVSEs: 4)

        XCTAssertTrue(available.matches(FilterState()))
        XCTAssertFalse(occupied.matches(FilterState()))
        XCTAssertTrue(occupied.matches(FilterState(availableOnly: false)))
    }

    private func sampleProperties(
        operatorName: String = "IONITY",
        maxPowerKW: Double = 150,
        amenityExamples: [AmenityExample] = [],
        amenityCounts: [String: Int] = [:],
        occupancyTotalEVSEs: Int = 4,
        occupancyAvailableEVSEs: Int = 1,
        occupancyOccupiedEVSEs: Int = 0
    ) -> ChargerProperties {
        ChargerProperties(
            stationID: "station-1",
            operatorName: operatorName,
            status: "In Betrieb",
            maxPowerKW: maxPowerKW,
            chargingPointsCount: 4,
            maxIndividualPowerKW: maxPowerKW,
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
            detailSourceUID: "",
            detailSourceName: "",
            detailLastUpdated: "",
            datexSiteID: "",
            datexStationIDs: "",
            datexChargePointIDs: "",
            priceDisplay: "",
            priceEnergyEURKwhMin: "",
            priceEnergyEURKwhMax: "",
            priceCurrency: "",
            priceQuality: "",
            openingHoursDisplay: "",
            openingHoursIs24_7: false,
            helpdeskPhone: "",
            paymentMethodsDisplay: "",
            authMethodsDisplay: "",
            connectorTypesDisplay: "",
            currentTypesDisplay: "",
            connectorCount: 0,
            greenEnergy: nil,
            serviceTypesDisplay: "",
            detailsJSON: "",
            amenitiesTotal: amenityCounts.values.reduce(0, +),
            amenitiesSource: "osm-pbf",
            amenityExamples: amenityExamples,
            amenityCounts: amenityCounts
        )
    }
}
