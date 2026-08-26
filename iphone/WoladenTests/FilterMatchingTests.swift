import XCTest
import CoreLocation
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

    func testCarPlayChargePlanMatchesWebChargeFitRules() {
        let settings = VehicleEnergySettings(
            batteryCapacityKWh: 75,
            consumptionKWhPer100KM: 18,
            reserveSOCPercent: 30,
            targetSOCPercent: 80,
            averageChargingPowerKW: 120
        )
        let plan = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: 200,
            status: .free,
            available: 2,
            total: 4
        )

        XCTAssertEqual(plan.targetEnergyKWh, 37.5, accuracy: 0.01)
        XCTAssertEqual(plan.estimatedMinutes, 21)
        XCTAssertEqual(plan.tier, .great)
    }

    func testCarPlayChargePlanDowngradesOccupiedAndBrokenStations() {
        let settings = VehicleEnergySettings()
        let busy = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: 150,
            status: .occupied,
            available: 0,
            total: 4
        )
        let broken = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: 150,
            status: .outOfOrder,
            available: 0,
            total: 4
        )

        XCTAssertEqual(busy.tier, .busy)
        XCTAssertEqual(broken.tier, .unavailable)
    }

    func testCarPlayChargePlanUsesProjectedTripArrivalCharge() {
        let settings = VehicleEnergySettings(
            batteryCapacityKWh: 86,
            consumptionKWhPer100KM: 18,
            reserveSOCPercent: 10,
            targetSOCPercent: 80,
            averageChargingPowerKW: 120
        )
        let plan = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: 200,
            status: .free,
            available: 2,
            total: 4,
            fromSOCPercent: 30
        )

        XCTAssertEqual(plan.fromPercent, 30)
        XCTAssertEqual(plan.targetEnergyKWh, 43, accuracy: 0.01)
        XCTAssertEqual(plan.estimatedMinutes, 24)
    }

    func testCarPlayChargePlanExplainsWhenNoChargingIsNeeded() {
        let settings = VehicleEnergySettings(
            batteryCapacityKWh: 86,
            reserveSOCPercent: 10,
            targetSOCPercent: 80,
            averageChargingPowerKW: 120
        )
        let plan = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: 200,
            status: .free,
            available: 2,
            total: 4,
            fromSOCPercent: 90
        )

        XCTAssertEqual(plan.targetEnergyKWh, 0, accuracy: 0.01)
        XCTAssertEqual(plan.estimatedMinutes, 0)
        XCTAssertEqual(plan.tier, .notNeeded)
    }

    func testSharedOccupancyAndLegendRulesKeepDetailedLiveState() {
        let counts = AvailabilityCounts(total: 4, available: 2, occupied: 0, outOfOrder: 1, unknown: 1)
        let summary = woladenAvailabilitySummary(counts)

        XCTAssertTrue(summary?.contains("2") == true)
        XCTAssertTrue(summary?.contains("1") == true)
        XCTAssertEqual(
            woladenStationCardState(status: .free, counts: counts, oftenBroken: true),
            .oftenBroken
        )
    }

    func testActiveFilterDisplayLabelsAreSharedWithCarPlay() {
        let filter = FilterState(
            selectedOperatorNames: ["IONITY"],
            minPowerKW: 150,
            selectedAmenities: ["amenity_bakery"],
            availableOnly: true
        )

        XCTAssertEqual(filter.activeDisplayLabels.first, "IONITY")
        XCTAssertTrue(filter.activeDisplayLabels.contains(AmenityCatalog.label(for: "amenity_bakery")))
        XCTAssertFalse(filter.activeDisplaySummary.isEmpty)
    }

    func testCarPlayAvailableFilterUsesHydratedLiveAvailability() {
        let filter = FilterState(availableOnly: true)
        let staticFreeLiveOccupied = sampleFeature(
            properties: sampleProperties(
                occupancyTotalEVSEs: 4,
                occupancyAvailableEVSEs: 1
            ),
            liveSummary: sampleLiveSummary(status: .occupied, available: 0, occupied: 4, total: 4)
        )
        let staticOccupiedLiveFree = sampleFeature(
            properties: sampleProperties(
                occupancyTotalEVSEs: 4,
                occupancyAvailableEVSEs: 0,
                occupancyOccupiedEVSEs: 4
            ),
            liveSummary: sampleLiveSummary(status: .free, available: 2, occupied: 2, total: 4)
        )

        XCTAssertFalse(carPlayPlanningFeatureMatches(staticFreeLiveOccupied, filter: filter))
        XCTAssertTrue(carPlayPlanningFeatureMatches(staticOccupiedLiveFree, filter: filter))
    }

    func testCarPlayPlanningLocationRejectsStaleAndInaccurateSamples() {
        let now = Date(timeIntervalSince1970: 10_000)
        let fresh = CLLocation(
            coordinate: CLLocationCoordinate2D(latitude: 48.1, longitude: 8.3),
            altitude: 0,
            horizontalAccuracy: 25,
            verticalAccuracy: -1,
            timestamp: now.addingTimeInterval(-30)
        )
        let stale = CLLocation(
            coordinate: fresh.coordinate,
            altitude: 0,
            horizontalAccuracy: 25,
            verticalAccuracy: -1,
            timestamp: now.addingTimeInterval(-(5 * 60))
        )
        let inaccurate = CLLocation(
            coordinate: fresh.coordinate,
            altitude: 0,
            horizontalAccuracy: 251,
            verticalAccuracy: -1,
            timestamp: now.addingTimeInterval(-30)
        )

        XCTAssertTrue(carPlayPlanningLocationIsUsable(fresh, now: now))
        XCTAssertFalse(carPlayPlanningLocationIsUsable(stale, now: now))
        XCTAssertFalse(carPlayPlanningLocationIsUsable(inaccurate, now: now))
    }

    func testCarPlayPlanningLocationSelectsFreshestValidSampleRegardlessOfInputOrder() {
        let now = Date(timeIntervalSince1970: 10_000)
        func location(age: TimeInterval) -> CLLocation {
            CLLocation(
                coordinate: CLLocationCoordinate2D(latitude: 48.1, longitude: 8.3),
                altitude: 0,
                horizontalAccuracy: 25,
                verticalAccuracy: -1,
                timestamp: now.addingTimeInterval(-age)
            )
        }
        let newest = location(age: 10)
        let older = location(age: 60)
        let stale = location(age: 3_600)

        let selected = carPlayFreshestPlanningLocation(from: [newest, stale, older], now: now)

        XCTAssertEqual(selected?.timestamp, newest.timestamp)
    }

    func testTripStationFeatureSnapshotKeepsAmenitiesAndRouteOverride() {
        let feature = sampleFeature(
            properties: sampleProperties(
                amenityExamples: [
                    AmenityExample(
                        category: "bakery",
                        name: "Bäckerei",
                        openingHours: "Mo-Su 06:00-20:00",
                        distanceM: 50,
                        lat: 48.1,
                        lon: 8.3
                    )
                ],
                amenityCounts: ["amenity_bakery": 1],
                priceDisplay: "0,60 €/kWh"
            ),
            liveSummary: sampleLiveSummary(
                status: .free,
                available: 3,
                occupied: 0,
                total: 4,
                outOfOrder: 1,
                oftenBroken: true
            )
        )

        let snapshot = TripStationSnapshot(feature: feature, routePositionM: 42_000)

        XCTAssertEqual(snapshot.routePositionM, 42_000)
        XCTAssertEqual(snapshot.amenitiesTotal, 1)
        XCTAssertEqual(snapshot.amenities?.first?.name, "Bäckerei")
        XCTAssertEqual(snapshot.amenities?.first?.coordinate?.longitude, 8.3)
        XCTAssertEqual(snapshot.outOfOrderEVSEs, 1)
        XCTAssertEqual(snapshot.priceDisplay, "0,65 €/kWh")
        XCTAssertEqual(snapshot.oftenBrokenDailyAnalysis, true)
    }

    private func sampleFeature(
        properties: ChargerProperties,
        liveSummary: LiveStationSummary? = nil
    ) -> GeoJSONFeature {
        GeoJSONFeature(
            id: properties.stationID,
            geometry: GeoJSONPointGeometry(coordinates: [8.3, 48.1]),
            properties: properties,
            liveSummary: liveSummary
        )
    }

    private func sampleLiveSummary(
        status: AvailabilityStatus,
        available: Int,
        occupied: Int,
        total: Int,
        outOfOrder: Int = 0,
        unknown: Int = 0,
        oftenBroken: Bool = false,
        oftenOccupied: Bool = false
    ) -> LiveStationSummary {
        LiveStationSummary(
            stationID: "station-1",
            availabilityStatus: status,
            availableEVSEs: available,
            occupiedEVSEs: occupied,
            outOfOrderEVSEs: outOfOrder,
            unknownEVSEs: unknown,
            totalEVSEs: total,
            priceDisplay: "0,65 €/kWh",
            priceCurrency: "EUR",
            priceEnergyEURKwhMin: "0.65",
            priceEnergyEURKwhMax: "0.65",
            sourceObservedAt: "2026-08-25T20:00:00Z",
            fetchedAt: "2026-08-25T20:00:30Z",
            ingestedAt: "2026-08-25T20:00:30Z",
            frequentlyOutOfOrderDailyAnalysis: oftenBroken,
            frequentlyOccupiedDailyAnalysis: oftenOccupied
        )
    }

    private func sampleProperties(
        operatorName: String = "IONITY",
        maxPowerKW: Double = 150,
        amenityExamples: [AmenityExample] = [],
        amenityCounts: [String: Int] = [:],
        priceDisplay: String = "",
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
            priceDisplay: priceDisplay,
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
