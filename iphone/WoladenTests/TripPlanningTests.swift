import XCTest
import CoreLocation
@testable import Woladen

final class TripPlanningTests: XCTestCase {
    private let settings = VehicleEnergySettings(
        batteryCapacityKWh: 60,
        consumptionKWhPer100KM: 20,
        reserveSOCPercent: 10,
        targetSOCPercent: 80,
        averageChargingPowerKW: 120,
        earlyWindowKM: 40,
        maximumDetourMinutes: 15
    )

    func testEnergyPlannerShowsOnlyStationsNearRechargeNeed() {
        let stations = [
            station("too-early", provider: "EnBW", positionKM: 40),
            station("first-a", provider: "IONITY", positionKM: 160),
            station("first-b", provider: "EnBW", positionKM: 175),
            station("second", provider: "IONITY", positionKM: 360)
        ]

        let result = EnergyRoutePlanner.build(
            routeDistanceM: 500_000,
            stations: stations,
            selectedStationIDs: [],
            initialSOCPercent: 70,
            settings: settings,
            providerMode: .prefer,
            selectedProviderNames: []
        )

        XCTAssertTrue(result.isFeasible)
        XCTAssertEqual(result.windows.first?.candidateStationIDs, ["first-a", "first-b"])
        XCTAssertFalse(result.windows.flatMap(\.candidateStationIDs).contains("too-early"))
    }

    func testSelectedEarlierStopRepositionsLaterEnergyWindow() {
        let stations = [
            station("first-a", provider: "IONITY", positionKM: 160),
            station("first-b", provider: "EnBW", positionKM: 175),
            station("later-a", provider: "IONITY", positionKM: 330),
            station("later-b", provider: "EnBW", positionKM: 370)
        ]
        let withoutSelection = EnergyRoutePlanner.build(
            routeDistanceM: 500_000,
            stations: stations,
            selectedStationIDs: [],
            initialSOCPercent: 70,
            settings: settings,
            providerMode: .prefer,
            selectedProviderNames: []
        )
        let withSelection = EnergyRoutePlanner.build(
            routeDistanceM: 500_000,
            stations: stations,
            selectedStationIDs: ["first-a"],
            initialSOCPercent: 70,
            settings: settings,
            providerMode: .prefer,
            selectedProviderNames: []
        )

        XCTAssertEqual(withoutSelection.windows.first?.endPositionM, withSelection.windows.first?.endPositionM)
        XCTAssertLessThan(
            withSelection.windows.dropFirst().first?.endPositionM ?? .max,
            withoutSelection.windows.dropFirst().first?.endPositionM ?? .max
        )
    }

    func testProviderCoverageCountsDistinctStationsAndWindows() {
        let routePlan = plan(
            stations: [
                station("ionity-1", provider: "IONITY", positionKM: 170),
                station("enbw-1", provider: "EnBW", positionKM: 175),
                station("ionity-2", provider: "IONITY", positionKM: 365),
                station("tesla-1", provider: "Tesla", positionKM: 370)
            ],
            windows: [
                window(index: 0, ids: ["ionity-1", "enbw-1"], selected: "ionity-1"),
                window(index: 1, ids: ["ionity-2", "tesla-1"], selected: nil)
            ],
            selections: [.init(stationID: "ionity-1", state: .planned, selectedAt: Date())]
        )

        let coverage = routePlan.providerCoverage
        let ionity = coverage.first { $0.providerName == "IONITY" }
        let enbw = coverage.first { $0.providerName == "EnBW" }

        XCTAssertEqual(ionity?.stationCount, 2)
        XCTAssertEqual(ionity?.coveredWindowCount, 2)
        XCTAssertEqual(ionity?.selectedStopCount, 1)
        XCTAssertEqual(enbw?.coveredWindowCount, 1)
    }

    func testProviderOnlyModeReportsUncoveredRoute() {
        let result = EnergyRoutePlanner.build(
            routeDistanceM: 500_000,
            stations: [
                station("ionity-1", provider: "IONITY", positionKM: 170),
                station("enbw-1", provider: "EnBW", positionKM: 365)
            ],
            selectedStationIDs: [],
            initialSOCPercent: 70,
            settings: settings,
            providerMode: .only,
            selectedProviderNames: ["IONITY"]
        )

        XCTAssertFalse(result.isFeasible)
        XCTAssertNotNil(result.firstUncoveredPositionM)
    }

    func testProviderCoverageConsolidatesOperatorNameVariantsIntoPackageBrand() {
        let routePlan = plan(
            stations: [
                station("enbw-short", provider: "ENBW", positionKM: 170),
                station("enbw-legal", provider: "EnBW mobility+ AG und Co.KG", positionKM: 175),
                station("ionity-legal", provider: "IONITY GmbH", positionKM: 180)
            ],
            windows: [
                window(index: 0, ids: ["enbw-short", "enbw-legal", "ionity-legal"], selected: nil)
            ],
            selections: []
        )

        XCTAssertEqual(routePlan.providerCoverage.count, 2)
        XCTAssertEqual(
            routePlan.providerCoverage.first { $0.providerName == "EnBW" }?.stationCount,
            2
        )
        XCTAssertEqual(normalizedProviderKey("IONITY GmbH"), normalizedProviderKey("IONITY"))
    }

    func testSubstitutesPreferEarlierAheadCandidatesAndKeepDetoursSeparate() {
        let routePlan = plan(
            stations: [
                station("earlier-behind", provider: "EnBW", positionKM: 140),
                station("earlier-ahead", provider: "IONITY", positionKM: 165),
                station("selected", provider: "EnBW", positionKM: 175),
                station("detour", provider: "Tesla", positionKM: 185)
            ],
            windows: [
                window(
                    index: 0,
                    ids: ["earlier-behind", "earlier-ahead", "selected", "detour"],
                    selected: "selected"
                )
            ],
            selections: [.init(stationID: "selected", state: .planned, selectedAt: Date())]
        )

        let substitutes = EnergyRoutePlanner.substituteStationIDs(
            in: routePlan,
            replacing: "selected",
            currentRoutePositionM: 150_000
        )

        XCTAssertEqual(substitutes.earlier, ["earlier-ahead"])
        XCTAssertEqual(substitutes.detour, ["detour"])
    }

    func testRouteProgressEstimatorUsesPolylinePosition() {
        let position = RouteProgressEstimator.positionM(
            for: CLLocationCoordinate2D(latitude: 0, longitude: 1),
            routeCoordinates: [[0, 0], [1, 0], [2, 0]],
            routeDistanceM: 200_000
        )

        XCTAssertEqual(position, 100_000)
    }

    func testTrafficETAIncludesEstimatedChargingTime() {
        let selectedStation = station("selected", provider: "IONITY", positionKM: 170)
        let routePlan = plan(
            stations: [selectedStation],
            windows: [
                ChargingWindow(
                    index: 0,
                    startPositionM: 150_000,
                    endPositionM: 190_000,
                    departurePositionM: 0,
                    departureSOCPercent: 70,
                    candidateStationIDs: [selectedStation.stationID],
                    projectedArrivalSOCByStationID: [selectedStation.stationID: 20],
                    selectedStationID: selectedStation.stationID
                )
            ],
            selections: [.init(stationID: selectedStation.stationID, state: .planned, selectedAt: Date())]
        )

        let duration = TripETAService.estimatedChargingDuration(at: selectedStation, plan: routePlan)

        XCTAssertEqual(duration, 1_080, accuracy: 0.01)
    }

    @MainActor
    func testRoadSpeedSuggestsTripModeWithoutAutomaticallySwitching() throws {
        let suiteName = "TripPlanningTests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }

        let selectedStation = station("selected", provider: "IONITY", positionKM: 170)
        let readyPlan = plan(
            stations: [selectedStation],
            windows: [window(index: 0, ids: [selectedStation.stationID], selected: selectedStation.stationID)],
            selections: [.init(stationID: selectedStation.stationID, state: .planned, selectedAt: Date())]
        )
        defaults.set(try JSONEncoder().encode([readyPlan]), forKey: "woladen.trip.plans.v1")
        defaults.set(try JSONEncoder().encode(TripPreferences()), forKey: "woladen.trip.preferences.v1")

        let store = TripStore(defaults: defaults)
        let movingLocation = CLLocation(
            coordinate: CLLocationCoordinate2D(latitude: 48, longitude: 8),
            altitude: 0,
            horizontalAccuracy: 10,
            verticalAccuracy: 10,
            course: 0,
            speed: 20,
            timestamp: Date()
        )

        store.updateLocation(movingLocation)
        XCTAssertNil(store.activePlanID)
        store.updateLocation(movingLocation)

        XCTAssertNil(store.activePlanID)
        XCTAssertEqual(store.mode, .plan)
        XCTAssertTrue(store.isLikelyDriving)
        XCTAssertTrue(store.isTripModeSuggested)

        store.acceptTripModeSuggestion()

        XCTAssertEqual(store.activePlanID, readyPlan.id)
        XCTAssertEqual(store.mode, .trip)
    }

    func testOnlyFirstUnresolvedChargingWindowIsVisible() {
        let routePlan = plan(
            stations: [
                station("first", provider: "IONITY", positionKM: 170),
                station("second", provider: "EnBW", positionKM: 360)
            ],
            windows: [
                window(index: 0, ids: ["first"], selected: nil),
                window(index: 1, ids: ["second"], selected: nil)
            ],
            selections: []
        )

        XCTAssertEqual(routePlan.visibleWindows.map(\.index), [0])
    }

    func testMultipleVehicleProfilesPreserveIndependentEnergySettings() {
        let city = VehicleProfile(
            name: "City",
            settings: VehicleEnergySettings(batteryCapacityKWh: 45)
        )
        let touring = VehicleProfile(
            name: "Touring",
            settings: VehicleEnergySettings(batteryCapacityKWh: 90)
        )
        var preferences = TripPreferences(
            vehicleProfiles: [city, touring],
            selectedVehicleProfileID: touring.id
        ).normalized

        XCTAssertEqual(preferences.activeVehicleSettings.batteryCapacityKWh, 90)
        preferences.selectedVehicleProfileID = city.id
        XCTAssertEqual(preferences.normalized.activeVehicleSettings.batteryCapacityKWh, 45)
    }

    @MainActor
    func testStationTargetTripKeepsArrivalSOCUnknownWithoutVehicleSOC() throws {
        let suiteName = "TripPlanningTests.\(UUID().uuidString)"
        let defaults = try XCTUnwrap(UserDefaults(suiteName: suiteName))
        defer { defaults.removePersistentDomain(forName: suiteName) }
        let store = TripStore(defaults: defaults)
        let feature = stationFeature(id: "direct-target")
        let origin = CLLocation(latitude: 48.09, longitude: 8.29)

        XCTAssertTrue(store.activateStationTarget(feature: feature, alternatives: [], from: origin))

        let plan = try XCTUnwrap(store.activePlan)
        XCTAssertEqual(
            plan.route.initialSOCPercent,
            plan.vehicleSettings.normalized.reserveSOCPercent,
            accuracy: 0.01
        )
        XCTAssertNil(plan.projectedArrivalSOC(for: feature.properties.stationID))

        let chargePlan = CarPlayChargePlan(
            settings: plan.vehicleSettings,
            maxPowerKW: feature.properties.displayedMaxPowerKW,
            status: feature.availabilityStatus,
            available: feature.availabilityCounts.available,
            total: feature.availabilityCounts.total,
            fromSOCPercent: plan.projectedArrivalSOC(for: feature.properties.stationID)
        )
        XCTAssertNotEqual(chargePlan.tier, .notNeeded)
    }

    func testTripStationAmenitiesPersistAndRemainBackwardCompatible() throws {
        let station = TripStationSnapshot(
            stationID: "amenity-station",
            operatorName: "IONITY",
            routePositionM: 170_000,
            amenitiesTotal: 2,
            amenities: [
                TripAmenitySnapshot(
                    category: "restaurant",
                    name: "Roadhouse",
                    openingHours: "Mo-Su 06:00-23:00",
                    distanceM: 80,
                    latitude: 48.12,
                    longitude: 8.34
                )
            ]
        )

        let encoded = try JSONEncoder().encode(station)
        let decoded = try JSONDecoder().decode(TripStationSnapshot.self, from: encoded)
        XCTAssertEqual(decoded.amenitiesTotal, 2)
        XCTAssertEqual(decoded.amenities?.first?.openingHours, "Mo-Su 06:00-23:00")
        XCTAssertEqual(decoded.amenities?.first?.coordinate?.latitude, 48.12)

        var legacyObject = try XCTUnwrap(JSONSerialization.jsonObject(with: encoded) as? [String: Any])
        if var legacyAmenity = (legacyObject["amenities"] as? [[String: Any]])?.first {
            legacyAmenity.removeValue(forKey: "latitude")
            legacyAmenity.removeValue(forKey: "longitude")
            legacyObject["amenities"] = [legacyAmenity]
        }
        let legacyAmenityData = try JSONSerialization.data(withJSONObject: legacyObject)
        let legacyAmenityDecoded = try JSONDecoder().decode(TripStationSnapshot.self, from: legacyAmenityData)
        XCTAssertNil(legacyAmenityDecoded.amenities?.first?.coordinate)

        legacyObject.removeValue(forKey: "amenities")
        legacyObject.removeValue(forKey: "amenitiesTotal")
        let legacyData = try JSONSerialization.data(withJSONObject: legacyObject)
        let legacyDecoded = try JSONDecoder().decode(TripStationSnapshot.self, from: legacyData)
        XCTAssertNil(legacyDecoded.amenities)
        XCTAssertNil(legacyDecoded.amenitiesTotal)
    }

    private func station(_ id: String, provider: String, positionKM: Int) -> TripStationSnapshot {
        TripStationSnapshot(
            stationID: id,
            operatorName: provider,
            latitude: 48,
            longitude: 8,
            routePositionM: positionKM * 1000
        )
    }

    private func stationFeature(id: String) -> GeoJSONFeature {
        GeoJSONFeature(
            id: id,
            geometry: GeoJSONPointGeometry(coordinates: [8.3, 48.1]),
            properties: ChargerProperties(
                stationID: id,
                operatorName: "IONITY",
                status: "In Betrieb",
                maxPowerKW: 300,
                chargingPointsCount: 4,
                maxIndividualPowerKW: 300,
                postcode: "76571",
                city: "Gaggenau",
                address: "Teststraße 1",
                occupancySourceUID: "",
                occupancySourceName: "",
                occupancyStatus: "free",
                occupancyLastUpdated: "",
                occupancyTotalEVSEs: 4,
                occupancyAvailableEVSEs: 2,
                occupancyOccupiedEVSEs: 2,
                occupancyChargingEVSEs: 0,
                occupancyOutOfOrderEVSEs: 0,
                occupancyUnknownEVSEs: 0,
                detailSourceUID: "",
                detailSourceName: "",
                detailLastUpdated: "",
                datexSiteID: "",
                datexStationIDs: "",
                datexChargePointIDs: "",
                priceDisplay: "0,65 €/kWh",
                priceEnergyEURKwhMin: "0.65",
                priceEnergyEURKwhMax: "0.65",
                priceCurrency: "EUR",
                priceQuality: "live",
                openingHoursDisplay: "24/7",
                openingHoursIs24_7: true,
                helpdeskPhone: "",
                paymentMethodsDisplay: "",
                authMethodsDisplay: "",
                connectorTypesDisplay: "CCS",
                currentTypesDisplay: "DC",
                connectorCount: 4,
                greenEnergy: nil,
                serviceTypesDisplay: "",
                detailsJSON: "",
                amenitiesTotal: 0,
                amenitiesSource: "",
                amenityExamples: [],
                amenityCounts: [:]
            )
        )
    }

    private func window(index: Int, ids: [String], selected: String?) -> ChargingWindow {
        ChargingWindow(
            index: index,
            startPositionM: index * 180_000,
            endPositionM: (index + 1) * 200_000,
            departurePositionM: index * 180_000,
            departureSOCPercent: 80,
            candidateStationIDs: ids,
            projectedArrivalSOCByStationID: Dictionary(uniqueKeysWithValues: ids.map { ($0, 15) }),
            selectedStationID: selected
        )
    }

    private func plan(
        stations: [TripStationSnapshot],
        windows: [ChargingWindow],
        selections: [TripStopSelection]
    ) -> RoutePlan {
        RoutePlan(
            id: UUID(),
            name: "Test route",
            route: TripRouteSnapshot(
                origin: RouteEndpoint(lat: 48, lon: 8, label: "Origin"),
                destination: RouteEndpoint(lat: 52, lon: 13, label: "Destination"),
                summary: try! JSONDecoder().decode(
                    RouteSummary.self,
                    from: Data(#"{"source":"test","profile":"driving","distance_m":500000,"duration_s":18000,"geometry":{"type":"LineString","coordinates":[[8,48],[13,52]]}}"#.utf8)
                ),
                filter: RouteFilterPayload(filter: FilterState()),
                initialSOCPercent: 70
            ),
            vehicleSettings: settings,
            rawStations: stations,
            windows: windows,
            stopSelections: selections,
            invalidatedStationIDs: [],
            providerMode: .prefer,
            selectedProviderNames: [],
            state: .draft,
            createdAt: Date(),
            updatedAt: Date()
        )
    }
}
