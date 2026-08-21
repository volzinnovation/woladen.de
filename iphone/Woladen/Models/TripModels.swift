import Foundation
import CoreLocation

enum WoladenMode: String, Codable, CaseIterable, Identifiable {
    case plan
    case trip

    var id: String { rawValue }

    var title: String {
        switch self {
        case .plan: return String(localized: "trip.mode.plan", defaultValue: "Plan")
        case .trip: return String(localized: "trip.mode.trip", defaultValue: "Trip")
        }
    }
}

enum TripVisualStyle: String, Codable, CaseIterable, Identifiable {
    case commandCenter
    case routeProgression
    case mapGlance

    var id: String { rawValue }

    var title: String {
        switch self {
        case .commandCenter:
            return String(localized: "trip.layout.command", defaultValue: "Command center")
        case .routeProgression:
            return String(localized: "trip.layout.progression", defaultValue: "Route progression")
        case .mapGlance:
            return String(localized: "trip.layout.map", defaultValue: "Map view")
        }
    }
}

enum NavigationApp: String, Codable, CaseIterable, Identifiable {
    case appleMaps
    case googleMaps

    var id: String { rawValue }

    var title: String {
        switch self {
        case .appleMaps: return "Apple Maps"
        case .googleMaps: return "Google Maps"
        }
    }
}

enum ProviderPackageMode: String, Codable, CaseIterable, Identifiable {
    case prefer
    case only

    var id: String { rawValue }

    var title: String {
        switch self {
        case .prefer:
            return String(localized: "trip.providers.prefer", defaultValue: "Prefer")
        case .only:
            return String(localized: "trip.providers.only", defaultValue: "Only these providers")
        }
    }
}

struct VehicleEnergySettings: Codable, Equatable {
    var batteryCapacityKWh: Double = 75
    var consumptionKWhPer100KM: Double = 18
    var reserveSOCPercent: Double = 10
    var targetSOCPercent: Double = 80
    var averageChargingPowerKW: Double = 120
    var earlyWindowKM: Double = 60
    var maximumDetourMinutes: Double = 15

    var normalized: VehicleEnergySettings {
        var value = self
        value.batteryCapacityKWh = value.batteryCapacityKWh.clamped(to: 10...200)
        value.consumptionKWhPer100KM = value.consumptionKWhPer100KM.clamped(to: 5...50)
        value.reserveSOCPercent = value.reserveSOCPercent.clamped(to: 5...40)
        value.targetSOCPercent = value.targetSOCPercent.clamped(
            to: min(90, value.reserveSOCPercent + 5)...90
        )
        value.averageChargingPowerKW = value.averageChargingPowerKW.clamped(to: 3...400)
        value.earlyWindowKM = value.earlyWindowKM.clamped(to: 10...120)
        value.maximumDetourMinutes = value.maximumDetourMinutes.clamped(to: 5...30)
        return value
    }

    func usableRangeKM(departureSOCPercent: Double) -> Double {
        let settings = normalized
        let usablePercent = max(0, departureSOCPercent.clamped(to: 0...100) - settings.reserveSOCPercent)
        let usableEnergy = settings.batteryCapacityKWh * usablePercent / 100
        guard settings.consumptionKWhPer100KM > 0 else { return 0 }
        return usableEnergy / settings.consumptionKWhPer100KM * 100
    }

    func projectedArrivalSOC(departureSOCPercent: Double, distanceKM: Double) -> Double {
        let settings = normalized
        guard settings.batteryCapacityKWh > 0 else { return 0 }
        let consumedKWh = max(0, distanceKM) * settings.consumptionKWhPer100KM / 100
        return (departureSOCPercent - consumedKWh / settings.batteryCapacityKWh * 100).clamped(to: 0...100)
    }
}

struct VehicleProfile: Codable, Identifiable, Equatable {
    var id: UUID
    var name: String
    var settings: VehicleEnergySettings

    init(id: UUID = UUID(), name: String, settings: VehicleEnergySettings = VehicleEnergySettings()) {
        self.id = id
        self.name = name
        self.settings = settings
    }
}

struct TripPreferences: Codable, Equatable {
    private static let migratedVehicleID = UUID(uuidString: "9D1435E2-AE7F-4FB7-A4E1-5748D9D67500")!

    // Retained as a migration field for preferences written by the first trip prototype.
    var vehicle = VehicleEnergySettings()
    var vehicleProfiles: [VehicleProfile]?
    var selectedVehicleProfileID: UUID?
    var visualStyle: TripVisualStyle = .commandCenter
    var providerMode: ProviderPackageMode = .prefer
    var selectedProviderNames: [String] = []
    var navigationApp: NavigationApp?
    var suggestTripMode: Bool? = true
    var automaticallyEnterTripMode = false
    var roadSpeedThresholdKPH: Double = 16

    var activeVehicleProfile: VehicleProfile {
        let profiles = normalized.vehicleProfiles ?? []
        return profiles.first { $0.id == normalized.selectedVehicleProfileID }
            ?? profiles.first
            ?? VehicleProfile(id: Self.migratedVehicleID, name: defaultVehicleName, settings: vehicle)
    }

    var activeVehicleSettings: VehicleEnergySettings {
        activeVehicleProfile.settings
    }

    var preferredNavigationApp: NavigationApp {
        navigationApp ?? .appleMaps
    }

    var shouldSuggestTripMode: Bool {
        suggestTripMode ?? true
    }

    mutating func updateActiveVehicleSettings(_ settings: VehicleEnergySettings) {
        var profiles = normalized.vehicleProfiles ?? []
        let activeID = normalized.selectedVehicleProfileID
        if let index = profiles.firstIndex(where: { $0.id == activeID }) {
            profiles[index].settings = settings.normalized
        }
        vehicleProfiles = profiles
        vehicle = settings.normalized
    }

    var normalized: TripPreferences {
        var value = self
        var profiles = value.vehicleProfiles ?? []
        if profiles.isEmpty {
            profiles = [
                VehicleProfile(
                    id: Self.migratedVehicleID,
                    name: defaultVehicleName,
                    settings: value.vehicle.normalized
                )
            ]
        }
        profiles = profiles.map { profile in
            var next = profile
            let trimmedName = profile.name.trimmingCharacters(in: .whitespacesAndNewlines)
            next.name = trimmedName.isEmpty ? defaultVehicleName : trimmedName
            next.settings = profile.settings.normalized
            return next
        }
        if !profiles.contains(where: { $0.id == value.selectedVehicleProfileID }) {
            value.selectedVehicleProfileID = profiles[0].id
        }
        value.vehicleProfiles = profiles
        value.vehicle = profiles.first { $0.id == value.selectedVehicleProfileID }?.settings ?? profiles[0].settings
        value.navigationApp = value.navigationApp ?? .appleMaps
        value.suggestTripMode = value.suggestTripMode ?? value.automaticallyEnterTripMode
        value.automaticallyEnterTripMode = false
        value.roadSpeedThresholdKPH = value.roadSpeedThresholdKPH.clamped(to: 10...40)
        value.selectedProviderNames = Array(Set(
            value.selectedProviderNames
                .map(normalizedProviderKey)
                .filter { !$0.isEmpty }
        )).sorted()
        return value
    }
}

private var defaultVehicleName: String {
    String(localized: "trip.vehicle.defaultName", defaultValue: "My car")
}

struct TripRouteSnapshot: Codable, Equatable {
    let origin: RouteEndpoint
    let destination: RouteEndpoint
    let distanceM: Int
    let durationS: Int
    let geometryCoordinates: [[Double]]
    let calculatedAt: Date
    let filter: RouteFilterPayload
    var initialSOCPercent: Double

    init(
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        summary: RouteSummary,
        filter: RouteFilterPayload,
        initialSOCPercent: Double,
        calculatedAt: Date = Date()
    ) {
        self.origin = origin
        self.destination = destination
        self.distanceM = summary.distanceM
        self.durationS = summary.durationS
        self.geometryCoordinates = summary.geometry.coordinates
        self.calculatedAt = calculatedAt
        self.filter = filter
        self.initialSOCPercent = initialSOCPercent.clamped(to: 1...100)
    }
}

extension TripRouteSnapshot {
    /// Rehydrates the persisted route into the same map-facing model used by
    /// a freshly calculated route. Persisted plans otherwise only appeared in
    /// their dedicated minimap.
    var mapRouteSummary: RouteSummary {
        RouteSummary(
            source: "saved-plan",
            distanceM: distanceM,
            durationS: durationS,
            geometry: RouteGeometry(coordinates: geometryCoordinates)
        )
    }
}

struct TripAmenitySnapshot: Codable, Identifiable, Equatable {
    let category: String
    let name: String?
    let openingHours: String?
    let distanceM: Double?

    var id: String {
        [
            category,
            name ?? "",
            openingHours ?? "",
            distanceM.map { String(format: "%.1f", $0) } ?? ""
        ].joined(separator: "|")
    }

    init(example: AmenityExample) {
        category = example.category
        name = example.name
        openingHours = example.openingHours
        distanceM = example.distanceM
    }

    init(
        category: String,
        name: String? = nil,
        openingHours: String? = nil,
        distanceM: Double? = nil
    ) {
        self.category = category
        self.name = name
        self.openingHours = openingHours
        self.distanceM = distanceM
    }
}

struct TripStationSnapshot: Codable, Identifiable, Equatable {
    let stationID: String
    let operatorName: String
    let city: String
    let address: String
    let latitude: Double
    let longitude: Double
    let maxPowerKW: Double
    let chargingPointsCount: Int
    let routePositionM: Int
    let driveDistanceToRouteM: Int
    let routeDetourM: Int
    let availabilityStatusRaw: String
    let availableEVSEs: Int
    let totalEVSEs: Int
    let lastUpdated: String
    let classificationRaw: String?
    let reliabilityPercent: Double?
    let lastUnavailableAt: String?
    let providerID: String?
    let amenitiesTotal: Int?
    let amenities: [TripAmenitySnapshot]?

    var id: String { stationID }
    var coordinate: CLLocationCoordinate2D { .init(latitude: latitude, longitude: longitude) }
    var availabilityStatus: AvailabilityStatus {
        AvailabilityStatus(rawValue: availabilityStatusRaw) ?? .unknown
    }
    var classification: StationClassification {
        if let explicit = classificationRaw.flatMap(StationClassification.init(rawValue:)) {
            return explicit
        }
        return .unclassified
    }

    init(feature: GeoJSONFeature) {
        let counts = feature.availabilityCounts
        stationID = feature.properties.stationID
        operatorName = feature.properties.operatorName
        city = feature.properties.city
        address = feature.properties.address
        latitude = feature.coordinate.latitude
        longitude = feature.coordinate.longitude
        maxPowerKW = feature.properties.displayedMaxPowerKW
        chargingPointsCount = feature.properties.chargingPointsCount
        routePositionM = feature.routeMetadata?.routePositionM ?? 0
        driveDistanceToRouteM = feature.routeMetadata?.driveDistanceToRouteM ?? 0
        routeDetourM = feature.routeMetadata?.routeDetourM ?? 0
        availabilityStatusRaw = feature.availabilityStatus.rawValue
        availableEVSEs = counts.available
        totalEVSEs = counts.total
        lastUpdated = [
            feature.liveSummary?.sourceObservedAt ?? "",
            feature.liveSummary?.fetchedAt ?? "",
            feature.properties.occupancyLastUpdated
        ].first { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty } ?? ""
        classificationRaw = feature.stationClassification.rawValue
        reliabilityPercent = feature.properties.reliabilityPercent
        lastUnavailableAt = feature.properties.lastUnavailableAt
        providerID = normalizedProviderKey(feature.properties.providerCanonicalID ?? feature.properties.operatorName)
        amenitiesTotal = feature.properties.amenitiesTotal
        amenities = feature.properties.amenityExamples.map(TripAmenitySnapshot.init(example:))
    }

    init(
        stationID: String,
        operatorName: String,
        city: String = "",
        address: String = "",
        latitude: Double = 0,
        longitude: Double = 0,
        maxPowerKW: Double = 150,
        chargingPointsCount: Int = 1,
        routePositionM: Int,
        driveDistanceToRouteM: Int = 0,
        routeDetourM: Int = 0,
        availabilityStatus: AvailabilityStatus = .unknown,
        availableEVSEs: Int = 0,
        totalEVSEs: Int = 0,
        lastUpdated: String = "",
        classification: StationClassification = .unclassified,
        reliabilityPercent: Double? = nil,
        lastUnavailableAt: String? = nil,
        providerID: String? = nil,
        amenitiesTotal: Int? = nil,
        amenities: [TripAmenitySnapshot]? = nil
    ) {
        self.stationID = stationID
        self.operatorName = operatorName
        self.city = city
        self.address = address
        self.latitude = latitude
        self.longitude = longitude
        self.maxPowerKW = maxPowerKW
        self.chargingPointsCount = chargingPointsCount
        self.routePositionM = max(0, routePositionM)
        self.driveDistanceToRouteM = max(0, driveDistanceToRouteM)
        self.routeDetourM = max(0, routeDetourM)
        self.availabilityStatusRaw = availabilityStatus.rawValue
        self.availableEVSEs = max(0, availableEVSEs)
        self.totalEVSEs = max(0, totalEVSEs)
        self.lastUpdated = lastUpdated
        self.classificationRaw = classification.rawValue
        self.reliabilityPercent = reliabilityPercent
        self.lastUnavailableAt = lastUnavailableAt
        self.providerID = providerID ?? normalizedProviderKey(operatorName)
        self.amenitiesTotal = amenitiesTotal
        self.amenities = amenities
    }
}

enum StationClassification: String, Codable, CaseIterable {
    case gold
    case silver
    case bronze
    case unclassified

    var title: String {
        switch self {
        case .gold: return "Gold"
        case .silver: return "Silber"
        case .bronze: return "Bronze"
        case .unclassified: return String(localized: "trip.station.unclassified", defaultValue: "Nicht klassifiziert")
        }
    }
}

enum TripStopState: String, Codable {
    case planned
    case completed
    case skipped
    case rejected
}

struct TripStopSelection: Codable, Identifiable, Equatable {
    let stationID: String
    var state: TripStopState
    let selectedAt: Date

    var id: String { stationID }
}

struct ChargingWindow: Codable, Identifiable, Equatable {
    let index: Int
    let startPositionM: Int
    let endPositionM: Int
    let departurePositionM: Int
    let departureSOCPercent: Double
    let candidateStationIDs: [String]
    let projectedArrivalSOCByStationID: [String: Double]
    var selectedStationID: String?

    var id: Int { index }
}

struct ProviderCoverage: Identifiable, Equatable {
    let providerName: String
    let stationCount: Int
    let coveredWindowCount: Int
    let totalWindowCount: Int
    let selectedStopCount: Int

    var id: String { normalizedProviderKey(providerName) }
    var coversEveryWindow: Bool { totalWindowCount > 0 && coveredWindowCount == totalWindowCount }
}

enum RoutePlanState: String, Codable {
    case draft
    case active
    case completed
}

struct RoutePlan: Codable, Identifiable, Equatable {
    let id: UUID
    var name: String
    var route: TripRouteSnapshot
    var vehicleSettings: VehicleEnergySettings
    var rawStations: [TripStationSnapshot]
    var windows: [ChargingWindow]
    var stopSelections: [TripStopSelection]
    var invalidatedStationIDs: [String]
    var providerMode: ProviderPackageMode
    var selectedProviderNames: [String]
    var state: RoutePlanState
    let createdAt: Date
    var updatedAt: Date
    var stationTargetID: String? = nil

    var selectedStopIDs: [String] {
        stopSelections
            .filter { $0.state == .planned }
            .map(\.stationID)
            .sorted { lhs, rhs in
                station(lhs)?.routePositionM ?? .max < station(rhs)?.routePositionM ?? .max
            }
    }

    var completedStopIDs: Set<String> {
        Set(stopSelections.filter { $0.state == .completed }.map(\.stationID))
    }

    var rejectedStopIDs: Set<String> {
        Set(stopSelections.filter { [.rejected, .skipped].contains($0.state) }.map(\.stationID))
    }

    var nextStop: TripStationSnapshot? {
        selectedStopIDs.compactMap(station).first
    }

    var isReadyForTrip: Bool {
        windows.isEmpty || windows.allSatisfy { $0.selectedStationID != nil }
    }

    var isStationTargetTrip: Bool {
        stationTargetID != nil
    }

    var visibleWindows: [ChargingWindow] {
        guard let firstUnresolved = windows.firstIndex(where: { $0.selectedStationID == nil }) else {
            return windows
        }
        return Array(windows.prefix(firstUnresolved + 1))
    }

    var providerCoverage: [ProviderCoverage] {
        EnergyRoutePlanner.providerCoverage(for: self)
    }

    func station(_ stationID: String) -> TripStationSnapshot? {
        rawStations.first { $0.stationID == stationID }
    }

    func window(containing stationID: String) -> ChargingWindow? {
        windows.first { $0.candidateStationIDs.contains(stationID) || $0.selectedStationID == stationID }
    }

    func projectedArrivalSOC(for stationID: String) -> Double? {
        windows.lazy.compactMap { $0.projectedArrivalSOCByStationID[stationID] }.first
    }
}

struct EnergyPlanResult: Equatable {
    let windows: [ChargingWindow]
    let isFeasible: Bool
    let firstUncoveredPositionM: Int?
    let invalidatedStationIDs: [String]
}

struct TripETAState: Equatable {
    var nextStopArrival: Date?
    var destinationArrival: Date?
    var nextStopTravelTime: TimeInterval?
    var totalTravelTime: TimeInterval?
    var updatedAt: Date
    var isStale: Bool
    var nextStopUsesTraffic = false
    var destinationUsesTraffic = false
    var nextStopIsLoading = false
    var destinationIsLoading = false

    static var unavailable: TripETAState {
        .init(
            nextStopArrival: nil,
            destinationArrival: nil,
            nextStopTravelTime: nil,
            totalTravelTime: nil,
            updatedAt: Date(),
            isStale: true,
            nextStopIsLoading: false,
            destinationIsLoading: false
        )
    }
}

func normalizedProviderKey(_ value: String) -> String {
    let rawKey = value.folding(options: [.caseInsensitive, .diacriticInsensitive], locale: .current)
        .replacingOccurrences(of: "[^a-z0-9]+", with: "", options: .regularExpression)
    switch rawKey {
    case let key where key.contains("ionity"):
        return "ionity"
    case let key where key.contains("tesla"):
        return "tesla"
    case let key where key.contains("enbw"):
        return "enbw"
    case let key where key.contains("mblty"):
        return "mblty"
    case let key where key.contains("mergermany"):
        return "mer"
    case let key where key.contains("shell"):
        return "shellrecharge"
    case let key where key.contains("aral") && key.contains("pulse"):
        return "aralpulse"
    case let key where key.contains("fastned"):
        return "fastned"
    case let key where key.contains("allego"):
        return "allego"
    default:
        return rawKey
    }
}

func providerPackageDisplayName(_ value: String) -> String {
    switch normalizedProviderKey(value) {
    case "ionity": return "IONITY"
    case "tesla": return "Tesla"
    case "enbw": return "EnBW"
    case "mblty": return "mblty"
    case "mer": return "Mer"
    case "shellrecharge": return "Shell Recharge"
    case "aralpulse": return "Aral pulse"
    case "fastned": return "Fastned"
    case "allego": return "Allego"
    default: return value
    }
}

extension GeoJSONFeature {
    var stationClassification: StationClassification {
        if let explicit = properties.stationClassification,
           let value = StationClassification(rawValue: explicit.lowercased()) {
            return value
        }
        if properties.amenitiesTotal > 10 { return .gold }
        if properties.amenitiesTotal > 5 { return .silver }
        if properties.amenitiesTotal > 0 { return .bronze }
        return .unclassified
    }
}

private extension Double {
    func clamped(to range: ClosedRange<Double>) -> Double {
        min(max(self, range.lowerBound), range.upperBound)
    }
}
