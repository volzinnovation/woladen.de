import UIKit
import CarPlay
import MapKit
import CoreLocation

private let carPlayPlanningStationLimit = 12
private let carPlayAmenityLimit = 5
private let carPlayChargingPointLimit = 5
private let carPlayPlanningLocationMaximumAge: TimeInterval = 5 * 60
private let carPlayPlanningLocationMaximumHorizontalAccuracy: CLLocationAccuracy = 250

func carPlayPlanningLocationIsUsable(_ location: CLLocation, now: Date = Date()) -> Bool {
    let age = now.timeIntervalSince(location.timestamp)
    return location.horizontalAccuracy >= 0
        && location.horizontalAccuracy <= carPlayPlanningLocationMaximumHorizontalAccuracy
        && age >= 0
        && age < carPlayPlanningLocationMaximumAge
}

func carPlayFreshestPlanningLocation(from locations: [CLLocation], now: Date = Date()) -> CLLocation? {
    locations
        .filter { carPlayPlanningLocationIsUsable($0, now: now) }
        .max { $0.timestamp < $1.timestamp }
}

func carPlayPlanningFeatureMatches(_ feature: GeoJSONFeature, filter: FilterState) -> Bool {
    var nonLiveFilter = filter
    nonLiveFilter.availableOnly = false
    guard feature.properties.matches(nonLiveFilter) else { return false }
    guard filter.availableOnly else { return true }
    let counts = feature.availabilityCounts
    return counts.total > 0 && counts.available > 0
}

func carPlayAmenityDetailText(
    distance: String?,
    openingHours: String?,
    countryCode: String?,
    now: Date = Date(),
    locale: Locale = .current
) -> String {
    let openingText = woladenAmenityOpeningDisplay(
        openingHours,
        now: now,
        timeZone: woladenOpeningTimeZone(countryCode: countryCode),
        countryCode: countryCode,
        locale: locale
    )
    return [distance, openingText]
        .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
        .filter { !$0.isEmpty }
        .joined(separator: " · ")
}

struct CarPlayChargePlan {
    enum Tier: Int, Equatable {
        case great
        case good
        case partial
        case slow
        case notNeeded
        case busy
        case unavailable
        case unknown
    }

    let fromPercent: Int
    let toPercent: Int
    let targetEnergyKWh: Double
    let estimatedMinutes: Int
    let tier: Tier

    init(
        settings rawSettings: VehicleEnergySettings,
        maxPowerKW: Double,
        status: AvailabilityStatus,
        available: Int,
        total: Int,
        fromSOCPercent: Double? = nil
    ) {
        let settings = rawSettings.normalized
        let chargeFromPercent = min(
            max(fromSOCPercent ?? settings.reserveSOCPercent, 0),
            settings.targetSOCPercent
        )
        fromPercent = Int(chargeFromPercent.rounded())
        toPercent = Int(settings.targetSOCPercent.rounded())
        targetEnergyKWh = settings.batteryCapacityKWh
            * max(0, settings.targetSOCPercent - chargeFromPercent) / 100
        let effectivePowerKW = min(max(0, maxPowerKW), settings.averageChargingPowerKW)
        estimatedMinutes = effectivePowerKW > 0 && targetEnergyKWh > 0
            ? Int(ceil(targetEnergyKWh / (effectivePowerKW * 0.9) * 60))
            : 0
        let ratio = settings.averageChargingPowerKW > 0
            ? effectivePowerKW / settings.averageChargingPowerKW
            : 0
        if targetEnergyKWh <= 0 {
            tier = .notNeeded
        } else if estimatedMinutes <= 0 {
            tier = .unknown
        } else if status == .outOfOrder {
            tier = .unavailable
        } else if total > 0, available <= 0, status == .occupied {
            tier = .busy
        } else if ratio >= 0.9 {
            tier = .great
        } else if ratio >= 0.65 {
            tier = .good
        } else if ratio >= 0.35 {
            tier = .partial
        } else {
            tier = .slow
        }
    }

    var tierLabel: String {
        switch tier {
        case .great:
            return String(localized: "carplay.fit.great", defaultValue: "Sehr passend")
        case .good:
            return String(localized: "carplay.fit.good", defaultValue: "Passt gut")
        case .partial:
            return String(localized: "carplay.fit.partial", defaultValue: "Teilweise passend")
        case .slow:
            return String(localized: "carplay.fit.slow", defaultValue: "Eher langsam")
        case .notNeeded:
            return String(localized: "carplay.fit.notNeeded", defaultValue: "Kein Laden nötig")
        case .busy:
            return String(localized: "carplay.fit.busy", defaultValue: "Derzeit belegt")
        case .unavailable:
            return String(localized: "carplay.fit.unavailable", defaultValue: "Nicht verfügbar")
        case .unknown:
            return String(localized: "carplay.fit.unknown", defaultValue: "Ladezeit unbekannt")
        }
    }

    var energyText: String {
        targetEnergyKWh.formatted(.number.precision(.fractionLength(0...1)))
    }

    var needTitle: String {
        "\(String(localized: "carplay.need", defaultValue: "Bedarf")) \(fromPercent)% → \(toPercent)% · \(energyText) kWh"
    }

    var stationSummary: String {
        guard estimatedMinutes > 0 else { return tierLabel }
        return String(localized: "carplay.fit.estimate", defaultValue: "{fit} · ~{energy} kWh in {minutes} min")
            .replacingOccurrences(of: "{fit}", with: tierLabel)
            .replacingOccurrences(of: "{energy}", with: energyText)
            .replacingOccurrences(of: "{minutes}", with: "\(estimatedMinutes)")
    }
}

private struct CarPlayAmenityContent {
    let category: String
    let name: String
    let openingHours: String?
    let countryCode: String?
    let distanceM: Double?
    let coordinate: CLLocationCoordinate2D?

    init(_ example: AmenityExample, countryCode: String?) {
        category = example.category
        name = example.name ?? AmenityCatalog.label(for: "amenity_\(example.category)")
        openingHours = example.openingHours
        self.countryCode = countryCode
        distanceM = example.distanceM
        coordinate = example.coordinate
    }

    init(_ snapshot: TripAmenitySnapshot, countryCode: String?) {
        category = snapshot.category
        name = snapshot.name ?? AmenityCatalog.label(for: "amenity_\(snapshot.category)")
        openingHours = snapshot.openingHours
        self.countryCode = countryCode
        distanceM = snapshot.distanceM
        coordinate = snapshot.coordinate
    }
}

private struct CarPlayChargingPointContent {
    let title: String
    let status: AvailabilityStatus
    let detail: String
}

private struct CarPlayStationContent {
    let stationID: String
    let name: String
    let operatorName: String
    let city: String
    let address: String
    let coordinate: CLLocationCoordinate2D
    let maxPowerKW: Double
    let chargingPointsCount: Int
    let availabilityStatus: AvailabilityStatus
    let availabilityText: String
    let price: String
    let freshness: String
    let amenities: [CarPlayAmenityContent]
    let amenitiesTotal: Int
    let chargingPoints: [CarPlayChargingPointContent]
    let classification: StationClassification
    let cardState: StationCardState
    let locationDistance: String?
    let eta: String?
    let routeDistance: String?
    let chargePlan: CarPlayChargePlan

    init(feature: GeoJSONFeature, location: CLLocation?, settings: VehicleEnergySettings) {
        let rawStationName = feature.properties.stationName.trimmingCharacters(in: .whitespacesAndNewlines)
        let rawOperatorName = feature.properties.operatorName.trimmingCharacters(in: .whitespacesAndNewlines)
        let counts = feature.availabilityCounts
        stationID = feature.properties.stationID
        name = [rawStationName, rawOperatorName]
            .first { !$0.isEmpty }
            ?? String(localized: "station.title", defaultValue: "Charging station")
        operatorName = rawOperatorName
        city = feature.properties.city
        address = feature.properties.address
        coordinate = feature.coordinate
        maxPowerKW = feature.properties.displayedMaxPowerKW
        chargingPointsCount = max(counts.total, feature.properties.chargingPointsCount)
        availabilityStatus = feature.availabilityStatus
        availabilityText = feature.occupancySummaryLabel
            ?? (counts.total > 0
                ? "\(counts.available)/\(counts.total) \(String(localized: "availability.free").lowercased())"
                : feature.availabilityStatus.label)
        price = feature.displayPrice.trimmingCharacters(in: .whitespacesAndNewlines)
        freshness = Self.relativeFreshness(
            feature.liveSummary?.sourceObservedAt
                ?? feature.liveSummary?.fetchedAt
                ?? feature.properties.occupancyLastUpdated
        )
        amenities = feature.properties.amenityExamples
            .map { CarPlayAmenityContent($0, countryCode: feature.properties.countryCode) }
            .sorted { ($0.distanceM ?? .greatestFiniteMagnitude) < ($1.distanceM ?? .greatestFiniteMagnitude) }
        amenitiesTotal = max(feature.properties.amenitiesTotal, amenities.count)
        let rows = feature.liveEVSERows
        if rows.isEmpty {
            chargingPoints = [CarPlayChargingPointContent(
                title: "\(chargingPointsCount) \(String(localized: "station.chargingPoints", defaultValue: "Ladepunkte"))",
                status: feature.availabilityStatus,
                detail: "\(Int(feature.properties.displayedMaxPowerKW.rounded())) kW · \(availabilityText)"
            )]
        } else {
            chargingPoints = rows.map { row in
                let note = row.notes.first.map { "\($0.label): \($0.value)" }
                let detail = [row.status.label, row.meta, row.price, note]
                    .compactMap { value -> String? in
                        guard let value else { return nil }
                        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
                        return trimmed.isEmpty ? nil : trimmed
                    }
                    .joined(separator: " · ")
                return CarPlayChargingPointContent(title: row.title, status: row.status, detail: detail)
            }
        }
        classification = feature.stationClassification
        cardState = feature.stationCardState
        locationDistance = location.map {
            Self.formatMeasurement(
                $0.distance(from: CLLocation(
                    latitude: feature.coordinate.latitude,
                    longitude: feature.coordinate.longitude
                ))
            )
        }
        eta = nil
        routeDistance = nil
        chargePlan = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: feature.properties.displayedMaxPowerKW,
            status: feature.availabilityStatus,
            available: counts.available,
            total: counts.total
        )
    }

    init(
        station: TripStationSnapshot,
        liveSummary: LiveStationSummary?,
        liveDetail: LiveStationDetail?,
        eta: String?,
        routeDistance: String?,
        settings: VehicleEnergySettings,
        arrivalSOCPercent: Double?,
        catalogFeature: GeoJSONFeature?
    ) {
        let summary = liveDetail?.station ?? liveSummary
        let status = summary?.availabilityStatus ?? station.availabilityStatus
        let available = summary?.availableEVSEs ?? station.availableEVSEs
        let total = summary?.totalEVSEs ?? station.totalEVSEs
        let rawStationName = [catalogFeature?.properties.stationName, station.stationName]
            .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
            .first { !$0.isEmpty } ?? ""
        let rawOperatorName = [catalogFeature?.properties.operatorName, station.operatorName]
            .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
            .first { !$0.isEmpty } ?? ""
        let countryCode = catalogFeature?.properties.countryCode.nilIfBlank ?? station.countryCode
        stationID = station.stationID
        name = [rawStationName, rawOperatorName]
            .first { !$0.isEmpty }
            ?? String(localized: "station.title", defaultValue: "Charging station")
        operatorName = rawOperatorName
        city = station.city
        address = station.address
        coordinate = station.coordinate
        maxPowerKW = station.maxPowerKW
        chargingPointsCount = max(total, station.chargingPointsCount)
        availabilityStatus = status
        let counts = AvailabilityCounts(
            total: total,
            available: available,
            occupied: summary?.occupiedEVSEs ?? station.occupiedEVSEs ?? 0,
            outOfOrder: summary?.outOfOrderEVSEs ?? station.outOfOrderEVSEs ?? 0,
            unknown: summary?.unknownEVSEs ?? station.unknownEVSEs ?? 0
        )
        availabilityText = woladenAvailabilitySummary(counts) ?? status.label
        let summaryPrice = summary?.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let detailPrice = liveDetail?.evses.lazy.map(\.priceDisplay)
            .first { !$0.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
        let catalogPrice = catalogFeature?.displayPrice.trimmingCharacters(in: .whitespacesAndNewlines)
        price = [summaryPrice, detailPrice, catalogPrice, station.priceDisplay]
            .compactMap { $0?.trimmingCharacters(in: .whitespacesAndNewlines) }
            .first { !$0.isEmpty } ?? ""
        freshness = Self.relativeFreshness(
            summary?.sourceObservedAt
                ?? summary?.fetchedAt
                ?? station.lastUpdated
        )
        let detailedAmenities = catalogFeature?.properties.amenityExamples ?? []
        amenities = (detailedAmenities.isEmpty
            ? (station.amenities ?? []).map { CarPlayAmenityContent($0, countryCode: countryCode) }
            : detailedAmenities.map { CarPlayAmenityContent($0, countryCode: countryCode) })
            .sorted { ($0.distanceM ?? .greatestFiniteMagnitude) < ($1.distanceM ?? .greatestFiniteMagnitude) }
        amenitiesTotal = max(
            catalogFeature?.properties.amenitiesTotal ?? 0,
            station.amenitiesTotal ?? amenities.count
        )
        if let liveDetail, !liveDetail.evses.isEmpty {
            chargingPoints = liveDetail.evses.enumerated().map { index, evse in
                let evseID = evse.providerEVSEID.trimmingCharacters(in: .whitespacesAndNewlines)
                let evsePrice = evse.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines)
                return CarPlayChargingPointContent(
                    title: String(localized: "station.evse")
                        .replacingOccurrences(of: "{index}", with: "\(index + 1)"),
                    status: evse.availabilityStatus,
                    detail: [evse.availabilityStatus.label, evseID, evsePrice]
                        .filter { !$0.isEmpty }
                        .joined(separator: " · ")
                )
            }
        } else {
            chargingPoints = [CarPlayChargingPointContent(
                title: "\(chargingPointsCount) \(String(localized: "station.chargingPoints", defaultValue: "Ladepunkte"))",
                status: status,
                detail: "\(Int(station.maxPowerKW.rounded())) kW · \(availabilityText)"
            )]
        }
        classification = station.classification
        cardState = woladenStationCardState(
            status: status,
            counts: counts,
            oftenBroken: summary?.isOftenBrokenFromDailyAnalysis ?? station.oftenBrokenDailyAnalysis ?? false,
            oftenOccupied: summary?.isOftenOccupiedFromDailyAnalysis ?? station.oftenOccupiedDailyAnalysis ?? false
        )
        locationDistance = nil
        self.eta = eta
        self.routeDistance = routeDistance
        chargePlan = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: station.maxPowerKW,
            status: status,
            available: available,
            total: total,
            fromSOCPercent: arrivalSOCPercent
        )
    }

    var locationLine: String {
        [address, city, locationDistance]
            .compactMap { value in
                guard let value else { return nil }
                let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
                return trimmed.isEmpty ? nil : trimmed
            }
            .joined(separator: " · ")
    }

    var operatorLine: String? {
        let value = operatorName.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !value.isEmpty, value.caseInsensitiveCompare(name) != .orderedSame else { return nil }
        return value
    }

    var etaLine: String? {
        eta?.nilIfBlank.map { "\(String(localized: "trip.eta", defaultValue: "ETA")) \($0)" }
    }

    var distanceLine: String? {
        routeDistance?.nilIfBlank.map {
            "\(String(localized: "trip.distance", defaultValue: "Distance")) \($0)"
        }
    }

    var classificationText: String {
        switch classification {
        case .gold:
            return String(localized: "carplay.classification.gold", defaultValue: "🥇 Gold")
        case .silver:
            return String(localized: "carplay.classification.silver", defaultValue: "🥈 Silver")
        case .bronze:
            return String(localized: "carplay.classification.bronze", defaultValue: "🥉 Bronze")
        case .unclassified:
            return String(localized: "carplay.classification.unclassified", defaultValue: "Unclassified")
        }
    }

    var classificationEmoji: String? {
        switch classification {
        case .gold: return "🥇"
        case .silver: return "🥈"
        case .bronze: return "🥉"
        case .unclassified: return nil
        }
    }

    var statusText: String {
        switch cardState {
        case .outOfOrder:
            return String(localized: "carplay.status.outOfOrder", defaultValue: "🔴 Red · Out of order")
        case .occupied:
            return String(localized: "carplay.status.occupied", defaultValue: "⬜ Grey · Occupied")
        case .oneFreeLeft:
            return String(localized: "carplay.status.oneFreeLeft", defaultValue: "🟡 Nearly occupied · One charging point left")
        case .oftenBroken:
            return String(localized: "carplay.status.oftenBroken", defaultValue: "🔴 Red · Often out of order")
        case .oftenOccupied:
            return String(localized: "carplay.status.oftenOccupied", defaultValue: "⬜ Grey · Often occupied")
        case .unknown:
            return String(localized: "carplay.status.unknown", defaultValue: "❔ Availability unknown")
        case .default:
            return String(localized: "carplay.status.available", defaultValue: "🟢 Available")
        }
    }

    var classificationAndStatusText: String {
        "\(classificationText) · \(statusText)"
    }

    var headerSubtitle: String? {
        operatorLine ?? etaLine ?? locationLine.nilIfBlank
    }

    var powerAndPointsText: String {
        String(localized: "carplay.powerAndPoints", defaultValue: "{power} kW max · {count} Ladepunkte")
            .replacingOccurrences(of: "{power}", with: "\(Int(maxPowerKW.rounded()))")
            .replacingOccurrences(of: "{count}", with: "\(chargingPointsCount)")
    }

    var overlayText: String {
        [classificationEmoji, price.nilIfBlank, "\(Int(maxPowerKW.rounded())) kW"]
            .compactMap { $0 }
            .joined(separator: " · ")
    }

    func bodyVariants(headerSubtitle: String?) -> [NSAttributedString] {
        let amenityText = amenitiesTotal <= 0
            ? nil
            : String(localized: "carplay.amenitiesCount", defaultValue: "{count} Angebote vor Ort")
                .replacingOccurrences(of: "{count}", with: "\(amenitiesTotal)")
        let etaBodyLine = headerSubtitle == etaLine ? nil : etaLine
        let locationBodyLine = headerSubtitle == locationLine.nilIfBlank ? nil : locationLine.nilIfBlank
        let full = [
            etaBodyLine,
            distanceLine,
            classificationAndStatusText,
            locationBodyLine,
            chargePlan.stationSummary,
            powerAndPointsText,
            amenityText,
            freshness.nilIfBlank
        ]
            .compactMap { $0 }
            .joined(separator: "\n")
        let compact = [etaBodyLine, distanceLine, classificationAndStatusText]
            .compactMap { $0 }
            .joined(separator: "\n")
        return [full, compact]
            .filter { !$0.isEmpty }
            .map { NSAttributedString(string: $0) }
    }

    private static func relativeFreshness(_ raw: String) -> String {
        guard let date = woladenParseISO8601(raw) else { return "" }
        return date.formatted(.relative(presentation: .named, unitsStyle: .abbreviated))
    }

    private static func formatMeasurement(_ meters: CLLocationDistance) -> String {
        Measurement(value: max(0, meters), unit: UnitLength.meters)
            .formatted(.measurement(width: .abbreviated, usage: .road))
    }
}

private extension String {
    var nilIfBlank: String? {
        let value = trimmingCharacters(in: .whitespacesAndNewlines)
        return value.isEmpty ? nil : value
    }
}

@MainActor
final class CarPlaySceneDelegate: UIResponder, CPTemplateApplicationSceneDelegate, @preconcurrency CLLocationManagerDelegate {
    private weak var interfaceController: CPInterfaceController?
    private let locationManager = CLLocationManager()
    private let repository = ChargerRepository()
    private let liveAPIClient = LiveAPIClient()
    private var latestLocation: CLLocation?
    private var planningFeatures: [GeoJSONFeature] = []
    private var planningFilter = FilterStateStore.load()
    private var planningTemplate: CPListTemplate?
    private weak var planningDetailListTemplate: CPListTemplate?
    private var planningDetailFeature: GeoJSONFeature?
    private var tripPOITemplate: CPPointOfInterestTemplate?
    private var lastTripPOIUpdateAt: Date?
    private var tripListTemplate: CPListTemplate?
    private var tripTemplateStationID: String?
    private var tripCatalogFeature: GeoJSONFeature?
    private var tripCatalogStationID: String?
    private var tripLiveDetail: LiveStationDetail?
    private var tripLiveStationID: String?
    private var mapImagesByStationID: [String: UIImage] = [:]
    private var refreshTimer: Timer?
    private var refreshTask: Task<Void, Never>?
    private var planningDetailTask: Task<Void, Never>?
    private var tripDetailTask: Task<Void, Never>?
    private var refreshPending = false
    private var displayedMode: WoladenWidgetMode?
    private var hasRefreshedPlanningWithLocation = false

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didConnect interfaceController: CPInterfaceController
    ) {
        self.interfaceController = interfaceController
        configureLocationUpdates()
        showImmediateRoot()
        scheduleRefreshes()
        refresh(force: true)
    }

    func templateApplicationScene(
        _ templateApplicationScene: CPTemplateApplicationScene,
        didDisconnectInterfaceController interfaceController: CPInterfaceController
    ) {
        refreshTimer?.invalidate()
        refreshTimer = nil
        refreshTask?.cancel()
        refreshTask = nil
        planningDetailTask?.cancel()
        planningDetailTask = nil
        planningDetailListTemplate = nil
        planningDetailFeature = nil
        tripDetailTask?.cancel()
        tripDetailTask = nil
        locationManager.stopUpdatingLocation()
        self.interfaceController = nil
        planningTemplate = nil
        clearTripTemplates()
        displayedMode = nil
        hasRefreshedPlanningWithLocation = false
    }

    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard let location = carPlayFreshestPlanningLocation(from: locations) else { return }
        if let latestLocation,
           carPlayPlanningLocationIsUsable(latestLocation),
           latestLocation.timestamp >= location.timestamp {
            return
        }
        latestLocation = location
        TripStore.shared.updateLocation(location)
        if currentMode == .plan, !hasRefreshedPlanningWithLocation {
            refresh(force: true)
        }
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        // Keep the last known planning grid if a location refresh fails.
    }

    private func configureLocationUpdates() {
        locationManager.delegate = self
        locationManager.desiredAccuracy = kCLLocationAccuracyHundredMeters
        locationManager.distanceFilter = 100
        switch locationManager.authorizationStatus {
        case .authorizedAlways, .authorizedWhenInUse:
            locationManager.startUpdatingLocation()
        case .notDetermined:
            locationManager.requestWhenInUseAuthorization()
            locationManager.startUpdatingLocation()
        default:
            break
        }
    }

    private func scheduleRefreshes() {
        refreshTimer?.invalidate()
        refreshTimer = Timer.scheduledTimer(withTimeInterval: 30, repeats: true) { [weak self] _ in
            Task { @MainActor in
                guard let self else { return }
                self.refresh(force: false)
            }
        }
    }

    private func showImmediateRoot() {
        displayedMode = currentMode
        if currentMode == .trip {
            setRoot(makeTripTemplate(), animated: false)
        } else {
            planningFilter = FilterStateStore.load()
            let template = makePlanningTemplate(
                items: [placeholderListItem(title: String(localized: "carplay.loading", defaultValue: "Stationen werden geladen …"))],
                filter: planningFilter
            )
            planningTemplate = template
            setRoot(template, animated: false)
        }
    }

    private var currentMode: WoladenWidgetMode {
        TripStore.shared.mode == .trip && TripStore.shared.activePlan?.nextStop != nil ? .trip : .plan
    }

    private var activeVehicleSettings: VehicleEnergySettings {
        TripStore.shared.preferences.activeVehicleSettings
    }

    private func refresh(force: Bool) {
        guard refreshTask == nil else {
            refreshPending = true
            return
        }
        refreshTask = Task { [weak self] in
            guard let self else { return }
            defer {
                refreshTask = nil
                if refreshPending {
                    refreshPending = false
                    refresh(force: true)
                }
            }
            let mode = currentMode
            if mode != displayedMode {
                displayedMode = mode
                if mode == .trip {
                    planningTemplate = nil
                    setRoot(makeTripTemplate(), animated: true)
                } else {
                    clearTripTemplates()
                    planningFilter = FilterStateStore.load()
                    let template = makePlanningTemplate(
                        items: [placeholderListItem(title: String(localized: "carplay.loading", defaultValue: "Stationen werden geladen …"))],
                        filter: planningFilter
                    )
                    planningTemplate = template
                    setRoot(template, animated: true)
                }
            }
            if mode == .trip {
                await TripStore.shared.refreshLive(force: force)
                guard !Task.isCancelled else { return }
                updateTripTemplate()
            } else {
                await refreshPlanningStations()
            }
        }
    }

    private func refreshPlanningStations() async {
        planningFilter = FilterStateStore.load()
        let locationCandidates = [latestLocation, locationManager.location].compactMap { $0 }
        guard let location = carPlayFreshestPlanningLocation(from: locationCandidates) else {
            latestLocation = nil
            hasRefreshedPlanningWithLocation = false
            planningFeatures = []
            updatePlanningList(
                items: [placeholderListItem(title: String(localized: "carplay.location.required", defaultValue: "Standort in Woladen erlauben"))],
                filter: planningFilter
            )
            return
        }
        latestLocation = location
        hasRefreshedPlanningWithLocation = true
        do {
            var catalogFilter = planningFilter
            catalogFilter.availableOnly = false
            let result = try await repository.searchCatalog(
                center: location.coordinate,
                filter: catalogFilter,
                radiusM: 20_000,
                limit: 100
            )
            guard !Task.isCancelled else { return }
            var candidates = result.features
                .filter { $0.properties.matches(catalogFilter) }
                .sorted { stationDistance($0, from: location) < stationDistance($1, from: location) }
            candidates = Array(candidates.prefix(60))
            if !candidates.isEmpty {
                let lookup = try await liveAPIClient.lookupStations(
                    stationIDs: candidates.map { $0.properties.stationID }
                )
                let liveByID = Dictionary(uniqueKeysWithValues: lookup.stations.map { ($0.stationID, $0) })
                candidates = candidates.map { feature in
                    var hydrated = feature
                    hydrated.liveSummary = liveByID[feature.properties.stationID]
                    return hydrated
                }
            }
            if let selectedID = planningDetailFeature?.properties.stationID,
               let refreshed = candidates.first(where: { $0.properties.stationID == selectedID }) {
                var current = planningDetailFeature ?? refreshed
                current.liveSummary = refreshed.liveSummary
                planningDetailFeature = current
            }
            guard !Task.isCancelled else { return }
            let rankingSettings = activeVehicleSettings
            let filtered = candidates
                .filter { carPlayPlanningFeatureMatches($0, filter: planningFilter) }
                .sorted {
                    planningStationPrecedes($0, $1, from: location, settings: rankingSettings)
                }
            planningFeatures = Array(filtered.prefix(planningResultLimit))
            let items = planningFeatures.isEmpty
                ? [placeholderListItem(title: String(localized: "carplay.noMatch", defaultValue: "Keine passende Station"))]
                : planningFeatures.map { planningListItem(for: $0, location: location) }
            updatePlanningList(items: items, filter: planningFilter)
            if #available(iOS 26.4, *),
               let template = planningDetailListTemplate,
               interfaceController?.topTemplate === template,
               let detailFeature = planningDetailFeature {
                schedulePlanningDetailRefresh(detailFeature, from: location, template: template)
            }
        } catch {
            planningFeatures = planningFeatures.filter {
                carPlayPlanningFeatureMatches($0, filter: planningFilter)
            }
            let items = planningFeatures.isEmpty
                ? [placeholderListItem(title: String(localized: "carplay.offline", defaultValue: "Keine Live-Daten"))]
                : planningFeatures.map { planningListItem(for: $0, location: location, stale: true) }
            updatePlanningList(items: items, filter: planningFilter)
        }
    }

    private var planningResultLimit: Int {
        min(carPlayPlanningStationLimit, CPListTemplate.maximumItemCount)
    }

    private func makePlanningTemplate(items: [CPListItem], filter: FilterState) -> CPListTemplate {
        let template = CPListTemplate(
            title: planningTitle(filter),
            sections: [CPListSection(items: Array(items.prefix(planningResultLimit)))]
        )
        configureFilterButton(on: template, filter: filter)
        return template
    }

    private func updatePlanningList(items: [CPListItem], filter: FilterState) {
        let section = CPListSection(items: Array(items.prefix(planningResultLimit)))
        if let planningTemplate {
            if planningTemplate.title != planningTitle(filter) {
                let template = makePlanningTemplate(items: items, filter: filter)
                self.planningTemplate = template
                if currentMode == .plan { setRoot(template, animated: false) }
                return
            }
            planningTemplate.updateSections([section])
            configureFilterButton(on: planningTemplate, filter: filter)
        } else {
            let template = makePlanningTemplate(items: items, filter: filter)
            planningTemplate = template
            if currentMode == .plan { setRoot(template, animated: false) }
        }
    }

    private func planningTitle(_ filter: FilterState) -> String {
        let nearby = String(localized: "carplay.nearby", defaultValue: "Laden in der Nähe")
        let compact = filter.activeDisplayLabels.prefix(2).joined(separator: " · ")
        return compact.isEmpty ? nearby : "\(nearby) · \(compact)"
    }

    private func configureFilterButton(on template: CPListTemplate, filter: FilterState) {
        let settings = activeVehicleSettings.normalized
        let need = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: settings.averageChargingPowerKW,
            status: .free,
            available: 1,
            total: 1
        )
        let needButton = CPBarButton(title: need.needTitle) { [weak self] _ in
            self?.showChargeNeedInformation(need, settings: settings)
        }
        needButton.buttonStyle = .rounded
        let button = CPBarButton(title: "\(String(localized: "carplay.filter", defaultValue: "Filter")) (\(filter.activeCount))") { [weak self] _ in
            self?.showFilterInformation(filter)
        }
        button.buttonStyle = .rounded
        template.leadingNavigationBarButtons = [needButton]
        template.trailingNavigationBarButtons = [button]
    }

    private func showChargeNeedInformation(_ need: CarPlayChargePlan, settings: VehicleEnergySettings) {
        guard let interfaceController else { return }
        let addedRange = settings.consumptionKWhPer100KM > 0
            ? Int((need.targetEnergyKWh / settings.consumptionKWhPer100KM * 100).rounded())
            : 0
        let items = [
            CPInformationItem(
                title: String(localized: "carplay.need.window", defaultValue: "Ladefenster"),
                detail: "\(need.fromPercent)% → \(need.toPercent)%"
            ),
            CPInformationItem(
                title: String(localized: "carplay.need.energy", defaultValue: "Energiebedarf"),
                detail: "~\(need.energyText) kWh"
            ),
            CPInformationItem(
                title: String(localized: "carplay.need.range", defaultValue: "Zusätzliche Reichweite"),
                detail: "~\(addedRange) km"
            ),
            CPInformationItem(
                title: String(localized: "carplay.need.vehicle", defaultValue: "Fahrzeugprofil"),
                detail: TripStore.shared.preferences.activeVehicleProfile.name
            )
        ]
        let template = CPInformationTemplate(
            title: String(localized: "carplay.need.title", defaultValue: "Dein Ladebedarf"),
            layout: .twoColumn,
            items: items,
            actions: []
        )
        interfaceController.pushTemplate(template, animated: true, completion: nil)
    }

    private func showFilterInformation(_ filter: FilterState) {
        guard let interfaceController else { return }
        let labels = filter.activeDisplayLabels
        let items: [CPInformationItem]
        if labels.isEmpty {
            items = [CPInformationItem(
                title: String(localized: "carplay.filter.none", defaultValue: "Keine zusätzlichen Filter"),
                detail: String(localized: "carplay.filter.noneDetail", defaultValue: "Alle passenden Stationen werden berücksichtigt.")
            )]
        } else {
            items = Array(labels.prefix(10)).enumerated().map { index, label in
                CPInformationItem(title: "\(index + 1)", detail: label)
            }
        }
        let template = CPInformationTemplate(
            title: String(localized: "carplay.filter.active", defaultValue: "Aktive Filter"),
            layout: .leading,
            items: items,
            actions: []
        )
        interfaceController.pushTemplate(template, animated: true, completion: nil)
    }

    private func planningListItem(for feature: GeoJSONFeature, location: CLLocation, stale: Bool = false) -> CPListItem {
        let content = CarPlayStationContent(feature: feature, location: location, settings: activeVehicleSettings)
        let staleText = stale ? " · \(String(localized: "carplay.stale", defaultValue: "veraltet"))" : ""
        let price = content.price.nilIfBlank
            ?? String(localized: "carplay.price.unknown", defaultValue: "Preis unbekannt")
        let locationDistance = content.locationDistance ?? content.city
        let item = CPListItem(
            text: [planningClassificationPrefix(content.classification), content.name]
                .filter { !$0.isEmpty }
                .joined(separator: " "),
            detailText: "\(locationDistance) · \(content.availabilityText) · \(Int(content.maxPowerKW.rounded())) kW · \(price)\(staleText)"
        )
        item.accessoryType = .disclosureIndicator
        item.handler = { [weak self] _, completion in
            self?.showPlanningStation(feature, from: location)
            completion()
        }
        return item
    }

    private func planningClassificationPrefix(_ classification: StationClassification) -> String {
        switch classification {
        case .gold: return "[GOLD]"
        case .silver: return "[SILVER]"
        case .bronze: return "[BRONZE]"
        case .unclassified: return ""
        }
    }

    private func placeholderListItem(title: String) -> CPListItem {
        let item = CPListItem(text: title, detailText: nil)
        item.isEnabled = false
        return item
    }

    private func showPlanningStation(_ feature: GeoJSONFeature, from location: CLLocation) {
        guard let interfaceController else { return }
        planningDetailTask?.cancel()
        if #available(iOS 26.4, *) {
            let content = CarPlayStationContent(feature: feature, location: location, settings: activeVehicleSettings)
            let template = makeStationListTemplate(
                content: content,
                mapImage: cachedMapImage(for: content) ?? placeholderMapImage(for: content.cardState),
                actions: planningHeaderButtons(feature)
            )
            planningDetailListTemplate = template
            planningDetailFeature = feature
            interfaceController.pushTemplate(template, animated: true, completion: nil)
            schedulePlanningDetailRefresh(feature, from: location, template: template)
        } else {
            let content = CarPlayStationContent(feature: feature, location: location, settings: activeVehicleSettings)
            let template = makePOITemplate(
                content: content,
                primary: CPTextButton(title: String(localized: "carplay.select", defaultValue: "Auswählen"), textStyle: .confirm) { [weak self] _ in
                    self?.selectPlanningStation(feature)
                },
                secondary: navigationTextButton(for: content),
                detailsHandler: { [weak self] in self?.showInformation(for: content) }
            )
            interfaceController.pushTemplate(template, animated: true, completion: nil)
        }
    }

    private func selectPlanningStation(_ feature: GeoJSONFeature) {
        _ = TripStore.shared.activateStationTarget(
            feature: feature,
            alternatives: planningFeatures,
            from: latestLocation
        )
        displayedMode = .trip
        planningTemplate = nil
        planningDetailTask?.cancel()
        planningDetailListTemplate = nil
        planningDetailFeature = nil
        clearTripTemplates()
        setRoot(makeTripTemplate(), animated: true)
    }

    @available(iOS 26.4, *)
    private func schedulePlanningDetailRefresh(
        _ feature: GeoJSONFeature,
        from location: CLLocation,
        template: CPListTemplate
    ) {
        planningDetailTask?.cancel()
        planningDetailTask = Task { [weak self, weak template] in
            guard let self, let template else { return }
            let detailed = (try? await repository.stationDetail(
                stationID: feature.properties.stationID,
                preserving: feature
            )) ?? feature
            var enriched = detailed
            if let live = try? await liveAPIClient.stationDetail(stationID: feature.properties.stationID) {
                enriched.liveDetail = live
                enriched.liveSummary = live.station
            } else if enriched.liveSummary == nil {
                enriched.liveSummary = feature.liveSummary
            }
            guard !Task.isCancelled else { return }
            let enrichedContent = CarPlayStationContent(feature: enriched, location: location, settings: activeVehicleSettings)
            let mapImage = await stationMapImage(for: enrichedContent)
            guard !Task.isCancelled,
                  self.planningDetailListTemplate === template else { return }
            planningDetailFeature = enriched
            if let index = planningFeatures.firstIndex(where: { $0.properties.stationID == enriched.properties.stationID }) {
                planningFeatures[index] = enriched
            }
            applyStationListContent(
                enrichedContent,
                mapImage: mapImage,
                actions: planningHeaderButtons(enriched),
                to: template
            )
        }
    }

    private func makeTripTemplate() -> CPTemplate {
        guard let plan = TripStore.shared.activePlan, let next = plan.nextStop else {
            planningFilter = FilterStateStore.load()
            return makePlanningTemplate(
                items: [placeholderListItem(title: String(localized: "trip.empty.title", defaultValue: "Keine aktive Fahrt"))],
                filter: planningFilter
            )
        }
        let content = tripContent(next)
        if #available(iOS 26.4, *) {
            let template = makeStationListTemplate(
                content: content,
                mapImage: cachedMapImage(for: content) ?? placeholderMapImage(for: content.cardState),
                actions: tripHeaderButtons(content)
            )
            tripListTemplate = template
            tripPOITemplate = nil
            tripTemplateStationID = next.stationID
            scheduleTripDetailRefresh(next, template: template)
            return template
        }
        let template = makePOITemplate(
            content: content,
            primary: navigationTextButton(for: content),
            secondary: CPTextButton(title: String(localized: "trip.substitute", defaultValue: "Ersetzen"), textStyle: .normal) { [weak self] _ in
                self?.showSubstitutes()
            },
            detailsHandler: { [weak self] in self?.showInformation(for: content) }
        )
        tripPOITemplate = template
        tripListTemplate = nil
        tripTemplateStationID = next.stationID
        lastTripPOIUpdateAt = Date()
        return template
    }

    private func updateTripTemplate() {
        guard let next = TripStore.shared.activePlan?.nextStop else {
            displayedMode = .plan
            clearTripTemplates()
            planningFilter = FilterStateStore.load()
            let template = makePlanningTemplate(
                items: [placeholderListItem(title: String(localized: "carplay.loading", defaultValue: "Stationen werden geladen …"))],
                filter: planningFilter
            )
            planningTemplate = template
            setRoot(template, animated: false)
            refresh(force: true)
            return
        }
        if (tripListTemplate != nil || tripPOITemplate != nil), tripTemplateStationID != next.stationID {
            clearTripTemplates()
            setRoot(makeTripTemplate(), animated: false)
            return
        }
        let content = tripContent(next)
        if #available(iOS 26.4, *), let tripListTemplate {
            applyStationListContent(
                content,
                mapImage: cachedMapImage(for: content) ?? placeholderMapImage(for: content.cardState),
                actions: tripHeaderButtons(content),
                to: tripListTemplate
            )
            scheduleTripDetailRefresh(next, template: tripListTemplate)
        } else if let tripPOITemplate {
            let now = Date()
            if now.timeIntervalSince(lastTripPOIUpdateAt ?? .distantPast) >= 60 {
                let points = makePOIs(for: content)
                tripPOITemplate.setPointsOfInterest(points, selectedIndex: 0)
                lastTripPOIUpdateAt = now
            }
        } else {
            setRoot(makeTripTemplate(), animated: false)
        }
    }

    private func tripContent(_ station: TripStationSnapshot) -> CarPlayStationContent {
        let remaining = max(0, station.routePositionM - TripStore.shared.currentRoutePositionM)
        let eta = TripStore.shared.eta.nextStopArrival?.formatted(date: .omitted, time: .shortened)
        let live = tripLiveStationID == station.stationID ? tripLiveDetail : nil
        return CarPlayStationContent(
            station: station,
            liveSummary: TripStore.shared.liveSummary(for: station.stationID),
            liveDetail: live,
            eta: eta,
            routeDistance: formatRouteDistance(remaining),
            settings: TripStore.shared.activePlan?.vehicleSettings ?? activeVehicleSettings,
            arrivalSOCPercent: TripStore.shared.activePlan?.projectedArrivalSOC(for: station.stationID),
            catalogFeature: tripCatalogStationID == station.stationID ? tripCatalogFeature : nil
        )
    }

    @available(iOS 26.4, *)
    private func scheduleTripDetailRefresh(_ station: TripStationSnapshot, template: CPListTemplate) {
        tripDetailTask?.cancel()
        tripDetailTask = Task { [weak self, weak template] in
            guard let self, let template else { return }
            async let liveRequest = try? liveAPIClient.stationDetail(stationID: station.stationID)
            async let catalogRequest = try? repository.stationDetail(
                stationID: station.stationID,
                preserving: nil
            )
            let (detail, catalogFeature) = await (liveRequest, catalogRequest)
            guard !Task.isCancelled,
                  TripStore.shared.activePlan?.nextStop?.stationID == station.stationID else { return }
            if let detail {
                tripLiveStationID = station.stationID
                tripLiveDetail = detail
            }
            if let catalogFeature {
                tripCatalogStationID = station.stationID
                tripCatalogFeature = catalogFeature
            }
            let content = tripContent(station)
            let mapImage = await stationMapImage(for: content)
            guard !Task.isCancelled,
                  self.tripListTemplate === template else { return }
            applyStationListContent(
                content,
                mapImage: mapImage,
                actions: tripHeaderButtons(content),
                to: template
            )
        }
    }

    @available(iOS 26.4, *)
    private func makeStationListTemplate(
        content: CarPlayStationContent,
        mapImage: UIImage,
        actions: [CPButton]
    ) -> CPListTemplate {
        CPListTemplate(
            title: content.name,
            listHeader: makeDetailsHeader(
                content: content,
                mapImage: mapImage,
                actions: actions,
                navigationTitle: content.name
            ),
            sections: stationSections(content),
            assistantCellConfiguration: nil
        )
    }

    @available(iOS 26.4, *)
    private func applyStationListContent(
        _ content: CarPlayStationContent,
        mapImage: UIImage,
        actions: [CPButton],
        to template: CPListTemplate
    ) {
        template.listHeader = makeDetailsHeader(
            content: content,
            mapImage: mapImage,
            actions: actions,
            navigationTitle: template.title
        )
        template.updateSections(stationSections(content))
    }

    @available(iOS 26.4, *)
    private func makeDetailsHeader(
        content: CarPlayStationContent,
        mapImage: UIImage,
        actions: [CPButton],
        navigationTitle: String?
    ) -> CPListTemplateDetailsHeader {
        let colors = statusColors(for: content.cardState)
        let overlay = CPImageOverlay(
            text: content.overlayText,
            textColor: colors.text,
            backgroundColor: colors.background,
            alignment: .leading
        )
        let thumbnail = CPThumbnailImage(image: mapImage, imageOverlay: overlay, sportsOverlay: nil)
        let normalizedNavigationTitle = navigationTitle?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        let needsDynamicTitle = normalizedNavigationTitle.caseInsensitiveCompare(content.name) != .orderedSame
        let navigationShowsOperator = !content.operatorName.isEmpty
            && normalizedNavigationTitle.caseInsensitiveCompare(content.operatorName) == .orderedSame
        let subtitle = needsDynamicTitle && navigationShowsOperator
            ? (content.etaLine ?? content.locationLine.nilIfBlank)
            : content.headerSubtitle
        let header = CPListTemplateDetailsHeader(
            thumbnail: thumbnail,
            title: needsDynamicTitle ? content.name : nil,
            subtitle: subtitle,
            bodyVariants: content.bodyVariants(headerSubtitle: subtitle),
            actionButtons: actions
        )
        header.wantsAdaptiveBackgroundStyle = true
        return header
    }

    private func stationSections(_ content: CarPlayStationContent) -> [CPListSection] {
        let runtimeItemLimit = max(2, min(CPListTemplate.maximumItemCount, carPlayAmenityLimit + carPlayChargingPointLimit))
        let desiredAmenityCount = min(carPlayAmenityLimit, max(1, content.amenities.count))
        let amenityBudget = min(desiredAmenityCount, max(1, runtimeItemLimit / 2))
        let chargingBudget = min(
            carPlayChargingPointLimit,
            max(1, runtimeItemLimit - amenityBudget)
        )
        let amenityItems: [CPListItem]
        if content.amenities.isEmpty {
            amenityItems = [CPListItem(
                text: String(localized: "amenity.noDetails"),
                detailText: String(localized: "carplay.amenities.none", defaultValue: "Keine Angebote mit Positionsdaten verfügbar")
            )]
        } else {
            amenityItems = content.amenities.prefix(amenityBudget).map { amenity in
                let distance = amenity.distanceM.map { formatDistance($0) }
                let detail = carPlayAmenityDetailText(
                    distance: distance,
                    openingHours: amenity.openingHours,
                    countryCode: amenity.countryCode
                )
                return CPListItem(
                    text: amenity.name,
                    detailText: detail,
                    image: brandImage(symbol: AmenityCatalog.symbol(for: "amenity_\(amenity.category)"), pointSize: 24)
                )
            }
        }

        let chargingItems = content.chargingPoints.prefix(chargingBudget).map { point in
            CPListItem(
                text: point.title,
                detailText: point.detail,
                image: statusImage(point.status, cardState: nil, pointSize: 24)
            )
        }
        let sections = [
            CPListSection(
                items: Array(amenityItems),
                header: String(localized: "carplay.amenities", defaultValue: "Angebote"),
                sectionIndexTitle: nil
            ),
            CPListSection(
                items: Array(chargingItems),
                header: String(localized: "station.chargingPoints", defaultValue: "Ladepunkte"),
                sectionIndexTitle: nil
            )
        ]
        guard CPListTemplate.maximumSectionCount < 2 else { return sections }
        return [CPListSection(items: Array((chargingItems + amenityItems).prefix(runtimeItemLimit)))]
    }

    @available(iOS 26.4, *)
    private func planningHeaderButtons(_ feature: GeoJSONFeature) -> [CPButton] {
        [
            headerButton(title: String(localized: "carplay.select", defaultValue: "Auswählen"), symbol: "checkmark.circle.fill") { [weak self] in
                self?.selectPlanningStation(feature)
            },
            headerButton(title: String(localized: "trip.navigateApple", defaultValue: "Navigieren"), symbol: "arrow.triangle.turn.up.right.diamond.fill") { [weak self] in
                self?.openNavigation(to: feature.coordinate, name: feature.properties.operatorName)
            }
        ]
    }

    @available(iOS 26.4, *)
    private func tripHeaderButtons(_ content: CarPlayStationContent) -> [CPButton] {
        [
            headerButton(title: String(localized: "trip.navigateApple", defaultValue: "Navigieren"), symbol: "arrow.triangle.turn.up.right.diamond.fill") { [weak self] in
                self?.openNavigation(to: content.coordinate, name: content.name)
            },
            headerButton(title: String(localized: "trip.substitute", defaultValue: "Ersetzen"), symbol: "arrow.triangle.2.circlepath") { [weak self] in
                self?.showSubstitutes()
            }
        ]
    }

    @available(iOS 26.4, *)
    private func headerButton(title: String, symbol: String, handler: @escaping () -> Void) -> CPButton {
        let button = CPButton(image: brandImage(symbol: symbol, pointSize: 24)) { _ in handler() }
        button.title = title
        return button
    }

    private func makePOITemplate(
        content: CarPlayStationContent,
        primary: CPTextButton,
        secondary: CPTextButton,
        detailsHandler: @escaping () -> Void
    ) -> CPPointOfInterestTemplate {
        let points = makePOIs(for: content)
        points[0].primaryButton = primary
        points[0].secondaryButton = secondary
        let template = CPPointOfInterestTemplate(
            title: content.name,
            pointsOfInterest: points,
            selectedIndex: 0
        )
        let details = CPBarButton(title: String(localized: "station.details")) { _ in detailsHandler() }
        details.buttonStyle = .rounded
        template.trailingNavigationBarButtons = [details]
        return template
    }

    private func makePOIs(for content: CarPlayStationContent) -> [CPPointOfInterest] {
        // Apple permits only EV-charger locations on maps in EV-charging CarPlay apps.
        let stationItem = mapItem(coordinate: content.coordinate, name: content.name)
        let station = CPPointOfInterest(
            location: stationItem,
            title: content.name,
            subtitle: content.operatorLine ?? content.etaLine ?? content.locationLine.nilIfBlank,
            summary: content.classificationAndStatusText,
            detailTitle: content.name,
            detailSubtitle: content.operatorLine ?? content.etaLine,
            detailSummary: [
                content.operatorLine == nil ? nil : content.etaLine,
                content.distanceLine,
                content.classificationAndStatusText,
                content.availabilityText,
                content.price.nilIfBlank,
                content.freshness.nilIfBlank
            ]
                .compactMap { $0 }
                .joined(separator: "\n"),
            pinImage: statusImage(content.availabilityStatus, cardState: content.cardState, pointSize: 28),
            selectedPinImage: statusImage(content.availabilityStatus, cardState: content.cardState, pointSize: 34)
        )
        return [station]
    }

    private func showInformation(for content: CarPlayStationContent) {
        guard let interfaceController else { return }
        var items = [
            CPInformationItem(title: String(localized: "station.address", defaultValue: "Adresse"), detail: content.locationLine),
            CPInformationItem(title: String(localized: "station.availability", defaultValue: "Verfügbarkeit"), detail: content.availabilityText),
            CPInformationItem(title: String(localized: "station.price", defaultValue: "Preis"), detail: content.price.nilIfBlank ?? String(localized: "carplay.price.unknown", defaultValue: "Preis unbekannt")),
            CPInformationItem(title: String(localized: "station.power", defaultValue: "Leistung"), detail: content.powerAndPointsText)
        ]
        let amenities = content.amenities.prefix(2).map { $0.name }.joined(separator: " · ")
        if !amenities.isEmpty {
            items.append(CPInformationItem(title: String(localized: "carplay.amenities", defaultValue: "Angebote"), detail: amenities))
        }
        items.append(contentsOf: content.chargingPoints.prefix(5).map {
            CPInformationItem(title: $0.title, detail: $0.detail)
        })
        let template = CPInformationTemplate(
            title: content.name,
            layout: .leading,
            items: Array(items.prefix(10)),
            actions: []
        )
        interfaceController.pushTemplate(template, animated: true, completion: nil)
    }

    private func navigationTextButton(for content: CarPlayStationContent) -> CPTextButton {
        CPTextButton(title: String(localized: "trip.navigateApple", defaultValue: "Navigieren"), textStyle: .confirm) { [weak self] _ in
            self?.openNavigation(to: content.coordinate, name: content.name)
        }
    }

    private func openNavigation(to coordinate: CLLocationCoordinate2D, name: String) {
        mapItem(coordinate: coordinate, name: name).openInMaps(
            launchOptions: [MKLaunchOptionsDirectionsModeKey: MKLaunchOptionsDirectionsModeDriving]
        )
    }

    private func showSubstitutes() {
        guard let interfaceController else { return }
        let choices = TripStore.shared.substitutesForNextStop()
        var items = (choices.earlier + choices.detour).prefix(6).map { station in
            let live = TripStore.shared.liveSummary(for: station.stationID)
            let status = live?.availabilityStatus ?? station.availabilityStatus
            let item = CPListItem(
                text: station.operatorName,
                detailText: "\(station.city) · \(status.label) · \(formatRouteDistance(station.routePositionM))",
                image: statusImage(status, cardState: nil, pointSize: 24)
            )
            item.handler = { [weak self] _, completion in
                self?.confirmSubstitute(station)
                completion()
            }
            return item
        }
        if items.isEmpty {
            items = [CPListItem(
                text: String(localized: "trip.substitute.none", defaultValue: "Keine sichere Alternative"),
                detailText: String(localized: "trip.substitute.noneHelp", defaultValue: "Behalte den aktuellen Ladestopp bei.")
            )]
        }
        let template = CPListTemplate(
            title: String(localized: "trip.substitute.title", defaultValue: "Ladestopp ersetzen"),
            sections: [CPListSection(items: Array(items))]
        )
        interfaceController.pushTemplate(template, animated: true, completion: nil)
    }

    private func confirmSubstitute(_ station: TripStationSnapshot) {
        guard let interfaceController else { return }
        let cancel = CPTextButton(title: String(localized: "common.cancel", defaultValue: "Abbrechen"), textStyle: .normal) { [weak self] _ in
            self?.interfaceController?.popTemplate(animated: true, completion: nil)
        }
        let confirm = CPTextButton(title: String(localized: "trip.substitute", defaultValue: "Ersetzen"), textStyle: .confirm) { [weak self] _ in
            TripStore.shared.replaceNextStop(with: station.stationID)
            guard let self else { return }
            clearTripTemplates()
            setRoot(makeTripTemplate(), animated: true)
        }
        let template = CPInformationTemplate(
            title: String(localized: "trip.substitute.confirm", defaultValue: "Ladestopp ersetzen?"),
            layout: .leading,
            items: [CPInformationItem(title: station.operatorName, detail: station.city)],
            actions: [cancel, confirm]
        )
        interfaceController.pushTemplate(template, animated: true, completion: nil)
    }

    private func stationMapImage(for content: CarPlayStationContent) async -> UIImage {
        let cacheKey = mapCacheKey(for: content)
        if let cached = mapImagesByStationID[cacheKey] { return cached }
        if let image = await renderMapSnapshot(for: content) {
            mapImagesByStationID[cacheKey] = image
            return image
        }
        return placeholderMapImage(for: content.cardState)
    }

    private func cachedMapImage(for content: CarPlayStationContent) -> UIImage? {
        mapImagesByStationID[mapCacheKey(for: content)]
    }

    private func mapCacheKey(for content: CarPlayStationContent) -> String {
        let style = interfaceController?.carTraitCollection.userInterfaceStyle.rawValue
            ?? UITraitCollection.current.userInterfaceStyle.rawValue
        return "\(content.stationID)|\(style)"
    }

    private func renderMapSnapshot(for content: CarPlayStationContent) async -> UIImage? {
        // Keep the snapshot charger-only; amenities belong in the system list below it.
        let options = MKMapSnapshotter.Options()
        options.region = MKCoordinateRegion(
            center: content.coordinate,
            span: MKCoordinateSpan(latitudeDelta: 0.012, longitudeDelta: 0.016)
        )
        options.size = CGSize(width: 720, height: 360)
        options.pointOfInterestFilter = .excludingAll
        if let traitCollection = interfaceController?.carTraitCollection {
            options.traitCollection = traitCollection
        }
        do {
            let snapshot = try await MKMapSnapshotter(options: options).start()
            let format = UIGraphicsImageRendererFormat(for: options.traitCollection)
            format.scale = 1
            return UIGraphicsImageRenderer(size: snapshot.image.size, format: format).image { context in
                snapshot.image.draw(at: .zero)
                drawMapMarker(
                    symbol: "bolt.fill",
                    color: brandColor,
                    point: snapshot.point(for: content.coordinate),
                    pointSize: 30,
                    context: context.cgContext
                )
            }
        } catch {
            return nil
        }
    }

    private func drawMapMarker(
        symbol: String,
        color: UIColor,
        point: CGPoint,
        pointSize: CGFloat,
        context: CGContext
    ) {
        guard point.x >= 0, point.y >= 0 else { return }
        let configuration = UIImage.SymbolConfiguration(pointSize: pointSize, weight: .bold)
        guard let image = UIImage(systemName: symbol, withConfiguration: configuration)?
            .withTintColor(color, renderingMode: .alwaysOriginal) else { return }
        let padding: CGFloat = 7
        let backgroundRect = CGRect(
            x: point.x - image.size.width / 2 - padding,
            y: point.y - image.size.height / 2 - padding,
            width: image.size.width + padding * 2,
            height: image.size.height + padding * 2
        )
        context.setFillColor(UIColor.systemBackground.withAlphaComponent(0.92).cgColor)
        context.fillEllipse(in: backgroundRect)
        image.draw(at: CGPoint(x: point.x - image.size.width / 2, y: point.y - image.size.height / 2))
    }

    private func placeholderMapImage(for state: StationCardState) -> UIImage {
        let colors = statusColors(for: state)
        let size = CGSize(width: 720, height: 360)
        return UIGraphicsImageRenderer(size: size).image { context in
            colors.background.setFill()
            context.fill(CGRect(origin: .zero, size: size))
            let configuration = UIImage.SymbolConfiguration(pointSize: 84, weight: .semibold)
            UIImage(systemName: "map.fill", withConfiguration: configuration)?
                .withTintColor(colors.accent, renderingMode: .alwaysOriginal)
                .draw(at: CGPoint(x: 318, y: 138))
        }
    }

    private var brandColor: UIColor {
        UIColor { traits in
            traits.userInterfaceStyle == .dark
                ? UIColor(red: 94 / 255, green: 234 / 255, blue: 212 / 255, alpha: 1)
                : UIColor(red: 15 / 255, green: 118 / 255, blue: 110 / 255, alpha: 1)
        }
    }

    private func statusColors(for state: StationCardState) -> (background: UIColor, text: UIColor, accent: UIColor) {
        switch state {
        case .outOfOrder:
            return (
                adaptiveUIColor(light: (255, 241, 242), dark: (59, 18, 28)),
                adaptiveUIColor(light: (153, 27, 27), dark: (254, 202, 202)),
                .systemRed
            )
        case .occupied:
            return (
                adaptiveUIColor(light: (226, 232, 240), dark: (38, 50, 61)),
                adaptiveUIColor(light: (30, 41, 59), dark: (226, 232, 240)),
                .systemGray
            )
        case .oftenBroken:
            return (
                adaptiveUIColor(light: (255, 247, 248), dark: (54, 22, 31)),
                adaptiveUIColor(light: (159, 18, 57), dark: (254, 205, 211)),
                .systemRed
            )
        case .oftenOccupied:
            return (
                adaptiveUIColor(light: (248, 250, 252), dark: (15, 30, 39)),
                adaptiveUIColor(light: (51, 65, 85), dark: (203, 213, 225)),
                .systemGray
            )
        case .oneFreeLeft:
            return (
                adaptiveUIColor(light: (255, 251, 235), dark: (51, 43, 18)),
                adaptiveUIColor(light: (146, 64, 14), dark: (253, 230, 138)),
                .systemYellow
            )
        case .unknown:
            return (
                adaptiveUIColor(light: (241, 245, 249), dark: (22, 33, 43)),
                adaptiveUIColor(light: (71, 85, 105), dark: (203, 213, 225)),
                .systemGray
            )
        case .default:
            let accent = brandColor
            return (
                UIColor { $0.userInterfaceStyle == .dark ? .secondarySystemBackground : .systemBackground },
                UIColor { $0.userInterfaceStyle == .dark ? .label : accent },
                accent
            )
        }
    }

    private func adaptiveUIColor(light: (CGFloat, CGFloat, CGFloat), dark: (CGFloat, CGFloat, CGFloat)) -> UIColor {
        UIColor { traits in
            let values = traits.userInterfaceStyle == .dark ? dark : light
            return UIColor(red: values.0 / 255, green: values.1 / 255, blue: values.2 / 255, alpha: 1)
        }
    }

    private func statusImage(
        _ status: AvailabilityStatus,
        cardState: StationCardState?,
        pointSize: CGFloat
    ) -> UIImage {
        let symbol: String
        switch status {
        case .free: symbol = "checkmark.circle.fill"
        case .occupied: symbol = "clock.fill"
        case .outOfOrder: symbol = "exclamationmark.triangle.fill"
        case .unknown: symbol = "questionmark.circle.fill"
        }
        let state = cardState ?? {
            switch status {
            case .free: return StationCardState.default
            case .occupied: return .occupied
            case .outOfOrder: return .outOfOrder
            case .unknown: return .unknown
            }
        }()
        let color = statusColors(for: state).accent
        let configuration = UIImage.SymbolConfiguration(pointSize: pointSize, weight: .semibold)
        return (UIImage(systemName: symbol, withConfiguration: configuration) ?? UIImage())
            .withTintColor(color, renderingMode: .alwaysOriginal)
    }

    private func brandImage(symbol: String, pointSize: CGFloat) -> UIImage {
        let configuration = UIImage.SymbolConfiguration(pointSize: pointSize, weight: .semibold)
        return (UIImage(systemName: symbol, withConfiguration: configuration)
            ?? UIImage(systemName: "mappin.and.ellipse", withConfiguration: configuration)
            ?? UIImage())
            .withTintColor(brandColor, renderingMode: .alwaysOriginal)
    }

    private func clearTripTemplates() {
        tripDetailTask?.cancel()
        tripDetailTask = nil
        tripPOITemplate = nil
        lastTripPOIUpdateAt = nil
        tripListTemplate = nil
        tripTemplateStationID = nil
        tripLiveDetail = nil
        tripLiveStationID = nil
        tripCatalogFeature = nil
        tripCatalogStationID = nil
    }

    private func stationDistance(_ feature: GeoJSONFeature, from location: CLLocation) -> CLLocationDistance {
        location.distance(from: CLLocation(latitude: feature.coordinate.latitude, longitude: feature.coordinate.longitude))
    }

    private func planningStationPrecedes(
        _ left: GeoJSONFeature,
        _ right: GeoJSONFeature,
        from location: CLLocation,
        settings: VehicleEnergySettings
    ) -> Bool {
        let leftDistance = stationDistance(left, from: location)
        let rightDistance = stationDistance(right, from: location)
        let leftBand = Int(leftDistance / 500)
        let rightBand = Int(rightDistance / 500)
        if leftBand != rightBand { return leftBand < rightBand }

        let leftCounts = left.availabilityCounts
        let rightCounts = right.availabilityCounts
        let leftFit = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: left.properties.displayedMaxPowerKW,
            status: left.availabilityStatus,
            available: leftCounts.available,
            total: leftCounts.total
        )
        let rightFit = CarPlayChargePlan(
            settings: settings,
            maxPowerKW: right.properties.displayedMaxPowerKW,
            status: right.availabilityStatus,
            available: rightCounts.available,
            total: rightCounts.total
        )
        if leftFit.tier.rawValue != rightFit.tier.rawValue {
            return leftFit.tier.rawValue < rightFit.tier.rawValue
        }
        if left.properties.amenitiesTotal != right.properties.amenitiesTotal {
            return left.properties.amenitiesTotal > right.properties.amenitiesTotal
        }
        if leftDistance != rightDistance { return leftDistance < rightDistance }
        return left.properties.stationID < right.properties.stationID
    }

    private func formatDistance(_ meters: CLLocationDistance) -> String {
        Measurement(value: max(0, meters), unit: UnitLength.meters)
            .formatted(.measurement(width: .abbreviated, usage: .road))
    }

    private func setRoot(_ template: CPTemplate, animated: Bool) {
        interfaceController?.setRootTemplate(template, animated: animated, completion: nil)
    }

    private func mapItem(coordinate: CLLocationCoordinate2D, name: String) -> MKMapItem {
        let item = MKMapItem(placemark: MKPlacemark(coordinate: coordinate))
        item.name = name
        return item
    }
}
