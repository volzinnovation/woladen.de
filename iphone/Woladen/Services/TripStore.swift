import Foundation
import CoreLocation

@MainActor
final class TripStore: ObservableObject {
    static let shared = TripStore()

    @Published var mode: WoladenMode {
        didSet { persistScalarState() }
    }
    @Published private(set) var preferences: TripPreferences
    @Published private(set) var plans: [RoutePlan]
    @Published private(set) var activePlanID: UUID?
    @Published private(set) var liveSummaries: [String: LiveStationSummary] = [:]
    @Published private(set) var liveUpdatedAt: Date?
    @Published private(set) var eta: TripETAState = .unavailable
    @Published private(set) var currentRoutePositionM = 0
    @Published private(set) var isLikelyDriving = false
    @Published private(set) var isTripModeSuggested = false
    @Published private(set) var requestedPlanID: UUID?

    private let defaults: UserDefaults
    private let liveAPIClient: LiveAPIClient
    private let encoder = JSONEncoder()
    private let decoder = JSONDecoder()
    private var liveRefreshTask: Task<Void, Never>?
    private var etaTask: Task<Void, Never>?
    private var lastETALocation: CLLocation?
    private var roadSpeedFixCount = 0
    private var lowSpeedFixCount = 0

    private enum Keys {
        static let mode = "woladen.trip.mode.v1"
        static let preferences = "woladen.trip.preferences.v1"
        static let plans = "woladen.trip.plans.v1"
        static let activePlanID = "woladen.trip.activePlanID.v1"
    }

    init(defaults: UserDefaults = .standard, liveAPIClient: LiveAPIClient = LiveAPIClient()) {
        self.defaults = defaults
        self.liveAPIClient = liveAPIClient
        mode = defaults.string(forKey: Keys.mode).flatMap(WoladenMode.init(rawValue:)) ?? .plan
        if let data = defaults.data(forKey: Keys.preferences),
           let decoded = try? decoder.decode(TripPreferences.self, from: data) {
            preferences = decoded.normalized
        } else {
            preferences = TripPreferences()
        }
        if let data = defaults.data(forKey: Keys.plans),
           let decoded = try? decoder.decode([RoutePlan].self, from: data) {
            plans = decoded
        } else {
            plans = []
        }
        if let rawID = defaults.string(forKey: Keys.activePlanID), let id = UUID(uuidString: rawID) {
            activePlanID = id
        } else {
            activePlanID = nil
        }
        if activePlanID.flatMap({ id in plans.first { $0.id == id } }) == nil {
            activePlanID = nil
            if mode == .trip { mode = .plan }
        }
        if let activeID = activePlanID,
           let active = plans.first(where: { $0.id == activeID }) {
            eta = TripETAService.baseEstimate(plan: active, currentRoutePositionM: 0)
        }
        startLiveRefreshLoop()
    }

    deinit {
        liveRefreshTask?.cancel()
        etaTask?.cancel()
    }

    var activePlan: RoutePlan? {
        guard let activePlanID else { return nil }
        return plans.first { $0.id == activePlanID }
    }

    var sortedPlans: [RoutePlan] {
        plans
            .filter { !$0.isStationTargetTrip }
            .sorted { $0.updatedAt > $1.updatedAt }
    }

    func plan(id: UUID?) -> RoutePlan? {
        guard let id else { return nil }
        return plans.first { $0.id == id }
    }

    func requestPlanEditing(_ planID: UUID? = nil) {
        requestedPlanID = planID
        mode = .plan
    }

    func clearPlanEditingRequest() {
        requestedPlanID = nil
    }

    @discardableResult
    func saveCalculatedRoute(
        existingPlanID: UUID?,
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        summary: RouteSummary,
        features: [GeoJSONFeature],
        filter: FilterState,
        initialSOCPercent: Double
    ) -> UUID {
        let id = existingPlanID ?? UUID()
        let existing = plan(id: existingPlanID)
        let settings = preferences.activeVehicleSettings.normalized
        let route = TripRouteSnapshot(
            origin: origin,
            destination: destination,
            summary: summary,
            filter: RouteFilterPayload(filter: filter),
            initialSOCPercent: initialSOCPercent
        )
        let snapshots = features.map(TripStationSnapshot.init).sorted { $0.routePositionM < $1.routePositionM }
        let previousSelections = existing?.stopSelections ?? []
        let energyAnchors = previousSelections
            .filter { [.planned, .completed].contains($0.state) }
            .map(\.stationID)
        let result = EnergyRoutePlanner.build(
            routeDistanceM: route.distanceM,
            stations: snapshots,
            selectedStationIDs: energyAnchors,
            initialSOCPercent: route.initialSOCPercent,
            settings: settings,
            providerMode: preferences.providerMode,
            selectedProviderNames: preferences.selectedProviderNames
        )
        let now = Date()
        let routeName = "\(origin.label) → \(destination.label)"
        let updated = RoutePlan(
            id: id,
            name: routeName,
            route: route,
            vehicleSettings: settings,
            rawStations: snapshots,
            windows: result.windows,
            stopSelections: previousSelections,
            invalidatedStationIDs: result.invalidatedStationIDs,
            providerMode: preferences.providerMode,
            selectedProviderNames: preferences.selectedProviderNames,
            state: existing?.state == .active ? .active : .draft,
            createdAt: existing?.createdAt ?? now,
            updatedAt: now
        )
        upsert(updated)
        return id
    }

    func updatePreferences(_ next: TripPreferences, recalculatePlanID: UUID? = nil) {
        preferences = next.normalized
        persistScalarState()
        if let recalculatePlanID {
            recalculate(planID: recalculatePlanID)
        } else if let activePlanID {
            recalculate(planID: activePlanID)
        }
    }

    func acceptTripModeSuggestion() {
        guard let readyPlan = activePlan ?? sortedPlans.first(where: { $0.isReadyForTrip && $0.state != .completed }) else {
            isTripModeSuggested = false
            return
        }
        isTripModeSuggested = false
        if activePlan == nil {
            _ = activate(planID: readyPlan.id)
        } else {
            mode = .trip
        }
    }

    func dismissTripModeSuggestion() {
        isTripModeSuggested = false
    }

    func updateInitialSOC(_ percent: Double, planID: UUID) {
        guard var routePlan = plan(id: planID) else { return }
        routePlan.route.initialSOCPercent = min(max(percent, 1), 100)
        routePlan.updatedAt = Date()
        replacePlan(routePlan)
        recalculate(planID: planID)
    }

    func setProviderSelection(
        names: [String],
        mode: ProviderPackageMode,
        planID: UUID
    ) {
        var next = preferences
        next.selectedProviderNames = names
        next.providerMode = mode
        preferences = next.normalized
        persistScalarState()
        guard var routePlan = plan(id: planID) else { return }
        routePlan.selectedProviderNames = preferences.selectedProviderNames
        routePlan.providerMode = preferences.providerMode
        replacePlan(routePlan)
        recalculate(planID: planID)
    }

    func toggleProvider(_ providerName: String, planID: UUID) {
        var names = preferences.selectedProviderNames
        let key = normalizedProviderKey(providerName)
        if let index = names.firstIndex(where: { normalizedProviderKey($0) == key }) {
            names.remove(at: index)
        } else {
            names.append(providerName)
        }
        setProviderSelection(names: names, mode: preferences.providerMode, planID: planID)
    }

    func toggleStop(stationID: String, planID: UUID) {
        guard var routePlan = plan(id: planID),
              let window = routePlan.window(containing: stationID) else { return }

        if let index = routePlan.stopSelections.firstIndex(where: { $0.stationID == stationID && $0.state == .planned }) {
            routePlan.stopSelections.remove(at: index)
        } else {
            let windowIDs = Set(window.candidateStationIDs)
            routePlan.stopSelections.removeAll {
                $0.state == .planned && windowIDs.contains($0.stationID)
            }
            routePlan.stopSelections.removeAll { $0.stationID == stationID }
            routePlan.stopSelections.append(
                TripStopSelection(stationID: stationID, state: .planned, selectedAt: Date())
            )
        }
        routePlan.updatedAt = Date()
        replacePlan(routePlan)
        recalculate(planID: planID)
    }

    @discardableResult
    func activate(planID: UUID) -> Bool {
        guard let selected = plan(id: planID), selected.isReadyForTrip else { return false }
        plans = plans.map { routePlan in
            var value = routePlan
            if value.id == planID {
                value.state = .active
                value.updatedAt = Date()
            } else if value.state == .active {
                value.state = .draft
            }
            return value
        }
        activePlanID = planID
        mode = .trip
        isTripModeSuggested = false
        eta = TripETAService.baseEstimate(plan: selected, currentRoutePositionM: 0)
        persistPlans()
        persistScalarState()
        Task { await refreshLive(force: true) }
        return true
    }

    @discardableResult
    func activateFocusedStation(stationID: String, planID: UUID) -> Bool {
        guard var routePlan = plan(id: planID),
              routePlan.station(stationID) != nil,
              let window = routePlan.window(containing: stationID) else { return false }
        let candidateIDs = Set(window.candidateStationIDs)
        routePlan.stopSelections.removeAll {
            $0.state == .planned && candidateIDs.contains($0.stationID)
        }
        routePlan.stopSelections.removeAll { $0.stationID == stationID }
        routePlan.stopSelections.append(
            TripStopSelection(stationID: stationID, state: .planned, selectedAt: Date())
        )
        routePlan.state = .active
        routePlan.updatedAt = Date()
        plans = plans.map { existing in
            var value = existing
            if value.id == routePlan.id {
                value = routePlan
            } else if value.state == .active {
                value.state = .draft
            }
            return value
        }
        activePlanID = routePlan.id
        mode = .trip
        isTripModeSuggested = false
        eta = TripETAService.baseEstimate(plan: routePlan, currentRoutePositionM: 0)
        persistPlans()
        persistScalarState()
        Task { await refreshLive(force: true) }
        return true
    }

    @discardableResult
    func activateStationTarget(
        feature: GeoJSONFeature,
        alternatives: [GeoJSONFeature],
        from currentLocation: CLLocation?
    ) -> Bool {
        let targetLocation = CLLocation(
            latitude: feature.coordinate.latitude,
            longitude: feature.coordinate.longitude
        )
        let originLocation = currentLocation ?? targetLocation
        let targetDistanceM = max(1, Int(originLocation.distance(from: targetLocation).rounded()))
        let maximumAdditionalDistanceM = Int(
            preferences.activeVehicleSettings.maximumDetourMinutes / 60 * 90_000
        )
        let eligible = alternatives
            .filter { $0.properties.stationID != feature.properties.stationID }
            .map { candidate -> (GeoJSONFeature, Int) in
                let location = CLLocation(
                    latitude: candidate.coordinate.latitude,
                    longitude: candidate.coordinate.longitude
                )
                return (candidate, max(1, Int(originLocation.distance(from: location).rounded())))
            }
            .filter { _, distance in
                distance <= targetDistanceM + maximumAdditionalDistanceM
                    && distance >= max(0, targetDistanceM - 60_000)
            }
            .sorted { $0.1 < $1.1 }
            .prefix(30)

        let target = stationSnapshot(feature, routePositionM: targetDistanceM)
        let candidateSnapshots = eligible.map {
            stationSnapshot($0.0, routePositionM: $0.1)
        }
        let allStations = ([target] + candidateSnapshots)
            .reduce(into: [String: TripStationSnapshot]()) { result, station in
                result[station.stationID] = station
            }
            .values
            .sorted { $0.routePositionM < $1.routePositionM }
        let candidateIDs = allStations.map(\.stationID)
        let settings = preferences.activeVehicleSettings.normalized
        let initialSOC = 100.0
        let projectedSOC = Dictionary(uniqueKeysWithValues: allStations.map {
            (
                $0.stationID,
                settings.projectedArrivalSOC(
                    departureSOCPercent: initialSOC,
                    distanceKM: Double($0.routePositionM) / 1000
                )
            )
        })
        let summary = RouteSummary(
            source: "station-target",
            distanceM: targetDistanceM,
            durationS: max(60, Int(Double(targetDistanceM) / 25)),
            geometry: RouteGeometry(coordinates: [
                [originLocation.coordinate.longitude, originLocation.coordinate.latitude],
                [feature.coordinate.longitude, feature.coordinate.latitude]
            ])
        )
        let origin = RouteEndpoint(
            coordinate: originLocation.coordinate,
            label: String(localized: "trip.station.currentStart", defaultValue: "Current start")
        )
        let destination = RouteEndpoint(
            coordinate: feature.coordinate,
            label: feature.properties.operatorName
        )
        let now = Date()
        let route = TripRouteSnapshot(
            origin: origin,
            destination: destination,
            summary: summary,
            filter: RouteFilterPayload(filter: FilterState()),
            initialSOCPercent: initialSOC,
            calculatedAt: now
        )
        let window = ChargingWindow(
            index: 0,
            startPositionM: 0,
            endPositionM: targetDistanceM + maximumAdditionalDistanceM,
            departurePositionM: 0,
            departureSOCPercent: initialSOC,
            candidateStationIDs: candidateIDs,
            projectedArrivalSOCByStationID: projectedSOC,
            selectedStationID: target.stationID
        )
        let plan = RoutePlan(
            id: UUID(),
            name: feature.properties.operatorName,
            route: route,
            vehicleSettings: settings,
            rawStations: allStations,
            windows: [window],
            stopSelections: [
                TripStopSelection(stationID: target.stationID, state: .planned, selectedAt: now)
            ],
            invalidatedStationIDs: [],
            providerMode: preferences.providerMode,
            selectedProviderNames: preferences.selectedProviderNames,
            state: .active,
            createdAt: now,
            updatedAt: now,
            stationTargetID: target.stationID
        )

        plans.removeAll { $0.isStationTargetTrip }
        plans = plans.map { existing in
            var value = existing
            if value.state == .active { value.state = .draft }
            return value
        }
        plans.append(plan)
        activePlanID = plan.id
        mode = .trip
        isTripModeSuggested = false
        eta = TripETAService.baseEstimate(plan: plan, currentRoutePositionM: 0)
        persistPlans()
        persistScalarState()
        Task { await refreshLive(force: true) }
        if let currentLocation {
            refreshETA(from: currentLocation, force: true)
        }
        return true
    }

    func completeNextStop() {
        guard var routePlan = activePlan, let next = routePlan.nextStop else { return }
        if routePlan.isStationTargetTrip {
            endTrip()
            return
        }
        setStopState(stationID: next.stationID, state: .completed, in: &routePlan)
        routePlan.updatedAt = Date()
        replacePlan(routePlan)
        if routePlan.nextStop == nil {
            eta = .unavailable
        }
        refreshETAAfterStopChange()
    }

    func skipNextStop() {
        guard var routePlan = activePlan, let next = routePlan.nextStop else { return }
        setStopState(stationID: next.stationID, state: .skipped, in: &routePlan)
        routePlan.updatedAt = Date()
        replacePlan(routePlan)
        refreshETAAfterStopChange()
    }

    func replaceNextStop(with replacementID: String) {
        guard var routePlan = activePlan,
              let current = routePlan.nextStop,
              routePlan.station(replacementID) != nil else { return }
        setStopState(stationID: current.stationID, state: .rejected, in: &routePlan)
        routePlan.stopSelections.removeAll { $0.stationID == replacementID }
        routePlan.stopSelections.append(
            TripStopSelection(stationID: replacementID, state: .planned, selectedAt: Date())
        )
        routePlan.updatedAt = Date()
        replacePlan(routePlan)
        recalculate(planID: routePlan.id)
        Task { await refreshLive(force: true) }
        refreshETAAfterStopChange()
    }

    func endTrip() {
        guard var routePlan = activePlan else { return }
        if routePlan.isStationTargetTrip {
            plans.removeAll { $0.id == routePlan.id }
            persistPlans()
        } else {
            routePlan.state = .completed
            routePlan.updatedAt = Date()
            replacePlan(routePlan)
        }
        activePlanID = nil
        mode = .plan
        liveSummaries = [:]
        liveUpdatedAt = nil
        eta = .unavailable
        currentRoutePositionM = 0
        isTripModeSuggested = false
        persistScalarState()
    }

    func deletePlan(id: UUID) {
        plans.removeAll { $0.id == id }
        if activePlanID == id {
            activePlanID = nil
            mode = .plan
        }
        persistPlans()
        persistScalarState()
    }

    func updateLocation(_ location: CLLocation) {
        if activePlan == nil,
           let readyPlan = sortedPlans.first(where: { $0.isReadyForTrip && $0.state != .completed }) {
            updateDrivingState(location: location, plan: readyPlan)
            if isLikelyDriving, preferences.shouldSuggestTripMode {
                isTripModeSuggested = true
            }
        }

        guard let routePlan = activePlan else {
            currentRoutePositionM = 0
            return
        }
        currentRoutePositionM = RouteProgressEstimator.positionM(
            for: location.coordinate,
            routeCoordinates: routePlan.route.geometryCoordinates,
            routeDistanceM: routePlan.route.distanceM
        )
        updateDrivingState(location: location, plan: routePlan)

        let shouldRefreshETA: Bool
        if let lastETALocation {
            shouldRefreshETA = location.distance(from: lastETALocation) >= 500 || eta.updatedAt.timeIntervalSinceNow <= -60
        } else {
            shouldRefreshETA = true
        }
        if shouldRefreshETA {
            refreshETA(from: location)
        }
    }

    func refreshETA(from location: CLLocation, force: Bool = false) {
        guard let routePlan = activePlan else {
            eta = .unavailable
            return
        }
        if !force, let lastETALocation,
           location.distance(from: lastETALocation) < 500,
           eta.updatedAt.timeIntervalSinceNow > -60 {
            return
        }
        lastETALocation = location
        etaTask?.cancel()
        eta = TripETAService.baseEstimate(
            plan: routePlan,
            currentRoutePositionM: currentRoutePositionM
        )
        etaTask = Task { [weak self] in
            guard let self else { return }
            do {
                if let next = try await TripETAService.calculateNextStop(from: location, plan: routePlan) {
                    guard !Task.isCancelled else { return }
                    self.eta.nextStopArrival = next.nextStopArrival
                    self.eta.nextStopTravelTime = next.nextStopTravelTime
                    self.eta.nextStopUsesTraffic = true
                    self.eta.nextStopIsLoading = false
                    self.eta.updatedAt = next.updatedAt
                }
            } catch {
                guard !Task.isCancelled else { return }
                self.eta.nextStopIsLoading = false
                self.eta.isStale = true
            }

            do {
                let value = try await TripETAService.calculateDestination(from: location, plan: routePlan)
                guard !Task.isCancelled else { return }
                self.eta.destinationArrival = value.destinationArrival
                self.eta.totalTravelTime = value.totalTravelTime
                self.eta.destinationUsesTraffic = true
                self.eta.destinationIsLoading = false
                self.eta.updatedAt = value.updatedAt
            } catch {
                guard !Task.isCancelled else { return }
                self.eta.destinationIsLoading = false
                self.eta.isStale = true
            }
        }
    }

    func refreshLive(force: Bool = false) async {
        guard let routePlan = activePlan, liveAPIClient.isEnabled else { return }
        if !force, let liveUpdatedAt, Date().timeIntervalSince(liveUpdatedAt) < 60 { return }
        let stationIDs = Array(
            Set(routePlan.windows.flatMap(\.candidateStationIDs).prefix(120))
        )
        guard !stationIDs.isEmpty else { return }
        do {
            let response = try await liveAPIClient.lookupStations(stationIDs: stationIDs)
            liveSummaries = Dictionary(uniqueKeysWithValues: response.stations.map { ($0.stationID, $0) })
            liveUpdatedAt = Date()
        } catch {
            // Keep the last known state visible and let freshness labels age naturally.
        }
    }

    func liveSummary(for stationID: String) -> LiveStationSummary? {
        liveSummaries[stationID]
    }

    func substitutesForNextStop() -> (earlier: [TripStationSnapshot], detour: [TripStationSnapshot]) {
        guard let routePlan = activePlan, let next = routePlan.nextStop else { return ([], []) }
        let ids = EnergyRoutePlanner.substituteStationIDs(
            in: routePlan,
            replacing: next.stationID,
            currentRoutePositionM: currentRoutePositionM
        )
        return (
            ids.earlier.compactMap(routePlan.station),
            ids.detour.compactMap(routePlan.station)
        )
    }

    private func recalculate(planID: UUID) {
        guard var routePlan = plan(id: planID) else { return }
        let anchorIDs = routePlan.stopSelections
            .filter { [.planned, .completed].contains($0.state) }
            .sorted { lhs, rhs in
                routePlan.station(lhs.stationID)?.routePositionM ?? .max < routePlan.station(rhs.stationID)?.routePositionM ?? .max
            }
            .map(\.stationID)
        let result = EnergyRoutePlanner.build(
            routeDistanceM: routePlan.route.distanceM,
            stations: routePlan.rawStations,
            selectedStationIDs: anchorIDs,
            initialSOCPercent: routePlan.route.initialSOCPercent,
            settings: preferences.activeVehicleSettings,
            providerMode: routePlan.providerMode,
            selectedProviderNames: routePlan.selectedProviderNames
        )
        routePlan.vehicleSettings = preferences.activeVehicleSettings.normalized
        routePlan.windows = result.windows
        routePlan.invalidatedStationIDs = result.invalidatedStationIDs
        routePlan.updatedAt = Date()
        replacePlan(routePlan)
    }

    private func setStopState(stationID: String, state: TripStopState, in plan: inout RoutePlan) {
        if let index = plan.stopSelections.firstIndex(where: { $0.stationID == stationID }) {
            plan.stopSelections[index].state = state
        } else {
            plan.stopSelections.append(
                TripStopSelection(stationID: stationID, state: state, selectedAt: Date())
            )
        }
    }

    private func stationSnapshot(_ feature: GeoJSONFeature, routePositionM: Int) -> TripStationSnapshot {
        let counts = feature.availabilityCounts
        return TripStationSnapshot(
            stationID: feature.properties.stationID,
            operatorName: feature.properties.operatorName,
            city: feature.properties.city,
            address: feature.properties.address,
            latitude: feature.coordinate.latitude,
            longitude: feature.coordinate.longitude,
            maxPowerKW: feature.properties.displayedMaxPowerKW,
            chargingPointsCount: feature.properties.chargingPointsCount,
            routePositionM: routePositionM,
            driveDistanceToRouteM: feature.routeMetadata?.driveDistanceToRouteM ?? 0,
            routeDetourM: feature.routeMetadata?.routeDetourM ?? 0,
            availabilityStatus: feature.availabilityStatus,
            availableEVSEs: counts.available,
            totalEVSEs: counts.total,
            lastUpdated: feature.liveSummary?.sourceObservedAt
                ?? feature.liveSummary?.fetchedAt
                ?? feature.properties.occupancyLastUpdated,
            classification: feature.stationClassification,
            reliabilityPercent: feature.properties.reliabilityPercent,
            lastUnavailableAt: feature.properties.lastUnavailableAt,
            providerID: feature.properties.providerCanonicalID
        )
    }

    private func refreshETAAfterStopChange() {
        if let lastETALocation {
            refreshETA(from: lastETALocation, force: true)
        } else {
            eta = .unavailable
        }
    }

    private func updateDrivingState(location: CLLocation, plan: RoutePlan) {
        let speedKPH = max(0, location.speed) * 3.6
        let validFix = location.horizontalAccuracy >= 0 && location.horizontalAccuracy <= 100
        if validFix, speedKPH >= preferences.roadSpeedThresholdKPH, plan.isReadyForTrip {
            roadSpeedFixCount += 1
            lowSpeedFixCount = 0
            if roadSpeedFixCount >= 2 {
                isLikelyDriving = true
                isTripModeSuggested = preferences.shouldSuggestTripMode && mode != .trip
            }
        } else if speedKPH < 5 {
            roadSpeedFixCount = 0
            lowSpeedFixCount += 1
            if lowSpeedFixCount >= 2 {
                isLikelyDriving = false
                isTripModeSuggested = false
            }
        }
    }

    private func upsert(_ plan: RoutePlan) {
        if let index = plans.firstIndex(where: { $0.id == plan.id }) {
            plans[index] = plan
        } else {
            plans.append(plan)
        }
        persistPlans()
    }

    private func replacePlan(_ plan: RoutePlan) {
        guard let index = plans.firstIndex(where: { $0.id == plan.id }) else { return }
        plans[index] = plan
        persistPlans()
    }

    private func startLiveRefreshLoop() {
        liveRefreshTask?.cancel()
        liveRefreshTask = Task { [weak self] in
            while !Task.isCancelled {
                await self?.refreshLive()
                try? await Task.sleep(nanoseconds: 60_000_000_000)
            }
        }
    }

    private func persistScalarState() {
        defaults.set(mode.rawValue, forKey: Keys.mode)
        if let data = try? encoder.encode(preferences.normalized) {
            defaults.set(data, forKey: Keys.preferences)
        }
        defaults.set(activePlanID?.uuidString, forKey: Keys.activePlanID)
    }

    private func persistPlans() {
        if let data = try? encoder.encode(plans) {
            defaults.set(data, forKey: Keys.plans)
        }
    }
}
