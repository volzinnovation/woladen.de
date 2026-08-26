import Foundation
import CoreLocation

enum WoladenShared {
    static let appGroupIdentifier = "group.de.woladen.ios"
    static let widgetKind = "de.woladen.ios.station-widget"

    static var defaults: UserDefaults {
        UserDefaults(suiteName: appGroupIdentifier) ?? .standard
    }
}

enum WoladenWidgetMode: String, Codable, Equatable {
    case plan
    case trip
}

struct WoladenWidgetFilter: Codable, Equatable {
    var selectedOperatorNames: Set<String> = []
    var minPowerKW: Double = 50
    var minAmenityCount: Double = 0
    var selectedAmenities: Set<String> = []
    var amenityNameQuery = ""
    var availableOnly = true
    var currentlyOpenOnly = false
}

struct WoladenWidgetStation: Codable, Equatable, Identifiable {
    let stationID: String
    let operatorName: String
    let stationName: String
    let city: String
    let address: String
    let latitude: Double
    let longitude: Double
    let maxPowerKW: Double
    let chargingPointCount: Int
    var availabilityStatus: String
    var availableEVSEs: Int
    var totalEVSEs: Int
    var priceDisplay: String
    var sourceUpdatedAt: Date?
    let distanceM: Double?
    let eta: Date?

    var id: String { stationID }

    var coordinate: CLLocationCoordinate2D {
        CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
    }

    var displayName: String {
        let trimmedStationName = stationName.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmedStationName.isEmpty ? operatorName : trimmedStationName
    }

    var deepLinkURL: URL? {
        var allowed = CharacterSet.urlPathAllowed
        allowed.remove(charactersIn: "/")
        let encoded = stationID.addingPercentEncoding(withAllowedCharacters: allowed) ?? stationID
        return URL(string: "woladen://station/\(encoded)")
    }
}

struct WoladenWidgetContent: Codable, Equatable {
    enum State: String, Codable {
        case station
        case locationPermissionRequired
        case locationUnavailable
        case noMatch
        case networkUnavailable
        case noTripTarget
    }

    let mode: WoladenWidgetMode
    let state: State
    let station: WoladenWidgetStation?
    let generatedAt: Date
    let locationIsStale: Bool

    static func empty(mode: WoladenWidgetMode, state: State, now: Date = Date()) -> WoladenWidgetContent {
        WoladenWidgetContent(
            mode: mode,
            state: state,
            station: nil,
            generatedAt: now,
            locationIsStale: false
        )
    }
}

enum WoladenWidgetStateStore {
    private enum Keys {
        static let mode = "woladen.shared.widget.mode.v1"
        static let filter = "woladen.shared.widget.filter.v1"
        static let planning = "woladen.shared.widget.planning.v1"
        static let trip = "woladen.shared.widget.trip.v1"
    }

    private static let encoder = JSONEncoder()
    private static let decoder = JSONDecoder()

    static func saveMode(_ mode: WoladenWidgetMode) {
        WoladenShared.defaults.set(mode.rawValue, forKey: Keys.mode)
    }

    static func loadMode() -> WoladenWidgetMode {
        WoladenShared.defaults.string(forKey: Keys.mode).flatMap(WoladenWidgetMode.init(rawValue:)) ?? .plan
    }

    static func saveFilter(_ filter: WoladenWidgetFilter) {
        save(filter, key: Keys.filter)
    }

    static func loadFilter() -> WoladenWidgetFilter {
        load(WoladenWidgetFilter.self, key: Keys.filter) ?? WoladenWidgetFilter()
    }

    static func savePlanning(_ content: WoladenWidgetContent) {
        save(content, key: Keys.planning)
    }

    static func loadPlanning() -> WoladenWidgetContent? {
        load(WoladenWidgetContent.self, key: Keys.planning)
    }

    static func saveTrip(_ content: WoladenWidgetContent) {
        save(content, key: Keys.trip)
    }

    static func loadTrip() -> WoladenWidgetContent? {
        load(WoladenWidgetContent.self, key: Keys.trip)
    }

    static func currentContent() -> WoladenWidgetContent? {
        loadMode() == .trip ? loadTrip() : loadPlanning()
    }

    private static func save<Value: Encodable>(_ value: Value, key: String) {
        guard let data = try? encoder.encode(value) else { return }
        WoladenShared.defaults.set(data, forKey: key)
    }

    private static func load<Value: Decodable>(_ type: Value.Type, key: String) -> Value? {
        guard let data = WoladenShared.defaults.data(forKey: key) else { return nil }
        return try? decoder.decode(type, from: data)
    }
}

struct WoladenWidgetCatalogSearchResponse: Decodable {
    let stations: [WoladenWidgetCatalogStation]
}

struct WoladenWidgetAmenity: Decodable, Equatable {
    let category: String
    let name: String?
    let openingHours: String?

    enum CodingKeys: String, CodingKey {
        case category
        case name
        case openingHours = "opening_hours"
    }
}

struct WoladenWidgetCatalogStation: Decodable, Equatable, Identifiable {
    let stationID: String
    let operatorName: String
    let stationName: String
    let city: String
    let address: String
    let latitude: Double
    let longitude: Double
    let chargerCount: Int
    let maxPowerKW: Double
    var priceDisplay: String
    let amenitiesTotal: Int
    let amenityCategoryCounts: [String: Int]
    let amenityExamples: [WoladenWidgetAmenity]
    var availabilityStatus: String
    var availableEVSEs: Int
    var totalEVSEs: Int
    var sourceObservedAt: String
    var fetchedAt: String

    var id: String { stationID }

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case operatorName = "operator_name"
        case stationName = "station_name"
        case city
        case address
        case latitude
        case longitude
        case chargerCount = "charger_count"
        case maxPowerKW = "max_power_kw"
        case priceDisplay = "price_display"
        case amenitiesTotal = "amenities_total"
        case amenityCategoryCounts = "amenity_category_counts"
        case amenityExamples = "amenity_examples"
        case availabilityStatus = "availability_status"
        case availableEVSEs = "available_evses"
        case totalEVSEs = "total_evses"
        case sourceObservedAt = "source_observed_at"
        case fetchedAt = "fetched_at"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        stationID = (try? container.decode(String.self, forKey: .stationID)) ?? ""
        operatorName = (try? container.decode(String.self, forKey: .operatorName)) ?? ""
        stationName = (try? container.decode(String.self, forKey: .stationName)) ?? ""
        city = (try? container.decode(String.self, forKey: .city)) ?? ""
        address = (try? container.decode(String.self, forKey: .address)) ?? ""
        latitude = (try? container.decode(Double.self, forKey: .latitude)) ?? 0
        longitude = (try? container.decode(Double.self, forKey: .longitude)) ?? 0
        chargerCount = (try? container.decode(Int.self, forKey: .chargerCount)) ?? 0
        maxPowerKW = (try? container.decode(Double.self, forKey: .maxPowerKW)) ?? 0
        priceDisplay = (try? container.decode(String.self, forKey: .priceDisplay)) ?? ""
        amenitiesTotal = (try? container.decode(Int.self, forKey: .amenitiesTotal)) ?? 0
        amenityCategoryCounts = (try? container.decode([String: Int].self, forKey: .amenityCategoryCounts)) ?? [:]
        amenityExamples = (try? container.decode([WoladenWidgetAmenity].self, forKey: .amenityExamples)) ?? []
        availabilityStatus = (try? container.decode(String.self, forKey: .availabilityStatus)) ?? "unknown"
        availableEVSEs = (try? container.decode(Int.self, forKey: .availableEVSEs)) ?? 0
        totalEVSEs = (try? container.decode(Int.self, forKey: .totalEVSEs)) ?? 0
        sourceObservedAt = (try? container.decode(String.self, forKey: .sourceObservedAt)) ?? ""
        fetchedAt = (try? container.decode(String.self, forKey: .fetchedAt)) ?? ""
    }

    func distance(from location: CLLocation) -> CLLocationDistance {
        location.distance(from: CLLocation(latitude: latitude, longitude: longitude))
    }

    func matches(_ filter: WoladenWidgetFilter, now: Date = Date()) -> Bool {
        if !filter.selectedOperatorNames.isEmpty && !filter.selectedOperatorNames.contains(operatorName) {
            return false
        }
        if maxPowerKW < filter.minPowerKW { return false }
        if filter.minAmenityCount > 0 && amenitiesTotal < Int(filter.minAmenityCount.rounded()) { return false }
        if filter.availableOnly && availableEVSEs <= 0 { return false }
        if filter.currentlyOpenOnly && !amenityExamples.contains(where: { woladenAmenityIsOpen($0.openingHours, now: now) }) {
            return false
        }
        for category in filter.selectedAmenities where (amenityCategoryCounts[category] ?? 0) <= 0 {
            return false
        }
        let query = woladenNormalizedAmenityQuery(filter.amenityNameQuery)
        if !query.isEmpty {
            return amenityExamples.contains { example in
                guard let name = example.name else { return false }
                return woladenNormalizedAmenityQuery(name).contains(query)
            }
        }
        return true
    }

    func snapshot(from location: CLLocation, eta: Date? = nil) -> WoladenWidgetStation {
        WoladenWidgetStation(
            stationID: stationID,
            operatorName: operatorName,
            stationName: stationName,
            city: city,
            address: address,
            latitude: latitude,
            longitude: longitude,
            maxPowerKW: maxPowerKW,
            chargingPointCount: max(chargerCount, totalEVSEs),
            availabilityStatus: availabilityStatus,
            availableEVSEs: availableEVSEs,
            totalEVSEs: totalEVSEs,
            priceDisplay: priceDisplay,
            sourceUpdatedAt: woladenParseISO8601(sourceObservedAt) ?? woladenParseISO8601(fetchedAt),
            distanceM: distance(from: location),
            eta: eta
        )
    }
}

struct WoladenWidgetLiveLookupResponse: Decodable {
    let stations: [WoladenWidgetLiveStation]
}

struct WoladenWidgetLiveStation: Decodable {
    let stationID: String
    let availabilityStatus: String
    let availableEVSEs: Int
    let totalEVSEs: Int
    let priceDisplay: String
    let sourceObservedAt: String
    let fetchedAt: String

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case availabilityStatus = "availability_status"
        case availableEVSEs = "available_evses"
        case totalEVSEs = "total_evses"
        case priceDisplay = "price_display"
        case sourceObservedAt = "source_observed_at"
        case fetchedAt = "fetched_at"
    }
}

extension WoladenWidgetCatalogStation {
    func merging(_ live: WoladenWidgetLiveStation) -> WoladenWidgetCatalogStation {
        var result = self
        result.availabilityStatus = live.availabilityStatus
        result.availableEVSEs = live.availableEVSEs
        result.totalEVSEs = live.totalEVSEs
        if !live.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            result.priceDisplay = live.priceDisplay
        }
        result.sourceObservedAt = live.sourceObservedAt
        result.fetchedAt = live.fetchedAt
        return result
    }
}

final class WoladenWidgetAPIClient {
    private let baseURL = URL(string: "https://live-eu.woladen.de")!
    private let decoder = JSONDecoder()

    func nearestStations(
        location: CLLocation,
        filter: WoladenWidgetFilter,
        radiusM: Int = 20_000,
        limit: Int = 100
    ) async throws -> [WoladenWidgetCatalogStation] {
        let operatorNames = filter.selectedOperatorNames.sorted()
        let searchOperators: [String?] = operatorNames.isEmpty ? [nil] : operatorNames.map(Optional.some)
        var byID: [String: WoladenWidgetCatalogStation] = [:]

        for operatorName in searchOperators {
            var components = URLComponents(
                url: baseURL.appending(path: "/v1/catalog/search"),
                resolvingAgainstBaseURL: false
            )!
            var items = [
                URLQueryItem(name: "lat", value: String(location.coordinate.latitude)),
                URLQueryItem(name: "lon", value: String(location.coordinate.longitude)),
                URLQueryItem(name: "radius_m", value: String(radiusM)),
                URLQueryItem(name: "limit", value: String(limit)),
                URLQueryItem(name: "mode", value: "travel"),
                URLQueryItem(name: "min_power_kw", value: String(filter.minPowerKW))
            ]
            if let operatorName { items.append(URLQueryItem(name: "operator", value: operatorName)) }
            components.queryItems = items
            var request = URLRequest(url: components.url!)
            request.timeoutInterval = 20
            request.setValue("application/json", forHTTPHeaderField: "Accept")
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
                throw URLError(.badServerResponse)
            }
            let decoded = try decoder.decode(WoladenWidgetCatalogSearchResponse.self, from: data)
            decoded.stations.forEach { byID[$0.stationID] = $0 }
        }

        let filterWithoutAvailability = WoladenWidgetFilter(
            selectedOperatorNames: filter.selectedOperatorNames,
            minPowerKW: filter.minPowerKW,
            minAmenityCount: filter.minAmenityCount,
            selectedAmenities: filter.selectedAmenities,
            amenityNameQuery: filter.amenityNameQuery,
            availableOnly: false,
            currentlyOpenOnly: filter.currentlyOpenOnly
        )
        var candidates = byID.values
            .filter { $0.matches(filterWithoutAvailability) }
            .sorted { lhs, rhs in
                let lhsDistance = lhs.distance(from: location)
                let rhsDistance = rhs.distance(from: location)
                return lhsDistance == rhsDistance ? lhs.stationID < rhs.stationID : lhsDistance < rhsDistance
            }
        candidates = Array(candidates.prefix(60))

        if !candidates.isEmpty {
            let live = try? await lookup(stationIDs: candidates.map(\.stationID))
            if let live {
                let liveByID = Dictionary(uniqueKeysWithValues: live.map { ($0.stationID, $0) })
                candidates = candidates.map { station in
                    liveByID[station.stationID].map(station.merging) ?? station
                }
            }
        }

        return candidates
            .filter { $0.matches(filter) }
            .sorted { lhs, rhs in
                let lhsDistance = lhs.distance(from: location)
                let rhsDistance = rhs.distance(from: location)
                return lhsDistance == rhsDistance ? lhs.stationID < rhs.stationID : lhsDistance < rhsDistance
            }
    }

    func lookup(stationIDs: [String]) async throws -> [WoladenWidgetLiveStation] {
        var result: [WoladenWidgetLiveStation] = []
        for start in stride(from: 0, to: stationIDs.count, by: 20) {
            let end = min(start + 20, stationIDs.count)
            var request = URLRequest(url: baseURL.appending(path: "/v1/stations/lookup"))
            request.httpMethod = "POST"
            request.timeoutInterval = 5
            request.setValue("application/json", forHTTPHeaderField: "Content-Type")
            request.setValue("application/json", forHTTPHeaderField: "Accept")
            request.httpBody = try JSONSerialization.data(withJSONObject: [
                "station_ids": Array(stationIDs[start..<end])
            ])
            let (data, response) = try await URLSession.shared.data(for: request)
            guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
                throw URLError(.badServerResponse)
            }
            result.append(contentsOf: try decoder.decode(WoladenWidgetLiveLookupResponse.self, from: data).stations)
        }
        return result
    }
}

func woladenParseISO8601(_ value: String) -> Date? {
    guard !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return nil }
    let fractional = ISO8601DateFormatter()
    fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    return fractional.date(from: value) ?? ISO8601DateFormatter().date(from: value)
}

func woladenNormalizedAmenityQuery(_ value: String) -> String {
    let folded = value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .folding(options: [.diacriticInsensitive, .widthInsensitive], locale: Locale(identifier: "en_US_POSIX"))
        .lowercased()
        .replacingOccurrences(of: "ß", with: "ss")
    return String(folded.unicodeScalars.filter { CharacterSet.alphanumerics.contains($0) })
}

private let woladenOpeningDayKeys = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]

private let woladenOpeningISO3CountryCodes = [
    "AUT": "AT", "BEL": "BE", "BGR": "BG", "CHE": "CH", "CYP": "CY", "CZE": "CZ",
    "DEU": "DE", "DNK": "DK", "ESP": "ES", "EST": "EE", "FIN": "FI", "FRA": "FR",
    "GRC": "GR", "HRV": "HR", "HUN": "HU", "IRL": "IE", "ITA": "IT", "LTU": "LT",
    "LUX": "LU", "LVA": "LV", "MLT": "MT", "NLD": "NL", "NOR": "NO", "POL": "PL",
    "PRT": "PT", "ROU": "RO", "SVK": "SK", "SVN": "SI", "SWE": "SE"
]

private let woladenOpeningCountryTimeZones = [
    "AT": "Europe/Vienna", "BE": "Europe/Brussels", "BG": "Europe/Sofia", "CH": "Europe/Zurich",
    "CY": "Asia/Nicosia", "CZ": "Europe/Prague", "DE": "Europe/Berlin", "DK": "Europe/Copenhagen",
    "EE": "Europe/Tallinn", "ES": "Europe/Madrid", "FI": "Europe/Helsinki", "FR": "Europe/Paris",
    "GR": "Europe/Athens", "HR": "Europe/Zagreb", "HU": "Europe/Budapest", "IE": "Europe/Dublin",
    "IT": "Europe/Rome", "LT": "Europe/Vilnius", "LU": "Europe/Luxembourg", "LV": "Europe/Riga",
    "MT": "Europe/Malta", "NL": "Europe/Amsterdam", "NO": "Europe/Oslo", "PL": "Europe/Warsaw",
    "PT": "Europe/Lisbon", "RO": "Europe/Bucharest", "SE": "Europe/Stockholm", "SI": "Europe/Ljubljana",
    "SK": "Europe/Bratislava"
]

enum WoladenOpeningState: Equatable {
    case open
    case closed
}

struct WoladenOpeningEvaluation: Equatable {
    let state: WoladenOpeningState
    let nextChange: Date?
}

private struct WoladenOpeningNowParts {
    let dayKey: String
    let previousDayKey: String
    let minuteOfDay: Int
    let isPublicHoliday: Bool
    let previousDayIsPublicHoliday: Bool
    let isUncertainPublicHoliday: Bool
    let previousDayIsUncertainPublicHoliday: Bool
}

private struct WoladenOpeningClause {
    let selectedDays: Set<String>?
    let matchesPublicHoliday: Bool
    let mode: WoladenOpeningMode
    let ranges: [WoladenOpeningRange]
}

private enum WoladenOpeningMode: Equatable {
    case open
    case closed
    case times
    case unknown
}

private struct WoladenOpeningRange {
    let start: Int
    let end: Int
    let openEnded: Bool
}

private struct WoladenOpeningContext {
    let calendar: Calendar
    let publicHolidayKeys: Set<String>
    let uncertainPublicHolidayKeys: Set<String>
}

func woladenOpeningTimeZone(countryCode: String?) -> TimeZone {
    let normalized = woladenNormalizedOpeningCountryCode(countryCode)
    guard let identifier = woladenOpeningCountryTimeZones[normalized],
          let timeZone = TimeZone(identifier: identifier) else { return .current }
    return timeZone
}

func woladenAmenityOpeningEvaluation(
    _ openingHours: String?,
    now: Date = Date(),
    timeZone: TimeZone = .current,
    countryCode: String? = nil
) -> WoladenOpeningEvaluation? {
    let normalized = (openingHours ?? "")
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
    guard !normalized.isEmpty else { return nil }
    if normalized.compare("24/7", options: .caseInsensitive) == .orderedSame
        || normalized.compare("open", options: .caseInsensitive) == .orderedSame {
        return WoladenOpeningEvaluation(state: .open, nextChange: nil)
    }
    if normalized.range(of: "^(?:off|closed)$", options: [.regularExpression, .caseInsensitive]) != nil {
        return WoladenOpeningEvaluation(state: .closed, nextChange: nil)
    }

    let rawClauses = normalized.split(separator: ";", omittingEmptySubsequences: false).map(String.init)
    let parsedClauses = rawClauses.compactMap(woladenParseOpeningClause)
    guard !parsedClauses.isEmpty,
          parsedClauses.count == rawClauses.count,
          !parsedClauses.contains(where: { $0.mode == .unknown }) else { return nil }

    let normalizedCountryCode = woladenNormalizedOpeningCountryCode(countryCode)
    let usesPublicHolidays = parsedClauses.contains(where: { $0.matchesPublicHoliday })
    guard !usesPublicHolidays || normalizedCountryCode == "DE" else { return nil }

    let context = woladenOpeningContext(
        now: now,
        timeZone: timeZone,
        includesGermanPublicHolidays: usesPublicHolidays
    )
    let currentMode = woladenOpeningMode(for: parsedClauses, now: now, context: context)
    guard let state = woladenPublicOpeningState(currentMode) else { return nil }
    let nextChange = woladenNextOpeningChange(
        clauses: parsedClauses,
        from: now,
        currentMode: currentMode,
        context: context
    )
    return WoladenOpeningEvaluation(state: state, nextChange: nextChange)
}

func woladenAmenityOpeningDisplay(
    _ openingHours: String?,
    now: Date = Date(),
    timeZone: TimeZone = .current,
    countryCode: String? = nil,
    locale: Locale = .current
) -> String? {
    let raw = (openingHours ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
    guard !raw.isEmpty else { return nil }
    guard let evaluation = woladenAmenityOpeningEvaluation(
        raw,
        now: now,
        timeZone: timeZone,
        countryCode: countryCode
    ) else { return raw }

    let stateText: String
    switch evaluation.state {
    case .open:
        stateText = String(localized: "amenity.open", defaultValue: "Open now", locale: locale)
    case .closed:
        stateText = String(localized: "amenity.closed", defaultValue: "Closed", locale: locale)
    }
    guard let nextChange = evaluation.nextChange else { return stateText }

    let calendar = woladenOpeningCalendar(timeZone: timeZone)
    let timeFormatter = DateFormatter()
    timeFormatter.locale = locale
    timeFormatter.timeZone = timeZone
    timeFormatter.dateStyle = .none
    timeFormatter.timeStyle = .short
    let time = timeFormatter.string(from: nextChange)

    let transition: String
    if calendar.isDate(now, inSameDayAs: nextChange) {
        switch evaluation.state {
        case .open:
            transition = String(
                localized: "amenity.opening.closesAt",
                defaultValue: "Closes at {time}",
                locale: locale
            ).replacingOccurrences(of: "{time}", with: time)
        case .closed:
            transition = String(
                localized: "amenity.opening.opensAt",
                defaultValue: "Opens at {time}",
                locale: locale
            ).replacingOccurrences(of: "{time}", with: time)
        }
    } else {
        let dayFormatter = DateFormatter()
        dayFormatter.locale = locale
        dayFormatter.timeZone = timeZone
        dayFormatter.setLocalizedDateFormatFromTemplate("EEE")
        let day = dayFormatter.string(from: nextChange)
        switch evaluation.state {
        case .open:
            transition = String(
                localized: "amenity.opening.closesOnAt",
                defaultValue: "Closes {day} at {time}",
                locale: locale
            )
            .replacingOccurrences(of: "{day}", with: day)
            .replacingOccurrences(of: "{time}", with: time)
        case .closed:
            transition = String(
                localized: "amenity.opening.opensOnAt",
                defaultValue: "Opens {day} at {time}",
                locale: locale
            )
            .replacingOccurrences(of: "{day}", with: day)
            .replacingOccurrences(of: "{time}", with: time)
        }
    }
    return "\(stateText) · \(transition)"
}

func woladenAmenityIsOpen(_ openingHours: String?, now: Date = Date()) -> Bool {
    woladenAmenityOpeningEvaluation(openingHours, now: now)?.state == .open
}

private func woladenOpeningContext(
    now: Date,
    timeZone: TimeZone,
    includesGermanPublicHolidays: Bool
) -> WoladenOpeningContext {
    let calendar = woladenOpeningCalendar(timeZone: timeZone)
    guard includesGermanPublicHolidays else {
        return WoladenOpeningContext(
            calendar: calendar,
            publicHolidayKeys: [],
            uncertainPublicHolidayKeys: []
        )
    }
    let year = calendar.component(.year, from: now)
    let years = (year - 1)...(year + 2)
    let nationalKeys = Set(years.flatMap {
        woladenGermanNationalPublicHolidayKeys(year: $0, calendar: calendar)
    })
    let regionalKeys = Set(years.flatMap {
        woladenGermanRegionalPublicHolidayKeys(year: $0, calendar: calendar)
    })
    return WoladenOpeningContext(
        calendar: calendar,
        publicHolidayKeys: nationalKeys,
        uncertainPublicHolidayKeys: regionalKeys.subtracting(nationalKeys)
    )
}

private func woladenOpeningCalendar(timeZone: TimeZone) -> Calendar {
    var calendar = Calendar(identifier: .gregorian)
    calendar.locale = Locale(identifier: "en_US_POSIX")
    calendar.timeZone = timeZone
    return calendar
}

private func woladenOpeningNowParts(now: Date, context: WoladenOpeningContext) -> WoladenOpeningNowParts {
    let calendar = context.calendar
    let dayIndex = (calendar.component(.weekday, from: now) + 5) % 7
    let previousDate = calendar.date(byAdding: .day, value: -1, to: now) ?? now
    return WoladenOpeningNowParts(
        dayKey: woladenOpeningDayKeys[dayIndex],
        previousDayKey: woladenOpeningDayKeys[(dayIndex + 6) % 7],
        minuteOfDay: calendar.component(.hour, from: now) * 60 + calendar.component(.minute, from: now),
        isPublicHoliday: context.publicHolidayKeys.contains(woladenOpeningDateKey(now, calendar: calendar)),
        previousDayIsPublicHoliday: context.publicHolidayKeys.contains(woladenOpeningDateKey(previousDate, calendar: calendar)),
        isUncertainPublicHoliday: context.uncertainPublicHolidayKeys.contains(
            woladenOpeningDateKey(now, calendar: calendar)
        ),
        previousDayIsUncertainPublicHoliday: context.uncertainPublicHolidayKeys.contains(
            woladenOpeningDateKey(previousDate, calendar: calendar)
        )
    )
}

private func woladenParseOpeningClause(_ value: String) -> WoladenOpeningClause? {
    let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return nil }
    let token = "(?:Mo|Tu|We|Th|Fr|Sa|Su|PH)"
    let pattern = "^(\(token)(?:\\s*-\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?(?:\\s*,\\s*\(token)(?:\\s*-\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\\s+(.+)$"
    let regex = try? NSRegularExpression(pattern: pattern)
    let range = NSRange(trimmed.startIndex..<trimmed.endIndex, in: trimmed)
    let match = regex?.firstMatch(in: trimmed, range: range)
    let selector: String?
    let body: String
    if let match, match.numberOfRanges >= 3,
       let selectorRange = Range(match.range(at: 1), in: trimmed),
       let bodyRange = Range(match.range(at: 2), in: trimmed) {
        selector = String(trimmed[selectorRange])
        body = String(trimmed[bodyRange]).trimmingCharacters(in: .whitespacesAndNewlines)
    } else {
        selector = nil
        body = trimmed
    }
    let parsedSelector = woladenOpeningSelector(selector)
    if body.range(of: "^(?:off|closed)$", options: [.regularExpression, .caseInsensitive]) != nil {
        return WoladenOpeningClause(
            selectedDays: parsedSelector.days,
            matchesPublicHoliday: parsedSelector.matchesPublicHoliday,
            mode: .closed,
            ranges: []
        )
    }
    if body.compare("open", options: .caseInsensitive) == .orderedSame {
        return WoladenOpeningClause(
            selectedDays: parsedSelector.days,
            matchesPublicHoliday: parsedSelector.matchesPublicHoliday,
            mode: .open,
            ranges: []
        )
    }
    let rawRanges = body.split(separator: ",", omittingEmptySubsequences: false).map(String.init)
    let ranges = rawRanges.compactMap(woladenParseOpeningRange)
    return WoladenOpeningClause(
        selectedDays: parsedSelector.days,
        matchesPublicHoliday: parsedSelector.matchesPublicHoliday,
        mode: ranges.count == rawRanges.count && !ranges.isEmpty ? .times : .unknown,
        ranges: ranges
    )
}

private func woladenOpeningSelector(_ selector: String?) -> (days: Set<String>?, matchesPublicHoliday: Bool) {
    guard let selector, !selector.isEmpty else { return (nil, false) }
    var selected = Set<String>()
    var matchesPublicHoliday = false
    for rawPart in selector.split(separator: ",") {
        let part = rawPart.trimmingCharacters(in: .whitespacesAndNewlines)
        if part == "PH" {
            matchesPublicHoliday = true
            continue
        }
        if part.contains("-") {
            let bounds = part.split(separator: "-").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            guard bounds.count == 2,
                  let start = woladenOpeningDayKeys.firstIndex(of: bounds[0]),
                  let end = woladenOpeningDayKeys.firstIndex(of: bounds[1]) else { continue }
            for offset in 0..<woladenOpeningDayKeys.count {
                let index = (start + offset) % woladenOpeningDayKeys.count
                selected.insert(woladenOpeningDayKeys[index])
                if index == end { break }
            }
        } else if woladenOpeningDayKeys.contains(part) {
            selected.insert(part)
        }
    }
    return (selected, matchesPublicHoliday)
}

private func woladenParseOpeningRange(_ value: String) -> WoladenOpeningRange? {
    let compact = value.replacingOccurrences(of: "\\s+", with: "", options: .regularExpression)
    if compact.hasSuffix("+"), !compact.contains("-") {
        guard let start = woladenParseOpeningMinute(String(compact.dropLast())) else { return nil }
        return WoladenOpeningRange(start: start, end: 24 * 60, openEnded: true)
    }
    let pieces = compact.replacingOccurrences(of: "+", with: "").split(separator: "-")
    guard pieces.count == 2,
          let start = woladenParseOpeningMinute(String(pieces[0])),
          let end = woladenParseOpeningMinute(String(pieces[1])) else { return nil }
    return WoladenOpeningRange(start: start, end: end, openEnded: false)
}

private func woladenParseOpeningMinute(_ value: String) -> Int? {
    let pieces = value.split(separator: ":")
    guard pieces.count == 2,
          let hour = Int(pieces[0]), let minute = Int(pieces[1]),
          hour >= 0, hour <= 24, minute >= 0, minute <= 59,
          !(hour == 24 && minute != 0) else { return nil }
    return hour * 60 + minute
}

private func woladenOpeningMode(
    for clauses: [WoladenOpeningClause],
    now: Date,
    context: WoladenOpeningContext
) -> WoladenOpeningMode {
    let parts = woladenOpeningNowParts(now: now, context: context)
    if parts.isUncertainPublicHoliday { return .unknown }
    if parts.previousDayIsUncertainPublicHoliday,
       clauses.contains(where: { clause in
           clause.matchesPublicHoliday && clause.ranges.contains(where: {
               $0.openEnded || $0.start >= $0.end
           })
       }) {
        return .unknown
    }
    var currentState: WoladenOpeningMode?
    for clause in clauses {
        if let state = woladenOpeningState(
            for: clause,
            dayKey: parts.dayKey,
            isPublicHoliday: parts.isPublicHoliday,
            minuteOfDay: parts.minuteOfDay,
            previousDay: false
        ) {
            currentState = state
        }
    }
    if currentState == .open || currentState == .unknown { return currentState ?? .unknown }
    if clauses.contains(where: {
        woladenOpeningState(
            for: $0,
            dayKey: parts.previousDayKey,
            isPublicHoliday: parts.previousDayIsPublicHoliday,
            minuteOfDay: parts.minuteOfDay,
            previousDay: true
        ) == .open
    }) {
        return .open
    }
    return currentState ?? .closed
}

private func woladenOpeningState(
    for clause: WoladenOpeningClause,
    dayKey: String,
    isPublicHoliday: Bool,
    minuteOfDay: Int,
    previousDay: Bool
) -> WoladenOpeningMode? {
    if isPublicHoliday, clause.matchesPublicHoliday {
        // A public-holiday clause applies independently of its regular weekday selector.
    } else if let selectedDays = clause.selectedDays, !selectedDays.contains(dayKey) {
        return nil
    }
    switch clause.mode {
    case .closed: return previousDay ? nil : .closed
    case .open: return previousDay ? nil : .open
    case .unknown: return previousDay ? nil : .unknown
    case .times:
        return clause.ranges.contains {
            woladenIsWithinOpeningRange($0, minuteOfDay: minuteOfDay, previousDay: previousDay)
        } ? .open : (previousDay ? nil : .closed)
    }
}

private func woladenIsWithinOpeningRange(
    _ range: WoladenOpeningRange,
    minuteOfDay: Int,
    previousDay: Bool
) -> Bool {
    if range.openEnded { return previousDay ? minuteOfDay < 6 * 60 : minuteOfDay >= range.start }
    if range.start == range.end {
        return previousDay ? minuteOfDay < range.end : minuteOfDay >= range.start
    }
    if range.start < range.end { return !previousDay && minuteOfDay >= range.start && minuteOfDay < range.end }
    return previousDay ? minuteOfDay < range.end : minuteOfDay >= range.start
}

private func woladenPublicOpeningState(_ mode: WoladenOpeningMode) -> WoladenOpeningState? {
    switch mode {
    case .open: return .open
    case .closed: return .closed
    case .times, .unknown: return nil
    }
}

private func woladenNextOpeningChange(
    clauses: [WoladenOpeningClause],
    from now: Date,
    currentMode: WoladenOpeningMode,
    context: WoladenOpeningContext
) -> Date? {
    let calendar = context.calendar
    for candidate in woladenOpeningTransitionCandidates(
        clauses: clauses,
        from: now,
        calendar: calendar
    ) {
        let candidateMode = woladenOpeningMode(for: clauses, now: candidate, context: context)
        if candidateMode == .unknown { return nil }
        if candidateMode != currentMode { return candidate }
    }
    return nil
}

private func woladenOpeningTransitionCandidates(
    clauses: [WoladenOpeningClause],
    from now: Date,
    calendar: Calendar
) -> [Date] {
    let today = calendar.startOfDay(for: now)
    let horizon = calendar.date(byAdding: .day, value: 8, to: now) ?? now.addingTimeInterval(8 * 86_400)
    var candidates = Set<Date>()

    for dayOffset in -1...8 {
        guard let dayStart = calendar.date(byAdding: .day, value: dayOffset, to: today) else { continue }
        candidates.insert(dayStart)
        for clause in clauses where clause.mode == .times {
            for range in clause.ranges {
                if let start = woladenOpeningDate(
                    on: dayStart,
                    minuteOfDay: range.start,
                    calendar: calendar
                ) {
                    candidates.insert(start)
                }

                let endDayOffset = range.openEnded || range.start >= range.end ? 1 : 0
                guard let endDay = calendar.date(byAdding: .day, value: endDayOffset, to: dayStart),
                      let end = woladenOpeningDate(
                        on: endDay,
                        minuteOfDay: range.openEnded ? 6 * 60 : range.end,
                        calendar: calendar
                      ) else { continue }
                candidates.insert(end)
            }
        }
    }

    return candidates
        .filter { $0 > now && $0 <= horizon }
        .sorted()
}

private func woladenOpeningDate(
    on dayStart: Date,
    minuteOfDay: Int,
    calendar: Calendar
) -> Date? {
    if minuteOfDay == 24 * 60 {
        return calendar.date(byAdding: .day, value: 1, to: dayStart)
    }
    guard minuteOfDay >= 0, minuteOfDay < 24 * 60 else { return nil }
    var components = calendar.dateComponents([.year, .month, .day], from: dayStart)
    components.hour = minuteOfDay / 60
    components.minute = minuteOfDay % 60
    return calendar.date(from: components)
}

private func woladenNormalizedOpeningCountryCode(_ countryCode: String?) -> String {
    let code = (countryCode ?? "").trimmingCharacters(in: .whitespacesAndNewlines).uppercased()
    if code.count == 2 { return code }
    return woladenOpeningISO3CountryCodes[code] ?? ""
}

private func woladenOpeningDateKey(_ date: Date, calendar: Calendar) -> String {
    let parts = calendar.dateComponents([.year, .month, .day], from: date)
    return String(format: "%04d-%02d-%02d", parts.year ?? 0, parts.month ?? 0, parts.day ?? 0)
}

private func woladenGermanNationalPublicHolidayKeys(year: Int, calendar: Calendar) -> [String] {
    var keys = [
        String(format: "%04d-01-01", year),
        String(format: "%04d-05-01", year),
        String(format: "%04d-10-03", year),
        String(format: "%04d-12-25", year),
        String(format: "%04d-12-26", year)
    ]
    guard let easter = woladenGermanEasterSunday(year: year, calendar: calendar) else { return keys }
    for offset in [-2, 1, 39, 50] {
        guard let holiday = calendar.date(byAdding: .day, value: offset, to: easter) else { continue }
        keys.append(woladenOpeningDateKey(holiday, calendar: calendar))
    }
    return keys
}

private func woladenGermanRegionalPublicHolidayKeys(year: Int, calendar: Calendar) -> [String] {
    var keys = [
        String(format: "%04d-01-06", year),
        String(format: "%04d-03-08", year),
        String(format: "%04d-08-08", year),
        String(format: "%04d-08-15", year),
        String(format: "%04d-09-20", year),
        String(format: "%04d-10-31", year),
        String(format: "%04d-11-01", year)
    ]
    if let easter = woladenGermanEasterSunday(year: year, calendar: calendar) {
        for offset in [0, 49, 60] {
            guard let holiday = calendar.date(byAdding: .day, value: offset, to: easter) else { continue }
            keys.append(woladenOpeningDateKey(holiday, calendar: calendar))
        }
    }
    for day in 16...22 {
        guard let date = calendar.date(from: DateComponents(year: year, month: 11, day: day)),
              calendar.component(.weekday, from: date) == 4 else { continue }
        keys.append(woladenOpeningDateKey(date, calendar: calendar))
        break
    }
    return keys
}

private func woladenGermanEasterSunday(year: Int, calendar: Calendar) -> Date? {
    let a = year % 19
    let b = year / 100
    let c = year % 100
    let d = b / 4
    let e = b % 4
    let f = (b + 8) / 25
    let g = (b - f + 1) / 3
    let h = (19 * a + b - d - g + 15) % 30
    let i = c / 4
    let k = c % 4
    let l = (32 + 2 * e + 2 * i - h - k) % 7
    let m = (a + 11 * h + 22 * l) / 451
    let month = (h + l - 7 * m + 114) / 31
    let day = ((h + l - 7 * m + 114) % 31) + 1
    return calendar.date(from: DateComponents(year: year, month: month, day: day, hour: 12))
}
