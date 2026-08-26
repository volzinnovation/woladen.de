import Foundation

private let liveDynamicKeyLabels: [String: String] = [
    "expectedAvailableFromTime": String(localized: "station.nextSlot"),
    "expectedAvailableToTime": String(localized: "station.nextSlot"),
    "expectedAvailableUntilTime": String(localized: "station.nextSlot"),
    "startTime": String(localized: "station.nextSlot"),
    "endTime": String(localized: "station.nextSlot"),
    "lastUpdated": String(localized: "station.updated").replacingOccurrences(of: "{date}", with: ""),
    "value": ""
]

struct LiveStationLookupResponse: Decodable {
    let stations: [LiveStationSummary]
    let missingStationIDs: [String]

    enum CodingKeys: String, CodingKey {
        case stations
        case missingStationIDs = "missing_station_ids"
    }

    init(stations: [LiveStationSummary], missingStationIDs: [String]) {
        self.stations = stations
        self.missingStationIDs = missingStationIDs
    }
}

struct LiveStationDetail: Decodable {
    let station: LiveStationSummary
    let evses: [LiveEVSE]
}

struct LiveStationSummary: Decodable {
    let stationID: String
    let availabilityStatus: AvailabilityStatus
    let availableEVSEs: Int
    let occupiedEVSEs: Int
    let outOfOrderEVSEs: Int
    let unknownEVSEs: Int
    let totalEVSEs: Int
    let priceDisplay: String
    let priceCurrency: String
    let priceEnergyEURKwhMin: String
    let priceEnergyEURKwhMax: String
    let sourceObservedAt: String
    let fetchedAt: String
    let ingestedAt: String
    let dailyAnalysisDataAvailable: Bool
    let frequentlyOutOfOrderDailyAnalysis: Bool
    let frequentlyOccupiedDailyAnalysis: Bool
    let dailyAnalysisOutOfOrderColor: String
    let dailyAnalysisOccupiedColor: String

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case availabilityStatus = "availability_status"
        case availableEVSEs = "available_evses"
        case occupiedEVSEs = "occupied_evses"
        case outOfOrderEVSEs = "out_of_order_evses"
        case unknownEVSEs = "unknown_evses"
        case totalEVSEs = "total_evses"
        case priceDisplay = "price_display"
        case priceCurrency = "price_currency"
        case priceEnergyEURKwhMin = "price_energy_eur_kwh_min"
        case priceEnergyEURKwhMax = "price_energy_eur_kwh_max"
        case sourceObservedAt = "source_observed_at"
        case fetchedAt = "fetched_at"
        case ingestedAt = "ingested_at"
        case dailyAnalysisDataAvailable = "daily_analysis_data_available"
        case frequentlyOutOfOrderDailyAnalysis = "frequently_out_of_order_daily_analysis"
        case frequentlyOccupiedDailyAnalysis = "frequently_occupied_daily_analysis"
        case dailyAnalysisOutOfOrderColor = "daily_analysis_out_of_order_color"
        case dailyAnalysisOccupiedColor = "daily_analysis_occupied_color"
    }

    init(
        stationID: String,
        availabilityStatus: AvailabilityStatus,
        availableEVSEs: Int,
        occupiedEVSEs: Int,
        outOfOrderEVSEs: Int,
        unknownEVSEs: Int,
        totalEVSEs: Int,
        priceDisplay: String,
        priceCurrency: String,
        priceEnergyEURKwhMin: String,
        priceEnergyEURKwhMax: String,
        sourceObservedAt: String,
        fetchedAt: String,
        ingestedAt: String,
        dailyAnalysisDataAvailable: Bool = false,
        frequentlyOutOfOrderDailyAnalysis: Bool = false,
        frequentlyOccupiedDailyAnalysis: Bool = false,
        dailyAnalysisOutOfOrderColor: String = "",
        dailyAnalysisOccupiedColor: String = ""
    ) {
        self.stationID = stationID
        self.availabilityStatus = availabilityStatus
        self.availableEVSEs = availableEVSEs
        self.occupiedEVSEs = occupiedEVSEs
        self.outOfOrderEVSEs = outOfOrderEVSEs
        self.unknownEVSEs = unknownEVSEs
        self.totalEVSEs = totalEVSEs
        self.priceDisplay = priceDisplay
        self.priceCurrency = priceCurrency
        self.priceEnergyEURKwhMin = priceEnergyEURKwhMin
        self.priceEnergyEURKwhMax = priceEnergyEURKwhMax
        self.sourceObservedAt = sourceObservedAt
        self.fetchedAt = fetchedAt
        self.ingestedAt = ingestedAt
        self.dailyAnalysisDataAvailable = dailyAnalysisDataAvailable
        self.frequentlyOutOfOrderDailyAnalysis = frequentlyOutOfOrderDailyAnalysis
        self.frequentlyOccupiedDailyAnalysis = frequentlyOccupiedDailyAnalysis
        self.dailyAnalysisOutOfOrderColor = dailyAnalysisOutOfOrderColor
        self.dailyAnalysisOccupiedColor = dailyAnalysisOccupiedColor
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        stationID = (try? container.decode(String.self, forKey: .stationID)) ?? ""
        availabilityStatus = AvailabilityStatus(rawValue: (try? container.decode(String.self, forKey: .availabilityStatus)) ?? "") ?? .unknown
        availableEVSEs = container.decodeLossyInt(forKey: .availableEVSEs) ?? 0
        occupiedEVSEs = container.decodeLossyInt(forKey: .occupiedEVSEs) ?? 0
        outOfOrderEVSEs = container.decodeLossyInt(forKey: .outOfOrderEVSEs) ?? 0
        unknownEVSEs = container.decodeLossyInt(forKey: .unknownEVSEs) ?? 0
        totalEVSEs = container.decodeLossyInt(forKey: .totalEVSEs) ?? 0
        priceDisplay = container.decodeLossyString(forKey: .priceDisplay)
        priceCurrency = container.decodeLossyString(forKey: .priceCurrency)
        priceEnergyEURKwhMin = container.decodeLossyString(forKey: .priceEnergyEURKwhMin)
        priceEnergyEURKwhMax = container.decodeLossyString(forKey: .priceEnergyEURKwhMax)
        sourceObservedAt = container.decodeLossyString(forKey: .sourceObservedAt)
        fetchedAt = container.decodeLossyString(forKey: .fetchedAt)
        ingestedAt = container.decodeLossyString(forKey: .ingestedAt)
        dailyAnalysisDataAvailable = container.decodeLossyBool(forKey: .dailyAnalysisDataAvailable) ?? false
        frequentlyOutOfOrderDailyAnalysis = container.decodeLossyBool(forKey: .frequentlyOutOfOrderDailyAnalysis) ?? false
        frequentlyOccupiedDailyAnalysis = container.decodeLossyBool(forKey: .frequentlyOccupiedDailyAnalysis) ?? false
        dailyAnalysisOutOfOrderColor = container.decodeLossyString(forKey: .dailyAnalysisOutOfOrderColor)
        dailyAnalysisOccupiedColor = container.decodeLossyString(forKey: .dailyAnalysisOccupiedColor)
    }
}

struct LiveEVSE: Decodable {
    let providerEVSEID: String
    let availabilityStatus: AvailabilityStatus
    let operationalStatus: String
    let priceDisplay: String
    let sourceObservedAt: String
    let fetchedAt: String
    let ingestedAt: String
    let nextAvailableChargingSlots: [LiveJSONValue]
    let supplementalFacilityStatus: [LiveJSONValue]

    enum CodingKeys: String, CodingKey {
        case providerEVSEID = "provider_evse_id"
        case availabilityStatus = "availability_status"
        case operationalStatus = "operational_status"
        case priceDisplay = "price_display"
        case sourceObservedAt = "source_observed_at"
        case fetchedAt = "fetched_at"
        case ingestedAt = "ingested_at"
        case nextAvailableChargingSlots = "next_available_charging_slots"
        case supplementalFacilityStatus = "supplemental_facility_status"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        providerEVSEID = container.decodeLossyString(forKey: .providerEVSEID)
        availabilityStatus = AvailabilityStatus(rawValue: (try? container.decode(String.self, forKey: .availabilityStatus)) ?? "") ?? .unknown
        operationalStatus = container.decodeLossyString(forKey: .operationalStatus)
        priceDisplay = container.decodeLossyString(forKey: .priceDisplay)
        sourceObservedAt = container.decodeLossyString(forKey: .sourceObservedAt)
        fetchedAt = container.decodeLossyString(forKey: .fetchedAt)
        ingestedAt = container.decodeLossyString(forKey: .ingestedAt)
        nextAvailableChargingSlots = (try? container.decode([LiveJSONValue].self, forKey: .nextAvailableChargingSlots)) ?? []
        supplementalFacilityStatus = (try? container.decode([LiveJSONValue].self, forKey: .supplementalFacilityStatus)) ?? []
    }
}

indirect enum LiveJSONValue: Decodable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case object([String: LiveJSONValue])
    case array([LiveJSONValue])
    case null

    init(from decoder: Decoder) throws {
        if let container = try? decoder.singleValueContainer() {
            if container.decodeNil() {
                self = .null
                return
            }
            if let value = try? container.decode(Bool.self) {
                self = .bool(value)
                return
            }
            if let value = try? container.decode(Double.self) {
                self = .number(value)
                return
            }
            if let value = try? container.decode(String.self) {
                self = .string(value)
                return
            }
        }

        if var arrayContainer = try? decoder.unkeyedContainer() {
            var items: [LiveJSONValue] = []
            while !arrayContainer.isAtEnd {
                if let item = try? arrayContainer.decode(LiveJSONValue.self) {
                    items.append(item)
                } else {
                    _ = try? arrayContainer.decode(String.self)
                }
            }
            self = .array(items)
            return
        }

        let objectContainer = try decoder.container(keyedBy: AnyCodingKey.self)
        var object: [String: LiveJSONValue] = [:]
        for key in objectContainer.allKeys {
            object[key.stringValue] = (try? objectContainer.decode(LiveJSONValue.self, forKey: key)) ?? .null
        }
        self = .object(object)
    }
}

enum AvailabilityStatus: String {
    case free
    case occupied
    case outOfOrder = "out_of_order"
    case unknown

    init?(rawValue: String) {
        switch rawValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
        case "free", "available":
            self = .free
        case "occupied", "charging":
            self = .occupied
        case "out_of_order", "outoforder", "inoperative", "faulted":
            self = .outOfOrder
        case "unknown", "":
            self = .unknown
        default:
            return nil
        }
    }

    var label: String {
        switch self {
        case .free:
            return String(localized: "availability.free")
        case .occupied:
            return String(localized: "availability.occupied")
        case .outOfOrder:
            return String(localized: "availability.out_of_order")
        case .unknown:
            return String(localized: "availability.unknown")
        }
    }
}

struct AvailabilityCounts {
    let total: Int
    let available: Int
    let occupied: Int
    let outOfOrder: Int
    let unknown: Int
}

struct LiveDetailNote: Identifiable {
    let id = UUID()
    let label: String
    let value: String
}

struct LiveEVSERow: Identifiable {
    let id = UUID()
    let title: String
    let status: AvailabilityStatus
    let meta: String
    let price: String
    let notes: [LiveDetailNote]
}

enum StationCardState: Equatable {
    case `default`
    case unknown
    case outOfOrder
    case occupied
    case oneFreeLeft
    case oftenBroken
    case oftenOccupied
}

func woladenStationCardState(
    status: AvailabilityStatus,
    counts: AvailabilityCounts,
    oftenBroken: Bool = false,
    oftenOccupied: Bool = false
) -> StationCardState {
    let hasAvailability = counts.total > 0
    if hasAvailability, status == .outOfOrder { return .outOfOrder }
    if hasAvailability, status == .occupied { return .occupied }
    if hasAvailability, counts.total > 1, counts.available == 1 { return .oneFreeLeft }
    if oftenBroken { return .oftenBroken }
    if oftenOccupied { return .oftenOccupied }
    if !hasAvailability || status == .unknown { return .unknown }
    return .default
}

func woladenAvailabilitySummary(_ counts: AvailabilityCounts) -> String? {
    guard counts.total > 0 else { return nil }

    var parts: [String] = []
    if counts.available > 0 {
        parts.append(localizedCount("availability.available", count: counts.available))
    }
    if counts.occupied > 0 {
        parts.append(localizedCount("availability.occupiedCount", count: counts.occupied))
    }
    if counts.outOfOrder > 0 {
        parts.append(localizedCount("availability.outOfOrderCount", count: counts.outOfOrder))
    }
    if counts.unknown > 0 {
        parts.append(localizedCount("availability.unknownCount", count: counts.unknown))
    }
    return parts.isEmpty ? String(localized: "availability.summaryUnknown") : parts.joined(separator: ", ")
}

extension GeoJSONFeature {
    var displayPrice: String {
        if let livePrice = liveSummaryForDisplay?.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines), !livePrice.isEmpty {
            return livePrice
        }

        if let liveDetail {
            let uniquePrices = Array(
                Set(
                    liveDetail.evses
                        .map { $0.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines) }
                        .filter { !$0.isEmpty }
                )
            )
            if let first = uniquePrices.first {
                return first
            }
        }

        return properties.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    var availabilityCounts: AvailabilityCounts {
        if let liveSummary = liveSummaryForDisplay {
            return AvailabilityCounts(
                total: liveSummary.totalEVSEs,
                available: liveSummary.availableEVSEs,
                occupied: liveSummary.occupiedEVSEs,
                outOfOrder: liveSummary.outOfOrderEVSEs,
                unknown: liveSummary.unknownEVSEs
            )
        }

        return AvailabilityCounts(
            total: properties.occupancyTotalEVSEs,
            available: properties.occupancyAvailableEVSEs,
            occupied: properties.occupancyOccupiedEVSEs,
            outOfOrder: properties.occupancyOutOfOrderEVSEs,
            unknown: properties.occupancyUnknownEVSEs
        )
    }

    var availabilityStatus: AvailabilityStatus {
        if let liveSummary = liveSummaryForDisplay {
            return liveSummary.availabilityStatus
        }

        let counts = availabilityCounts
        if counts.available > 0 {
            return .free
        }
        if counts.occupied > 0 {
            return .occupied
        }
        if counts.total > 0, counts.outOfOrder >= counts.total {
            return .outOfOrder
        }
        return .unknown
    }

    var isOftenBrokenFromDailyAnalysis: Bool {
        liveSummaryForDisplay?.isOftenBrokenFromDailyAnalysis == true
    }

    var isOftenOccupiedFromDailyAnalysis: Bool {
        liveSummaryForDisplay?.isOftenOccupiedFromDailyAnalysis == true
    }

    var stationCardState: StationCardState {
        let counts = availabilityCounts
        return woladenStationCardState(
            status: availabilityStatus,
            counts: counts,
            oftenBroken: isOftenBrokenFromDailyAnalysis,
            oftenOccupied: isOftenOccupiedFromDailyAnalysis
        )
    }

    var occupancySummaryLabel: String? {
        woladenAvailabilitySummary(availabilityCounts)
    }

    var occupancySourceLabel: String? {
        if liveSummaryForDisplay != nil {
            let provider = liveSourceLabel
            let elapsed = formattedElapsedLiveTime(liveObservedTimestamp)
            if let provider, let elapsed {
                return String(localized: "station.liveViaUpdated")
                    .replacingOccurrences(of: "{source}", with: provider)
                    .replacingOccurrences(of: "{date}", with: elapsed)
            }
            if let provider {
                return String(localized: "station.liveVia")
                    .replacingOccurrences(of: "{source}", with: provider)
            }
            if let elapsed {
                return String(localized: "station.updated")
                    .replacingOccurrences(of: "{date}", with: elapsed)
            }
            return String(localized: "station.live")
        }

        let counts = availabilityCounts
        guard counts.total > 0 else { return nil }
        if properties.occupancySourceName.hasPrefix("Mobilithek") {
            return String(localized: "station.liveVia")
                .replacingOccurrences(of: "{source}", with: properties.occupancySourceName)
        }
        if properties.occupancySourceUID.hasPrefix("mobilithek_") {
            if properties.occupancySourceName.isEmpty {
                return String(localized: "station.liveVia")
                    .replacingOccurrences(of: "{source}", with: "Mobilithek")
            }
            return String(localized: "station.liveVia")
                .replacingOccurrences(of: "{source}", with: "Mobilithek (\(properties.occupancySourceName))")
        }
        if properties.occupancySourceName.isEmpty {
            return String(localized: "station.liveVia")
                .replacingOccurrences(of: "{source}", with: "MobiData BW")
        }
        return String(localized: "station.liveVia")
            .replacingOccurrences(of: "{source}", with: "MobiData BW (\(properties.occupancySourceName))")
    }

    var liveUpdatedLabel: String? {
        guard liveSummaryForDisplay != nil else { return nil }
        guard let elapsed = formattedElapsedLiveTime(liveObservedTimestamp) else { return nil }
        return String(localized: "station.updated")
            .replacingOccurrences(of: "{date}", with: elapsed)
    }

    var hasPrimaryDetailHighlights: Bool {
        !displayPrice.isEmpty || !properties.openingHoursDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    var liveEVSERows: [LiveEVSERow] {
        if let liveDetail, !liveDetail.evses.isEmpty {
            return liveDetail.evses.enumerated().map { index, evse in
                let observedText = formattedElapsedLiveTime(
                    firstNonEmpty(evse.sourceObservedAt, evse.fetchedAt, evse.ingestedAt)
                )
                let meta = [formattedEVSECode(evse.providerEVSEID), observedText]
                    .compactMap { value in
                        guard let value, !value.isEmpty else { return nil }
                        if value == observedText {
                            return String(localized: "station.updated")
                                .replacingOccurrences(of: "{date}", with: value)
                        }
                        return value
                    }
                    .joined(separator: " • ")
                return LiveEVSERow(
                    title: String(localized: "station.evse")
                        .replacingOccurrences(of: "{index}", with: "\(index + 1)"),
                    status: evse.availabilityStatus,
                    meta: meta.isEmpty ? String(localized: "station.liveDataAvailable") : meta,
                    price: evse.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines),
                    notes: buildLiveNotes(for: evse)
                )
            }
        }

        guard liveSummaryForDisplay != nil else { return [] }
        return [
            LiveEVSERow(
                title: String(localized: "station.stationStatus"),
                status: availabilityStatus,
                meta: occupancySummaryLabel ?? String(localized: "station.liveDataAvailable"),
                price: displayPrice,
                notes: []
            )
        ]
    }

    private var liveSummaryForDisplay: LiveStationSummary? {
        liveDetail?.station ?? liveSummary
    }

    private var liveObservedTimestamp: String {
        firstNonEmpty(
            liveSummaryForDisplay?.sourceObservedAt ?? "",
            liveSummaryForDisplay?.fetchedAt ?? "",
            liveSummaryForDisplay?.ingestedAt ?? ""
        )
    }

    private var liveSourceLabel: String? {
        let raw = firstNonEmpty(properties.detailSourceName, properties.detailSourceUID)
        let value = formattedProviderLabel(raw)
        return value.isEmpty ? nil : value
    }

    private func buildLiveNotes(for evse: LiveEVSE) -> [LiveDetailNote] {
        var notes: [LiveDetailNote] = []
        let nextSlot = formatLiveCollection(evse.nextAvailableChargingSlots)
        if !nextSlot.isEmpty {
            notes.append(LiveDetailNote(label: String(localized: "station.nextSlot"), value: nextSlot))
        }
        let supplemental = formatLiveCollection(evse.supplementalFacilityStatus)
        if !supplemental.isEmpty {
            notes.append(LiveDetailNote(label: String(localized: "station.supplementalStatus"), value: supplemental))
        }
        return notes
    }
}

private func formattedProviderLabel(_ value: String) -> String {
    value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "^mobilithek_", with: "", options: .regularExpression)
        .replacingOccurrences(of: "_static$", with: "", options: .regularExpression)
        .replacingOccurrences(of: "-json$", with: "", options: .regularExpression)
        .replacingOccurrences(of: "_", with: " ")
}

private func localizedCount(_ key: String, count: Int) -> String {
    let template: String
    switch key {
    case "availability.available":
        template = String(localized: "availability.available")
    case "availability.occupiedCount":
        template = String(localized: "availability.occupiedCount")
    case "availability.outOfOrderCount":
        template = String(localized: "availability.outOfOrderCount")
    case "availability.unknownCount":
        template = String(localized: "availability.unknownCount")
    default:
        template = "{count}"
    }
    return template.replacingOccurrences(of: "{count}", with: "\(count)")
}

private func localizedElapsed(seconds: Int) -> String {
    let formatter = DateComponentsFormatter()
    formatter.unitsStyle = .abbreviated
    formatter.maximumUnitCount = 1
    formatter.allowedUnits = seconds < 60
        ? [.second]
        : seconds < 3_600
            ? [.minute]
            : seconds < 86_400
                ? [.hour]
                : seconds < 2_592_000
                    ? [.day]
                    : seconds < 31_536_000
                        ? [.month]
                        : [.year]
    return formatter.string(from: TimeInterval(seconds)) ?? "\(seconds) s"
}

func formattedElapsedLiveTime(_ value: String, now: Date = Date()) -> String? {
    let raw = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !raw.isEmpty else { return nil }
    guard let date = parsedLiveDate(raw) else { return nil }

    let elapsedSeconds = max(0, Int(now.timeIntervalSince(date)))
    if elapsedSeconds < 60 {
        return localizedElapsed(seconds: elapsedSeconds)
    }

    let elapsedMinutes = elapsedSeconds / 60
    if elapsedMinutes < 60 {
        return localizedElapsed(seconds: elapsedSeconds)
    }

    let elapsedHours = elapsedMinutes / 60
    if elapsedHours < 24 {
        return localizedElapsed(seconds: elapsedSeconds)
    }

    let elapsedDays = elapsedHours / 24
    if elapsedDays < 30 {
        return localizedElapsed(seconds: elapsedSeconds)
    }

    let elapsedMonths = elapsedDays / 30
    if elapsedMonths < 12 {
        return localizedElapsed(seconds: elapsedSeconds)
    }

    let elapsedYears = elapsedDays / 365
    return localizedElapsed(seconds: elapsedYears * 365 * 24 * 60 * 60)
}

private func formattedLiveTimestamp(_ value: String) -> String? {
    let raw = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !raw.isEmpty else { return nil }
    guard let date = parsedLiveDate(raw) else { return raw }

    let output = DateFormatter()
    output.locale = Locale.current
    output.dateStyle = .short
    output.timeStyle = .short
    return output.string(from: date)
}

private func parsedLiveDate(_ value: String) -> Date? {
    let preciseFormatter = ISO8601DateFormatter()
    preciseFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    let fallbackFormatter = ISO8601DateFormatter()
    fallbackFormatter.formatOptions = [.withInternetDateTime]
    return preciseFormatter.date(from: value) ?? fallbackFormatter.date(from: value)
}

private func formattedEVSECode(_ value: String) -> String? {
    let raw = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !raw.isEmpty else { return nil }
    if raw.count <= 20 {
        return raw
    }
    return "\(raw.prefix(10))…\(raw.suffix(6))"
}

private func formatLiveCollection(_ values: [LiveJSONValue]) -> String {
    values
        .map(formatLiveValue)
        .filter { !$0.isEmpty }
        .joined(separator: " • ")
}

private func formatLiveValue(_ value: LiveJSONValue) -> String {
    switch value {
    case .null:
        return ""
    case .bool(let flag):
        return flag ? String(localized: "common.yes") : String(localized: "common.no")
    case .number(let number):
        if number.rounded() == number {
            return String(Int(number))
        }
        return String(number)
    case .string(let text):
        let raw = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !raw.isEmpty else { return "" }
        if let timestamp = formattedLiveTimestamp(raw), timestamp != raw {
            return timestamp
        }
        return humanizedLiveCode(raw)
    case .array(let items):
        return items.map(formatLiveValue).filter { !$0.isEmpty }.joined(separator: " • ")
    case .object(let values):
        let entries = values
            .filter { key, value in
                if key.isEmpty {
                    return false
                }
                return !formatLiveValue(value).isEmpty
            }
            .sorted { $0.key < $1.key }

        if entries.count == 1, entries[0].key == "value" {
            return formatLiveValue(entries[0].value)
        }

        return entries
            .compactMap { key, value in
                let formattedValue = formatLiveValue(value)
                guard !formattedValue.isEmpty else { return nil }
                let label = liveDynamicKeyLabels[key] ?? humanizedLiveCode(key)
                if label.isEmpty {
                    return formattedValue
                }
                return "\(label): \(formattedValue)"
            }
            .joined(separator: ", ")
    }
}

private func humanizedLiveCode(_ value: String) -> String {
    let spaced = value
        .replacingOccurrences(of: "([a-z0-9])([A-Z])", with: "$1 $2", options: .regularExpression)
        .replacingOccurrences(of: "_", with: " ")
        .replacingOccurrences(of: "-", with: " ")
        .trimmingCharacters(in: .whitespacesAndNewlines)
    guard let first = spaced.first else { return "" }
    return String(first).uppercased() + spaced.dropFirst()
}

private func firstNonEmpty(_ values: String?...) -> String {
    for value in values {
        let raw = (value ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        if !raw.isEmpty {
            return raw
        }
    }
    return ""
}

extension LiveStationSummary {
    var isOftenBrokenFromDailyAnalysis: Bool {
        frequentlyOutOfOrderDailyAnalysis ||
        ["sehr_hellrot", "hellrot"].contains(normalizedDailyAnalysisColor(dailyAnalysisOutOfOrderColor))
    }

    var isOftenOccupiedFromDailyAnalysis: Bool {
        frequentlyOccupiedDailyAnalysis ||
        normalizedDailyAnalysisColor(dailyAnalysisOccupiedColor) == "hellgrau"
    }
}

private func normalizedDailyAnalysisColor(_ value: String) -> String {
    value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .lowercased()
        .replacingOccurrences(of: #"[\s-]+"#, with: "_", options: .regularExpression)
}

private extension KeyedDecodingContainer {
    func decodeLossyString(forKey key: Key) -> String {
        if let value = try? decode(String.self, forKey: key) {
            return value
        }
        if let value = try? decode(Double.self, forKey: key) {
            return String(value)
        }
        if let value = try? decode(Int.self, forKey: key) {
            return String(value)
        }
        if let value = try? decode(Bool.self, forKey: key) {
            return value ? "true" : "false"
        }
        return ""
    }

    func decodeLossyInt(forKey key: Key) -> Int? {
        if let value = try? decode(Int.self, forKey: key) {
            return value
        }
        if let value = try? decode(Double.self, forKey: key) {
            return Int(value)
        }
        if let value = try? decode(String.self, forKey: key) {
            let normalized = value.replacingOccurrences(of: ",", with: ".")
            if let integer = Int(normalized) {
                return integer
            }
            if let double = Double(normalized) {
                return Int(double)
            }
        }
        return nil
    }

    func decodeLossyBool(forKey key: Key) -> Bool? {
        if let value = try? decode(Bool.self, forKey: key) {
            return value
        }
        if let value = try? decode(Int.self, forKey: key) {
            return value != 0
        }
        if let value = try? decode(String.self, forKey: key) {
            switch value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased() {
            case "1", "true", "yes", "y", "ja":
                return true
            case "0", "false", "no", "n", "nein":
                return false
            default:
                return nil
            }
        }
        return nil
    }
}
