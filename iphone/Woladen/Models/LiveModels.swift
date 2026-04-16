import Foundation

private let liveDynamicKeyLabels: [String: String] = [
    "expectedAvailableFromTime": "Ab",
    "expectedAvailableToTime": "Bis",
    "expectedAvailableUntilTime": "Bis",
    "startTime": "Ab",
    "endTime": "Bis",
    "lastUpdated": "Seit",
    "value": ""
]

struct LiveStationLookupResponse: Decodable {
    let stations: [LiveStationSummary]
    let missingStationIDs: [String]

    enum CodingKeys: String, CodingKey {
        case stations
        case missingStationIDs = "missing_station_ids"
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
        ingestedAt: String
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
        case "free":
            self = .free
        case "occupied":
            self = .occupied
        case "out_of_order":
            self = .outOfOrder
        case "unknown":
            self = .unknown
        default:
            return nil
        }
    }

    var label: String {
        switch self {
        case .free:
            return "Frei"
        case .occupied:
            return "Belegt"
        case .outOfOrder:
            return "Defekt"
        case .unknown:
            return "Unbekannt"
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

    var occupancySummaryLabel: String? {
        let counts = availabilityCounts
        guard counts.total > 0 else { return nil }

        var parts: [String] = []
        if counts.available > 0 {
            parts.append("\(counts.available) frei")
        }
        if counts.occupied > 0 {
            parts.append("\(counts.occupied) belegt")
        }
        if counts.outOfOrder > 0 {
            parts.append("\(counts.outOfOrder) defekt")
        }
        if counts.unknown > 0 {
            parts.append("\(counts.unknown) unbekannt")
        }
        return parts.isEmpty ? "Belegung unbekannt" : parts.joined(separator: ", ")
    }

    var occupancySourceLabel: String? {
        if liveSummaryForDisplay != nil {
            let provider = liveSourceLabel
            let timestamp = formattedLiveTimestamp(liveObservedTimestamp)
            if let provider, let timestamp {
                return "Live via \(provider) • Stand \(timestamp)"
            }
            if let provider {
                return "Live via \(provider)"
            }
            if let timestamp {
                return "Live-Stand \(timestamp)"
            }
            return "Live via lokaler API"
        }

        let counts = availabilityCounts
        guard counts.total > 0 else { return nil }
        if properties.occupancySourceName.hasPrefix("Mobilithek") {
            return "Live via \(properties.occupancySourceName)"
        }
        if properties.occupancySourceUID.hasPrefix("mobilithek_") {
            if properties.occupancySourceName.isEmpty {
                return "Live via Mobilithek"
            }
            return "Live via Mobilithek (\(properties.occupancySourceName))"
        }
        if properties.occupancySourceName.isEmpty {
            return "Live via MobiData BW"
        }
        return "Live via MobiData BW (\(properties.occupancySourceName))"
    }

    var liveUpdatedLabel: String? {
        guard liveSummaryForDisplay != nil else { return nil }
        guard let timestamp = formattedLiveTimestamp(liveObservedTimestamp) else { return nil }
        return "Stand \(timestamp)"
    }

    var hasPrimaryDetailHighlights: Bool {
        !displayPrice.isEmpty || !properties.openingHoursDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }

    var liveEVSERows: [LiveEVSERow] {
        if let liveDetail, !liveDetail.evses.isEmpty {
            return liveDetail.evses.enumerated().map { index, evse in
                let observedText = formattedLiveTimestamp(
                    firstNonEmpty(evse.sourceObservedAt, evse.fetchedAt, evse.ingestedAt)
                )
                let meta = [formattedEVSECode(evse.providerEVSEID), observedText]
                    .compactMap { value in
                        guard let value, !value.isEmpty else { return nil }
                        if value == observedText {
                            return "Seit \(value)"
                        }
                        return value
                    }
                    .joined(separator: " • ")
                return LiveEVSERow(
                    title: "Ladepunkt \(index + 1)",
                    status: evse.availabilityStatus,
                    meta: meta.isEmpty ? "Live-Daten verfügbar" : meta,
                    price: evse.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines),
                    notes: buildLiveNotes(for: evse)
                )
            }
        }

        guard liveSummaryForDisplay != nil else { return [] }
        return [
            LiveEVSERow(
                title: "Stationsstatus",
                status: availabilityStatus,
                meta: occupancySummaryLabel ?? "Live-Daten verfügbar",
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
            notes.append(LiveDetailNote(label: "Nächster Slot", value: nextSlot))
        }
        let supplemental = formatLiveCollection(evse.supplementalFacilityStatus)
        if !supplemental.isEmpty {
            notes.append(LiveDetailNote(label: "Zusatzstatus", value: supplemental))
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

private func formattedLiveTimestamp(_ value: String) -> String? {
    let raw = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !raw.isEmpty else { return nil }

    let preciseFormatter = ISO8601DateFormatter()
    preciseFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    let fallbackFormatter = ISO8601DateFormatter()
    fallbackFormatter.formatOptions = [.withInternetDateTime]

    let date = preciseFormatter.date(from: raw) ?? fallbackFormatter.date(from: raw)
    guard let date else { return raw }

    let output = DateFormatter()
    output.locale = Locale(identifier: "de_DE")
    output.dateStyle = .short
    output.timeStyle = .short
    return output.string(from: date)
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
        return flag ? "Ja" : "Nein"
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
}
