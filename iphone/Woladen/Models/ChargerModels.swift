import Foundation
import CoreLocation

private let maxReasonableDisplayPowerKW = 400.0

struct GeoJSONFeatureCollection: Decodable {
    let generatedAt: String?
    let features: [GeoJSONFeature]

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case features
    }
}

struct GeoJSONFeature: Decodable, Identifiable {
    let id: String
    let geometry: GeoJSONPointGeometry
    let properties: ChargerProperties

    enum CodingKeys: String, CodingKey {
        case geometry
        case properties
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        geometry = try container.decode(GeoJSONPointGeometry.self, forKey: .geometry)
        properties = try container.decode(ChargerProperties.self, forKey: .properties)
        id = properties.stationID
    }

    var coordinate: CLLocationCoordinate2D {
        geometry.coordinate
    }
}

struct GeoJSONPointGeometry: Decodable {
    let type: String
    let coordinates: [Double]

    var coordinate: CLLocationCoordinate2D {
        guard coordinates.count == 2 else {
            return CLLocationCoordinate2D(latitude: 0, longitude: 0)
        }
        return CLLocationCoordinate2D(latitude: coordinates[1], longitude: coordinates[0])
    }
}

struct ChargerProperties: Decodable {
    let stationID: String
    let operatorName: String
    let status: String
    let maxPowerKW: Double
    let chargingPointsCount: Int
    let maxIndividualPowerKW: Double
    let postcode: String
    let city: String
    let address: String
    let occupancySourceUID: String
    let occupancySourceName: String
    let occupancyStatus: String
    let occupancyLastUpdated: String
    let occupancyTotalEVSEs: Int
    let occupancyAvailableEVSEs: Int
    let occupancyOccupiedEVSEs: Int
    let occupancyChargingEVSEs: Int
    let occupancyOutOfOrderEVSEs: Int
    let occupancyUnknownEVSEs: Int
    let detailSourceUID: String
    let detailSourceName: String
    let detailLastUpdated: String
    let datexSiteID: String
    let datexStationIDs: String
    let datexChargePointIDs: String
    let priceDisplay: String
    let priceEnergyEURKwhMin: String
    let priceEnergyEURKwhMax: String
    let priceCurrency: String
    let priceQuality: String
    let openingHoursDisplay: String
    let openingHoursIs24_7: Bool
    let helpdeskPhone: String
    let paymentMethodsDisplay: String
    let authMethodsDisplay: String
    let connectorTypesDisplay: String
    let currentTypesDisplay: String
    let connectorCount: Int
    let greenEnergy: Bool?
    let serviceTypesDisplay: String
    let detailsJSON: String
    let amenitiesTotal: Int
    let amenitiesSource: String
    let amenityExamples: [AmenityExample]
    let amenityCounts: [String: Int]

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case operatorName = "operator"
        case status
        case maxPowerKW = "max_power_kw"
        case chargingPointsCount = "charging_points_count"
        case maxIndividualPowerKW = "max_individual_power_kw"
        case postcode
        case city
        case address
        case occupancySourceUID = "occupancy_source_uid"
        case occupancySourceName = "occupancy_source_name"
        case occupancyStatus = "occupancy_status"
        case occupancyLastUpdated = "occupancy_last_updated"
        case occupancyTotalEVSEs = "occupancy_total_evses"
        case occupancyAvailableEVSEs = "occupancy_available_evses"
        case occupancyOccupiedEVSEs = "occupancy_occupied_evses"
        case occupancyChargingEVSEs = "occupancy_charging_evses"
        case occupancyOutOfOrderEVSEs = "occupancy_out_of_order_evses"
        case occupancyUnknownEVSEs = "occupancy_unknown_evses"
        case detailSourceUID = "detail_source_uid"
        case detailSourceName = "detail_source_name"
        case detailLastUpdated = "detail_last_updated"
        case datexSiteID = "datex_site_id"
        case datexStationIDs = "datex_station_ids"
        case datexChargePointIDs = "datex_charge_point_ids"
        case priceDisplay = "price_display"
        case priceEnergyEURKwhMin = "price_energy_eur_kwh_min"
        case priceEnergyEURKwhMax = "price_energy_eur_kwh_max"
        case priceCurrency = "price_currency"
        case priceQuality = "price_quality"
        case openingHoursDisplay = "opening_hours_display"
        case openingHoursIs24_7 = "opening_hours_is_24_7"
        case helpdeskPhone = "helpdesk_phone"
        case paymentMethodsDisplay = "payment_methods_display"
        case authMethodsDisplay = "auth_methods_display"
        case connectorTypesDisplay = "connector_types_display"
        case currentTypesDisplay = "current_types_display"
        case connectorCount = "connector_count"
        case greenEnergy = "green_energy"
        case serviceTypesDisplay = "service_types_display"
        case detailsJSON = "details_json"
        case amenitiesTotal = "amenities_total"
        case amenitiesSource = "amenities_source"
        case amenityExamples = "amenity_examples"
    }

    init(
        stationID: String,
        operatorName: String,
        status: String,
        maxPowerKW: Double,
        chargingPointsCount: Int,
        maxIndividualPowerKW: Double,
        postcode: String,
        city: String,
        address: String,
        occupancySourceUID: String,
        occupancySourceName: String,
        occupancyStatus: String,
        occupancyLastUpdated: String,
        occupancyTotalEVSEs: Int,
        occupancyAvailableEVSEs: Int,
        occupancyOccupiedEVSEs: Int,
        occupancyChargingEVSEs: Int,
        occupancyOutOfOrderEVSEs: Int,
        occupancyUnknownEVSEs: Int,
        detailSourceUID: String,
        detailSourceName: String,
        detailLastUpdated: String,
        datexSiteID: String,
        datexStationIDs: String,
        datexChargePointIDs: String,
        priceDisplay: String,
        priceEnergyEURKwhMin: String,
        priceEnergyEURKwhMax: String,
        priceCurrency: String,
        priceQuality: String,
        openingHoursDisplay: String,
        openingHoursIs24_7: Bool,
        helpdeskPhone: String,
        paymentMethodsDisplay: String,
        authMethodsDisplay: String,
        connectorTypesDisplay: String,
        currentTypesDisplay: String,
        connectorCount: Int,
        greenEnergy: Bool?,
        serviceTypesDisplay: String,
        detailsJSON: String,
        amenitiesTotal: Int,
        amenitiesSource: String,
        amenityExamples: [AmenityExample],
        amenityCounts: [String: Int]
    ) {
        self.stationID = stationID
        self.operatorName = operatorName
        self.status = status
        self.maxPowerKW = maxPowerKW
        self.chargingPointsCount = chargingPointsCount
        self.maxIndividualPowerKW = maxIndividualPowerKW
        self.postcode = postcode
        self.city = city
        self.address = address
        self.occupancySourceUID = occupancySourceUID
        self.occupancySourceName = occupancySourceName
        self.occupancyStatus = occupancyStatus
        self.occupancyLastUpdated = occupancyLastUpdated
        self.occupancyTotalEVSEs = occupancyTotalEVSEs
        self.occupancyAvailableEVSEs = occupancyAvailableEVSEs
        self.occupancyOccupiedEVSEs = occupancyOccupiedEVSEs
        self.occupancyChargingEVSEs = occupancyChargingEVSEs
        self.occupancyOutOfOrderEVSEs = occupancyOutOfOrderEVSEs
        self.occupancyUnknownEVSEs = occupancyUnknownEVSEs
        self.detailSourceUID = detailSourceUID
        self.detailSourceName = detailSourceName
        self.detailLastUpdated = detailLastUpdated
        self.datexSiteID = datexSiteID
        self.datexStationIDs = datexStationIDs
        self.datexChargePointIDs = datexChargePointIDs
        self.priceDisplay = priceDisplay
        self.priceEnergyEURKwhMin = priceEnergyEURKwhMin
        self.priceEnergyEURKwhMax = priceEnergyEURKwhMax
        self.priceCurrency = priceCurrency
        self.priceQuality = priceQuality
        self.openingHoursDisplay = openingHoursDisplay
        self.openingHoursIs24_7 = openingHoursIs24_7
        self.helpdeskPhone = helpdeskPhone
        self.paymentMethodsDisplay = paymentMethodsDisplay
        self.authMethodsDisplay = authMethodsDisplay
        self.connectorTypesDisplay = connectorTypesDisplay
        self.currentTypesDisplay = currentTypesDisplay
        self.connectorCount = connectorCount
        self.greenEnergy = greenEnergy
        self.serviceTypesDisplay = serviceTypesDisplay
        self.detailsJSON = detailsJSON
        self.amenitiesTotal = amenitiesTotal
        self.amenitiesSource = amenitiesSource
        self.amenityExamples = amenityExamples
        self.amenityCounts = amenityCounts
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)

        stationID = try container.decode(String.self, forKey: .stationID)
        operatorName = try container.decode(String.self, forKey: .operatorName)
        status = (try? container.decode(String.self, forKey: .status)) ?? ""
        maxPowerKW = container.decodeLossyDouble(forKey: .maxPowerKW) ?? 0
        chargingPointsCount = Int(container.decodeLossyDouble(forKey: .chargingPointsCount) ?? 1)
        maxIndividualPowerKW = container.decodeLossyDouble(forKey: .maxIndividualPowerKW) ?? maxPowerKW
        postcode = (try? container.decode(String.self, forKey: .postcode)) ?? ""
        city = (try? container.decode(String.self, forKey: .city)) ?? ""
        address = (try? container.decode(String.self, forKey: .address)) ?? ""
        occupancySourceUID = (try? container.decode(String.self, forKey: .occupancySourceUID)) ?? ""
        occupancySourceName = (try? container.decode(String.self, forKey: .occupancySourceName)) ?? ""
        occupancyStatus = (try? container.decode(String.self, forKey: .occupancyStatus)) ?? ""
        occupancyLastUpdated = (try? container.decode(String.self, forKey: .occupancyLastUpdated)) ?? ""
        occupancyTotalEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyTotalEVSEs) ?? 0)
        occupancyAvailableEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyAvailableEVSEs) ?? 0)
        occupancyOccupiedEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyOccupiedEVSEs) ?? 0)
        occupancyChargingEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyChargingEVSEs) ?? 0)
        occupancyOutOfOrderEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyOutOfOrderEVSEs) ?? 0)
        occupancyUnknownEVSEs = Int(container.decodeLossyDouble(forKey: .occupancyUnknownEVSEs) ?? 0)
        detailSourceUID = (try? container.decode(String.self, forKey: .detailSourceUID)) ?? ""
        detailSourceName = (try? container.decode(String.self, forKey: .detailSourceName)) ?? ""
        detailLastUpdated = (try? container.decode(String.self, forKey: .detailLastUpdated)) ?? ""
        datexSiteID = (try? container.decode(String.self, forKey: .datexSiteID)) ?? ""
        datexStationIDs = (try? container.decode(String.self, forKey: .datexStationIDs)) ?? ""
        datexChargePointIDs = (try? container.decode(String.self, forKey: .datexChargePointIDs)) ?? ""
        priceDisplay = (try? container.decode(String.self, forKey: .priceDisplay)) ?? ""
        priceEnergyEURKwhMin = container.decodeLossyString(forKey: .priceEnergyEURKwhMin)
        priceEnergyEURKwhMax = container.decodeLossyString(forKey: .priceEnergyEURKwhMax)
        priceCurrency = (try? container.decode(String.self, forKey: .priceCurrency)) ?? ""
        priceQuality = (try? container.decode(String.self, forKey: .priceQuality)) ?? ""
        openingHoursDisplay = (try? container.decode(String.self, forKey: .openingHoursDisplay)) ?? ""
        openingHoursIs24_7 = (try? container.decode(Bool.self, forKey: .openingHoursIs24_7))
            ?? ((container.decodeLossyDouble(forKey: .openingHoursIs24_7) ?? 0) > 0)
        helpdeskPhone = (try? container.decode(String.self, forKey: .helpdeskPhone)) ?? ""
        paymentMethodsDisplay = (try? container.decode(String.self, forKey: .paymentMethodsDisplay)) ?? ""
        authMethodsDisplay = (try? container.decode(String.self, forKey: .authMethodsDisplay)) ?? ""
        connectorTypesDisplay = (try? container.decode(String.self, forKey: .connectorTypesDisplay)) ?? ""
        currentTypesDisplay = (try? container.decode(String.self, forKey: .currentTypesDisplay)) ?? ""
        connectorCount = Int(container.decodeLossyDouble(forKey: .connectorCount) ?? 0)
        if let boolValue = try? container.decode(Bool.self, forKey: .greenEnergy) {
            greenEnergy = boolValue
        } else if let stringValue = try? container.decode(String.self, forKey: .greenEnergy) {
            let normalized = stringValue.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
            if normalized.isEmpty {
                greenEnergy = nil
            } else if ["true", "yes", "ja", "1"].contains(normalized) {
                greenEnergy = true
            } else if ["false", "no", "nein", "0"].contains(normalized) {
                greenEnergy = false
            } else {
                greenEnergy = nil
            }
        } else {
            greenEnergy = nil
        }
        serviceTypesDisplay = (try? container.decode(String.self, forKey: .serviceTypesDisplay)) ?? ""
        detailsJSON = (try? container.decode(String.self, forKey: .detailsJSON)) ?? ""
        amenitiesTotal = Int(container.decodeLossyDouble(forKey: .amenitiesTotal) ?? 0)
        amenitiesSource = (try? container.decode(String.self, forKey: .amenitiesSource)) ?? ""
        amenityExamples = (try? container.decode([AmenityExample].self, forKey: .amenityExamples)) ?? []

        let raw = try decoder.container(keyedBy: AnyCodingKey.self)
        var collected: [String: Int] = [:]
        for key in raw.allKeys where key.stringValue.hasPrefix("amenity_") {
            let value: Int
            if let intValue = try? raw.decode(Int.self, forKey: key) {
                value = intValue
            } else if let doubleValue = try? raw.decode(Double.self, forKey: key) {
                value = Int(doubleValue)
            } else if let stringValue = try? raw.decode(String.self, forKey: key) {
                value = Int(stringValue) ?? 0
            } else {
                value = 0
            }
            collected[key.stringValue] = value
        }
        amenityCounts = collected
    }
}

struct AmenityExample: Decodable, Identifiable {
    let id = UUID()
    let category: String
    let name: String?
    let openingHours: String?
    let distanceM: Double?
    let lat: Double?
    let lon: Double?

    enum CodingKeys: String, CodingKey {
        case category
        case name
        case openingHours = "opening_hours"
        case distanceM = "distance_m"
        case lat
        case lon
    }

    var coordinate: CLLocationCoordinate2D? {
        guard let lat, let lon else { return nil }
        return CLLocationCoordinate2D(latitude: lat, longitude: lon)
    }
}

extension ChargerProperties {
    var displayedMaxPowerKW: Double {
        let maxIndividual = sanitizedDisplayPower(maxIndividualPowerKW)
        if maxIndividual > 0 {
            return maxIndividual
        }
        return sanitizedDisplayPower(maxPowerKW)
    }

    func topAmenities(limit: Int = 3) -> [AmenityCount] {
        amenityCounts
            .filter { $0.value > 0 }
            .map { AmenityCount(key: $0.key, count: $0.value) }
            .sorted { lhs, rhs in
                if lhs.count == rhs.count { return lhs.key < rhs.key }
                return lhs.count > rhs.count
            }
            .prefix(limit)
            .map { $0 }
    }

    var occupancySummaryLabel: String? {
        guard occupancyTotalEVSEs > 0 else { return nil }
        let knownEVSEs = max(0, occupancyTotalEVSEs - occupancyUnknownEVSEs)
        if knownEVSEs > 0, occupancyUnknownEVSEs > 0 {
            var parts: [String] = []
            if occupancyAvailableEVSEs > 0 {
                parts.append("\(occupancyAvailableEVSEs) frei")
            }
            if occupancyOccupiedEVSEs > 0 {
                parts.append("\(occupancyOccupiedEVSEs) belegt")
            }
            if occupancyOutOfOrderEVSEs > 0 {
                parts.append("\(occupancyOutOfOrderEVSEs) defekt")
            }
            parts.append("\(occupancyUnknownEVSEs) unbekannt")
            return parts.joined(separator: ", ")
        }
        if occupancyAvailableEVSEs > 0 {
            return "\(occupancyAvailableEVSEs)/\(occupancyTotalEVSEs) frei"
        }
        if occupancyOccupiedEVSEs > 0 {
            return "\(occupancyOccupiedEVSEs)/\(occupancyTotalEVSEs) belegt"
        }
        if occupancyUnknownEVSEs <= 0, occupancyOutOfOrderEVSEs >= occupancyTotalEVSEs {
            return "Außer Betrieb"
        }
        return "Belegung unbekannt"
    }

    var occupancySourceLabel: String? {
        guard occupancyTotalEVSEs > 0 else { return nil }
        if occupancySourceName.hasPrefix("Mobilithek") {
            return "Live via \(occupancySourceName)"
        }
        if occupancySourceUID.hasPrefix("mobilithek_") {
            if occupancySourceName.isEmpty {
                return "Live via Mobilithek"
            }
            return "Live via Mobilithek (\(occupancySourceName))"
        }
        if occupancySourceName.isEmpty {
            return "Live via MobiData BW"
        }
        return "Live via MobiData BW (\(occupancySourceName))"
    }

    var hasPrimaryDetailHighlights: Bool {
        !priceDisplay.isEmpty || !openingHoursDisplay.isEmpty
    }

    var staticDetailRows: [DetailRow] {
        var rows: [DetailRow] = []
        if !paymentMethodsDisplay.isEmpty {
            rows.append(.init(label: "Bezahlen", value: paymentMethodsDisplay))
        }
        if !authMethodsDisplay.isEmpty {
            rows.append(.init(label: "Zugang", value: authMethodsDisplay))
        }
        if !connectorTypesDisplay.isEmpty {
            rows.append(.init(label: "Stecker", value: connectorTypesDisplay))
        }
        if !currentTypesDisplay.isEmpty {
            rows.append(.init(label: "Stromart", value: currentTypesDisplay))
        }
        if connectorCount > 0 {
            rows.append(.init(label: "Anschlüsse", value: "\(connectorCount) Steckplätze"))
        }
        if !serviceTypesDisplay.isEmpty {
            rows.append(.init(label: "Service", value: serviceTypesDisplay))
        }
        if let greenEnergy {
            rows.append(.init(label: "Strom", value: greenEnergy ? "100 % erneuerbar" : "Nicht als erneuerbar markiert"))
        }
        return rows
    }

    var detailSourceLabel: String? {
        let sourceName = detailSourceName.trimmingCharacters(in: .whitespacesAndNewlines)
        let timestamp = formattedDetailTimestamp(detailLastUpdated)
        if sourceName.isEmpty && timestamp == nil {
            return nil
        }
        if let timestamp {
            if sourceName.isEmpty {
                return "Stand \(timestamp)"
            }
            return "Details via \(sourceName) • Stand \(timestamp)"
        }
        return "Details via \(sourceName)"
    }
}

private func sanitizedDisplayPower(_ value: Double) -> Double {
    guard value.isFinite, value > 0 else { return 0 }
    return min(value, maxReasonableDisplayPowerKW)
}

private func formattedDetailTimestamp(_ value: String) -> String? {
    guard !value.isEmpty else { return nil }
    let formatter = ISO8601DateFormatter()
    formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    let fallbackFormatter = ISO8601DateFormatter()
    fallbackFormatter.formatOptions = [.withInternetDateTime]

    let date = formatter.date(from: value) ?? fallbackFormatter.date(from: value)
    guard let date else { return value }

    let output = DateFormatter()
    output.locale = Locale(identifier: "de_DE")
    output.dateStyle = .short
    output.timeStyle = .short
    return output.string(from: date)
}

struct AmenityCount: Identifiable {
    var id: String { key }
    let key: String
    let count: Int
}

struct DetailRow: Identifiable {
    var id: String { label }
    let label: String
    let value: String
}

struct AnyCodingKey: CodingKey {
    let stringValue: String
    let intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
}

extension KeyedDecodingContainer {
    fileprivate func decodeLossyString(forKey key: Key) -> String {
        if let value = try? decode(String.self, forKey: key) {
            return value
        }
        if let value = try? decode(Double.self, forKey: key) {
            return String(value)
        }
        if let value = try? decode(Int.self, forKey: key) {
            return String(value)
        }
        return ""
    }

    fileprivate func decodeLossyDouble(forKey key: Key) -> Double? {
        if let value = try? decode(Double.self, forKey: key) {
            return value
        }
        if let value = try? decode(Int.self, forKey: key) {
            return Double(value)
        }
        if let string = try? decode(String.self, forKey: key) {
            let normalized = string.replacingOccurrences(of: ",", with: ".")
            return Double(normalized)
        }
        return nil
    }
}
