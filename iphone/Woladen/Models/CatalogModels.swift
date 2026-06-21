import Foundation
import CoreLocation

private let catalogDetailSourceName = "woladen live-eu catalog"

struct CatalogSearchResponse: Decodable {
    let stations: [CatalogStation]
}

struct CatalogStationDetailResponse: Decodable {
    let station: CatalogStation
    let chargers: [CatalogCharger]
    let amenities: CatalogAmenities?

    func feature(preserving existing: GeoJSONFeature?) -> GeoJSONFeature {
        station.feature(chargers: chargers, amenities: amenities, preserving: existing)
    }
}

struct CatalogStation: Decodable {
    let stationID: String
    let countryCode: String
    let sourceUID: String
    let sourceStationID: String
    let license: String
    let providerUID: String
    let operatorName: String
    let stationName: String
    let address: String
    let postalCode: String
    let city: String
    let latitude: Double
    let longitude: Double
    let chargerCount: Int
    let maxPowerKW: Double
    let connectorTypes: String
    let sourceURL: String
    let publicBundleStatus: String
    let openingHours: String
    let paymentMethods: String
    let authMethods: String
    let greenEnergy: Bool?
    let helpdeskPhone: String
    let priceDisplay: String
    let priceCurrency: String
    let priceEnergyEURKwhMin: String
    let priceEnergyEURKwhMax: String
    let detailLastUpdated: String
    let amenitiesTotal: Int
    let nearestAmenityKind: String
    let nearestAmenityName: String
    let nearestAmenityDistanceM: Double?
    let amenityCategoryCounts: [String: Int]
    let distanceM: Double?

    let availabilityStatus: AvailabilityStatus?
    let availableEVSEs: Int
    let occupiedEVSEs: Int
    let outOfOrderEVSEs: Int
    let unknownEVSEs: Int
    let totalEVSEs: Int
    let sourceObservedAt: String
    let fetchedAt: String
    let ingestedAt: String

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case countryCode = "country_code"
        case sourceUID = "source_uid"
        case sourceStationID = "source_station_id"
        case license
        case providerUID = "provider_uid"
        case operatorName = "operator_name"
        case stationName = "station_name"
        case address
        case postalCode = "postal_code"
        case city
        case latitude
        case longitude
        case chargerCount = "charger_count"
        case maxPowerKW = "max_power_kw"
        case connectorTypes = "connector_types"
        case sourceURL = "source_url"
        case publicBundleStatus = "public_bundle_status"
        case openingHours = "opening_hours"
        case paymentMethods = "payment_methods"
        case authMethods = "auth_methods"
        case greenEnergy = "green_energy"
        case helpdeskPhone = "helpdesk_phone"
        case priceDisplay = "price_display"
        case priceCurrency = "price_currency"
        case priceEnergyEURKwhMin = "price_energy_eur_kwh_min"
        case priceEnergyEURKwhMax = "price_energy_eur_kwh_max"
        case detailLastUpdated = "detail_last_updated"
        case amenitiesTotal = "amenities_total"
        case nearestAmenityKind = "nearest_amenity_kind"
        case nearestAmenityName = "nearest_amenity_name"
        case nearestAmenityDistanceM = "nearest_amenity_distance_m"
        case amenityCategoryCounts = "amenity_category_counts"
        case distanceM = "distance_m"
        case availabilityStatus = "availability_status"
        case availableEVSEs = "available_evses"
        case occupiedEVSEs = "occupied_evses"
        case outOfOrderEVSEs = "out_of_order_evses"
        case unknownEVSEs = "unknown_evses"
        case totalEVSEs = "total_evses"
        case sourceObservedAt = "source_observed_at"
        case fetchedAt = "fetched_at"
        case ingestedAt = "ingested_at"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        stationID = container.decodeLossyString(forKey: .stationID)
        countryCode = container.decodeLossyString(forKey: .countryCode)
        sourceUID = container.decodeLossyString(forKey: .sourceUID)
        sourceStationID = container.decodeLossyString(forKey: .sourceStationID)
        license = container.decodeLossyString(forKey: .license)
        providerUID = container.decodeLossyString(forKey: .providerUID)
        operatorName = container.decodeLossyString(forKey: .operatorName)
        stationName = container.decodeLossyString(forKey: .stationName)
        address = container.decodeLossyString(forKey: .address)
        postalCode = container.decodeLossyString(forKey: .postalCode)
        city = container.decodeLossyString(forKey: .city)
        latitude = container.decodeLossyDouble(forKey: .latitude) ?? 0
        longitude = container.decodeLossyDouble(forKey: .longitude) ?? 0
        chargerCount = container.decodeLossyInt(forKey: .chargerCount) ?? 0
        maxPowerKW = container.decodeLossyDouble(forKey: .maxPowerKW) ?? 0
        connectorTypes = container.decodeLossyString(forKey: .connectorTypes)
        sourceURL = container.decodeLossyString(forKey: .sourceURL)
        publicBundleStatus = container.decodeLossyString(forKey: .publicBundleStatus)
        openingHours = container.decodeLossyString(forKey: .openingHours)
        paymentMethods = container.decodeLossyString(forKey: .paymentMethods)
        authMethods = container.decodeLossyString(forKey: .authMethods)
        greenEnergy = container.decodeLossyBool(forKey: .greenEnergy)
        helpdeskPhone = container.decodeLossyString(forKey: .helpdeskPhone)
        priceDisplay = container.decodeLossyString(forKey: .priceDisplay)
        priceCurrency = container.decodeLossyString(forKey: .priceCurrency)
        priceEnergyEURKwhMin = container.decodeLossyString(forKey: .priceEnergyEURKwhMin)
        priceEnergyEURKwhMax = container.decodeLossyString(forKey: .priceEnergyEURKwhMax)
        detailLastUpdated = container.decodeLossyString(forKey: .detailLastUpdated)
        amenitiesTotal = container.decodeLossyInt(forKey: .amenitiesTotal) ?? 0
        nearestAmenityKind = container.decodeLossyString(forKey: .nearestAmenityKind)
        nearestAmenityName = container.decodeLossyString(forKey: .nearestAmenityName)
        nearestAmenityDistanceM = container.decodeLossyDouble(forKey: .nearestAmenityDistanceM)
        amenityCategoryCounts = container.decodeLossyIntDictionary(forKey: .amenityCategoryCounts)
        distanceM = container.decodeLossyDouble(forKey: .distanceM)

        let rawStatus = container.decodeLossyString(forKey: .availabilityStatus)
            .trimmingCharacters(in: .whitespacesAndNewlines)
        availabilityStatus = rawStatus.isEmpty ? nil : AvailabilityStatus(rawValue: rawStatus)
        availableEVSEs = container.decodeLossyInt(forKey: .availableEVSEs) ?? 0
        occupiedEVSEs = container.decodeLossyInt(forKey: .occupiedEVSEs) ?? 0
        outOfOrderEVSEs = container.decodeLossyInt(forKey: .outOfOrderEVSEs) ?? 0
        unknownEVSEs = container.decodeLossyInt(forKey: .unknownEVSEs) ?? 0
        totalEVSEs = container.decodeLossyInt(forKey: .totalEVSEs) ?? 0
        sourceObservedAt = container.decodeLossyString(forKey: .sourceObservedAt)
        fetchedAt = container.decodeLossyString(forKey: .fetchedAt)
        ingestedAt = container.decodeLossyString(forKey: .ingestedAt)
    }

    func feature(
        chargers: [CatalogCharger] = [],
        amenities: CatalogAmenities? = nil,
        preserving existing: GeoJSONFeature? = nil
    ) -> GeoJSONFeature {
        let properties = chargerProperties(chargers: chargers, amenities: amenities)
        return GeoJSONFeature(
            id: properties.stationID,
            geometry: GeoJSONPointGeometry(type: "Point", coordinates: [longitude, latitude]),
            properties: properties,
            liveSummary: liveSummary ?? existing?.liveSummary,
            liveDetail: existing?.liveDetail
        )
    }

    private var liveSummary: LiveStationSummary? {
        guard let availabilityStatus else { return nil }
        return LiveStationSummary(
            stationID: stationID,
            availabilityStatus: availabilityStatus,
            availableEVSEs: availableEVSEs,
            occupiedEVSEs: occupiedEVSEs,
            outOfOrderEVSEs: outOfOrderEVSEs,
            unknownEVSEs: unknownEVSEs,
            totalEVSEs: totalEVSEs,
            priceDisplay: priceDisplay,
            priceCurrency: priceCurrency,
            priceEnergyEURKwhMin: priceEnergyEURKwhMin,
            priceEnergyEURKwhMax: priceEnergyEURKwhMax,
            sourceObservedAt: sourceObservedAt,
            fetchedAt: fetchedAt,
            ingestedAt: ingestedAt
        )
    }

    private func chargerProperties(chargers: [CatalogCharger], amenities: CatalogAmenities?) -> ChargerProperties {
        let normalizedCounts = normalizedAmenityCounts(amenities?.amenityCategoryCounts ?? amenityCategoryCounts)
        let detailExamples = amenities?.amenityExamples ?? []
        let examples = detailExamples.isEmpty ? nearestAmenityExamples() : detailExamples
        let chargerTotal = chargers.isEmpty ? chargerCount : chargers.count
        let maxChargerPower = chargers.map(\.maxPowerKW).filter { $0 > 0 }.max() ?? 0
        let effectiveMaxPower = max(maxPowerKW, maxChargerPower)
        let effectiveTotalEVSEs = totalEVSEs > 0 ? totalEVSEs : chargerTotal

        return ChargerProperties(
            stationID: stationID,
            operatorName: firstNonEmpty(operatorName, stationName, "Unbekannter Betreiber"),
            status: publicBundleStatus,
            maxPowerKW: effectiveMaxPower,
            chargingPointsCount: max(chargerTotal, 1),
            maxIndividualPowerKW: effectiveMaxPower,
            postcode: postalCode,
            city: city,
            address: address,
            occupancySourceUID: providerUID,
            occupancySourceName: firstNonEmpty(providerUID, sourceUID),
            occupancyStatus: availabilityStatus?.rawValue ?? "",
            occupancyLastUpdated: firstNonEmpty(sourceObservedAt, fetchedAt, ingestedAt),
            occupancyTotalEVSEs: effectiveTotalEVSEs,
            occupancyAvailableEVSEs: availableEVSEs,
            occupancyOccupiedEVSEs: occupiedEVSEs,
            occupancyChargingEVSEs: 0,
            occupancyOutOfOrderEVSEs: outOfOrderEVSEs,
            occupancyUnknownEVSEs: unknownEVSEs,
            detailSourceUID: sourceUID,
            detailSourceName: catalogDetailSourceName,
            detailLastUpdated: detailLastUpdated,
            datexSiteID: sourceStationID,
            datexStationIDs: sourceStationID,
            datexChargePointIDs: chargers.map(\.sourceEVSEID).filter { !$0.isEmpty }.joined(separator: ", "),
            priceDisplay: priceDisplay,
            priceEnergyEURKwhMin: priceEnergyEURKwhMin,
            priceEnergyEURKwhMax: priceEnergyEURKwhMax,
            priceCurrency: priceCurrency,
            priceQuality: priceDisplay.isEmpty ? "" : "catalog",
            openingHoursDisplay: openingHours,
            openingHoursIs24_7: isAlwaysOpen(openingHours),
            helpdeskPhone: helpdeskPhone,
            paymentMethodsDisplay: formattedCSV(paymentMethods),
            authMethodsDisplay: formattedCSV(authMethods),
            connectorTypesDisplay: connectorDisplay(chargers: chargers),
            currentTypesDisplay: currentTypeDisplay(chargers: chargers),
            connectorCount: chargerTotal,
            greenEnergy: greenEnergy,
            serviceTypesDisplay: "",
            detailsJSON: "",
            amenitiesTotal: max(amenities?.amenitiesTotal ?? amenitiesTotal, normalizedCounts.values.reduce(0, +)),
            amenitiesSource: catalogDetailSourceName,
            amenityExamples: examples,
            amenityCounts: normalizedCounts
        )
    }

    private func nearestAmenityExamples() -> [AmenityExample] {
        let category = normalizedAmenityCategory(nearestAmenityKind)
        guard category != "other" || !nearestAmenityName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return []
        }
        return [
            AmenityExample(
                category: category,
                name: nearestAmenityName.isEmpty ? nil : nearestAmenityName,
                openingHours: nil,
                distanceM: nearestAmenityDistanceM,
                lat: nil,
                lon: nil
            )
        ]
    }

    private func connectorDisplay(chargers: [CatalogCharger]) -> String {
        if !connectorTypes.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            return formattedCSV(connectorTypes)
        }
        return uniqueDisplayValues(chargers.map(\.connectorType)).joined(separator: ", ")
    }

    private func currentTypeDisplay(chargers: [CatalogCharger]) -> String {
        uniqueDisplayValues(chargers.map(\.currentType)).joined(separator: ", ")
    }
}

struct CatalogCharger: Decodable {
    let chargerID: String
    let sourceUID: String
    let providerUID: String
    let sourceStationID: String
    let sourceEVSEID: String
    let connectorID: String
    let connectorType: String
    let currentType: String
    let maxPowerKW: Double
    let operatorName: String

    enum CodingKeys: String, CodingKey {
        case chargerID = "charger_id"
        case sourceUID = "source_uid"
        case providerUID = "provider_uid"
        case sourceStationID = "source_station_id"
        case sourceEVSEID = "source_evse_id"
        case connectorID = "connector_id"
        case connectorType = "connector_type"
        case currentType = "current_type"
        case maxPowerKW = "max_power_kw"
        case operatorName = "operator_name"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        chargerID = container.decodeLossyString(forKey: .chargerID)
        sourceUID = container.decodeLossyString(forKey: .sourceUID)
        providerUID = container.decodeLossyString(forKey: .providerUID)
        sourceStationID = container.decodeLossyString(forKey: .sourceStationID)
        sourceEVSEID = container.decodeLossyString(forKey: .sourceEVSEID)
        connectorID = container.decodeLossyString(forKey: .connectorID)
        connectorType = formattedCode(container.decodeLossyString(forKey: .connectorType))
        currentType = formattedCode(container.decodeLossyString(forKey: .currentType))
        maxPowerKW = container.decodeLossyDouble(forKey: .maxPowerKW) ?? 0
        operatorName = container.decodeLossyString(forKey: .operatorName)
    }
}

struct CatalogAmenities: Decodable {
    let amenitiesTotal: Int
    let amenityCategoryCounts: [String: Int]
    let amenityExamples: [AmenityExample]

    enum CodingKeys: String, CodingKey {
        case amenitiesTotal = "amenities_total"
        case amenityCategoryCounts = "amenity_category_counts"
        case amenityExamples = "amenity_examples"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        amenitiesTotal = container.decodeLossyInt(forKey: .amenitiesTotal) ?? 0
        amenityCategoryCounts = container.decodeLossyIntDictionary(forKey: .amenityCategoryCounts)
        amenityExamples = (try? container.decode([AmenityExample].self, forKey: .amenityExamples)) ?? []
    }
}

private func normalizedAmenityCounts(_ raw: [String: Int]) -> [String: Int] {
    var counts: [String: Int] = [:]
    for (key, value) in raw where value > 0 {
        counts[normalizedAmenityKey(key), default: 0] += value
    }
    return counts
}

private func formattedCSV(_ value: String) -> String {
    uniqueDisplayValues(
        value
            .split(separator: ",")
            .map(String.init)
    ).joined(separator: ", ")
}

private func uniqueDisplayValues(_ values: [String]) -> [String] {
    var seen: Set<String> = []
    var result: [String] = []
    for value in values {
        let formatted = formattedCode(value)
        guard !formatted.isEmpty else { continue }
        let key = formatted.lowercased()
        guard seen.insert(key).inserted else { continue }
        result.append(formatted)
    }
    return result
}

private func formattedCode(_ value: String) -> String {
    value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "_", with: " ")
        .replacingOccurrences(of: "-", with: " ")
}

private func isAlwaysOpen(_ value: String) -> Bool {
    let normalized = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
    return ["24/7", "24x7", "always open"].contains(normalized)
}

private func firstNonEmpty(_ values: String...) -> String {
    for value in values {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if !trimmed.isEmpty {
            return trimmed
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

    func decodeLossyDouble(forKey key: Key) -> Double? {
        if let value = try? decode(Double.self, forKey: key) {
            return value
        }
        if let value = try? decode(Int.self, forKey: key) {
            return Double(value)
        }
        if let value = try? decode(String.self, forKey: key) {
            return Double(value.replacingOccurrences(of: ",", with: "."))
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

    func decodeLossyIntDictionary(forKey key: Key) -> [String: Int] {
        if let value = try? decode([String: Int].self, forKey: key) {
            return value
        }
        if let value = try? decode([String: Double].self, forKey: key) {
            return value.mapValues(Int.init)
        }
        if let value = try? decode([String: String].self, forKey: key) {
            return value.reduce(into: [:]) { result, item in
                let normalized = item.value.replacingOccurrences(of: ",", with: ".")
                if let integer = Int(normalized) {
                    result[item.key] = integer
                } else if let double = Double(normalized) {
                    result[item.key] = Int(double)
                } else {
                    result[item.key] = 0
                }
            }
        }
        return [:]
    }
}
