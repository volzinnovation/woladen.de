import Foundation

enum ChargerRepositoryError: LocalizedError {
    case invalidGeoJSON
    case invalidFeatureCollection
    case noStationsParsed
    case invalidOperatorsJSON
    case bundledFilesMissing

    var errorDescription: String? {
        switch self {
        case .invalidGeoJSON:
            return "Could not parse chargers_fast.geojson."
        case .invalidFeatureCollection:
            return "GeoJSON features are missing or invalid."
        case .noStationsParsed:
            return "No charger stations could be parsed."
        case .invalidOperatorsJSON:
            return "Could not parse operators.json."
        case .bundledFilesMissing:
            return "Bundled data files are missing (chargers_fast.geojson/operators.json)."
        }
    }
}

final class ChargerRepository {
    init() {}

    func loadDataset() async throws -> ChargerDataset {
        async let geojsonData = loadBundledData(
            bundledName: "chargers_fast",
            bundledExtension: "geojson"
        )
        async let operatorsData = loadBundledData(
            bundledName: "operators",
            bundledExtension: "json"
        )

        let (geoData, opData) = try await (geojsonData, operatorsData)
        let (stations, meta) = try parseStations(from: geoData)
        let operators = try parseOperators(from: opData)
        return ChargerDataset(stations: stations, operators: operators, buildMeta: meta)
    }

    private func loadBundledData(bundledName: String, bundledExtension: String) async throws -> Data {
        if let bundledURL = Bundle.main.url(forResource: bundledName, withExtension: bundledExtension) {
            return try Data(contentsOf: bundledURL)
        }

        throw ChargerRepositoryError.bundledFilesMissing
    }

    private func parseStations(from data: Data) throws -> ([ChargerStation], BuildMeta) {
        guard
            let root = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
            throw ChargerRepositoryError.invalidGeoJSON
        }

        guard let features = root["features"] as? [[String: Any]] else {
            throw ChargerRepositoryError.invalidFeatureCollection
        }

        let generatedAt = parseISODate(root["generated_at"] as? String)
        let sourceURL = ((root["source"] as? [String: Any])?["source_url"] as? String)
        let meta = BuildMeta(generatedAt: generatedAt, sourceURL: sourceURL)

        var stations: [ChargerStation] = []
        stations.reserveCapacity(features.count)

        for (index, feature) in features.enumerated() {
            if let station = parseStation(feature: feature, fallbackIndex: index) {
                stations.append(station)
            }
        }

        guard !stations.isEmpty else {
            throw ChargerRepositoryError.noStationsParsed
        }
        return (stations, meta)
    }

    private func parseStation(feature: [String: Any], fallbackIndex: Int) -> ChargerStation? {
        guard
            let geometry = feature["geometry"] as? [String: Any],
            let coordinates = geometry["coordinates"] as? [Any],
            coordinates.count >= 2,
            let lon = doubleValue(coordinates[0]),
            let lat = doubleValue(coordinates[1]),
            let properties = feature["properties"] as? [String: Any]
        else {
            return nil
        }

        let stationID = stringValue(properties["station_id"]) ?? "station-\(fallbackIndex)"
        let operatorName = stringValue(properties["operator"]) ?? "Unknown"
        let status = stringValue(properties["status"]) ?? ""
        let maxPowerKW = doubleValue(properties["max_power_kw"]) ?? 0
        let postcode = stringValue(properties["postcode"]) ?? ""
        let city = stringValue(properties["city"]) ?? ""
        let address = stringValue(properties["address"]) ?? ""
        let amenitiesTotal = intValue(properties["amenities_total"]) ?? 0
        let amenitiesSource = stringValue(properties["amenities_source"]) ?? ""

        var amenityCounts: [String: Int] = [:]
        for (key, value) in properties where key.hasPrefix("amenity_") && key != "amenity_examples" {
            if let count = intValue(value) {
                amenityCounts[key] = count
            }
        }

        let amenityExamples = parseAmenityExamples(from: properties["amenity_examples"])

        return ChargerStation(
            stationID: stationID,
            operatorName: operatorName,
            status: status,
            maxPowerKW: maxPowerKW,
            latitude: lat,
            longitude: lon,
            postcode: postcode,
            city: city,
            address: address,
            amenitiesTotal: amenitiesTotal,
            amenitiesSource: amenitiesSource,
            amenityExamples: amenityExamples,
            amenityCounts: amenityCounts
        )
    }

    private func parseAmenityExamples(from raw: Any?) -> [AmenityExample] {
        if let array = raw as? [[String: Any]] {
            return parseAmenityExamples(array: array)
        }

        if let jsonString = raw as? String, !jsonString.isEmpty,
           let data = jsonString.data(using: .utf8),
           let array = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]] {
            return parseAmenityExamples(array: array)
        }

        return []
    }

    private func parseAmenityExamples(array: [[String: Any]]) -> [AmenityExample] {
        array.compactMap { item in
            guard let category = stringValue(item["category"]) else {
                return nil
            }
            return AmenityExample(
                category: category,
                name: stringValue(item["name"]),
                openingHours: stringValue(item["opening_hours"]),
                distanceMeters: intValue(item["distance_m"])
            )
        }
    }

    private func parseOperators(from data: Data) throws -> [OperatorEntry] {
        guard
            let root = try JSONSerialization.jsonObject(with: data) as? [String: Any],
            let rawOperators = root["operators"] as? [[String: Any]]
        else {
            throw ChargerRepositoryError.invalidOperatorsJSON
        }

        return rawOperators.compactMap { item in
            guard let name = stringValue(item["name"]) else { return nil }
            let stations = intValue(item["stations"]) ?? 0
            return OperatorEntry(name: name, stations: stations)
        }
    }

    private func stringValue(_ raw: Any?) -> String? {
        guard let raw else { return nil }
        let text: String
        if let value = raw as? String {
            text = value
        } else {
            text = String(describing: raw)
        }
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        return trimmed.isEmpty ? nil : trimmed
    }

    private func intValue(_ raw: Any?) -> Int? {
        switch raw {
        case let value as Int:
            return value
        case let value as Double:
            return Int(value.rounded())
        case let value as NSNumber:
            return value.intValue
        case let value as String:
            return Int(value.trimmingCharacters(in: .whitespacesAndNewlines))
        default:
            return nil
        }
    }

    private func doubleValue(_ raw: Any?) -> Double? {
        switch raw {
        case let value as Double:
            return value
        case let value as Int:
            return Double(value)
        case let value as NSNumber:
            return value.doubleValue
        case let value as String:
            return Double(value.trimmingCharacters(in: .whitespacesAndNewlines))
        default:
            return nil
        }
    }

    private func parseISODate(_ raw: String?) -> Date? {
        guard let raw else { return nil }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let date = formatter.date(from: raw) {
            return date
        }
        formatter.formatOptions = [.withInternetDateTime]
        return formatter.date(from: raw)
    }
}
