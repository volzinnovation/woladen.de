import Foundation
import CoreLocation

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
        case amenitiesTotal = "amenities_total"
        case amenitiesSource = "amenities_source"
        case amenityExamples = "amenity_examples"
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
        max(maxIndividualPowerKW, maxPowerKW)
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
}

struct AmenityCount: Identifiable {
    var id: String { key }
    let key: String
    let count: Int
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
