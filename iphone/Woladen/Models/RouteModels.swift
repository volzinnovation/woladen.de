import Foundation
import CoreLocation

struct RouteEndpoint: Codable, Equatable {
    let lat: Double
    let lon: Double
    let label: String

    init(lat: Double, lon: Double, label: String) {
        self.lat = lat
        self.lon = lon
        self.label = label
    }

    init(coordinate: CLLocationCoordinate2D, label: String) {
        self.init(lat: coordinate.latitude, lon: coordinate.longitude, label: label)
    }
}

struct RouteFilterPayload: Codable, Equatable {
    let `operator`: String
    let minPowerKW: Int
    let minAmenitiesTotal: Int
    let selectedAmenities: [String]
    let amenityNameQuery: String
    let availableOnly: Bool
    let currentlyOpenOnly: Bool

    enum CodingKeys: String, CodingKey {
        case `operator`
        case minPowerKW = "min_power_kw"
        case minAmenitiesTotal = "min_amenities_total"
        case selectedAmenities = "selected_amenities"
        case amenityNameQuery = "amenity_name_query"
        case availableOnly = "available_only"
        case currentlyOpenOnly = "currently_open_only"
    }

    init(filter: FilterState) {
        self.operator = filter.operatorName.trimmingCharacters(in: .whitespacesAndNewlines)
        self.minPowerKW = max(0, Int(filter.minPowerKW.rounded()))
        self.minAmenitiesTotal = max(0, Int(filter.minAmenityCount.rounded()))
        self.selectedAmenities = filter.selectedAmenities
            .filter { $0.range(of: #"^amenity_[a-z0-9_]+$"#, options: .regularExpression) != nil }
            .sorted()
        self.amenityNameQuery = filter.amenityNameQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        self.availableOnly = false
        self.currentlyOpenOnly = filter.currentlyOpenOnly
    }
}

struct RouteGeometry: Decodable, Equatable {
    let type: String
    let coordinates: [[Double]]

    enum CodingKeys: String, CodingKey {
        case type
        case coordinates
    }

    init(type: String = "LineString", coordinates: [[Double]]) {
        self.type = type
        self.coordinates = coordinates
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        type = (try? container.decode(String.self, forKey: .type)) ?? "LineString"
        let rawCoordinates = (try? container.decode([[LossyDouble]].self, forKey: .coordinates)) ?? []
        coordinates = rawCoordinates.compactMap { point in
            guard point.count >= 2 else { return nil }
            let lon = point[0].value
            let lat = point[1].value
            guard lon.isFinite, lat.isFinite else { return nil }
            return [lon, lat]
        }
    }
}

struct RouteSummary: Decodable, Equatable {
    let source: String
    let profile: String
    let distanceM: Int
    let durationS: Int
    let geometry: RouteGeometry

    enum CodingKeys: String, CodingKey {
        case source
        case profile
        case distanceM = "distance_m"
        case durationS = "duration_s"
        case geometry
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        source = container.routeDecodeLossyString(forKey: .source)
        profile = container.routeDecodeLossyString(forKey: .profile)
        distanceM = max(0, Int((container.routeDecodeLossyDouble(forKey: .distanceM) ?? 0).rounded()))
        durationS = max(0, Int((container.routeDecodeLossyDouble(forKey: .durationS) ?? 0).rounded()))
        geometry = (try? container.decode(RouteGeometry.self, forKey: .geometry)) ?? RouteGeometry(coordinates: [])
    }
}

struct RouteNearestPoint: Decodable, Equatable {
    let lat: Double
    let lon: Double

    enum CodingKeys: String, CodingKey {
        case lat
        case lon
        case lng
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        lat = container.routeDecodeLossyDouble(forKey: .lat) ?? 0
        lon = container.routeDecodeLossyDouble(forKey: .lon) ?? container.routeDecodeLossyDouble(forKey: .lng) ?? 0
    }
}

struct RouteStationMetadata: Decodable, Equatable {
    let driveDistanceToRouteM: Int
    let routeDetourM: Int
    let straightLineDistanceToRouteM: Int
    let routePositionM: Int
    let nearestRoutePoint: RouteNearestPoint?

    enum CodingKeys: String, CodingKey {
        case driveDistanceToRouteM = "drive_distance_to_route_m"
        case routeDetourM = "route_detour_m"
        case straightLineDistanceToRouteM = "straight_line_distance_to_route_m"
        case routePositionM = "route_position_m"
        case nearestRoutePoint = "nearest_route_point"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        driveDistanceToRouteM = max(0, Int((container.routeDecodeLossyDouble(forKey: .driveDistanceToRouteM) ?? 0).rounded()))
        routeDetourM = max(0, Int((container.routeDecodeLossyDouble(forKey: .routeDetourM) ?? 0).rounded()))
        straightLineDistanceToRouteM = max(0, Int((container.routeDecodeLossyDouble(forKey: .straightLineDistanceToRouteM) ?? 0).rounded()))
        routePositionM = max(0, Int((container.routeDecodeLossyDouble(forKey: .routePositionM) ?? 0).rounded()))
        nearestRoutePoint = try? container.decode(RouteNearestPoint.self, forKey: .nearestRoutePoint)
    }
}

struct RouteStationCandidate: Decodable {
    let station: CatalogStation
    let route: RouteStationMetadata
}

struct RouteChargerResponse: Decodable {
    let route: RouteSummary
    let stations: [RouteStationCandidate]
    let source: String

    enum CodingKeys: String, CodingKey {
        case route
        case stations
        case source
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        route = try container.decode(RouteSummary.self, forKey: .route)
        stations = (try? container.decode([RouteStationCandidate].self, forKey: .stations)) ?? []
        source = container.routeDecodeLossyString(forKey: .source)
    }
}

private struct LossyDouble: Decodable {
    let value: Double

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if let value = try? container.decode(Double.self) {
            self.value = value
        } else if let value = try? container.decode(Int.self) {
            self.value = Double(value)
        } else if let value = try? container.decode(String.self),
                  let parsed = Double(value.replacingOccurrences(of: ",", with: ".")) {
            self.value = parsed
        } else {
            self.value = .nan
        }
    }
}

private extension KeyedDecodingContainer {
    func routeDecodeLossyString(forKey key: Key) -> String {
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

    func routeDecodeLossyDouble(forKey key: Key) -> Double? {
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
}
