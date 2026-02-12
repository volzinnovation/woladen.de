import Foundation
import CoreLocation

struct ChargerDataset {
    let stations: [ChargerStation]
    let operators: [OperatorEntry]
    let buildMeta: BuildMeta
}

struct BuildMeta {
    let generatedAt: Date?
    let sourceURL: String?
}

struct OperatorEntry: Identifiable {
    let name: String
    let stations: Int
    var id: String { name }
}

struct AmenityExample: Identifiable {
    let category: String
    let name: String?
    let openingHours: String?
    let distanceMeters: Int?
    let id: String

    init(category: String, name: String?, openingHours: String?, distanceMeters: Int?) {
        self.category = category
        self.name = name?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.openingHours = openingHours?.trimmingCharacters(in: .whitespacesAndNewlines).nilIfEmpty
        self.distanceMeters = distanceMeters
        self.id = [
            category,
            self.name ?? "",
            self.openingHours ?? "",
            self.distanceMeters.map(String.init) ?? "",
        ].joined(separator: "|")
    }

    var amenityKey: String { "amenity_\(category)" }
    var displayName: String { name ?? AmenityCatalog.label(for: amenityKey) }
}

struct ChargerStation: Identifiable {
    let stationID: String
    let operatorName: String
    let status: String
    let maxPowerKW: Double
    let latitude: Double
    let longitude: Double
    let postcode: String
    let city: String
    let address: String
    let amenitiesTotal: Int
    let amenitiesSource: String
    let amenityExamples: [AmenityExample]
    let amenityCounts: [String: Int]

    var id: String { stationID }
    var coordinate: CLLocationCoordinate2D {
        CLLocationCoordinate2D(latitude: latitude, longitude: longitude)
    }

    func amenityCount(for key: String) -> Int {
        amenityCounts[key] ?? 0
    }

    var sortedAmenityCounts: [(key: String, count: Int)] {
        amenityCounts
            .filter { $0.value > 0 }
            .sorted {
                if $0.value == $1.value {
                    return AmenityCatalog.label(for: $0.key) < AmenityCatalog.label(for: $1.key)
                }
                return $0.value > $1.value
            }
    }
}

enum AmenityCatalog {
    static let labels: [String: String] = [
        "amenity_restaurant": "Restaurant",
        "amenity_cafe": "Cafe",
        "amenity_fast_food": "Fast Food",
        "amenity_toilets": "Toilets",
        "amenity_supermarket": "Supermarket",
        "amenity_bakery": "Bakery",
        "amenity_convenience": "Kiosk",
        "amenity_pharmacy": "Pharmacy",
        "amenity_hotel": "Hotel",
        "amenity_museum": "Museum",
        "amenity_playground": "Playground",
        "amenity_park": "Park",
        "amenity_ice_cream": "Ice Cream",
        "amenity_bbq": "BBQ",
        "amenity_biergarten": "Biergarten",
        "amenity_cinema": "Cinema",
        "amenity_library": "Library",
        "amenity_theatre": "Theatre",
        "amenity_atm": "ATM",
        "amenity_bank": "Bank",
        "amenity_bench": "Bench",
        "amenity_bicycle_rental": "Bike Rental",
        "amenity_car_sharing": "Car Sharing",
        "amenity_fuel": "Fuel",
        "amenity_hospital": "Hospital",
        "amenity_police": "Police",
        "amenity_post_box": "Post Box",
        "amenity_post_office": "Post Office",
        "amenity_pub": "Pub",
        "amenity_school": "School",
        "amenity_taxi": "Taxi",
        "amenity_waste_basket": "Waste Basket",
        "shop_supermarket": "Supermarket",
        "shop_bakery": "Bakery",
        "shop_convenience": "Convenience",
        "shop_mall": "Mall",
    ]

    static let iconFilenames: [String: String] = [
        "amenity_restaurant": "amenity_restaurant.png",
        "amenity_cafe": "amenity_cafe.png",
        "amenity_fast_food": "amenity_fast_food.png",
        "amenity_toilets": "amenity_toilets.png",
        "amenity_supermarket": "shop_supermarket.png",
        "amenity_bakery": "shop_bakery.png",
        "amenity_convenience": "shop_convenience.png",
        "amenity_pharmacy": "amenity_pharmacy.png",
        "amenity_hotel": "amenity_hotel.png",
        "amenity_museum": "tourism_museum.png",
        "amenity_playground": "leisure_playground.png",
        "amenity_park": "leisure_park.png",
        "amenity_ice_cream": "amenity_ice_cream.png",
        "amenity_bbq": "amenity_bbq.png",
        "amenity_biergarten": "amenity_biergarten.png",
        "amenity_cinema": "amenity_cinema.png",
        "amenity_library": "amenity_library.png",
        "amenity_theatre": "amenity_theatre.png",
        "amenity_atm": "amenity_atm.png",
        "amenity_bank": "amenity_bank.png",
        "amenity_bench": "amenity_bench.png",
        "amenity_bicycle_rental": "amenity_bicycle_rental.png",
        "amenity_car_sharing": "amenity_car_sharing.png",
        "amenity_fuel": "amenity_fuel.png",
        "amenity_hospital": "amenity_hospital.png",
        "amenity_police": "amenity_police.png",
        "amenity_post_box": "amenity_post_box.png",
        "amenity_post_office": "amenity_post_office.png",
        "amenity_pub": "amenity_pub_.png",
        "amenity_school": "amenity_school.png",
        "amenity_taxi": "amenity_taxi.png",
        "amenity_waste_basket": "amenity_waste_basket.png",
        "shop_mall": "shop_mall_.png",
        "shop_supermarket": "shop_supermarket.png",
        "shop_bakery": "shop_bakery.png",
        "shop_convenience": "shop_convenience.png",
    ]

    static func label(for key: String) -> String {
        if let label = labels[key] {
            return label
        }
        return prettify(key: key)
    }

    static func iconFilename(for key: String) -> String? {
        iconFilenames[key]
    }

    static func preferredOrder(for keys: [String]) -> [String] {
        let unique = Array(Set(keys))
        return unique.sorted { lhs, rhs in
            label(for: lhs) < label(for: rhs)
        }
    }

    private static func prettify(key: String) -> String {
        key
            .replacingOccurrences(of: "_", with: " ")
            .split(separator: " ")
            .map { $0.capitalized }
            .joined(separator: " ")
    }
}

private extension String {
    var nilIfEmpty: String? {
        isEmpty ? nil : self
    }
}
