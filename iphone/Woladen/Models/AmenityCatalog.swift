import Foundation

struct AmenityMeta {
    let key: String
    let label: String
    let symbol: String
}

enum AmenityCatalog {
    static let all: [AmenityMeta] = [
        .init(key: "amenity_restaurant", label: String(localized: "amenity.labels.restaurant"), symbol: "fork.knife"),
        .init(key: "amenity_cafe", label: String(localized: "amenity.labels.cafe"), symbol: "cup.and.saucer"),
        .init(key: "amenity_fast_food", label: String(localized: "amenity.labels.fast_food"), symbol: "takeoutbag.and.cup.and.straw"),
        .init(key: "amenity_toilets", label: String(localized: "amenity.labels.toilets"), symbol: "figure.stand"),
        .init(key: "amenity_supermarket", label: String(localized: "amenity.labels.supermarket"), symbol: "cart"),
        .init(key: "amenity_bakery", label: String(localized: "amenity.labels.bakery"), symbol: "birthday.cake"),
        .init(key: "amenity_convenience", label: String(localized: "amenity.labels.convenience"), symbol: "building.2"),
        .init(key: "amenity_pharmacy", label: String(localized: "amenity.labels.pharmacy"), symbol: "cross.case"),
        .init(key: "amenity_hotel", label: String(localized: "amenity.labels.hotel"), symbol: "bed.double"),
        .init(key: "amenity_museum", label: String(localized: "amenity.labels.museum"), symbol: "building.columns"),
        .init(key: "amenity_playground", label: String(localized: "amenity.labels.playground"), symbol: "figure.play"),
        .init(key: "amenity_park", label: String(localized: "amenity.labels.park"), symbol: "tree"),
        .init(key: "amenity_ice_cream", label: String(localized: "amenity.labels.ice_cream"), symbol: "birthday.cake"),
    ]

    static let byKey: [String: AmenityMeta] = Dictionary(uniqueKeysWithValues: all.map { ($0.key, $0) })

    static func label(for key: String) -> String {
        byKey[key]?.label ?? key.replacingOccurrences(of: "amenity_", with: "")
    }

    static func symbol(for key: String) -> String {
        byKey[key]?.symbol ?? "mappin.and.ellipse"
    }
}
