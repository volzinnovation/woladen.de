import Foundation

struct AmenityMeta {
    let key: String
    let label: String
    let symbol: String
}

enum AmenityCatalog {
    static let all: [AmenityMeta] = [
        .init(key: "amenity_restaurant", label: "Restaurant", symbol: "fork.knife"),
        .init(key: "amenity_cafe", label: "Cafe", symbol: "cup.and.saucer"),
        .init(key: "amenity_fast_food", label: "Fast Food", symbol: "takeoutbag.and.cup.and.straw"),
        .init(key: "amenity_toilets", label: "Toiletten", symbol: "figure.stand"),
        .init(key: "amenity_supermarket", label: "Supermarkt", symbol: "cart"),
        .init(key: "amenity_bakery", label: "Backerei", symbol: "birthday.cake"),
        .init(key: "amenity_convenience", label: "Kiosk", symbol: "building.2"),
        .init(key: "amenity_pharmacy", label: "Apotheke", symbol: "cross.case"),
        .init(key: "amenity_hotel", label: "Hotel", symbol: "bed.double"),
        .init(key: "amenity_museum", label: "Museum", symbol: "building.columns"),
        .init(key: "amenity_playground", label: "Spielplatz", symbol: "figure.play"),
        .init(key: "amenity_park", label: "Park", symbol: "tree"),
        .init(key: "amenity_ice_cream", label: "Eis", symbol: "birthday.cake"),
    ]

    static let byKey: [String: AmenityMeta] = Dictionary(uniqueKeysWithValues: all.map { ($0.key, $0) })

    static func label(for key: String) -> String {
        byKey[key]?.label ?? key.replacingOccurrences(of: "amenity_", with: "")
    }

    static func symbol(for key: String) -> String {
        byKey[key]?.symbol ?? "mappin.and.ellipse"
    }
}
