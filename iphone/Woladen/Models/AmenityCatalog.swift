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
        .init(key: "amenity_bbq", label: String(localized: "amenity.labels.bbq"), symbol: "flame"),
        .init(key: "amenity_biergarten", label: String(localized: "amenity.labels.biergarten"), symbol: "mug"),
        .init(key: "amenity_cinema", label: String(localized: "amenity.labels.cinema"), symbol: "film"),
        .init(key: "amenity_library", label: String(localized: "amenity.labels.library"), symbol: "books.vertical"),
        .init(key: "amenity_theatre", label: String(localized: "amenity.labels.theatre"), symbol: "theatermasks"),
        .init(key: "amenity_atm", label: String(localized: "amenity.labels.atm"), symbol: "banknote"),
        .init(key: "amenity_bank", label: String(localized: "amenity.labels.bank"), symbol: "building.columns"),
        .init(key: "amenity_bench", label: String(localized: "amenity.labels.bench"), symbol: "chair.lounge"),
        .init(key: "amenity_bicycle_rental", label: String(localized: "amenity.labels.bicycle_rental"), symbol: "bicycle"),
        .init(key: "amenity_car_sharing", label: String(localized: "amenity.labels.car_sharing"), symbol: "car.2"),
        .init(key: "amenity_fuel", label: String(localized: "amenity.labels.fuel"), symbol: "fuelpump"),
        .init(key: "amenity_hospital", label: String(localized: "amenity.labels.hospital"), symbol: "cross.case"),
        .init(key: "amenity_police", label: String(localized: "amenity.labels.police"), symbol: "shield"),
        .init(key: "amenity_post_box", label: String(localized: "amenity.labels.post_box"), symbol: "envelope"),
        .init(key: "amenity_post_office", label: String(localized: "amenity.labels.post_office"), symbol: "building.2"),
        .init(key: "amenity_pub", label: String(localized: "amenity.labels.pub"), symbol: "wineglass"),
        .init(key: "amenity_school", label: String(localized: "amenity.labels.school"), symbol: "graduationcap"),
        .init(key: "amenity_taxi", label: String(localized: "amenity.labels.taxi"), symbol: "car"),
        .init(key: "amenity_waste_basket", label: String(localized: "amenity.labels.waste_basket"), symbol: "trash"),
        .init(key: "amenity_swimming", label: String(localized: "amenity.labels.swimming"), symbol: "figure.pool.swim"),
        .init(key: "amenity_gym", label: String(localized: "amenity.labels.gym"), symbol: "dumbbell"),
        .init(key: "amenity_camp_site", label: String(localized: "amenity.labels.camp_site"), symbol: "tent"),
        .init(key: "amenity_viewpoint", label: String(localized: "amenity.labels.viewpoint"), symbol: "binoculars"),
        .init(key: "amenity_zoo", label: String(localized: "amenity.labels.zoo"), symbol: "pawprint"),
        .init(key: "amenity_shop_mall", label: String(localized: "amenity.labels.mall"), symbol: "bag"),
        .init(key: "amenity_shop_doityourself", label: String(localized: "amenity.labels.doityourself"), symbol: "hammer"),
        .init(key: "amenity_shop_electronics", label: String(localized: "amenity.labels.electronics"), symbol: "desktopcomputer"),
    ]

    static let byKey: [String: AmenityMeta] = Dictionary(uniqueKeysWithValues: all.map { ($0.key, $0) })

    static func label(for key: String) -> String {
        byKey[key]?.label ?? key.replacingOccurrences(of: "amenity_", with: "")
    }

    static func symbol(for key: String) -> String {
        byKey[key]?.symbol ?? "mappin.and.ellipse"
    }
}
