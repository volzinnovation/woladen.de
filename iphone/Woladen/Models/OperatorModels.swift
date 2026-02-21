import Foundation

struct OperatorCatalog: Decodable {
    let generatedAt: String?
    let minStations: Int
    let totalOperators: Int
    let operators: [OperatorEntry]

    enum CodingKeys: String, CodingKey {
        case generatedAt = "generated_at"
        case minStations = "min_stations"
        case totalOperators = "total_operators"
        case operators
    }
}

struct OperatorEntry: Decodable, Identifiable, Hashable {
    var id: String { name }
    let name: String
    let stations: Int
}
