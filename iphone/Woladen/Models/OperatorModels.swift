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
    let id: String
    let name: String
    let stations: Int
    let aliases: [String]

    static func canonicalIDs(for selections: Set<String>, using operators: [OperatorEntry]) -> Set<String> {
        Set(selections.compactMap { selection in
            let selected = selection.trimmingCharacters(in: .whitespacesAndNewlines)
            guard !selected.isEmpty else { return nil }
            if let exactID = operators.first(where: {
                $0.id.caseInsensitiveCompare(selected) == .orderedSame
            })?.id {
                return exactID
            }
            return operators.first { entry in
                ([entry.name] + entry.aliases).contains { candidate in
                    candidate.trimmingCharacters(in: .whitespacesAndNewlines)
                        .caseInsensitiveCompare(selected) == .orderedSame
                }
            }?.id
        })
    }

    init(id: String? = nil, name: String, stations: Int, aliases: [String] = []) {
        let normalizedID = id?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        self.id = normalizedID.isEmpty ? name : normalizedID
        self.name = name
        self.stations = stations
        self.aliases = aliases
    }

    enum CodingKeys: String, CodingKey {
        case id
        case name
        case stations
        case aliases
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let decodedName = (try? container.decode(String.self, forKey: .name)) ?? ""
        let decodedID = (try? container.decode(String.self, forKey: .id)) ?? decodedName
        self.init(
            id: decodedID,
            name: decodedName,
            stations: (try? container.decode(Int.self, forKey: .stations)) ?? 0,
            aliases: (try? container.decode([String].self, forKey: .aliases)) ?? []
        )
    }
}
