import Foundation

struct FilterState: Codable, Equatable {
    var operatorName: String = ""
    var minPowerKW: Double = 50
    var minAmenityCount: Double = 0
    var selectedAmenities: Set<String> = []
    var amenityNameQuery: String = ""
    var availableOnly: Bool = true
    var currentlyOpenOnly: Bool = false

    enum CodingKeys: String, CodingKey {
        case operatorName
        case minPowerKW
        case minAmenityCount
        case selectedAmenities
        case amenityNameQuery
        case availableOnly
        case currentlyOpenOnly
    }

    init(
        operatorName: String = "",
        minPowerKW: Double = 50,
        minAmenityCount: Double = 0,
        selectedAmenities: Set<String> = [],
        amenityNameQuery: String = "",
        availableOnly: Bool = true,
        currentlyOpenOnly: Bool = false
    ) {
        self.operatorName = operatorName
        self.minPowerKW = minPowerKW
        self.minAmenityCount = minAmenityCount
        self.selectedAmenities = selectedAmenities
        self.amenityNameQuery = amenityNameQuery
        self.availableOnly = availableOnly
        self.currentlyOpenOnly = currentlyOpenOnly
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        operatorName = (try? container.decode(String.self, forKey: .operatorName)) ?? ""
        minPowerKW = (try? container.decode(Double.self, forKey: .minPowerKW)) ?? 50
        minAmenityCount = (try? container.decode(Double.self, forKey: .minAmenityCount)) ?? 0
        selectedAmenities = (try? container.decode(Set<String>.self, forKey: .selectedAmenities)) ?? []
        amenityNameQuery = (try? container.decode(String.self, forKey: .amenityNameQuery)) ?? ""
        availableOnly = (try? container.decode(Bool.self, forKey: .availableOnly)) ?? true
        currentlyOpenOnly = (try? container.decode(Bool.self, forKey: .currentlyOpenOnly)) ?? false
    }

    var activeCount: Int {
        var count = 0
        if !operatorName.isEmpty { count += 1 }
        if minPowerKW > 0 { count += 1 }
        if minAmenityCount > 0 { count += 1 }
        count += selectedAmenities.count
        if !amenityNameQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { count += 1 }
        if availableOnly { count += 1 }
        if currentlyOpenOnly { count += 1 }
        return count
    }

    var clearableState: FilterState {
        FilterState(
            operatorName: "",
            minPowerKW: 50,
            minAmenityCount: 0,
            selectedAmenities: [],
            amenityNameQuery: "",
            availableOnly: false,
            currentlyOpenOnly: false
        )
    }
}

enum FilterStateStore {
    private static let key = "woladen.filterState.v1"

    static func load() -> FilterState {
        guard let data = UserDefaults.standard.data(forKey: key) else {
            return FilterState()
        }
        return (try? JSONDecoder().decode(FilterState.self, from: data)) ?? FilterState()
    }

    static func save(_ state: FilterState) {
        guard let data = try? JSONEncoder().encode(state) else { return }
        UserDefaults.standard.set(data, forKey: key)
    }
}
