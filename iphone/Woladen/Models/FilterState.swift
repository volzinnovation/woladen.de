import Foundation

struct FilterState: Codable, Equatable {
    var selectedOperatorNames: Set<String> = []
    var minPowerKW: Double = 50
    var minAmenityCount: Double = 0
    var selectedAmenities: Set<String> = []
    var amenityNameQuery: String = ""
    var availableOnly: Bool = true
    var currentlyOpenOnly: Bool = false
    var routeMaxDistanceFromLocationKM: Double?

    enum CodingKeys: String, CodingKey {
        case operatorName
        case selectedOperatorNames
        case minPowerKW
        case minAmenityCount
        case selectedAmenities
        case amenityNameQuery
        case availableOnly
        case currentlyOpenOnly
        case routeMaxDistanceFromLocationKM
    }

    init(
        operatorName: String = "",
        selectedOperatorNames: Set<String> = [],
        minPowerKW: Double = 50,
        minAmenityCount: Double = 0,
        selectedAmenities: Set<String> = [],
        amenityNameQuery: String = "",
        availableOnly: Bool = true,
        currentlyOpenOnly: Bool = false,
        routeMaxDistanceFromLocationKM: Double? = nil
    ) {
        let selectedNames = Self.normalizedOperatorNames(selectedOperatorNames)
        self.selectedOperatorNames = selectedNames.isEmpty
            ? Self.normalizedOperatorNames([operatorName])
            : selectedNames
        self.minPowerKW = minPowerKW
        self.minAmenityCount = minAmenityCount
        self.selectedAmenities = selectedAmenities
        self.amenityNameQuery = amenityNameQuery
        self.availableOnly = availableOnly
        self.currentlyOpenOnly = currentlyOpenOnly
        self.routeMaxDistanceFromLocationKM = Self.normalizedRouteDistance(routeMaxDistanceFromLocationKM)
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        let decodedOperatorNames = (try? container.decode(Set<String>.self, forKey: .selectedOperatorNames)) ?? []
        let legacyOperatorName = (try? container.decode(String.self, forKey: .operatorName)) ?? ""
        let selectedNames = Self.normalizedOperatorNames(decodedOperatorNames)
        selectedOperatorNames = selectedNames.isEmpty
            ? Self.normalizedOperatorNames([legacyOperatorName])
            : selectedNames
        minPowerKW = (try? container.decode(Double.self, forKey: .minPowerKW)) ?? 50
        minAmenityCount = (try? container.decode(Double.self, forKey: .minAmenityCount)) ?? 0
        selectedAmenities = (try? container.decode(Set<String>.self, forKey: .selectedAmenities)) ?? []
        amenityNameQuery = (try? container.decode(String.self, forKey: .amenityNameQuery)) ?? ""
        availableOnly = (try? container.decode(Bool.self, forKey: .availableOnly)) ?? true
        currentlyOpenOnly = (try? container.decode(Bool.self, forKey: .currentlyOpenOnly)) ?? false
        routeMaxDistanceFromLocationKM = Self.normalizedRouteDistance(
            try? container.decode(Double.self, forKey: .routeMaxDistanceFromLocationKM)
        )
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(selectedOperatorNames.sorted(), forKey: .selectedOperatorNames)
        try container.encode(operatorName, forKey: .operatorName)
        try container.encode(minPowerKW, forKey: .minPowerKW)
        try container.encode(minAmenityCount, forKey: .minAmenityCount)
        try container.encode(selectedAmenities.sorted(), forKey: .selectedAmenities)
        try container.encode(amenityNameQuery, forKey: .amenityNameQuery)
        try container.encode(availableOnly, forKey: .availableOnly)
        try container.encode(currentlyOpenOnly, forKey: .currentlyOpenOnly)
        try container.encodeIfPresent(routeMaxDistanceFromLocationKM, forKey: .routeMaxDistanceFromLocationKM)
    }

    var operatorName: String {
        get {
            selectedOperatorNames.count == 1 ? selectedOperatorNames.sorted().first ?? "" : ""
        }
        set {
            selectedOperatorNames = Self.normalizedOperatorNames([newValue])
        }
    }

    var activeCount: Int {
        var count = 0
        if !selectedOperatorNames.isEmpty { count += 1 }
        if minPowerKW > 0 { count += 1 }
        if minAmenityCount > 0 { count += 1 }
        count += selectedAmenities.count
        if !amenityNameQuery.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { count += 1 }
        if availableOnly { count += 1 }
        if currentlyOpenOnly { count += 1 }
        if routeMaxDistanceFromLocationKM != nil { count += 1 }
        return count
    }

    var clearableState: FilterState {
        FilterState(
            selectedOperatorNames: [],
            minPowerKW: 50,
            minAmenityCount: 0,
            selectedAmenities: [],
            amenityNameQuery: "",
            availableOnly: false,
            currentlyOpenOnly: false,
            routeMaxDistanceFromLocationKM: nil
        )
    }

    private static func normalizedOperatorNames(_ values: Set<String>) -> Set<String> {
        Set(
            values
                .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
                .filter { !$0.isEmpty }
        )
    }

    private static func normalizedRouteDistance(_ value: Double?) -> Double? {
        guard let value, value.isFinite, value > 0 else { return nil }
        return min(value, 400)
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
