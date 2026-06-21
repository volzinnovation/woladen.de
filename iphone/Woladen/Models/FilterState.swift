import Foundation

struct FilterState: Codable, Equatable {
    var operatorName: String = ""
    var minPowerKW: Double = 50
    var selectedAmenities: Set<String> = []
    var amenityNameQuery: String = ""
    var availableOnly: Bool = true
    var currentlyOpenOnly: Bool = false

    var activeCount: Int {
        var count = 0
        if !operatorName.isEmpty { count += 1 }
        if minPowerKW > 0 { count += 1 }
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
