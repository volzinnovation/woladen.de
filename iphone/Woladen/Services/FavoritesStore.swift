import Foundation

final class FavoritesStore {
    private let key: String
    private let defaults: UserDefaults

    init(key: String = "woladen.favorite_station_ids", defaults: UserDefaults = .standard) {
        self.key = key
        self.defaults = defaults
    }

    func load() -> Set<String> {
        let values = defaults.stringArray(forKey: key) ?? []
        return Set(values)
    }

    func save(_ ids: Set<String>) {
        defaults.set(Array(ids).sorted(), forKey: key)
    }
}
