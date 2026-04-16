import Foundation

@MainActor
final class FavoritesStore: ObservableObject {
    @Published private(set) var favorites: Set<String> = []

    private let defaultsKey = "woladen_favorites"
    private let seededFavorites: Set<String>?

    init() {
        seededFavorites = FavoritesStore.resolveSeededFavorites()
        if let seededFavorites {
            favorites = seededFavorites
        } else {
            load()
        }
    }

    func toggle(_ stationID: String) {
        if favorites.contains(stationID) {
            favorites.remove(stationID)
        } else {
            favorites.insert(stationID)
        }
        save()
    }

    func remove(_ stationID: String) {
        guard favorites.contains(stationID) else { return }
        favorites.remove(stationID)
        save()
    }

    func isFavorite(_ stationID: String) -> Bool {
        favorites.contains(stationID)
    }

    private func load() {
        guard seededFavorites == nil else { return }
        let raw = UserDefaults.standard.array(forKey: defaultsKey) as? [String] ?? []
        favorites = Set(raw)
    }

    private func save() {
        guard seededFavorites == nil else { return }
        UserDefaults.standard.set(Array(favorites), forKey: defaultsKey)
    }

    private static func resolveSeededFavorites() -> Set<String>? {
        let environment = ProcessInfo.processInfo.environment
        guard let rawFavorites = environment["WOLADEN_SCREENSHOT_FAVORITES"] else {
            return nil
        }

        let favorites = rawFavorites
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }

        return Set(favorites)
    }
}
