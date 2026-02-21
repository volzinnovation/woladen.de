import Foundation

@MainActor
final class FavoritesStore: ObservableObject {
    @Published private(set) var favorites: Set<String> = []

    private let defaultsKey = "woladen_favorites"

    init() {
        load()
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
        let raw = UserDefaults.standard.array(forKey: defaultsKey) as? [String] ?? []
        favorites = Set(raw)
    }

    private func save() {
        UserDefaults.standard.set(Array(favorites), forKey: defaultsKey)
    }
}
