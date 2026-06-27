import Foundation

let favoriteCategoryUncategorized = "__uncategorized__"
let favoriteFilterAll = "__all__"

private let favoriteMetadataVersion = 2
private let maxFavoriteCategoryLength = 48
private let maxFavoriteCategories = 12

struct FavoriteItem: Codable, Equatable {
    var stationID: String
    var categories: [String]
    var createdAt: String
    var updatedAt: String
    var source: String

    enum CodingKeys: String, CodingKey {
        case stationID = "station_id"
        case categories
        case createdAt = "created_at"
        case updatedAt = "updated_at"
        case source
    }
}

private struct FavoriteMetadataPayload: Codable {
    let version: Int
    let items: [String: FavoriteItem]
}

@MainActor
final class FavoritesStore: ObservableObject {
    @Published private(set) var favorites: Set<String> = []
    @Published private(set) var items: [String: FavoriteItem] = [:]

    private let defaultsKey = "woladen_favorites_v2"
    private let legacyDefaultsKey = "woladen_favorites"
    private let seededFavorites: Set<String>?
    private var needsDeferredMigrationSave = false

    init() {
        seededFavorites = FavoritesStore.resolveSeededFavorites()
        if let seededFavorites {
            let timestamp = Self.nowISO8601()
            favorites = seededFavorites
            items = Dictionary(
                uniqueKeysWithValues: seededFavorites.map { stationID in
                    return (
                        stationID,
                        FavoriteItem(
                            stationID: stationID,
                            categories: [],
                            createdAt: timestamp,
                            updatedAt: timestamp,
                            source: "migration"
                        )
                    )
                }
            )
        } else {
            load()
            scheduleDeferredMigrationSaveIfNeeded()
        }
    }

    func toggle(_ stationID: String) {
        let id = normalizedStationID(stationID)
        guard !id.isEmpty else { return }
        if favorites.contains(id) {
            items.removeValue(forKey: id)
        } else {
            _ = ensureFavoriteItem(id, source: "manual")
        }
        syncFavorites()
        save()
    }

    func remove(_ stationID: String) {
        let id = normalizedStationID(stationID)
        guard favorites.contains(id) else { return }
        items.removeValue(forKey: id)
        syncFavorites()
        save()
    }

    func isFavorite(_ stationID: String) -> Bool {
        favorites.contains(normalizedStationID(stationID))
    }

    func categories(for stationID: String) -> [String] {
        items[normalizedStationID(stationID)]?.categories ?? []
    }

    func sortedCategories() -> [String] {
        var categories: [String] = []
        var seen: Set<String> = []
        for item in items.values {
            for category in normalizeCategories(item.categories, knownCategories: categories) {
                let key = categoryKey(category)
                guard seen.insert(key).inserted else { continue }
                categories.append(category)
            }
        }
        return categories.sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
    }

    func addCategory(_ category: String, to stationID: String, source: String = "manual") {
        let id = normalizedStationID(stationID)
        guard !id.isEmpty else { return }
        let label = normalizeCategoryLabel(category)
        guard !label.isEmpty else { return }

        let knownCategories = sortedCategories()
        var item = ensureFavoriteItem(id, source: source)
        item.categories = normalizeCategories(item.categories + [label], knownCategories: knownCategories)
        item.updatedAt = Self.nowISO8601()
        item.source = normalizedSource(item.source, fallback: source)
        items[id] = item
        syncFavorites()
        save()
    }

    func removeCategory(_ category: String, from stationID: String) {
        let id = normalizedStationID(stationID)
        let key = categoryKey(category)
        guard var item = items[id], !key.isEmpty else { return }
        item.categories.removeAll { categoryKey($0) == key }
        item.updatedAt = Self.nowISO8601()
        items[id] = item
        save()
    }

    func addRouteFavorites(stationIDs: [String], category: String) {
        let label = normalizeCategoryLabel(category)
        guard !label.isEmpty else { return }
        let knownCategories = sortedCategories()
        for stationID in stationIDs {
            let id = normalizedStationID(stationID)
            guard !id.isEmpty else { continue }
            var item = ensureFavoriteItem(id, source: "route")
            item.categories = normalizeCategories(item.categories + [label], knownCategories: knownCategories)
            item.updatedAt = Self.nowISO8601()
            item.source = normalizedSource(item.source, fallback: "route")
            items[id] = item
        }
        syncFavorites()
        save()
    }

    func categorySuggestions(query: String, excluding excluded: [String]) -> [String] {
        let excludedKeys = Set(excluded.map(categoryKey))
        let normalizedQuery = categoryKey(query)
        let categories = sortedCategories().filter { !excludedKeys.contains(categoryKey($0)) }
        if normalizedQuery.isEmpty {
            return Array(categories.prefix(6))
        }
        let prefix = categories.filter { categoryKey($0).hasPrefix(normalizedQuery) }
        let substring = categories.filter {
            let key = categoryKey($0)
            return !key.hasPrefix(normalizedQuery) && key.contains(normalizedQuery)
        }
        return Array((prefix + substring).prefix(6))
    }

    func favoriteIDs(matching filter: String) -> Set<String> {
        if filter == favoriteFilterAll {
            return favorites
        }
        if filter == favoriteCategoryUncategorized {
            return Set(items.values.filter { $0.categories.isEmpty }.map(\.stationID))
        }
        return Set(
            items.values
                .filter { item in item.categories.contains { categoryKey($0) == filter } }
                .map(\.stationID)
        )
    }

    func count(matching filter: String) -> Int {
        favoriteIDs(matching: filter).count
    }

    private func load() {
        guard seededFavorites == nil else { return }
        let defaults = UserDefaults.standard
        if let data = defaults.data(forKey: defaultsKey),
           let payload = try? JSONDecoder().decode(FavoriteMetadataPayload.self, from: data),
           payload.version == favoriteMetadataVersion {
            items = normalizeItems(payload.items)
            syncFavorites()
            return
        }

        let raw = defaults.array(forKey: legacyDefaultsKey) as? [String] ?? []
        let timestamp = Self.nowISO8601()
        items = Dictionary(
            uniqueKeysWithValues: Set(raw.map(normalizedStationID).filter { !$0.isEmpty }).map { stationID in
                (
                    stationID,
                    FavoriteItem(
                        stationID: stationID,
                        categories: [],
                        createdAt: timestamp,
                        updatedAt: timestamp,
                        source: "migration"
                    )
                )
            }
        )
        syncFavorites()
        needsDeferredMigrationSave = true
    }

    private func save() {
        guard seededFavorites == nil else { return }
        let knownCategories = sortedCategories()
        let payload = FavoriteMetadataPayload(
            version: favoriteMetadataVersion,
            items: Dictionary(
                uniqueKeysWithValues: items
                    .sorted { $0.key < $1.key }
                    .map { key, item in
                        (
                            key,
                            FavoriteItem(
                                stationID: key,
                                categories: normalizeCategories(item.categories, knownCategories: knownCategories),
                                createdAt: item.createdAt,
                                updatedAt: item.updatedAt,
                                source: normalizedSource(item.source)
                            )
                        )
                    }
            )
        )
        guard let data = try? JSONEncoder().encode(payload) else { return }
        UserDefaults.standard.set(data, forKey: defaultsKey)
    }

    private func ensureFavoriteItem(_ stationID: String, source: String) -> FavoriteItem {
        if let existing = items[stationID] {
            return existing
        }
        let timestamp = Self.nowISO8601()
        let item = FavoriteItem(
            stationID: stationID,
            categories: [],
            createdAt: timestamp,
            updatedAt: timestamp,
            source: normalizedSource(source)
        )
        items[stationID] = item
        return item
    }

    private func syncFavorites() {
        favorites = Set(items.keys)
    }

    private func scheduleDeferredMigrationSaveIfNeeded() {
        guard needsDeferredMigrationSave else { return }
        Task { @MainActor [weak self] in
            try? await Task.sleep(nanoseconds: 1_500_000_000)
            guard let self, self.needsDeferredMigrationSave else { return }
            self.needsDeferredMigrationSave = false
            self.save()
        }
    }

    private func normalizeItems(_ source: [String: FavoriteItem]) -> [String: FavoriteItem] {
        var normalized: [String: FavoriteItem] = [:]
        var knownCategories: [String] = []
        let fallbackTimestamp = Self.nowISO8601()
        for item in source.values {
            let stationID = normalizedStationID(item.stationID)
            guard !stationID.isEmpty else { continue }
            let categories = normalizeCategories(item.categories, knownCategories: knownCategories)
            knownCategories = normalizeCategories(knownCategories + categories)
            normalized[stationID] = FavoriteItem(
                stationID: stationID,
                categories: categories,
                createdAt: cleanTimestamp(item.createdAt, fallback: fallbackTimestamp),
                updatedAt: cleanTimestamp(item.updatedAt, fallback: item.createdAt),
                source: normalizedSource(item.source)
            )
        }
        return normalized
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

    private static func nowISO8601() -> String {
        ISO8601DateFormatter().string(from: Date())
    }
}

func normalizeCategoryLabel(_ value: String) -> String {
    String(value.replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .prefix(maxFavoriteCategoryLength))
}

func categoryKey(_ value: String) -> String {
    normalizeCategoryLabel(value).lowercased()
}

private func normalizedStationID(_ value: String) -> String {
    value.trimmingCharacters(in: .whitespacesAndNewlines)
}

private func normalizeCategories(_ value: [String], knownCategories: [String] = []) -> [String] {
    var displayByKey: [String: String] = [:]
    for category in knownCategories {
        let label = normalizeCategoryLabel(category)
        let key = categoryKey(label)
        if !label.isEmpty, displayByKey[key] == nil {
            displayByKey[key] = label
        }
    }

    var categories: [String] = []
    var seen: Set<String> = []
    for item in value {
        let label = normalizeCategoryLabel(item)
        let key = categoryKey(label)
        guard !label.isEmpty, seen.insert(key).inserted else { continue }
        categories.append(displayByKey[key] ?? label)
    }
    return Array(categories.prefix(maxFavoriteCategories))
}

private func normalizedSource(_ value: String, fallback: String = "manual") -> String {
    let source = value.trimmingCharacters(in: .whitespacesAndNewlines)
    return ["manual", "route", "migration"].contains(source) ? source : fallback
}

private func cleanTimestamp(_ value: String, fallback: String) -> String {
    let text = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !text.isEmpty else { return fallback }
    let formatter = ISO8601DateFormatter()
    return formatter.date(from: text) == nil ? fallback : text
}
