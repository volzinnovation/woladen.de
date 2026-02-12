import Foundation
import Combine
import CoreLocation

@MainActor
final class ChargerViewModel: ObservableObject {
    @Published private(set) var allStations: [ChargerStation] = []
    @Published private(set) var filteredStations: [ChargerStation] = []
    @Published private(set) var favoriteStations: [ChargerStation] = []
    @Published private(set) var operators: [OperatorEntry] = []
    @Published private(set) var availableAmenityKeys: [String] = []
    @Published private(set) var buildMeta: BuildMeta = BuildMeta(generatedAt: nil, sourceURL: nil)

    @Published var selectedOperator: String = ""
    @Published var minPowerKW: Double = 50
    @Published var selectedAmenityKeys: Set<String> = []

    @Published var selectedStation: ChargerStation?
    @Published var isFilterSheetPresented: Bool = false
    @Published var isLoading: Bool = false
    @Published var errorMessage: String?

    @Published private(set) var favoriteIDs: Set<String>

    let locationManager: LocationManager

    private let repository: ChargerRepository
    private let favoritesStore: FavoritesStore
    private var cancellables: Set<AnyCancellable> = []

    init(
        repository: ChargerRepository = ChargerRepository(),
        favoritesStore: FavoritesStore = FavoritesStore(),
        locationManager: LocationManager = LocationManager()
    ) {
        self.repository = repository
        self.favoritesStore = favoritesStore
        self.locationManager = locationManager
        self.favoriteIDs = favoritesStore.load()
        bind()
    }

    func load() async {
        if isLoading { return }
        isLoading = true
        errorMessage = nil

        do {
            let dataset = try await repository.loadDataset()
            allStations = dataset.stations
            operators = dataset.operators.sorted { $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending }
            buildMeta = dataset.buildMeta
            availableAmenityKeys = discoverAmenityKeys(in: dataset.stations)
            recomputeStations()
        } catch {
            errorMessage = error.localizedDescription
        }

        isLoading = false
    }

    func requestLocation() {
        locationManager.requestAccessAndLocation()
    }

    func resetFilters() {
        selectedOperator = ""
        minPowerKW = 50
        selectedAmenityKeys = []
    }

    func isFavorite(_ station: ChargerStation) -> Bool {
        favoriteIDs.contains(station.id)
    }

    func toggleFavorite(_ station: ChargerStation) {
        if favoriteIDs.contains(station.id) {
            favoriteIDs.remove(station.id)
        } else {
            favoriteIDs.insert(station.id)
        }
        favoritesStore.save(favoriteIDs)
        updateFavoriteStations()
    }

    func setAmenity(_ key: String, enabled: Bool) {
        if enabled {
            selectedAmenityKeys.insert(key)
        } else {
            selectedAmenityKeys.remove(key)
        }
    }

    func distanceMeters(to station: ChargerStation) -> Double? {
        guard let userLocation = locationManager.location else { return nil }
        let stationLocation = CLLocation(latitude: station.latitude, longitude: station.longitude)
        return userLocation.distance(from: stationLocation)
    }

    func distanceText(for station: ChargerStation) -> String? {
        guard let meters = distanceMeters(to: station) else { return nil }
        if meters >= 1000 {
            return String(format: "%.1f km", meters / 1000.0)
        }
        return "\(Int(meters.rounded())) m"
    }

    var buildMetaText: String {
        let dateText: String
        if let generated = buildMeta.generatedAt {
            let formatter = DateFormatter()
            formatter.dateStyle = .medium
            formatter.timeStyle = .short
            dateText = formatter.string(from: generated)
        } else {
            dateText = "Unknown"
        }

        if let sourceURL = buildMeta.sourceURL, !sourceURL.isEmpty {
            return "Data: \(dateText)\nSource: \(sourceURL)"
        }
        return "Data: \(dateText)"
    }

    private func bind() {
        Publishers.CombineLatest3($selectedOperator, $minPowerKW, $selectedAmenityKeys)
            .sink { [weak self] _, _, _ in
                self?.recomputeStations()
            }
            .store(in: &cancellables)

        locationManager.$location
            .sink { [weak self] _ in
                self?.recomputeStations()
            }
            .store(in: &cancellables)
    }

    private func discoverAmenityKeys(in stations: [ChargerStation]) -> [String] {
        var keys: Set<String> = []
        for station in stations {
            for (key, count) in station.amenityCounts where count > 0 {
                keys.insert(key)
            }
        }
        return AmenityCatalog.preferredOrder(for: Array(keys))
    }

    private func recomputeStations() {
        var stations = allStations.filter { station in
            if !selectedOperator.isEmpty && station.operatorName != selectedOperator {
                return false
            }
            if station.maxPowerKW < minPowerKW {
                return false
            }
            if selectedAmenityKeys.isEmpty {
                return true
            }
            for key in selectedAmenityKeys {
                if station.amenityCount(for: key) <= 0 {
                    return false
                }
            }
            return true
        }

        sortByDistanceIfPossible(&stations)
        filteredStations = stations
        updateFavoriteStations()
    }

    private func updateFavoriteStations() {
        var favorites = allStations.filter { favoriteIDs.contains($0.id) }
        sortByDistanceIfPossible(&favorites)
        favoriteStations = favorites
    }

    private func sortByDistanceIfPossible(_ stations: inout [ChargerStation]) {
        guard locationManager.location != nil else { return }
        stations.sort {
            (distanceMeters(to: $0) ?? .greatestFiniteMagnitude) < (distanceMeters(to: $1) ?? .greatestFiniteMagnitude)
        }
    }
}
