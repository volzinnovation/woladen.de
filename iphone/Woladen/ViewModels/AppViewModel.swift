import Foundation
import CoreLocation

@MainActor
final class AppViewModel: ObservableObject {
    enum AppTab: Hashable {
        case list
        case map
        case favorites
    }

    @Published private(set) var allFeatures: [GeoJSONFeature] = []
    @Published private(set) var discoveredFeatures: [GeoJSONFeature] = []
    @Published private(set) var operators: [OperatorEntry] = []
    @Published var filterState = FilterState()
    @Published var selectedFeature: GeoJSONFeature?
    @Published var selectedTab: AppTab = .list
    @Published private(set) var loadError: String?
    @Published private(set) var isLoading: Bool = false
    @Published private(set) var activeBundleInfo: ActiveDataBundleInfo?

    private let maxVisibleChargers = 20
    private var filterPool: [GeoJSONFeature] = []
    private var discoveredByID: [String: GeoJSONFeature] = [:]
    private var discoveredOrder: [String] = []
    private var didSeedFromUserLocation = false

    func load(userLocation: CLLocation?) {
        isLoading = true
        loadError = nil

        DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            let result: Result<(features: [GeoJSONFeature], operators: [OperatorEntry], bundle: ActiveDataBundleInfo), Error>
            do {
                result = .success(try ChargerRepository().loadData())
            } catch {
                result = .failure(error)
            }

            DispatchQueue.main.async {
                guard let self else { return }
                self.isLoading = false
                switch result {
                case .success(let loaded):
                    self.allFeatures = loaded.features
                    self.operators = loaded.operators
                    self.activeBundleInfo = loaded.bundle
                    self.loadError = nil
                    self.didSeedFromUserLocation = false
                    self.applyFilters(userLocation: userLocation)
                case .failure(let error):
                    self.loadError = error.localizedDescription
                    self.allFeatures = []
                    self.filterPool = []
                    self.discoveredFeatures = []
                    self.operators = []
                }
            }
        }
    }

    func reloadDataAfterBundleUpdate(userLocation: CLLocation?) {
        load(userLocation: userLocation)
    }

    func applyFilters(userLocation: CLLocation?) {
        filterPool = allFeatures.filter { feature in
            let p = feature.properties
            if !filterState.operatorName.isEmpty && p.operatorName != filterState.operatorName {
                return false
            }
            if p.maxPowerKW < filterState.minPowerKW {
                return false
            }
            if !filterState.selectedAmenities.isEmpty {
                for key in filterState.selectedAmenities {
                    if (p.amenityCounts[key] ?? 0) <= 0 {
                        return false
                    }
                }
            }
            return true
        }
        resetDiscoveredList()
        didSeedFromUserLocation = false
        if let userLocation {
            didSeedFromUserLocation = true
            refreshNearby(center: userLocation.coordinate)
        }
    }

    func handleMapCenterChange(_ center: CLLocationCoordinate2D) {
        refreshNearby(center: center)
    }

    func seedFromInitialUserLocation(_ location: CLLocation?) {
        guard let location else { return }
        guard !allFeatures.isEmpty else { return }
        if !didSeedFromUserLocation {
            if discoveredFeatures.isEmpty {
                applyFilters(userLocation: location)
            } else {
                refreshNearby(center: location.coordinate)
            }
            didSeedFromUserLocation = true
        }
    }

    func favoritesFeatures(_ favorites: Set<String>, userLocation: CLLocation?) -> [GeoJSONFeature] {
        var items = allFeatures.filter { favorites.contains($0.properties.stationID) }
        if let userLocation {
            items.sort { lhs, rhs in
                distance(from: userLocation, to: lhs.coordinate) < distance(from: userLocation, to: rhs.coordinate)
            }
        }
        return items
    }

    func distanceText(from userLocation: CLLocation?, to coordinate: CLLocationCoordinate2D) -> String? {
        guard let userLocation else { return nil }
        let meters = distance(from: userLocation, to: coordinate)
        if meters >= 1000 { return String(format: "%.1f km", meters / 1000) }
        return "\(Int(meters.rounded())) m"
    }

    func markerTint(for feature: GeoJSONFeature) -> String {
        let total = feature.properties.amenitiesTotal
        if total > 10 { return "gold" }
        if total > 5 { return "silver" }
        if total > 0 { return "bronze" }
        return "gray"
    }

    func humanReadableBundleSource() -> String {
        guard let activeBundleInfo else { return "unbekannt" }
        if activeBundleInfo.source == "installed" {
            return "Installiertes Datenbundle (\(activeBundleInfo.manifest.version))"
        }
        return "In App gebundeltes Baseline-Datenbundle"
    }

    private func distance(from userLocation: CLLocation, to coordinate: CLLocationCoordinate2D) -> CLLocationDistance {
        let target = CLLocation(latitude: coordinate.latitude, longitude: coordinate.longitude)
        return userLocation.distance(from: target)
    }

    private func distance(from coordinate: CLLocationCoordinate2D, to target: CLLocationCoordinate2D) -> CLLocationDistance {
        let lhs = CLLocation(latitude: coordinate.latitude, longitude: coordinate.longitude)
        let rhs = CLLocation(latitude: target.latitude, longitude: target.longitude)
        return lhs.distance(from: rhs)
    }

    private func resetDiscoveredList() {
        discoveredByID = [:]
        discoveredOrder = []
        discoveredFeatures = []
    }

    private func refreshNearby(center: CLLocationCoordinate2D) {
        guard !filterPool.isEmpty else {
            discoveredFeatures = []
            return
        }

        let nearest = filterPool
            .map { feature in
                (feature: feature, distance: distance(from: center, to: feature.coordinate))
            }
            .sorted { lhs, rhs in lhs.distance < rhs.distance }
            .prefix(maxVisibleChargers)
            .map { $0.feature }

        for feature in nearest {
            if discoveredByID[feature.id] == nil {
                discoveredOrder.append(feature.id)
            }
            discoveredByID[feature.id] = feature
        }
        discoveredFeatures = discoveredOrder.compactMap { discoveredByID[$0] }
    }
}
