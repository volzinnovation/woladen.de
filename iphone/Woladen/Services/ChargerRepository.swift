import Foundation

final class ChargerRepository {
    private let decoder = JSONDecoder()

    func loadData() throws -> (features: [GeoJSONFeature], operators: [OperatorEntry], bundle: ActiveDataBundleInfo) {
        let bundle = try DataBundleManager.shared.activeBundleInfo()
        let chargersURL = bundle.directory.appendingPathComponent("chargers_fast.geojson")
        let operatorsURL = bundle.directory.appendingPathComponent("operators.json")
        let chargersData = try Data(contentsOf: chargersURL, options: [.mappedIfSafe])
        let operatorsData = try Data(contentsOf: operatorsURL, options: [.mappedIfSafe])

        let featureCollection = try decoder.decode(
            GeoJSONFeatureCollection.self,
            from: chargersData
        )
        let operatorCatalog = try decoder.decode(
            OperatorCatalog.self,
            from: operatorsData
        )

        let sortedOperators = operatorCatalog.operators.sorted { lhs, rhs in
            if lhs.stations == rhs.stations { return lhs.name < rhs.name }
            return lhs.stations > rhs.stations
        }

        return (featureCollection.features, sortedOperators, bundle)
    }
}
