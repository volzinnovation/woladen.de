import Foundation

struct DataBundleManifest: Codable {
    let version: String
    let generatedAt: String
    let schema: String

    static let baseline = DataBundleManifest(
        version: "baseline",
        generatedAt: "unknown",
        schema: "chargers_fast.geojson+operators.json"
    )
}

struct ActiveDataBundleInfo {
    let source: String
    let directory: URL
    let manifest: DataBundleManifest
}

enum DataBundleError: LocalizedError {
    case missingBaseline
    case malformedBundle(String)

    var errorDescription: String? {
        switch self {
        case .missingBaseline:
            return "Bundled baseline data is missing."
        case .malformedBundle(let reason):
            return "Invalid data bundle: \(reason)"
        }
    }
}

final class DataBundleManager {
    static let shared = DataBundleManager()

    private let fm = FileManager.default
    private let baselineFolder: String = "Data/baseline"
    private let activeFolderName = "WoladenDataBundle/current"

    private init() {}

    func activeBundleInfo() throws -> ActiveDataBundleInfo {
        if let custom = try loadCustomBundleIfValid() {
            return custom
        }
        let baselineURL = try resolveBundledBaselineDirectory()
        let manifest = (try? loadManifest(at: baselineURL)) ?? DataBundleManifest.baseline
        return ActiveDataBundleInfo(source: "baseline", directory: baselineURL, manifest: manifest)
    }

    func bundleFileURL(named fileName: String) throws -> URL {
        let info = try activeBundleInfo()
        return info.directory.appendingPathComponent(fileName)
    }

    func installBundle(from selectedDirectory: URL) throws {
        let chargers = selectedDirectory.appendingPathComponent("chargers_fast.geojson")
        let operators = selectedDirectory.appendingPathComponent("operators.json")
        guard fm.fileExists(atPath: chargers.path) else {
            throw DataBundleError.malformedBundle("chargers_fast.geojson missing")
        }
        guard fm.fileExists(atPath: operators.path) else {
            throw DataBundleError.malformedBundle("operators.json missing")
        }

        let support = try appSupportRoot()
        let destination = support.appendingPathComponent(activeFolderName, isDirectory: true)
        if fm.fileExists(atPath: destination.path) {
            try fm.removeItem(at: destination)
        }
        try fm.createDirectory(at: destination, withIntermediateDirectories: true)

        try fm.copyItem(at: chargers, to: destination.appendingPathComponent("chargers_fast.geojson"))
        try fm.copyItem(at: operators, to: destination.appendingPathComponent("operators.json"))

        let manifestSource = selectedDirectory.appendingPathComponent("data_manifest.json")
        if fm.fileExists(atPath: manifestSource.path) {
            try fm.copyItem(at: manifestSource, to: destination.appendingPathComponent("data_manifest.json"))
        } else {
            let fallback = DataBundleManifest(
                version: "local-import-\(ISO8601DateFormatter().string(from: Date()))",
                generatedAt: ISO8601DateFormatter().string(from: Date()),
                schema: "chargers_fast.geojson+operators.json"
            )
            let data = try JSONEncoder().encode(fallback)
            try data.write(to: destination.appendingPathComponent("data_manifest.json"), options: .atomic)
        }

        _ = try loadManifest(at: destination)
    }

    func removeInstalledBundle() throws {
        let support = try appSupportRoot()
        let destination = support.appendingPathComponent(activeFolderName, isDirectory: true)
        if fm.fileExists(atPath: destination.path) {
            try fm.removeItem(at: destination)
        }
    }

    private func loadCustomBundleIfValid() throws -> ActiveDataBundleInfo? {
        let support = try appSupportRoot()
        let path = support.appendingPathComponent(activeFolderName, isDirectory: true)
        guard fm.fileExists(atPath: path.path) else { return nil }

        let chargers = path.appendingPathComponent("chargers_fast.geojson")
        let operators = path.appendingPathComponent("operators.json")
        guard fm.fileExists(atPath: chargers.path), fm.fileExists(atPath: operators.path) else {
            return nil
        }

        let manifest = (try? loadManifest(at: path)) ?? DataBundleManifest(
            version: "custom",
            generatedAt: "unknown",
            schema: "chargers_fast.geojson+operators.json"
        )
        return ActiveDataBundleInfo(source: "installed", directory: path, manifest: manifest)
    }

    private func loadManifest(at directory: URL) throws -> DataBundleManifest {
        let url = directory.appendingPathComponent("data_manifest.json")
        guard fm.fileExists(atPath: url.path) else {
            throw DataBundleError.malformedBundle("data_manifest.json missing")
        }
        let data = try Data(contentsOf: url)
        return try JSONDecoder().decode(DataBundleManifest.self, from: data)
    }

    private func resolveBundledBaselineDirectory() throws -> URL {
        let candidateSubdirectories = [
            "Data/baseline",
            "Resources/Data/baseline",
            "baseline"
        ]

        var candidates: [URL] = []
        if let resourceURL = Bundle.main.resourceURL {
            candidates.append(resourceURL.appendingPathComponent(baselineFolder))
            for subdirectory in candidateSubdirectories where subdirectory != baselineFolder {
                candidates.append(resourceURL.appendingPathComponent(subdirectory))
            }
        }

        if let explicit = Bundle.main.url(forResource: "chargers_fast", withExtension: "geojson", subdirectory: "Data/baseline") {
            candidates.append(explicit.deletingLastPathComponent())
        }
        if let explicit = Bundle.main.url(forResource: "chargers_fast", withExtension: "geojson", subdirectory: "Resources/Data/baseline") {
            candidates.append(explicit.deletingLastPathComponent())
        }
        if let flat = Bundle.main.url(forResource: "chargers_fast", withExtension: "geojson") {
            candidates.append(flat.deletingLastPathComponent())
        }

        for directory in candidates {
            let chargers = directory.appendingPathComponent("chargers_fast.geojson")
            let operators = directory.appendingPathComponent("operators.json")
            if fm.fileExists(atPath: chargers.path), fm.fileExists(atPath: operators.path) {
                return directory
            }
        }

        throw DataBundleError.missingBaseline
    }

    private func appSupportRoot() throws -> URL {
        let root = try fm.url(for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
        let bundleRoot = root.appendingPathComponent("Woladen", isDirectory: true)
        if !fm.fileExists(atPath: bundleRoot.path) {
            try fm.createDirectory(at: bundleRoot, withIntermediateDirectories: true)
        }
        return bundleRoot
    }
}
