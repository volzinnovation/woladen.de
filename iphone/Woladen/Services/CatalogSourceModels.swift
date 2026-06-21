import Foundation

struct CatalogSourceManifest: Codable {
    let version: String
    let generatedAt: String
    let schema: String
}

struct ActiveCatalogSourceInfo {
    let source: String
    let manifest: CatalogSourceManifest
}
