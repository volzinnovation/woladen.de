package de.woladen.android.model

data class CatalogSourceManifest(
    val version: String,
    val generatedAt: String,
    val schema: String
)

data class ActiveCatalogSourceInfo(
    val source: String,
    val manifest: CatalogSourceManifest
)
