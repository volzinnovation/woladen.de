package de.woladen.android.model

data class OperatorCatalog(
    val generatedAt: String?,
    val minStations: Int,
    val totalOperators: Int,
    val operators: List<OperatorEntry>
)

data class OperatorEntry(
    val name: String,
    val stations: Int,
    val id: String = name,
    val aliases: List<String> = emptyList()
)
