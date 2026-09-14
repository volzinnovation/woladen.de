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

internal fun resolvedOperatorGroupIds(groupIds: List<String>, groupId: String): Set<String> {
    val normalized = groupIds.map { it.trim() }.filter { it.isNotBlank() }.toSet()
    if (normalized.isNotEmpty()) return normalized
    return groupId.trim().takeIf { it.isNotBlank() }?.let { setOf(it) }.orEmpty()
}
