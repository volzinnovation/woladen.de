package de.woladen.android.model

data class FilterState(
    val selectedOperatorNames: Set<String> = emptySet(),
    val minPowerKw: Double = 50.0,
    val minAmenityCount: Double = 0.0,
    val selectedAmenities: Set<String> = emptySet(),
    val amenityNameQuery: String = "",
    val availableOnly: Boolean = true,
    val currentlyOpenOnly: Boolean = false,
    val routeMaxDistanceFromLocationKm: Double? = null
) {
    fun canonicalized(using: List<OperatorEntry>): FilterState {
        if (normalizedOperatorNames.isEmpty() || using.isEmpty()) return this
        val migrated = normalizedOperatorNames.mapNotNull { selected ->
            val exactID = using.firstOrNull { it.id.equals(selected, ignoreCase = true) }
            val namedEntry = using.firstOrNull { entry ->
                listOf(entry.name).plus(entry.aliases)
                    .any { candidate -> candidate.trim().equals(selected, ignoreCase = true) }
            }
            (exactID ?: namedEntry)?.id
        }.toSet()
        return if (migrated == normalizedOperatorNames) this else copy(selectedOperatorNames = migrated)
    }

    val operatorName: String
        get() = normalizedOperatorNames.singleOrNull().orEmpty()

    val normalizedOperatorNames: Set<String>
        get() = selectedOperatorNames
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .toSet()

    val activeCount: Int
        get() {
            var count = 0
            if (normalizedOperatorNames.isNotEmpty()) count += 1
            if (minPowerKw > 0.0) count += 1
            if (minAmenityCount > 0.0) count += 1
            count += selectedAmenities.size
            if (amenityNameQuery.isNotBlank()) count += 1
            if (availableOnly) count += 1
            if (currentlyOpenOnly) count += 1
            if (routeMaxDistanceFromLocationKm != null) count += 1
            return count
        }

    val clearableState: FilterState
        get() = FilterState(
            selectedOperatorNames = emptySet(),
            minPowerKw = 50.0,
            minAmenityCount = 0.0,
            selectedAmenities = emptySet(),
            amenityNameQuery = "",
            availableOnly = false,
            currentlyOpenOnly = false,
            routeMaxDistanceFromLocationKm = null
        )
}
