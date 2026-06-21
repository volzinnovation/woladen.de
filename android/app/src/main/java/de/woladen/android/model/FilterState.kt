package de.woladen.android.model

data class FilterState(
    val operatorName: String = "",
    val minPowerKw: Double = 50.0,
    val selectedAmenities: Set<String> = emptySet(),
    val amenityNameQuery: String = "",
    val availableOnly: Boolean = true,
    val currentlyOpenOnly: Boolean = false
) {
    val activeCount: Int
        get() {
            var count = 0
            if (operatorName.isNotEmpty()) count += 1
            if (minPowerKw > 0.0) count += 1
            count += selectedAmenities.size
            if (amenityNameQuery.isNotBlank()) count += 1
            if (availableOnly) count += 1
            if (currentlyOpenOnly) count += 1
            return count
        }

    val clearableState: FilterState
        get() = FilterState(
            operatorName = "",
            minPowerKw = 50.0,
            selectedAmenities = emptySet(),
            amenityNameQuery = "",
            availableOnly = false,
            currentlyOpenOnly = false
        )
}
