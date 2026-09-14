package de.woladen.android.store

import android.content.Context
import de.woladen.android.model.FilterState

/**
 * Read-through view of the phone filter preferences for non-Compose clients
 * such as Android Auto. AppViewModel remains the writer for the existing keys.
 */
class FilterStateStore(context: Context) {
    private val preferences = context.getSharedPreferences("woladen", Context.MODE_PRIVATE)

    var state: FilterState = load()
        private set

    fun refresh() {
        state = load()
    }

    private fun load(): FilterState {
        val selectedOperators = preferences
            .getStringSet(SELECTED_OPERATOR_NAMES_KEY, emptySet())
            .orEmpty()
            .map { it.trim() }
            .filter { it.isNotBlank() }
            .toSet()
            .ifEmpty {
                preferences.getString(OPERATOR_NAME_KEY, "").orEmpty()
                    .trim()
                    .takeIf { it.isNotBlank() }
                    ?.let { setOf(it) }
                    .orEmpty()
            }
        val routeRange = preferences.getFloat(ROUTE_MAX_DISTANCE_KM_KEY, -1f).toDouble()
        return FilterState(
            selectedOperatorNames = selectedOperators,
            minPowerKw = preferences.getFloat(MIN_POWER_KW_KEY, 50f).toDouble(),
            minAmenityCount = preferences.getFloat(MIN_AMENITY_COUNT_KEY, 0f).toDouble(),
            selectedAmenities = preferences.getStringSet(SELECTED_AMENITIES_KEY, emptySet()).orEmpty().toSet(),
            amenityNameQuery = preferences.getString(AMENITY_NAME_QUERY_KEY, "").orEmpty(),
            availableOnly = preferences.getBoolean(AVAILABLE_ONLY_KEY, true),
            currentlyOpenOnly = preferences.getBoolean(CURRENTLY_OPEN_ONLY_KEY, false),
            routeMaxDistanceFromLocationKm = routeRange.takeIf { it > 0.0 }?.coerceAtMost(400.0)
        )
    }

    private companion object {
        const val OPERATOR_NAME_KEY = "filter.operatorName"
        const val SELECTED_OPERATOR_NAMES_KEY = "filter.selectedOperatorNames"
        const val MIN_POWER_KW_KEY = "filter.minPowerKw"
        const val MIN_AMENITY_COUNT_KEY = "filter.minAmenityCount"
        const val SELECTED_AMENITIES_KEY = "filter.selectedAmenities"
        const val AMENITY_NAME_QUERY_KEY = "filter.amenityNameQuery"
        const val AVAILABLE_ONLY_KEY = "filter.availableOnly"
        const val CURRENTLY_OPEN_ONLY_KEY = "filter.currentlyOpenOnly"
        const val ROUTE_MAX_DISTANCE_KM_KEY = "filter.routeMaxDistanceFromLocationKm"
    }
}
