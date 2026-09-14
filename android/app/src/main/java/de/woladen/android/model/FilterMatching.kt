package de.woladen.android.model

import java.text.Normalizer
import java.util.Locale
import kotlin.math.roundToInt

private val combiningMarksRegex = "\\p{M}+".toRegex()
private val nonAlphanumericRegex = "[^\\p{L}\\p{N}]+".toRegex()

fun ChargerProperties.matches(filterState: FilterState): Boolean {
    val selectedOperators = filterState.normalizedOperatorNames
    if (selectedOperators.isNotEmpty() && operatorName !in selectedOperators) {
        return false
    }
    if (maxPowerKw < filterState.minPowerKw) {
        return false
    }
    if (filterState.minAmenityCount > 0.0 && amenitiesTotal < filterState.minAmenityCount.roundToInt()) {
        return false
    }
    if (filterState.availableOnly && !hasAvailableChargingPoint) {
        return false
    }
    if (filterState.currentlyOpenOnly && !hasOpenAmenity) {
        return false
    }
    if (filterState.selectedAmenities.isNotEmpty()) {
        for (key in filterState.selectedAmenities) {
            if ((amenityCounts[key] ?: 0) <= 0) {
                return false
            }
        }
    }
    return matchesAmenityNameQuery(filterState.amenityNameQuery)
}

val ChargerProperties.hasAvailableChargingPoint: Boolean
    get() = occupancyTotalEvses > 0 && occupancyAvailableEvses > 0

val ChargerProperties.hasOpenAmenity: Boolean
    get() = amenityExamples.any { isAmenityOpen(it.openingHours, countryCode = countryCode) }

fun ChargerProperties.matchesAmenityNameQuery(query: String): Boolean {
    val normalizedQuery = normalizeAmenityNameQuery(query)
    if (normalizedQuery.isEmpty()) {
        return true
    }

    return amenityExamples.any { example ->
        val name = example.name ?: return@any false
        normalizeAmenityNameQuery(name).contains(normalizedQuery)
    }
}

private fun normalizeAmenityNameQuery(value: String): String {
    if (value.isBlank()) {
        return ""
    }

    return Normalizer.normalize(value.trim(), Normalizer.Form.NFD)
        .lowercase(Locale.ROOT)
        .replace("ß", "ss")
        .replace(combiningMarksRegex, "")
        .replace(nonAlphanumericRegex, "")
}
