package de.woladen.android.model

import java.text.Normalizer
import java.util.Locale

private val combiningMarksRegex = "\\p{M}+".toRegex()
private val nonAlphanumericRegex = "[^\\p{L}\\p{N}]+".toRegex()

fun ChargerProperties.matches(filterState: FilterState): Boolean {
    if (filterState.operatorName.isNotEmpty() && operatorName != filterState.operatorName) {
        return false
    }
    if (maxPowerKw < filterState.minPowerKw) {
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
