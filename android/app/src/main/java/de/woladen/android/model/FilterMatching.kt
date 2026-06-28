package de.woladen.android.model

import java.text.Normalizer
import java.time.ZoneId
import java.time.ZonedDateTime
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
    get() = amenityExamples.any { isAmenityOpen(it.openingHours) }

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

private val openingDayKeys = listOf("Mo", "Tu", "We", "Th", "Fr", "Sa", "Su")
private val openingDaySelectorRegex =
    "^((?:Mo|Tu|We|Th|Fr|Sa|Su)(?:\\s*-\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?(?:\\s*,\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su)(?:\\s*-\\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\\s+(.+)$".toRegex()

private data class OpeningNowParts(
    val dayKey: String,
    val previousDayKey: String,
    val minuteOfDay: Int
)

private data class OpeningClause(
    val selectedDays: Set<String>?,
    val mode: OpeningMode,
    val ranges: List<OpeningRange>
)

private enum class OpeningMode {
    OPEN,
    CLOSED,
    TIMES,
    UNKNOWN
}

private data class OpeningRange(
    val start: Int,
    val end: Int,
    val openEnded: Boolean
)

private fun isAmenityOpen(openingHours: String?, now: ZonedDateTime = ZonedDateTime.now(ZoneId.of("Europe/Berlin"))): Boolean {
    val normalized = openingHours
        ?.trim()
        ?.replace("\\s+".toRegex(), " ")
        .orEmpty()

    if (normalized.isBlank()) return false
    if (normalized.equals("24/7", ignoreCase = true)) return true
    if (normalized.matches("(?i)^(?:off|closed)$".toRegex())) return false
    if (normalized.equals("open", ignoreCase = true)) return true

    val parts = openingNowParts(now)
    val clauses = normalized
        .split(";")
        .mapNotNull { parseOpeningClause(it) }

    if (clauses.isEmpty()) return false

    var currentState: OpeningMode? = null
    for (clause in clauses) {
        openingState(clause, parts.dayKey, parts.minuteOfDay, previousDay = false)?.let {
            currentState = it
        }
    }
    if (currentState == OpeningMode.OPEN) return true
    if (currentState == OpeningMode.UNKNOWN) return false

    return clauses.any { clause ->
        openingState(clause, parts.previousDayKey, parts.minuteOfDay, previousDay = true) == OpeningMode.OPEN
    }
}

private fun openingNowParts(now: ZonedDateTime): OpeningNowParts {
    val dayIndex = now.dayOfWeek.value - 1
    return OpeningNowParts(
        dayKey = openingDayKeys[dayIndex],
        previousDayKey = openingDayKeys[(dayIndex + 6) % openingDayKeys.size],
        minuteOfDay = now.hour * 60 + now.minute
    )
}

private fun parseOpeningClause(value: String): OpeningClause? {
    val trimmed = value.trim()
    if (trimmed.isBlank()) return null

    val match = openingDaySelectorRegex.find(trimmed)
    val selector = match?.groupValues?.getOrNull(1)
    val body = match?.groupValues?.getOrNull(2)?.trim() ?: trimmed

    if (body.matches("(?i)^(?:off|closed)$".toRegex())) {
        return OpeningClause(selectedOpeningDays(selector), OpeningMode.CLOSED, emptyList())
    }
    if (body.equals("open", ignoreCase = true)) {
        return OpeningClause(selectedOpeningDays(selector), OpeningMode.OPEN, emptyList())
    }

    val ranges = body.split(",").mapNotNull { parseOpeningRange(it) }
    if (ranges.isEmpty()) {
        return OpeningClause(selectedOpeningDays(selector), OpeningMode.UNKNOWN, emptyList())
    }
    return OpeningClause(selectedOpeningDays(selector), OpeningMode.TIMES, ranges)
}

private fun selectedOpeningDays(selector: String?): Set<String>? {
    if (selector.isNullOrBlank()) return null
    val selected = linkedSetOf<String>()
    for (rawPart in selector.split(",")) {
        val part = rawPart.trim()
        if (part == "PH") continue
        if ("-" in part) {
            val bounds = part.split("-").map { it.trim() }
            val start = openingDayKeys.indexOf(bounds.getOrNull(0))
            val end = openingDayKeys.indexOf(bounds.getOrNull(1))
            if (start < 0 || end < 0) continue
            for (offset in openingDayKeys.indices) {
                val index = (start + offset) % openingDayKeys.size
                selected += openingDayKeys[index]
                if (index == end) break
            }
        } else if (part in openingDayKeys) {
            selected += part
        }
    }
    return selected
}

private fun parseOpeningRange(value: String): OpeningRange? {
    val compact = value.replace("\\s+".toRegex(), "")
    if (compact.endsWith("+") && "-" !in compact) {
        val start = parseOpeningMinute(compact.dropLast(1)) ?: return null
        return OpeningRange(start = start, end = 24 * 60, openEnded = true)
    }
    val parts = compact.replace("+", "").split("-")
    if (parts.size != 2) return null
    val start = parseOpeningMinute(parts[0]) ?: return null
    val end = parseOpeningMinute(parts[1]) ?: return null
    return OpeningRange(start = start, end = end, openEnded = false)
}

private fun parseOpeningMinute(value: String): Int? {
    val parts = value.split(":")
    if (parts.size != 2) return null
    val hour = parts[0].toIntOrNull() ?: return null
    val minute = parts[1].toIntOrNull() ?: return null
    if (hour !in 0..24 || minute !in 0..59 || (hour == 24 && minute != 0)) return null
    return hour * 60 + minute
}

private fun openingState(
    clause: OpeningClause,
    dayKey: String,
    minuteOfDay: Int,
    previousDay: Boolean
): OpeningMode? {
    val selectedDays = clause.selectedDays
    if (selectedDays != null && dayKey !in selectedDays) return null

    return when (clause.mode) {
        OpeningMode.CLOSED -> if (previousDay) null else OpeningMode.CLOSED
        OpeningMode.OPEN -> OpeningMode.OPEN
        OpeningMode.UNKNOWN -> if (previousDay) null else OpeningMode.UNKNOWN
        OpeningMode.TIMES -> {
            if (clause.ranges.any { isWithinOpeningRange(it, minuteOfDay, previousDay) }) {
                OpeningMode.OPEN
            } else if (previousDay) {
                null
            } else {
                OpeningMode.CLOSED
            }
        }
    }
}

private fun isWithinOpeningRange(range: OpeningRange, minuteOfDay: Int, previousDay: Boolean): Boolean {
    if (range.openEnded) {
        return if (previousDay) minuteOfDay < 6 * 60 else minuteOfDay >= range.start
    }
    if (range.start == range.end) return true
    if (range.start < range.end) {
        return !previousDay && minuteOfDay >= range.start && minuteOfDay < range.end
    }
    return if (previousDay) minuteOfDay < range.end else minuteOfDay >= range.start
}
