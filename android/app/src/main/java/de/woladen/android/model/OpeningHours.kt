package de.woladen.android.model

import de.woladen.android.R
import de.woladen.android.util.AppStrings
import java.time.DayOfWeek
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle
import java.util.Locale

enum class OpeningState {
    OPEN,
    CLOSED
}

data class OpeningEvaluation(
    val state: OpeningState,
    val nextChange: ZonedDateTime?
)

private val openingDays = listOf("Mo", "Tu", "We", "Th", "Fr", "Sa", "Su")
private val openingDayToken = "(?:Mo|Tu|We|Th|Fr|Sa|Su|PH)"
private val openingWeekdayToken = "(?:Mo|Tu|We|Th|Fr|Sa|Su)"
private val openingClauseRegex = Regex(
    "^((?:$openingDayToken)(?:\\s*-\\s*$openingWeekdayToken)?" +
        "(?:\\s*,\\s*(?:$openingDayToken)(?:\\s*-\\s*$openingWeekdayToken)?)*)\\s+(.+)$"
)
private val whitespaceRegex = Regex("\\s+")

private val openingCountryTimeZones = mapOf(
    "AT" to "Europe/Vienna", "BE" to "Europe/Brussels", "BG" to "Europe/Sofia", "CH" to "Europe/Zurich",
    "CY" to "Asia/Nicosia", "CZ" to "Europe/Prague", "DE" to "Europe/Berlin", "DK" to "Europe/Copenhagen",
    "EE" to "Europe/Tallinn", "ES" to "Europe/Madrid", "FI" to "Europe/Helsinki", "FR" to "Europe/Paris",
    "GR" to "Europe/Athens", "HR" to "Europe/Zagreb", "HU" to "Europe/Budapest", "IE" to "Europe/Dublin",
    "IT" to "Europe/Rome", "LT" to "Europe/Vilnius", "LU" to "Europe/Luxembourg", "LV" to "Europe/Riga",
    "MT" to "Europe/Malta", "NL" to "Europe/Amsterdam", "NO" to "Europe/Oslo", "PL" to "Europe/Warsaw",
    "PT" to "Europe/Lisbon", "RO" to "Europe/Bucharest", "SE" to "Europe/Stockholm", "SI" to "Europe/Ljubljana",
    "SK" to "Europe/Bratislava"
)

private val openingIso3ToIso2 = mapOf(
    "AUT" to "AT", "BEL" to "BE", "BGR" to "BG", "CHE" to "CH", "CYP" to "CY", "CZE" to "CZ",
    "DEU" to "DE", "DNK" to "DK", "EST" to "EE", "ESP" to "ES", "FIN" to "FI", "FRA" to "FR",
    "GRC" to "GR", "HRV" to "HR", "HUN" to "HU", "IRL" to "IE", "ITA" to "IT", "LTU" to "LT",
    "LUX" to "LU", "LVA" to "LV", "MLT" to "MT", "NLD" to "NL", "NOR" to "NO", "POL" to "PL",
    "PRT" to "PT", "ROU" to "RO", "SVK" to "SK", "SVN" to "SI", "SWE" to "SE"
)

private enum class OpeningScheduleMode {
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

private data class OpeningClause(
    val selectedDays: Set<String>?,
    val matchesPublicHoliday: Boolean,
    val mode: OpeningScheduleMode,
    val ranges: List<OpeningRange>
)

private data class OpeningContext(
    val zone: ZoneId,
    val publicHolidays: Set<LocalDate>,
    val uncertainPublicHolidays: Set<LocalDate>
)

private data class OpeningNowParts(
    val dayKey: String,
    val previousDayKey: String,
    val minuteOfDay: Int,
    val isPublicHoliday: Boolean,
    val previousDayIsPublicHoliday: Boolean,
    val isUncertainPublicHoliday: Boolean,
    val previousDayIsUncertainPublicHoliday: Boolean
)

fun openingTimeZone(countryCode: String?): ZoneId {
    val normalized = normalizeOpeningCountryCode(countryCode)
    return openingCountryTimeZones[normalized]?.let(ZoneId::of) ?: ZoneId.systemDefault()
}

fun evaluateOpeningHours(
    openingHours: String?,
    now: ZonedDateTime = ZonedDateTime.now(),
    timeZone: ZoneId? = null,
    countryCode: String? = null
): OpeningEvaluation? {
    val normalized = openingHours?.trim()?.replace(whitespaceRegex, " ").orEmpty()
    if (normalized.isBlank()) return null
    if (normalized.equals("24/7", ignoreCase = true) || normalized.equals("open", ignoreCase = true)) {
        return OpeningEvaluation(OpeningState.OPEN, null)
    }
    if (normalized.matches(Regex("(?i)^(?:off|closed)$"))) {
        return OpeningEvaluation(OpeningState.CLOSED, null)
    }

    val rawClauses = normalized.split(';', limit = Int.MAX_VALUE)
    val clauses = rawClauses.map { parseOpeningClause(it) }
    if (clauses.any { it == null } || clauses.any { it!!.mode == OpeningScheduleMode.UNKNOWN }) return null
    val parsedClauses = clauses.filterNotNull()
    if (parsedClauses.isEmpty()) return null

    val normalizedCountry = normalizeOpeningCountryCode(countryCode)
    val usesPublicHolidays = parsedClauses.any { it.matchesPublicHoliday }
    if (usesPublicHolidays && normalizedCountry != "DE") return null

    val zone = timeZone ?: openingTimeZone(countryCode)
    val localNow = now.withZoneSameInstant(zone)
    val context = openingContext(localNow, zone, usesPublicHolidays)
    val currentMode = openingMode(parsedClauses, localNow, context)
    val state = when (currentMode) {
        OpeningScheduleMode.OPEN -> OpeningState.OPEN
        OpeningScheduleMode.CLOSED -> OpeningState.CLOSED
        OpeningScheduleMode.TIMES, OpeningScheduleMode.UNKNOWN -> return null
    }
    val nextChange = nextOpeningChange(parsedClauses, localNow, currentMode, context)
    return OpeningEvaluation(state, nextChange)
}

fun isAmenityOpen(
    openingHours: String?,
    now: ZonedDateTime = ZonedDateTime.now(),
    countryCode: String? = null
): Boolean = evaluateOpeningHours(openingHours, now = now, countryCode = countryCode)?.state == OpeningState.OPEN

fun formatAmenityOpeningHours(
    openingHours: String?,
    now: ZonedDateTime = ZonedDateTime.now(),
    timeZone: ZoneId? = null,
    countryCode: String? = null,
    locale: Locale = Locale.getDefault()
): String? {
    val raw = openingHours?.trim().orEmpty()
    if (raw.isBlank()) return null
    val evaluation = evaluateOpeningHours(raw, now = now, timeZone = timeZone, countryCode = countryCode)
        ?: return raw
    val stateText = when (evaluation.state) {
        OpeningState.OPEN -> AppStrings.get(R.string.i18n_amenity_open)
        OpeningState.CLOSED -> AppStrings.get(R.string.i18n_amenity_closed)
    }
    val nextChange = evaluation.nextChange ?: return stateText

    val zone = timeZone ?: openingTimeZone(countryCode)
    val localNow = now.withZoneSameInstant(zone)
    val localNext = nextChange.withZoneSameInstant(zone)
    val time = DateTimeFormatter.ofLocalizedTime(FormatStyle.SHORT)
        .withLocale(locale)
        .format(localNext)
    val sameDay = localNow.toLocalDate() == localNext.toLocalDate()
    val transitionResource = when {
        sameDay && evaluation.state == OpeningState.OPEN -> R.string.i18n_amenity_opening_closes_at
        sameDay -> R.string.i18n_amenity_opening_opens_at
        evaluation.state == OpeningState.OPEN -> R.string.i18n_amenity_opening_closes_on_at
        else -> R.string.i18n_amenity_opening_opens_on_at
    }
    val replacements = mutableMapOf("time" to time)
    if (!sameDay) {
        replacements["day"] = DateTimeFormatter.ofPattern("EEE", locale).format(localNext)
    }
    val transition = AppStrings.get(transitionResource, replacements)
    return "$stateText · $transition"
}

private fun parseOpeningClause(value: String): OpeningClause? {
    val trimmed = value.trim()
    if (trimmed.isBlank()) return null
    val match = openingClauseRegex.find(trimmed)
    val selector = match?.groupValues?.getOrNull(1)
    val body = match?.groupValues?.getOrNull(2)?.trim() ?: trimmed
    val selected = parseOpeningSelector(selector)
    if (body.matches(Regex("(?i)^(?:off|closed)$"))) {
        return OpeningClause(selected.days, selected.matchesPublicHoliday, OpeningScheduleMode.CLOSED, emptyList())
    }
    if (body.equals("open", ignoreCase = true)) {
        return OpeningClause(selected.days, selected.matchesPublicHoliday, OpeningScheduleMode.OPEN, emptyList())
    }
    val rawRanges = body.split(',', limit = Int.MAX_VALUE)
    val ranges = rawRanges.mapNotNull(::parseOpeningRange)
    val mode = if (ranges.isNotEmpty() && ranges.size == rawRanges.size) {
        OpeningScheduleMode.TIMES
    } else {
        OpeningScheduleMode.UNKNOWN
    }
    return OpeningClause(selected.days, selected.matchesPublicHoliday, mode, ranges)
}

private data class OpeningSelector(val days: Set<String>?, val matchesPublicHoliday: Boolean)

private fun parseOpeningSelector(selector: String?): OpeningSelector {
    if (selector.isNullOrBlank()) return OpeningSelector(null, false)
    val selected = linkedSetOf<String>()
    var matchesPublicHoliday = false
    for (rawPart in selector.split(',')) {
        val part = rawPart.trim()
        if (part == "PH") {
            matchesPublicHoliday = true
            continue
        }
        if ("-" in part) {
            val bounds = part.split('-').map(String::trim)
            val start = openingDays.indexOf(bounds.getOrNull(0))
            val end = openingDays.indexOf(bounds.getOrNull(1))
            if (start < 0 || end < 0) continue
            for (offset in openingDays.indices) {
                val index = (start + offset) % openingDays.size
                selected += openingDays[index]
                if (index == end) break
            }
        } else if (part in openingDays) {
            selected += part
        }
    }
    return OpeningSelector(selected, matchesPublicHoliday)
}

private fun parseOpeningRange(value: String): OpeningRange? {
    val compact = value.replace(whitespaceRegex, "")
    if (compact.endsWith('+') && '-' !in compact) {
        val start = parseOpeningMinute(compact.dropLast(1)) ?: return null
        return OpeningRange(start, 24 * 60, openEnded = true)
    }
    val pieces = compact.replace("+", "").split('-')
    if (pieces.size != 2) return null
    val start = parseOpeningMinute(pieces[0]) ?: return null
    val end = parseOpeningMinute(pieces[1]) ?: return null
    return OpeningRange(start, end, openEnded = false)
}

private fun parseOpeningMinute(value: String): Int? {
    val pieces = value.split(':')
    if (pieces.size != 2) return null
    val hour = pieces[0].toIntOrNull() ?: return null
    val minute = pieces[1].toIntOrNull() ?: return null
    if (hour !in 0..24 || minute !in 0..59 || (hour == 24 && minute != 0)) return null
    return hour * 60 + minute
}

private fun openingMode(
    clauses: List<OpeningClause>,
    now: ZonedDateTime,
    context: OpeningContext
): OpeningScheduleMode {
    val parts = openingNowParts(now, context)
    if (parts.isUncertainPublicHoliday) return OpeningScheduleMode.UNKNOWN
    if (parts.previousDayIsUncertainPublicHoliday && clauses.any {
            it.matchesPublicHoliday && it.ranges.any { range -> range.openEnded || range.start >= range.end }
        }) return OpeningScheduleMode.UNKNOWN

    var currentState: OpeningScheduleMode? = null
    for (clause in clauses) {
        openingState(clause, parts.dayKey, parts.isPublicHoliday, parts.minuteOfDay, previousDay = false)?.let {
            currentState = it
        }
    }
    if (currentState == OpeningScheduleMode.OPEN || currentState == OpeningScheduleMode.UNKNOWN) {
        return currentState ?: OpeningScheduleMode.UNKNOWN
    }
    if (clauses.any {
            openingState(
                it,
                parts.previousDayKey,
                parts.previousDayIsPublicHoliday,
                parts.minuteOfDay,
                previousDay = true
            ) == OpeningScheduleMode.OPEN
        }) return OpeningScheduleMode.OPEN
    return currentState ?: OpeningScheduleMode.CLOSED
}

private fun openingState(
    clause: OpeningClause,
    dayKey: String,
    isPublicHoliday: Boolean,
    minuteOfDay: Int,
    previousDay: Boolean
): OpeningScheduleMode? {
    if (!(isPublicHoliday && clause.matchesPublicHoliday)) {
        if (clause.selectedDays != null && dayKey !in clause.selectedDays) return null
    }
    return when (clause.mode) {
        OpeningScheduleMode.CLOSED -> if (previousDay) null else OpeningScheduleMode.CLOSED
        OpeningScheduleMode.OPEN -> if (previousDay) null else OpeningScheduleMode.OPEN
        OpeningScheduleMode.UNKNOWN -> if (previousDay) null else OpeningScheduleMode.UNKNOWN
        OpeningScheduleMode.TIMES -> {
            if (clause.ranges.any { isWithinOpeningRange(it, minuteOfDay, previousDay) }) {
                OpeningScheduleMode.OPEN
            } else if (previousDay) {
                null
            } else {
                OpeningScheduleMode.CLOSED
            }
        }
    }
}

private fun isWithinOpeningRange(range: OpeningRange, minuteOfDay: Int, previousDay: Boolean): Boolean {
    if (range.openEnded) return if (previousDay) minuteOfDay < 6 * 60 else minuteOfDay >= range.start
    if (range.start == range.end) return if (previousDay) minuteOfDay < range.end else minuteOfDay >= range.start
    if (range.start < range.end) return !previousDay && minuteOfDay >= range.start && minuteOfDay < range.end
    return if (previousDay) minuteOfDay < range.end else minuteOfDay >= range.start
}

private fun openingNowParts(now: ZonedDateTime, context: OpeningContext): OpeningNowParts {
    val dayIndex = now.dayOfWeek.value - 1
    val previousDate = now.toLocalDate().minusDays(1)
    return OpeningNowParts(
        dayKey = openingDays[dayIndex],
        previousDayKey = openingDays[(dayIndex + 6) % openingDays.size],
        minuteOfDay = now.hour * 60 + now.minute,
        isPublicHoliday = now.toLocalDate() in context.publicHolidays,
        previousDayIsPublicHoliday = previousDate in context.publicHolidays,
        isUncertainPublicHoliday = now.toLocalDate() in context.uncertainPublicHolidays,
        previousDayIsUncertainPublicHoliday = previousDate in context.uncertainPublicHolidays
    )
}

private fun nextOpeningChange(
    clauses: List<OpeningClause>,
    from: ZonedDateTime,
    currentMode: OpeningScheduleMode,
    context: OpeningContext
): ZonedDateTime? {
    val horizon = from.plusDays(8)
    val today = from.toLocalDate()
    val candidates = linkedSetOf<ZonedDateTime>()
    for (dayOffset in -1..8) {
        val day = today.plusDays(dayOffset.toLong())
        candidates += day.atStartOfDay(context.zone)
        for (clause in clauses) {
            if (clause.mode != OpeningScheduleMode.TIMES) continue
            for (range in clause.ranges) {
                candidates += openingDate(day, range.start, context.zone)
                val endDay = if (range.openEnded || range.start >= range.end) day.plusDays(1) else day
                candidates += openingDate(endDay, if (range.openEnded) 6 * 60 else range.end, context.zone)
            }
        }
    }
    return candidates
        .asSequence()
        .filter { it.isAfter(from) && !it.isAfter(horizon) }
        .sorted()
        .firstOrNull { candidate ->
            val candidateMode = openingMode(clauses, candidate, context)
            candidateMode == OpeningScheduleMode.UNKNOWN || candidateMode != currentMode
        }
        ?.takeUnless { openingMode(clauses, it, context) == OpeningScheduleMode.UNKNOWN }
}

private fun openingDate(day: LocalDate, minuteOfDay: Int, zone: ZoneId): ZonedDateTime {
    if (minuteOfDay >= 24 * 60) return day.plusDays(1).atStartOfDay(zone)
    return day.atStartOfDay(zone).plusMinutes(minuteOfDay.toLong())
}

private fun openingContext(now: ZonedDateTime, zone: ZoneId, includesGermanPublicHolidays: Boolean): OpeningContext {
    if (!includesGermanPublicHolidays) return OpeningContext(zone, emptySet(), emptySet())
    val years = (now.year - 1)..(now.year + 2)
    val national = years.flatMap { germanNationalPublicHolidays(it) }.toSet()
    val regional = years.flatMap { germanRegionalPublicHolidays(it) }.toSet()
    return OpeningContext(zone, national, regional - national)
}

private fun germanNationalPublicHolidays(year: Int): Set<LocalDate> {
    val easter = germanEasterSunday(year)
    return buildSet {
        add(LocalDate.of(year, 1, 1))
        add(LocalDate.of(year, 5, 1))
        add(LocalDate.of(year, 10, 3))
        add(LocalDate.of(year, 12, 25))
        add(LocalDate.of(year, 12, 26))
        add(easter.minusDays(2))
        add(easter.plusDays(1))
        add(easter.plusDays(39))
        add(easter.plusDays(50))
    }
}

private fun germanRegionalPublicHolidays(year: Int): Set<LocalDate> {
    val easter = germanEasterSunday(year)
    return buildSet {
        add(LocalDate.of(year, 1, 6))
        add(LocalDate.of(year, 3, 8))
        add(LocalDate.of(year, 8, 8))
        add(LocalDate.of(year, 8, 15))
        add(LocalDate.of(year, 9, 20))
        add(LocalDate.of(year, 10, 31))
        add(LocalDate.of(year, 11, 1))
        add(easter)
        add(easter.plusDays(49))
        add(easter.plusDays(60))
        for (day in 16..22) {
            val date = LocalDate.of(year, 11, day)
            if (date.dayOfWeek == DayOfWeek.WEDNESDAY) {
                add(date)
                break
            }
        }
    }
}

private fun germanEasterSunday(year: Int): LocalDate {
    val a = year % 19
    val b = year / 100
    val c = year % 100
    val d = b / 4
    val e = b % 4
    val f = (b + 8) / 25
    val g = (b - f + 1) / 3
    val h = (19 * a + b - d - g + 15) % 30
    val i = c / 4
    val k = c % 4
    val l = (32 + 2 * e + 2 * i - h - k) % 7
    val m = (a + 11 * h + 22 * l) / 451
    val month = (h + l - 7 * m + 114) / 31
    val day = ((h + l - 7 * m + 114) % 31) + 1
    return LocalDate.of(year, month, day)
}

private fun normalizeOpeningCountryCode(countryCode: String?): String {
    val code = countryCode?.trim()?.uppercase(Locale.ROOT).orEmpty()
    return when {
        code.length == 2 -> code
        else -> openingIso3ToIso2[code].orEmpty()
    }
}
