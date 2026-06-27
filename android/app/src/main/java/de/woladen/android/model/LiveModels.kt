package de.woladen.android.model

import android.text.format.DateUtils
import de.woladen.android.R
import de.woladen.android.util.AppStrings
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.FormatStyle
import java.util.LinkedHashMap
import java.util.Locale

data class LiveStationLookupResponse(
    val stations: List<LiveStationSummary>,
    val missingStationIds: List<String>
)

data class LiveStationDetail(
    val station: LiveStationSummary,
    val evses: List<LiveEvse>
)

data class LiveStationSummary(
    val stationId: String,
    val availabilityStatus: AvailabilityStatus,
    val availableEvses: Int,
    val occupiedEvses: Int,
    val outOfOrderEvses: Int,
    val unknownEvses: Int,
    val totalEvses: Int,
    val priceDisplay: String,
    val priceCurrency: String,
    val priceEnergyEurKwhMin: String,
    val priceEnergyEurKwhMax: String,
    val sourceObservedAt: String,
    val fetchedAt: String,
    val ingestedAt: String,
    val dailyAnalysisDataAvailable: Boolean = false,
    val frequentlyOutOfOrderDailyAnalysis: Boolean = false,
    val frequentlyOccupiedDailyAnalysis: Boolean = false,
    val dailyAnalysisOutOfOrderColor: String = "",
    val dailyAnalysisOccupiedColor: String = ""
)

data class LiveEvse(
    val providerEvseId: String,
    val availabilityStatus: AvailabilityStatus,
    val operationalStatus: String,
    val priceDisplay: String,
    val sourceObservedAt: String,
    val fetchedAt: String,
    val ingestedAt: String,
    val nextAvailableChargingSlots: List<LiveJsonValue>,
    val supplementalFacilityStatus: List<LiveJsonValue>
)

sealed interface LiveJsonValue {
    data class StringValue(val value: String) : LiveJsonValue
    data class NumberValue(val value: Double) : LiveJsonValue
    data class BoolValue(val value: Boolean) : LiveJsonValue
    data class ObjectValue(val entries: LinkedHashMap<String, LiveJsonValue>) : LiveJsonValue
    data class ArrayValue(val items: List<LiveJsonValue>) : LiveJsonValue
    data object NullValue : LiveJsonValue
}

enum class AvailabilityStatus(val rawValue: String) {
    FREE("free"),
    OCCUPIED("occupied"),
    OUT_OF_ORDER("out_of_order"),
    UNKNOWN("unknown");

    val label: String
        get() = when (this) {
            FREE -> AppStrings.get(R.string.i18n_availability_free)
            OCCUPIED -> AppStrings.get(R.string.i18n_availability_occupied)
            OUT_OF_ORDER -> AppStrings.get(R.string.i18n_availability_out_of_order)
            UNKNOWN -> AppStrings.get(R.string.i18n_availability_unknown)
        }

    companion object {
        fun fromRaw(value: String?): AvailabilityStatus {
            return when (value?.trim()?.lowercase()) {
                FREE.rawValue, "available" -> FREE
                OCCUPIED.rawValue, "charging", "in_use", "inuse" -> OCCUPIED
                OUT_OF_ORDER.rawValue, "outoforder", "faulted", "unavailable" -> OUT_OF_ORDER
                else -> UNKNOWN
            }
        }
    }
}

data class AvailabilityCounts(
    val total: Int,
    val available: Int,
    val occupied: Int,
    val outOfOrder: Int,
    val unknown: Int
)

enum class StationCardState {
    DEFAULT,
    UNKNOWN,
    OUT_OF_ORDER,
    OCCUPIED,
    ONE_FREE_LEFT,
    OFTEN_BROKEN,
    OFTEN_OCCUPIED
}

data class LiveDetailNote(
    val label: String,
    val value: String
)

data class LiveEvseRow(
    val title: String,
    val status: AvailabilityStatus,
    val meta: String,
    val price: String,
    val notes: List<LiveDetailNote>
)

val GeoJsonFeature.displayPrice: String
    get() {
        val livePrice = liveSummaryForDisplay?.priceDisplay?.trim().orEmpty()
        if (livePrice.isNotBlank()) {
            return livePrice
        }

        val liveDetailPrice = liveDetail
            ?.evses
            ?.map { it.priceDisplay.trim() }
            ?.firstOrNull { it.isNotBlank() }
            .orEmpty()
        if (liveDetailPrice.isNotBlank()) {
            return liveDetailPrice
        }

        return properties.priceDisplay.trim()
    }

val GeoJsonFeature.availabilityCounts: AvailabilityCounts
    get() {
        val live = liveSummaryForDisplay
        if (live != null) {
            return AvailabilityCounts(
                total = live.totalEvses,
                available = live.availableEvses,
                occupied = live.occupiedEvses,
                outOfOrder = live.outOfOrderEvses,
                unknown = live.unknownEvses
            )
        }

        return AvailabilityCounts(
            total = properties.occupancyTotalEvses,
            available = properties.occupancyAvailableEvses,
            occupied = properties.occupancyOccupiedEvses,
            outOfOrder = properties.occupancyOutOfOrderEvses,
            unknown = properties.occupancyUnknownEvses
        )
    }

val GeoJsonFeature.availabilityStatus: AvailabilityStatus
    get() {
        liveSummaryForDisplay?.let { return it.availabilityStatus }
        val counts = availabilityCounts
        return when {
            counts.available > 0 -> AvailabilityStatus.FREE
            counts.occupied > 0 -> AvailabilityStatus.OCCUPIED
            counts.total > 0 && counts.outOfOrder >= counts.total -> AvailabilityStatus.OUT_OF_ORDER
            else -> AvailabilityStatus.UNKNOWN
        }
    }

val LiveStationSummary.isOftenBrokenFromDailyAnalysis: Boolean
    get() = frequentlyOutOfOrderDailyAnalysis ||
        normalizedDailyAnalysisColor(dailyAnalysisOutOfOrderColor) in setOf("sehr_hellrot", "hellrot")

val LiveStationSummary.isOftenOccupiedFromDailyAnalysis: Boolean
    get() = frequentlyOccupiedDailyAnalysis ||
        normalizedDailyAnalysisColor(dailyAnalysisOccupiedColor) == "hellgrau"

val GeoJsonFeature.isOftenBrokenFromDailyAnalysis: Boolean
    get() = liveSummaryForDisplay?.isOftenBrokenFromDailyAnalysis == true

val GeoJsonFeature.isOftenOccupiedFromDailyAnalysis: Boolean
    get() = liveSummaryForDisplay?.isOftenOccupiedFromDailyAnalysis == true

val GeoJsonFeature.stationCardState: StationCardState
    get() {
        val counts = availabilityCounts
        val hasAvailability = counts.total > 0
        return when {
            hasAvailability && availabilityStatus == AvailabilityStatus.OUT_OF_ORDER -> StationCardState.OUT_OF_ORDER
            hasAvailability && availabilityStatus == AvailabilityStatus.OCCUPIED -> StationCardState.OCCUPIED
            hasAvailability && counts.total > 1 && counts.available == 1 -> StationCardState.ONE_FREE_LEFT
            isOftenBrokenFromDailyAnalysis -> StationCardState.OFTEN_BROKEN
            isOftenOccupiedFromDailyAnalysis -> StationCardState.OFTEN_OCCUPIED
            !hasAvailability || availabilityStatus == AvailabilityStatus.UNKNOWN -> StationCardState.UNKNOWN
            else -> StationCardState.DEFAULT
        }
    }

val GeoJsonFeature.occupancySummaryLabel: String?
    get() {
        val counts = availabilityCounts
        if (counts.total <= 0) return null

        val parts = buildList {
            if (counts.available > 0) add(AppStrings.count(R.string.i18n_availability_available, counts.available))
            if (counts.occupied > 0) add(AppStrings.count(R.string.i18n_availability_occupiedcount, counts.occupied))
            if (counts.outOfOrder > 0) add(AppStrings.count(R.string.i18n_availability_outofordercount, counts.outOfOrder))
            if (counts.unknown > 0) add(AppStrings.count(R.string.i18n_availability_unknowncount, counts.unknown))
        }
        return if (parts.isEmpty()) AppStrings.get(R.string.i18n_availability_summaryunknown) else parts.joinToString(", ")
    }

val GeoJsonFeature.occupancySourceLabel: String?
    get() {
        if (liveSummaryForDisplay != null) {
            val provider = liveSourceLabel
            val elapsed = formatElapsedLiveTime(liveObservedTimestamp)
            return when {
                !provider.isNullOrBlank() && !elapsed.isNullOrBlank() -> liveViaUpdated(provider, elapsed)
                !provider.isNullOrBlank() -> liveVia(provider)
                !elapsed.isNullOrBlank() -> updatedLabel(elapsed)
                else -> AppStrings.get(R.string.i18n_station_live)
            }
        }

        val counts = availabilityCounts
        if (counts.total <= 0) return null
        return when {
            properties.occupancySourceName.startsWith("Mobilithek") -> liveVia(properties.occupancySourceName)
            properties.occupancySourceUid.startsWith("mobilithek_") && properties.occupancySourceName.isBlank() -> liveVia("Mobilithek")
            properties.occupancySourceUid.startsWith("mobilithek_") -> liveVia("Mobilithek (${properties.occupancySourceName})")
            properties.occupancySourceName.isBlank() -> liveVia("MobiData BW")
            else -> liveVia("MobiData BW (${properties.occupancySourceName})")
        }
    }

val GeoJsonFeature.liveUpdatedLabel: String?
    get() {
        if (liveSummaryForDisplay == null) return null
        return formatElapsedLiveTime(liveObservedTimestamp)?.let { updatedLabel(it) }
    }

val GeoJsonFeature.hasPrimaryDetailHighlights: Boolean
    get() = displayPrice.isNotBlank() || properties.openingHoursDisplay.isNotBlank()

val GeoJsonFeature.liveEvseRows: List<LiveEvseRow>
    get() {
        val detail = liveDetail
        if (detail != null && detail.evses.isNotEmpty()) {
            return detail.evses.mapIndexed { index, evse ->
                val meta = listOfNotNull(
                    formatEvseCode(evse.providerEvseId),
                    formatElapsedLiveTime(firstNonEmpty(evse.sourceObservedAt, evse.fetchedAt, evse.ingestedAt))
                        ?.let { updatedLabel(it) }
                ).joinToString(" • ")
                LiveEvseRow(
                    title = AppStrings.get(R.string.i18n_station_evse, mapOf("index" to (index + 1).toString())),
                    status = evse.availabilityStatus,
                    meta = if (meta.isBlank()) AppStrings.get(R.string.i18n_station_livedataavailable) else meta,
                    price = evse.priceDisplay.trim(),
                    notes = buildLiveNotes(evse)
                )
            }
        }

        if (liveSummaryForDisplay == null) {
            return emptyList()
        }

        return listOf(
            LiveEvseRow(
                title = AppStrings.get(R.string.i18n_station_stationstatus),
                status = availabilityStatus,
                meta = occupancySummaryLabel ?: AppStrings.get(R.string.i18n_station_livedataavailable),
                price = displayPrice,
                notes = emptyList()
            )
        )
    }

private val GeoJsonFeature.liveSummaryForDisplay: LiveStationSummary?
    get() = liveDetail?.station ?: liveSummary

private val GeoJsonFeature.liveObservedTimestamp: String
    get() = firstNonEmpty(
        liveSummaryForDisplay?.sourceObservedAt.orEmpty(),
        liveSummaryForDisplay?.fetchedAt.orEmpty(),
        liveSummaryForDisplay?.ingestedAt.orEmpty()
    )

private fun normalizedDailyAnalysisColor(value: String): String {
    return value
        .trim()
        .lowercase(Locale.ROOT)
        .replace(Regex("[\\s-]+"), "_")
}

private val GeoJsonFeature.liveSourceLabel: String?
    get() {
        val source = firstNonEmpty(properties.detailSourceName, properties.detailSourceUid)
        val formatted = formatProviderLabel(source)
        return formatted.ifBlank { null }
    }

private fun buildLiveNotes(evse: LiveEvse): List<LiveDetailNote> {
    val notes = mutableListOf<LiveDetailNote>()
    val nextSlot = formatLiveCollection(evse.nextAvailableChargingSlots)
    if (nextSlot.isNotBlank()) {
        notes += LiveDetailNote(label = AppStrings.get(R.string.i18n_station_nextslot), value = nextSlot)
    }
    val supplemental = formatLiveCollection(evse.supplementalFacilityStatus)
    if (supplemental.isNotBlank()) {
        notes += LiveDetailNote(label = AppStrings.get(R.string.i18n_station_supplementalstatus), value = supplemental)
    }
    return notes
}

private fun formatProviderLabel(value: String): String {
    return value
        .trim()
        .removePrefix("mobilithek_")
        .removeSuffix("_static")
        .removeSuffix("-json")
        .replace('_', ' ')
}

private fun formatEvseCode(value: String): String? {
    val raw = value.trim()
    if (raw.isBlank()) return null
    return if (raw.length <= 20) raw else "${raw.take(10)}…${raw.takeLast(6)}"
}

internal fun formatElapsedLiveTime(value: String, now: Instant = Instant.now()): String? {
    val raw = value.trim()
    if (raw.isBlank()) return null
    val instant = parseLiveInstant(raw) ?: return null

    if (!AppStrings.isInitialized()) {
        return formatFallbackElapsedLiveTime(instant, now)
    }
    return DateUtils.getRelativeTimeSpanString(
        instant.toEpochMilli(),
        now.toEpochMilli(),
        DateUtils.SECOND_IN_MILLIS,
        DateUtils.FORMAT_ABBREV_RELATIVE
    ).toString()
}

private fun formatFallbackElapsedLiveTime(instant: Instant, now: Instant): String {
    val elapsedSeconds = Duration.between(instant, now).seconds.coerceAtLeast(0)
    return when {
        elapsedSeconds < 60 -> "now"
        elapsedSeconds < 60 * 60 -> "${elapsedSeconds / 60} min ago"
        elapsedSeconds < 60 * 60 * 24 -> "${elapsedSeconds / (60 * 60)} h ago"
        elapsedSeconds < 60L * 60 * 24 * 30 -> {
            val days = elapsedSeconds / (60 * 60 * 24)
            if (days == 1L) "1 day ago" else "$days days ago"
        }
        elapsedSeconds < 60L * 60 * 24 * 365 -> "${elapsedSeconds / (60L * 60 * 24 * 30)} mo ago"
        else -> "${elapsedSeconds / (60L * 60 * 24 * 365)} y ago"
    }
}

private fun formatLiveTimestamp(value: String): String? {
    val raw = value.trim()
    if (raw.isBlank()) return null
    val instant = parseLiveInstant(raw) ?: return raw
    return LIVE_TIMESTAMP_FORMATTER.format(instant)
}

private fun parseLiveInstant(raw: String): Instant? {
    return runCatching { Instant.parse(raw) }.getOrNull()
        ?: runCatching { OffsetDateTime.parse(raw).toInstant() }.getOrNull()
}

private fun firstNonEmpty(vararg values: String): String {
    return values.firstOrNull { it.trim().isNotBlank() }?.trim().orEmpty()
}

private fun formatLiveCollection(values: List<LiveJsonValue>): String {
    return values.map(::formatLiveValue).filter { it.isNotBlank() }.joinToString(" • ")
}

private fun formatLiveValue(value: LiveJsonValue): String {
    return when (value) {
        LiveJsonValue.NullValue -> ""
        is LiveJsonValue.BoolValue -> AppStrings.get(if (value.value) R.string.i18n_common_yes else R.string.i18n_common_no)
        is LiveJsonValue.NumberValue -> {
            val numeric = value.value
            if (numeric % 1.0 == 0.0) numeric.toInt().toString() else numeric.toString()
        }
        is LiveJsonValue.StringValue -> {
            val raw = value.value.trim()
            if (raw.isBlank()) {
                ""
            } else {
                formatLiveTimestamp(raw)?.takeIf { it != raw } ?: humanizeLiveCode(raw)
            }
        }
        is LiveJsonValue.ArrayValue -> value.items.map(::formatLiveValue).filter { it.isNotBlank() }.joinToString(" • ")
        is LiveJsonValue.ObjectValue -> {
            val entries = value.entries.entries.filter { formatLiveValue(it.value).isNotBlank() }
            if (entries.size == 1 && entries.first().key == "value") {
                return formatLiveValue(entries.first().value)
            }
            entries.joinToString(", ") { entry ->
                val formatted = formatLiveValue(entry.value)
                val label = liveDynamicKeyLabel(entry.key) ?: humanizeLiveCode(entry.key)
                if (label.isBlank()) formatted else "$label: $formatted"
            }
        }
    }
}

private fun humanizeLiveCode(value: String): String {
    val spaced = value
        .replace(Regex("([a-z0-9])([A-Z])"), "$1 $2")
        .replace('_', ' ')
        .replace('-', ' ')
        .trim()
    if (spaced.isBlank()) return ""
    return spaced.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
}

private fun liveVia(source: String): String =
    AppStrings.get(R.string.i18n_station_livevia, mapOf("source" to source))

private fun liveViaUpdated(source: String, date: String): String =
    AppStrings.get(R.string.i18n_station_liveviaupdated, mapOf("source" to source, "date" to date))

private fun updatedLabel(date: String): String =
    AppStrings.get(R.string.i18n_station_updated, mapOf("date" to date))

private fun liveDynamicKeyLabel(key: String): String? {
    return when (key) {
        "expectedAvailableFromTime",
        "expectedAvailableToTime",
        "expectedAvailableUntilTime",
        "startTime",
        "endTime" -> AppStrings.get(R.string.i18n_station_nextslot)
        "lastUpdated" -> AppStrings.get(R.string.i18n_station_updated, mapOf("date" to "")).trim()
        "value" -> ""
        else -> null
    }
}

private val LIVE_TIMESTAMP_FORMATTER: DateTimeFormatter = DateTimeFormatter
    .ofLocalizedDateTime(FormatStyle.SHORT)
    .withLocale(Locale.getDefault())
    .withZone(ZoneId.systemDefault())
