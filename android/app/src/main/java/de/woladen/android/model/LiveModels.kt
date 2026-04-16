package de.woladen.android.model

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.LinkedHashMap

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
    val ingestedAt: String
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

enum class AvailabilityStatus(val rawValue: String, val label: String) {
    FREE("free", "Frei"),
    OCCUPIED("occupied", "Belegt"),
    OUT_OF_ORDER("out_of_order", "Defekt"),
    UNKNOWN("unknown", "Unbekannt");

    companion object {
        fun fromRaw(value: String?): AvailabilityStatus {
            return when (value?.trim()?.lowercase()) {
                FREE.rawValue -> FREE
                OCCUPIED.rawValue -> OCCUPIED
                OUT_OF_ORDER.rawValue -> OUT_OF_ORDER
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

val GeoJsonFeature.occupancySummaryLabel: String?
    get() {
        val counts = availabilityCounts
        if (counts.total <= 0) return null

        val parts = buildList {
            if (counts.available > 0) add("${counts.available} frei")
            if (counts.occupied > 0) add("${counts.occupied} belegt")
            if (counts.outOfOrder > 0) add("${counts.outOfOrder} defekt")
            if (counts.unknown > 0) add("${counts.unknown} unbekannt")
        }
        return if (parts.isEmpty()) "Belegung unbekannt" else parts.joinToString(", ")
    }

val GeoJsonFeature.occupancySourceLabel: String?
    get() {
        if (liveSummaryForDisplay != null) {
            val provider = liveSourceLabel
            val timestamp = formatLiveTimestamp(liveObservedTimestamp)
            return when {
                !provider.isNullOrBlank() && !timestamp.isNullOrBlank() -> "Live via $provider • Stand $timestamp"
                !provider.isNullOrBlank() -> "Live via $provider"
                !timestamp.isNullOrBlank() -> "Live-Stand $timestamp"
                else -> "Live via lokaler API"
            }
        }

        val counts = availabilityCounts
        if (counts.total <= 0) return null
        return when {
            properties.occupancySourceName.startsWith("Mobilithek") -> "Live via ${properties.occupancySourceName}"
            properties.occupancySourceUid.startsWith("mobilithek_") && properties.occupancySourceName.isBlank() -> "Live via Mobilithek"
            properties.occupancySourceUid.startsWith("mobilithek_") -> "Live via Mobilithek (${properties.occupancySourceName})"
            properties.occupancySourceName.isBlank() -> "Live via MobiData BW"
            else -> "Live via MobiData BW (${properties.occupancySourceName})"
        }
    }

val GeoJsonFeature.liveUpdatedLabel: String?
    get() {
        if (liveSummaryForDisplay == null) return null
        return formatLiveTimestamp(liveObservedTimestamp)?.let { "Stand $it" }
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
                    formatLiveTimestamp(firstNonEmpty(evse.sourceObservedAt, evse.fetchedAt, evse.ingestedAt))
                        ?.let { "Seit $it" }
                ).joinToString(" • ")
                LiveEvseRow(
                    title = "Ladepunkt ${index + 1}",
                    status = evse.availabilityStatus,
                    meta = if (meta.isBlank()) "Live-Daten verfügbar" else meta,
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
                title = "Stationsstatus",
                status = availabilityStatus,
                meta = occupancySummaryLabel ?: "Live-Daten verfügbar",
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
        notes += LiveDetailNote(label = "Nächster Slot", value = nextSlot)
    }
    val supplemental = formatLiveCollection(evse.supplementalFacilityStatus)
    if (supplemental.isNotBlank()) {
        notes += LiveDetailNote(label = "Zusatzstatus", value = supplemental)
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

private fun formatLiveTimestamp(value: String): String? {
    val raw = value.trim()
    if (raw.isBlank()) return null
    return try {
        val instant = Instant.parse(raw)
        LIVE_TIMESTAMP_FORMATTER.format(instant)
    } catch (_: Exception) {
        raw
    }
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
        is LiveJsonValue.BoolValue -> if (value.value) "Ja" else "Nein"
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
                val label = LIVE_DYNAMIC_KEY_LABELS[entry.key] ?: humanizeLiveCode(entry.key)
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

private val LIVE_DYNAMIC_KEY_LABELS = mapOf(
    "expectedAvailableFromTime" to "Ab",
    "expectedAvailableToTime" to "Bis",
    "expectedAvailableUntilTime" to "Bis",
    "startTime" to "Ab",
    "endTime" to "Bis",
    "lastUpdated" to "Seit",
    "value" to ""
)

private val LIVE_TIMESTAMP_FORMATTER: DateTimeFormatter = DateTimeFormatter
    .ofPattern("dd.MM.yyyy, HH:mm")
    .withZone(ZoneId.systemDefault())
