package de.woladen.android.model

import java.util.Locale

data class CatalogInfoSummary(
    val openStaticSummary: OpenStaticSummary?,
    val buildSummary: BundleBuildSummary?
) {
    val generatedAt: String
        get() = firstNonBlank(openStaticSummary?.generatedAt.orEmpty(), buildSummary?.run?.finishedAt.orEmpty())

    val stationCount: Int
        get() {
            val bundleCount = openStaticSummary?.bundle?.stationCount ?: 0
            if (bundleCount > 0) return bundleCount
            val countryTotal = countries.sumOf { it.stationCount }
            if (countryTotal > 0) return countryTotal
            return buildSummary?.records?.fullRegistryActiveStationsTotal ?: 0
        }

    val chargerCount: Int
        get() {
            val bundleCount = openStaticSummary?.bundle?.chargerCount ?: 0
            if (bundleCount > 0) return bundleCount
            val countryTotal = countries.sumOf { it.chargerCount }
            if (countryTotal > 0) return countryTotal
            return buildSummary?.records?.rawRows ?: 0
        }

    val countries: List<OpenStaticCountry>
        get() = openStaticSummary?.countries.orEmpty()

    val sources: List<OpenStaticSource>
        get() = openStaticSummary?.normalizedSources.orEmpty()

    fun sortedCountries(locale: Locale = Locale.getDefault()): List<OpenStaticCountry> =
        countries.sortedWith(
            compareBy<OpenStaticCountry> { it.localizedName(locale) }
                .thenBy { it.code }
        )

    fun countrySourceLinks(countryCode: String): List<InfoSourceLink> {
        val code = countryCode.trim().uppercase(Locale.ROOT)
        if (code == "DE") {
            return listOf(
                InfoSourceLink(
                    label = "Mobilithek",
                    urlString = "https://mobilithek.info/offers/842113170303512576"
                )
            )
        }

        val seen = linkedSetOf<String>()
        return sources
            .filter { it.countryCode == code }
            .mapNotNull { source ->
                val label = source.compactCountrySourceLabel
                if (label.isBlank()) return@mapNotNull null
                val key = "$label\u0001${source.sourceUrl}"
                if (!seen.add(key)) return@mapNotNull null
                InfoSourceLink(label = label, urlString = source.sourceUrl)
            }
    }

    fun dataSourceLinks(): List<InfoSourceLink> {
        val seen = linkedSetOf<String>()
        return sources
            .sortedWith(compareBy<OpenStaticSource> { "${it.countryCode}:${it.displayName}" })
            .mapNotNull { source ->
                val label = source.bundleSourceTitle
                if (label.isBlank()) return@mapNotNull null
                val key = "$label\u0001${source.sourceUrl}"
                if (!seen.add(key)) return@mapNotNull null
                InfoSourceLink(label = label, urlString = source.sourceUrl)
            }
    }
}

data class OpenStaticSummary(
    val bundle: OpenStaticBundle?,
    val countries: List<OpenStaticCountry>,
    val generatedAt: String,
    val schemaVersion: Int,
    private val rawSources: List<OpenStaticSource>
) {
    val normalizedSources: List<OpenStaticSource>
        get() {
            val seen = linkedSetOf<String>()
            return rawSources.filter { source ->
                val key = listOf(
                    source.countryCode,
                    source.sourceUid,
                    source.sourceUrl,
                    source.displayName
                ).joinToString("\u0001")
                seen.add(key) &&
                    (source.countryCode.isNotBlank() || source.displayName.isNotBlank() || source.sourceUrl.isNotBlank())
            }
        }
}

data class OpenStaticBundle(
    val stationCount: Int,
    val chargerCount: Int,
    val countryCount: Int,
    val schemaVersion: Int
)

data class OpenStaticCountry(
    val code: String,
    val name: String,
    val stationCount: Int,
    val chargerCount: Int,
    val fastStationCount: Int
) {
    fun localizedName(locale: Locale = Locale.getDefault()): String {
        val displayName = runCatching {
            Locale.Builder().setRegion(code).build().getDisplayCountry(locale)
        }.getOrNull().orEmpty()
        return firstNonBlank(displayName, name, code)
    }
}

data class OpenStaticSource(
    val countryCode: String,
    val sourceUid: String,
    val displayName: String,
    val sourceUrl: String,
    val license: String,
    val licenseUrl: String
) {
    val bundleSourceTitle: String
        get() {
            val rawLabel = firstNonBlank(displayName, sourceUid, sourceUrl)
            val label = stripLeadingCountryCode(rawLabel, countryCode)
            return if (countryCode.isBlank()) label else "$countryCode: $label"
        }

    val compactCountrySourceLabel: String
        get() = stripLeadingCountryCode(firstNonBlank(displayName, bundleSourceTitle), countryCode)
}

data class InfoSourceLink(
    val label: String,
    val urlString: String
)

data class BundleBuildSummary(
    val run: BundleBuildRun?,
    val records: BundleBuildRecords?
)

data class BundleBuildRun(
    val startedAt: String,
    val finishedAt: String
)

data class BundleBuildRecords(
    val rawRows: Int,
    val fullRegistryActiveStationsTotal: Int
)

private fun firstNonBlank(vararg values: String): String =
    values.firstOrNull { it.trim().isNotBlank() }?.trim().orEmpty()

private fun stripLeadingCountryCode(label: String, countryCode: String): String {
    val trimmed = label.trim()
    val code = countryCode.trim()
    if (trimmed.isBlank() || code.isBlank()) return trimmed
    return Regex("^${Regex.escape(code)}(?:\\s*:\\s*|\\s+)", RegexOption.IGNORE_CASE)
        .replace(trimmed, "")
        .trim()
}
