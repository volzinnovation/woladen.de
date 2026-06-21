package de.woladen.android.ui

import android.content.Intent
import android.net.Uri
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.unit.dp
import de.woladen.android.R
import de.woladen.android.model.CatalogInfoSummary
import de.woladen.android.model.InfoSourceLink
import de.woladen.android.model.OpenStaticCountry
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.util.formatTimestamp
import de.woladen.android.viewmodel.AppViewModel
import java.text.NumberFormat
import java.util.Locale

private const val PRIVACY_POLICY_URL = "https://woladen.de/privacy.html"
private const val IMPRINT_URL = "https://woladen.de/imprint.html"
private const val WEBSITE_URL = "https://woladen.de/"
private const val STUDIOS_URL = "https://studios.moonshots.gmbh/"
private const val GEOCODER_URL = "https://openrouteservice.org/dev/#/api-docs/geocode/autocomplete/get"
private const val EASTER_EGG_URL = "https://hellmood.111mb.de//wake_up_16b_writeup.html"

private val contributors = listOf(
    "Ramona Fleischer",
    "Johanna Thiele",
    "Greta Reutter",
    "Tara Golle",
    "Benedikt Schulz"
)

@Composable
fun InfoTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    onRequestLocationPermission: () -> Unit
) {
    val isDarkMode = isSystemInDarkTheme()
    val infoBackground = if (isDarkMode) Color(0xFF111114) else Color.White
    val infoForeground = if (isDarkMode) Color.White else Color(0xFF111114)
    val infoMuted = infoForeground.copy(alpha = 0.78f)
    val shouldShowLocationSection =
        locationService.authorizationStatus != LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE ||
            locationService.currentLocation == null

    LaunchedEffect(Unit) {
        viewModel.loadInfoSummaryIfNeeded()
    }

    Surface(
        modifier = Modifier
            .fillMaxSize()
            .testTag("info-root"),
        color = infoBackground,
        contentColor = infoForeground
    ) {
        Column(
            modifier = Modifier.fillMaxSize()
        ) {
            WoladenBrandIntro(showProductMessage = false)

            Column(
                modifier = Modifier
                    .weight(1f)
                    .verticalScroll(rememberScrollState())
                    .padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 84.dp),
                verticalArrangement = Arrangement.spacedBy(14.dp)
            ) {
                InfoSection(title = stringResource(R.string.i18n_info_legendtitle)) {
                    Text(
                        stringResource(R.string.i18n_info_cardbackgroundtitle),
                        style = MaterialTheme.typography.titleSmall
                    )
                    CardBackgroundLegendRow(cardOneFreeColor(isDarkMode), stringResource(R.string.i18n_info_legendonefreeleft))
                    CardBackgroundLegendRow(cardOccupiedColor(isDarkMode), stringResource(R.string.i18n_info_legendfullyoccupied))
                    CardBackgroundLegendRow(cardOutOfOrderColor(isDarkMode), stringResource(R.string.i18n_info_legendoutoforder))
                    CardBackgroundLegendRow(cardOftenBrokenColor(isDarkMode), stringResource(R.string.i18n_info_legendoftenbroken))
                    CardBackgroundLegendRow(cardOftenOccupiedColor(isDarkMode), stringResource(R.string.i18n_info_legendoftenoccupied))

                    Text(
                        stringResource(R.string.i18n_info_mapmarkertitle),
                        style = MaterialTheme.typography.titleSmall,
                        modifier = Modifier.padding(top = 8.dp)
                    )
                    MarkerLegendRow(Color(0xFFF59E0B), stringResource(R.string.i18n_info_legendgold))
                    MarkerLegendRow(Color(0xFF94A3B8), stringResource(R.string.i18n_info_legendsilver))
                    MarkerLegendRow(Color(0xFFB45309), stringResource(R.string.i18n_info_legendbronze))
                    MarkerLegendRow(infoMuted, stringResource(R.string.i18n_info_legendgrey))
                    FavoriteLegendRow(stringResource(R.string.i18n_info_legendfavorite))
                    MarkerOutOfOrderLegendRow(stringResource(R.string.i18n_info_legendmarkeroutoforder))
                    MarkerFullyOccupiedLegendRow(stringResource(R.string.i18n_info_legendmarkerfullyoccupied))
                }

                InfoSection(title = stringResource(R.string.i18n_info_abouttitle)) {
                    Text(aboutText(viewModel))
                    val generatedAt = viewModel.infoSummary?.generatedAt.orEmpty()
                    if (generatedAt.isNotBlank()) {
                        Text(
                            dataUpdatedText(generatedAt, viewModel.infoSummary),
                            color = infoMuted
                        )
                    }
                }

                InfoSection(title = stringResource(R.string.i18n_info_countriestitle)) {
                    val summary = viewModel.infoSummary
                    when {
                        viewModel.isLoadingInfoSummary && summary == null -> {
                            Row(
                                horizontalArrangement = Arrangement.spacedBy(10.dp),
                                verticalAlignment = Alignment.CenterVertically
                            ) {
                                CircularProgressIndicator(modifier = Modifier.size(18.dp), strokeWidth = 2.dp)
                                Text(stringResource(R.string.i18n_info_loadingcountries), color = infoMuted)
                            }
                        }
                        summary != null && summary.countries.isNotEmpty() -> {
                            summary.sortedCountries().forEach { country ->
                                CountryCoverageRow(country, summary, infoMuted)
                            }
                        }
                        else -> {
                            Text(
                                viewModel.infoSummaryError ?: stringResource(R.string.i18n_info_countryloaderror),
                                color = infoMuted
                            )
                            OutlinedButton(onClick = { viewModel.reloadInfoSummary() }) {
                                Text(stringResource(R.string.i18n_errors_reload))
                            }
                        }
                    }
                }

                InfoSection(title = stringResource(R.string.i18n_info_datasourcestitle)) {
                    LinkButton(stringResource(R.string.i18n_sources_geocoder), GEOCODER_URL)
                    LinkButton(stringResource(R.string.i18n_sources_easteregg), EASTER_EGG_URL)
                }

                InfoSection(title = stringResource(R.string.i18n_info_licensestitle)) {
                    Text(stringResource(R.string.i18n_info_osmnote))
                    LinkButton(stringResource(R.string.i18n_info_osmcopyright), "https://www.openstreetmap.org/copyright")
                    LinkButton(stringResource(R.string.i18n_info_odbllicense), "https://opendatacommons.org/licenses/odbl/1.0/")
                }

                InfoSection(title = stringResource(R.string.i18n_info_contacttitle)) {
                    Text(stringResource(R.string.i18n_info_developedby))
                    LinkButton(stringResource(R.string.i18n_info_github), "https://github.com/volzinnovation/woladen.de")
                    Text("${stringResource(R.string.i18n_info_distributedby)} Moonshots Studios GmbH")
                    LinkButton("woladen.de", WEBSITE_URL)
                    LinkButton("studios.moonshots.gmbh", STUDIOS_URL)
                    LinkButton(stringResource(R.string.i18n_info_imprintlink), IMPRINT_URL)
                }

                InfoSection(title = stringResource(R.string.i18n_info_contributorstitle)) {
                    Text(stringResource(R.string.i18n_info_studentsgroup), color = infoMuted)
                    contributors.forEach { contributor ->
                        Text(contributor)
                    }
                }

                InfoSection(title = stringResource(R.string.i18n_info_privacytitle)) {
                    Text(stringResource(R.string.i18n_info_privacybody))
                    LinkButton(stringResource(R.string.i18n_info_privacylink), PRIVACY_POLICY_URL)
                }

                if (shouldShowLocationSection) {
                    InfoSection(title = locationSectionTitle(locationService.authorizationStatus)) {
                        Text(locationStatusText(locationService.authorizationStatus))
                        Text(
                            locationMessageText(locationService.authorizationStatus),
                            color = infoMuted
                        )
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            OutlinedButton(
                                onClick = {
                                    if (locationService.authorizationStatus == LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE) {
                                        locationService.requestSingleLocation()
                                        locationService.startUpdates()
                                    } else {
                                        onRequestLocationPermission()
                                    }
                                },
                                modifier = Modifier.testTag("info-location-refresh-button")
                            ) {
                                Text(stringResource(R.string.i18n_location_retry))
                            }
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun aboutText(viewModel: AppViewModel): String {
    val stationCount = countText(
        summaryValue = viewModel.infoSummary?.stationCount,
        fallback = viewModel.allFeatures.size,
        isLoading = viewModel.isLoadingInfoSummary && viewModel.infoSummary == null
    )
    val chargerCount = countText(
        summaryValue = viewModel.infoSummary?.chargerCount,
        fallback = viewModel.allFeatures.sumOf { it.properties.chargingPointsCount },
        isLoading = viewModel.isLoadingInfoSummary && viewModel.infoSummary == null
    )
    return listOf(
        stringResource(R.string.i18n_info_aboutintro),
        stationCount,
        stringResource(R.string.i18n_info_aboutstationcountjoin),
        chargerCount,
        stringResource(R.string.i18n_info_aboutoutro)
    ).joinToString(" ")
}

@Composable
private fun dataUpdatedText(raw: String, summary: CatalogInfoSummary?): String {
    val countSuffix = if (summary != null && summary.stationCount > 0 && summary.chargerCount > 0) {
        stringResource(R.string.i18n_info_countsuffix)
            .replace("{stations}", formatInteger(summary.stationCount))
            .replace("{chargers}", formatInteger(summary.chargerCount))
    } else {
        ""
    }
    return stringResource(R.string.i18n_info_dataupdated)
        .replace("{date}", formatTimestamp(raw))
        .replace("{counts}", countSuffix)
}

private fun countText(summaryValue: Int?, fallback: Int, isLoading: Boolean): String {
    if (summaryValue != null && summaryValue > 0) return formatInteger(summaryValue)
    if (isLoading) return "..."
    return formatInteger(fallback)
}

private fun formatInteger(value: Int): String =
    NumberFormat.getIntegerInstance(Locale.getDefault()).format(value)

@Composable
private fun CountryCoverageRow(
    country: OpenStaticCountry,
    summary: CatalogInfoSummary,
    mutedColor: Color
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 4.dp),
        verticalArrangement = Arrangement.spacedBy(6.dp)
    ) {
        Row(
            modifier = Modifier.fillMaxWidth(),
            horizontalArrangement = Arrangement.spacedBy(10.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Column(modifier = Modifier.weight(1f)) {
                Text(
                    country.localizedName(),
                    style = MaterialTheme.typography.bodyLarge,
                    fontWeight = FontWeight.SemiBold
                )
                Text(
                    "(${country.code})",
                    style = MaterialTheme.typography.bodySmall,
                    color = mutedColor
                )
            }
            Text(
                formatInteger(country.stationCount),
                color = mutedColor
            )
        }

        val links = summary.countrySourceLinks(country.code)
        if (links.isEmpty()) {
            Text(
                stringResource(R.string.i18n_info_sourceunknown),
                style = MaterialTheme.typography.bodySmall,
                color = mutedColor
            )
        } else {
            links.forEach { source ->
                SourceLink(source)
            }
        }
    }
}

@Composable
private fun SourceLink(source: InfoSourceLink) {
    if (source.urlString.isBlank()) {
        Text(source.label, style = MaterialTheme.typography.bodySmall)
    } else {
        LinkButton(source.label, source.urlString)
    }
}

@Composable
private fun locationStatusText(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE -> stringResource(R.string.i18n_location_pendingtitle)
        LocationAuthorizationStatus.DENIED -> stringResource(R.string.i18n_location_deniedtitle)
        LocationAuthorizationStatus.NOT_DETERMINED -> stringResource(R.string.i18n_location_idletitle)
    }
}

@Composable
private fun locationSectionTitle(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE -> stringResource(R.string.i18n_location_pendingtitle)
        LocationAuthorizationStatus.DENIED -> stringResource(R.string.i18n_location_deniedtitle)
        LocationAuthorizationStatus.NOT_DETERMINED -> stringResource(R.string.i18n_location_idletitle)
    }
}

@Composable
private fun locationMessageText(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE -> stringResource(R.string.i18n_location_pendingmessage)
        LocationAuthorizationStatus.DENIED -> stringResource(R.string.i18n_location_deniedmessage)
        LocationAuthorizationStatus.NOT_DETERMINED -> stringResource(R.string.i18n_location_idlemessage)
    }
}

@Composable
private fun InfoSection(
    title: String? = null,
    content: @Composable ColumnScope.() -> Unit
) {
    Column(
        modifier = Modifier
            .fillMaxWidth()
            .padding(vertical = 2.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
        content = {
            if (title != null) {
                Text(title, style = MaterialTheme.typography.titleMedium)
            }
            content()
        }
    )
}

@Composable
private fun MarkerLegendRow(color: Color, text: String) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .size(18.dp)
                .background(color, CircleShape)
        )
        Text(text)
    }
}

@Composable
private fun CardBackgroundLegendRow(color: Color, text: String) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .size(width = 34.dp, height = 22.dp)
                .background(color, RoundedCornerShape(6.dp))
                .border(1.dp, Color(0x33000000), RoundedCornerShape(6.dp))
        )
        Text(text)
    }
}

@Composable
private fun FavoriteLegendRow(text: String) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Text("★", color = Color(0xFFF59E0B), style = MaterialTheme.typography.titleMedium)
        Text(text)
    }
}

@Composable
private fun MarkerOutOfOrderLegendRow(text: String) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .size(20.dp)
                .background(Color(0xFFB91C1C), CircleShape)
                .border(1.dp, Color.White, CircleShape),
            contentAlignment = Alignment.Center
        ) {
            Text("X", color = Color.White, style = MaterialTheme.typography.labelSmall, fontWeight = FontWeight.Bold)
        }
        Text(text)
    }
}

@Composable
private fun MarkerFullyOccupiedLegendRow(text: String) {
    Row(
        horizontalArrangement = Arrangement.spacedBy(10.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        Box(
            modifier = Modifier
                .size(20.dp)
                .background(Color.White, CircleShape)
                .border(2.dp, Color(0xFFB45309), CircleShape)
        )
        Text(text)
    }
}

@Composable
private fun LinkButton(title: String, url: String) {
    val context = LocalContext.current
    TextButton(onClick = {
        val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
        context.startActivity(intent)
    }) {
        Text(title)
    }
}

private fun cardOneFreeColor(isDarkMode: Boolean): Color =
    if (isDarkMode) Color(0xFF332B12) else Color(0xFFFFFBEB)

private fun cardOccupiedColor(isDarkMode: Boolean): Color =
    if (isDarkMode) Color(0xFF26323D) else Color(0xFFE2E8F0)

private fun cardOutOfOrderColor(isDarkMode: Boolean): Color =
    if (isDarkMode) Color(0xFF3B121C) else Color(0xFFFFF1F2)

private fun cardOftenBrokenColor(isDarkMode: Boolean): Color =
    if (isDarkMode) Color(0xFF36161F) else Color(0xFFFFF7F8)

private fun cardOftenOccupiedColor(isDarkMode: Boolean): Color =
    if (isDarkMode) Color(0xFF0F1E27) else Color(0xFFF8FAFC)
