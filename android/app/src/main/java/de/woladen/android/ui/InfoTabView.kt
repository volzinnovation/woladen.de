package de.woladen.android.ui

import android.content.Intent
import android.net.Uri
import androidx.compose.foundation.background
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.unit.dp
import androidx.compose.foundation.isSystemInDarkTheme
import de.woladen.android.R
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.util.formatTimestamp
import de.woladen.android.viewmodel.AppViewModel

private const val PRIVACY_POLICY_URL = "https://woladen.de/privacy.html"
private const val IMPRINT_URL = "https://woladen.de/imprint.html"
private const val WEBSITE_URL = "https://woladen.de/"
private const val STUDIOS_URL = "https://studios.moonshots.gmbh/"
private const val MOBILITHEK_URL = "https://mobilithek.info/"

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

    androidx.compose.material3.Surface(
        modifier = Modifier
            .fillMaxSize()
            .testTag("info-root"),
        color = infoBackground,
        contentColor = infoForeground
    ) {
        Column(
            modifier = Modifier
                .fillMaxSize()
        ) {
            WoladenBrandIntro(showProductMessage = false)

            Column(
                modifier = Modifier
                    .weight(1f)
                    .verticalScroll(rememberScrollState())
                    .padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 84.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                InfoSection(title = stringResource(R.string.i18n_info_abouttitle)) {
                    Text(aboutText(viewModel))
                    viewModel.activeCatalogInfo?.let {
                        Text(
                            stringResource(R.string.i18n_info_dataupdated)
                                .replace("{date}", formatTimestamp(it.manifest.generatedAt))
                                .replace("{counts}", ""),
                            color = infoMuted
                        )
                    }
                }

                InfoSection(title = stringResource(R.string.i18n_info_legendtitle)) {
                    Text(
                        stringResource(R.string.i18n_info_cardbackgroundtitle),
                        style = MaterialTheme.typography.titleSmall
                    )
                    CardBackgroundLegendRow(cardOneFreeColor(isDarkMode), stringResource(R.string.i18n_info_legendonefreeleft))
                    CardBackgroundLegendRow(cardOccupiedColor(isDarkMode), stringResource(R.string.i18n_info_legendfullyoccupied))
                    CardBackgroundLegendRow(cardOutOfOrderColor(isDarkMode), stringResource(R.string.i18n_info_legendoutoforder))

                    Text(
                        stringResource(R.string.i18n_info_mapmarkertitle),
                        style = MaterialTheme.typography.titleSmall,
                        modifier = Modifier.padding(top = 8.dp)
                    )
                    MarkerLegendRow(Color(0xFFFFD700), stringResource(R.string.i18n_info_legendgold))
                    MarkerLegendRow(Color.Gray, stringResource(R.string.i18n_info_legendsilver))
                    MarkerLegendRow(Color(0xFF964B00), stringResource(R.string.i18n_info_legendbronze))
                    MarkerLegendRow(infoMuted, stringResource(R.string.i18n_info_legendgrey))
                    FavoriteLegendRow(stringResource(R.string.i18n_info_legendfavorite))
                }

                InfoSection(title = stringResource(R.string.i18n_info_contacttitle)) {
                    Text(stringResource(R.string.i18n_info_developedby))
                    LinkButton("raphael.volz@hs-pforzheim.de", "mailto:raphael.volz@hs-pforzheim.de")
                    LinkButton(stringResource(R.string.i18n_info_github), "https://github.com/volzinnovation/woladen.de")
                    Text("${stringResource(R.string.i18n_info_distributedby)} Moonshots Studios GmbH")
                    LinkButton("woladen.de", WEBSITE_URL)
                    LinkButton("studios.moonshots.gmbh", STUDIOS_URL)
                    LinkButton(stringResource(R.string.i18n_info_imprintlink), IMPRINT_URL)
                }

                InfoSection(title = stringResource(R.string.i18n_info_privacytitle)) {
                    Text(stringResource(R.string.i18n_info_privacybody))
                    LinkButton(stringResource(R.string.i18n_info_privacylink), PRIVACY_POLICY_URL)
                }

                InfoSection(title = stringResource(R.string.i18n_info_datasourcestitle)) {
                    LinkButton("BNetzA: Ladesäulenregister", "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html")
                    LinkButton("Mobilithek", MOBILITHEK_URL)
                    Text(stringResource(R.string.i18n_info_osmnote))
                    LinkButton(stringResource(R.string.i18n_info_osmcopyright), "https://www.openstreetmap.org/copyright")
                    LinkButton(stringResource(R.string.i18n_info_odbllicense), "https://opendatacommons.org/licenses/odbl/1.0/")
                }

                if (shouldShowLocationSection) {
                    InfoSection(title = locationSectionTitle(locationService.authorizationStatus)) {
                        Text(locationStatusText(locationService.authorizationStatus))
                        Text(
                            locationMessageText(locationService.authorizationStatus),
                            color = infoMuted
                        )
                        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                            OutlinedButton(onClick = {
                                if (locationService.authorizationStatus == LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE) {
                                    locationService.requestSingleLocation()
                                    locationService.startUpdates()
                                } else {
                                    onRequestLocationPermission()
                                }
                            }, modifier = Modifier.testTag("info-location-refresh-button")) {
                                Text(stringResource(R.string.i18n_location_retry))
                            }
                        }
                    }
                }

                InfoSection(title = "API-Katalog") {
                    Text(viewModel.humanReadableCatalogSource())
                    viewModel.activeCatalogInfo?.let {
                        Text("Version: ${it.manifest.version}")
                        Text(
                            stringResource(R.string.i18n_station_updated)
                                .replace("{date}", formatTimestamp(it.manifest.generatedAt))
                        )
                        Text("Schema: ${it.manifest.schema}", color = infoMuted)
                    }

                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        OutlinedButton(onClick = { viewModel.reloadCatalog(locationService.currentLocation) }) {
                            Text(stringResource(R.string.i18n_errors_reload))
                        }
                    }
                }

                InfoSection(title = "Hinweis für getrennte Updates") {
                    Text("Code und Daten sind getrennt: Die App lädt den öffentlichen Katalog über die Live-EU-API und nutzt einen begrenzten lokalen Cache.")
                }
            }
        }
    }
}

@Composable
private fun aboutText(viewModel: AppViewModel): String {
    val stationCount = viewModel.allFeatures.size
    val chargerCount = viewModel.allFeatures.sumOf { it.properties.chargingPointsCount }
    return listOf(
        stringResource(R.string.i18n_info_aboutintro),
        stationCount.toString(),
        stringResource(R.string.i18n_info_aboutstationcountjoin),
        chargerCount.toString(),
        stringResource(R.string.i18n_info_aboutoutro)
    ).joinToString(" ")
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
    Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
        Text("●", color = color)
        Text(text)
    }
}

@Composable
private fun CardBackgroundLegendRow(color: Color, text: String) {
    Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
        Box(
            modifier = Modifier
                .padding(top = 4.dp)
                .size(width = 34.dp, height = 22.dp)
                .background(color, RoundedCornerShape(6.dp))
        )
        Text(text)
    }
}

@Composable
private fun FavoriteLegendRow(text: String) {
    Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
        Text("★", color = Color(0xFFF59E0B))
        Text(text)
    }
}

@Composable
private fun LinkButton(title: String, url: String) {
    val context = LocalContext.current
    OutlinedButton(onClick = {
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
