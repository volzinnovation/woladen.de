package de.woladen.android.ui

import android.content.Intent
import android.net.Uri
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ColumnScope
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import androidx.compose.foundation.isSystemInDarkTheme
import de.woladen.android.service.LocationAuthorizationStatus
import de.woladen.android.service.LocationService
import de.woladen.android.util.formatTimestamp
import de.woladen.android.viewmodel.AppViewModel
import kotlinx.coroutines.launch

private const val PRIVACY_POLICY_URL = "https://woladen.de/privacy.html"
private const val IMPRINT_URL = "https://woladen.de/imprint.html"
private const val WEBSITE_URL = "https://woladen.de/"
private const val STUDIOS_URL = "https://studios.moonshots.gmbh/"

@Composable
fun InfoTabView(
    viewModel: AppViewModel,
    locationService: LocationService,
    onRequestLocationPermission: () -> Unit
) {
    val context = LocalContext.current
    val scope = rememberCoroutineScope()
    var importMessage by remember { mutableStateOf<String?>(null) }
    var importError by remember { mutableStateOf<String?>(null) }
    val isDarkMode = isSystemInDarkTheme()
    val infoBackground = if (isDarkMode) Color(0xFF111114) else Color.White
    val infoForeground = if (isDarkMode) Color.White else Color(0xFF111114)
    val infoMuted = infoForeground.copy(alpha = 0.78f)

    val importLauncher = rememberLauncherForActivityResult(
        contract = ActivityResultContracts.OpenDocumentTree()
    ) { uri ->
        if (uri == null) return@rememberLauncherForActivityResult

        runCatching {
            context.contentResolver.takePersistableUriPermission(
                uri,
                Intent.FLAG_GRANT_READ_URI_PERMISSION
            )
        }

        scope.launch {
            val result = viewModel.installBundleFromTreeUri(uri, locationService.currentLocation)
            if (result.isSuccess) {
                importMessage = "Datenbundle erfolgreich importiert."
                importError = null
            } else {
                importError = result.exceptionOrNull()?.localizedMessage
                importMessage = null
            }
        }
    }

    androidx.compose.material3.Surface(
        modifier = Modifier
            .fillMaxSize()
            .testTag("info-root"),
        color = infoBackground,
        contentColor = infoForeground
    ) {
        Column(
            modifier = Modifier
                .verticalScroll(rememberScrollState())
                .padding(start = 16.dp, end = 16.dp, top = 12.dp, bottom = 84.dp),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            InfoSection(title = "Über woladen.de") {
                Text("Finde Schnellladesäulen mit der besten Aufenthaltsqualität. Wir zeigen dir, wo es sich lohnt zu laden. Ohne Ladeweile.")
                viewModel.activeBundleInfo?.let {
                    Text(
                        "Datenstand: ${formatTimestamp(it.manifest.generatedAt)}",
                        color = infoMuted
                    )
                }
            }

            InfoSection(title = "Legende") {
                LegendRow(Color(0xFFFFD700), ">10 Angebote vor Ort (Gold)")
                LegendRow(Color.Gray, ">5 Angebote vor Ort (Silber)")
                LegendRow(Color(0xFF964B00), ">1 Angebote vor Ort (Bronze)")
                LegendRow(infoMuted, "Keine Angebote vor Ort")
            }

            InfoSection(title = "Kontakt & Code") {
                Text("Entwickelt von Prof. Dr. Raphael Volz")
                Text("Hochschule Pforzheim")
                LinkButton("raphael.volz@hs-pforzheim.de", "mailto:raphael.volz@hs-pforzheim.de")
                LinkButton("GitHub Projekt", "https://github.com/volzinnovation/woladen.de")
                Text("Die Moonshots Studios GmbH betreibt und vertreibt woladen.de und die begleitenden Apps für iPhone und Android.")
                LinkButton("woladen.de", WEBSITE_URL)
                LinkButton("studios.moonshots.gmbh", STUDIOS_URL)
                LinkButton("Impressum", IMPRINT_URL)
            }

            InfoSection(title = "Datenschutz") {
                Text("Standortzugriff ist optional. Wenn du ihn aktiv nutzt, verwendet die App ihn, um die Karte auf deine Umgebung zu fokussieren und nahe Ladepunkte zu sortieren.")
                Text("Favoriten und importierte Datenbundles bleiben auf deinem Gerät.")
                LinkButton("Datenschutzerklärung", PRIVACY_POLICY_URL)
            }

            InfoSection(title = "Datenquellen & Lizenzen") {
                LinkButton("BNetzA: Ladesäulenregister", "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html")
                Text("Kartendaten und POI-Daten © OpenStreetMap-Mitwirkende, verfügbar unter ODbL v1.0.")
                LinkButton("OpenStreetMap: Copyright", "https://www.openstreetmap.org/copyright")
                LinkButton("ODbL v1.0", "https://opendatacommons.org/licenses/odbl/1.0/")
            }

            InfoSection(title = "Standort") {
                Text(locationStatusText(locationService.authorizationStatus))
                Text(
                    "Die App funktioniert auch ohne Standortfreigabe. Der Standort wird nur nach deiner Aktion abgefragt.",
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
                        Text("Standort aktualisieren")
                    }
                }
            }

            InfoSection(title = "Datenbundle") {
                Text(viewModel.humanReadableBundleSource())
                viewModel.activeBundleInfo?.let {
                    Text("Version: ${it.manifest.version}")
                    Text("Erstellt am: ${formatTimestamp(it.manifest.generatedAt)}")
                }

                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    Button(onClick = { importLauncher.launch(null) }) {
                        Text("Datenbundle importieren")
                    }
                }

                OutlinedButton(
                    onClick = {
                        scope.launch {
                            val result = viewModel.removeInstalledBundle(locationService.currentLocation)
                            if (result.isSuccess) {
                                importMessage = "Installiertes Bundle entfernt. Baseline aktiv."
                                importError = null
                            } else {
                                importError = result.exceptionOrNull()?.localizedMessage
                                importMessage = null
                            }
                        }
                    }
                ) {
                    Text("Installiertes Datenbundle entfernen")
                }
            }

            InfoSection(title = "Hinweis für getrennte Updates") {
                Text("Code und Daten sind getrennt: Die App enthält ein Baseline-Datenbundle. Optional kann ein neues Datenbundle als Ordner importiert werden (muss chargers_fast.geojson, operators.json und optional data_manifest.json enthalten).")
            }

            if (importMessage != null) {
                InfoSection {
                    Text(importMessage.orEmpty(), color = Color(0xFF0B8A35))
                }
            }

            if (importError != null) {
                InfoSection {
                    Text(importError.orEmpty(), color = MaterialTheme.colorScheme.error)
                }
            }
        }
    }
}

private fun locationStatusText(status: LocationAuthorizationStatus): String {
    return when (status) {
        LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE -> "Standortzugriff erlaubt"
        LocationAuthorizationStatus.DENIED -> "Standortzugriff nicht erlaubt"
        LocationAuthorizationStatus.NOT_DETERMINED -> "Standortzugriff noch nicht entschieden"
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
private fun LegendRow(color: Color, text: String) {
    Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
        Text("●", color = color)
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
