package de.woladen.android.ui

import android.content.Intent
import android.net.Uri
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.ArrowBack
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.Star
import androidx.compose.material.icons.filled.Star
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.model.AmenityExample
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.ui.components.AmenityIcon
import de.woladen.android.ui.components.DetailMapPoint
import de.woladen.android.ui.components.DetailMiniMapView
import de.woladen.android.util.AmenityCatalog
import kotlinx.coroutines.delay

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun StationDetailSheet(
    feature: GeoJsonFeature,
    isFavorite: Boolean,
    onToggleFavorite: () -> Unit,
    onDismiss: () -> Unit
) {
    val context = LocalContext.current
    val detailPoints = remember(feature.id) { buildDetailMapPoints(feature) }
    var showMiniMap by remember(feature.id) { mutableStateOf(false) }

    LaunchedEffect(feature.id) {
        showMiniMap = false
        delay(150)
        showMiniMap = true
    }

    ModalBottomSheet(onDismissRequest = onDismiss) {
        Column(
            modifier = Modifier
                .testTag("station-detail-sheet")
                .verticalScroll(rememberScrollState())
                .padding(bottom = 24.dp)
        ) {
            Box {
                if (showMiniMap) {
                    DetailMiniMapView(
                        points = detailPoints,
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(260.dp)
                    )
                } else {
                    Box(
                        modifier = Modifier
                            .fillMaxWidth()
                            .height(260.dp)
                    )
                }

                IconButton(
                    onClick = onDismiss,
                    modifier = Modifier
                        .testTag("detail-close-button")
                        .align(Alignment.TopStart)
                        .padding(12.dp)
                ) {
                    Icon(Icons.Outlined.ArrowBack, contentDescription = "Zurück")
                }
            }

            Column(
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 12.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                Row(verticalAlignment = Alignment.CenterVertically) {
                    Text(
                        text = feature.properties.operatorName,
                        style = MaterialTheme.typography.titleLarge,
                        modifier = Modifier.weight(1f)
                    )
                    IconButton(
                        onClick = onToggleFavorite,
                        modifier = Modifier.testTag("detail-favorite-button")
                    ) {
                        Icon(
                            imageVector = if (isFavorite) Icons.Filled.Star else Icons.Outlined.Star,
                            contentDescription = "Favorit"
                        )
                    }
                }

                Text(
                    "${feature.properties.address}, ${feature.properties.postcode} ${feature.properties.city}",
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )

                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween
                ) {
                    Row(horizontalArrangement = Arrangement.spacedBy(4.dp), verticalAlignment = Alignment.CenterVertically) {
                        Icon(Icons.Outlined.Info, contentDescription = null)
                        Text("${feature.properties.displayedMaxPowerKw.toInt()} kW max / ${feature.properties.chargingPointsCount} Ladepunkte")
                    }
                    Row(horizontalArrangement = Arrangement.spacedBy(4.dp), verticalAlignment = Alignment.CenterVertically) {
                        Icon(Icons.Outlined.Info, contentDescription = null)
                        Text("${feature.properties.amenitiesTotal} ${formatAmenityCountLabel(feature.properties.amenitiesTotal)}")
                    }
                }

                Row(horizontalArrangement = Arrangement.spacedBy(10.dp)) {
                    Button(onClick = {
                        openUri(
                            context,
                            "https://www.google.com/maps/dir/?api=1&destination=${feature.latitude},${feature.longitude}"
                        )
                    }, modifier = Modifier.testTag("detail-google-nav-button")) {
                        Text("Google Navi")
                    }
                    OutlinedButton(onClick = {
                        openUri(context, "geo:${feature.latitude},${feature.longitude}?q=${feature.latitude},${feature.longitude}")
                    }, modifier = Modifier.testTag("detail-system-nav-button")) {
                        Text("System Navi")
                    }
                }

                Text("In der Nähe", style = MaterialTheme.typography.titleMedium)
                if (feature.properties.amenityExamples.isEmpty()) {
                    Text("Keine Details verfügbar.", color = MaterialTheme.colorScheme.onSurfaceVariant)
                } else {
                    for (item in feature.properties.amenityExamples) {
                        AmenityRow(item)
                    }
                }
            }
        }
    }
}

private fun formatAmenityCountLabel(count: Int): String =
    if (count == 1) "Angebot vor Ort" else "Angebote vor Ort"

@Composable
private fun AmenityRow(item: AmenityExample) {
    Row(
        modifier = Modifier.fillMaxWidth(),
        verticalAlignment = Alignment.Top,
        horizontalArrangement = Arrangement.spacedBy(10.dp)
    ) {
        AmenityIcon(
            key = "amenity_${item.category}",
            contentDescription = null,
            modifier = Modifier.size(16.dp)
        )

        Column(modifier = Modifier.weight(1f), verticalArrangement = Arrangement.spacedBy(2.dp)) {
            Text(item.name ?: AmenityCatalog.labelFor("amenity_${item.category}"))
            val meta = metaForAmenity(item)
            if (meta.isNotEmpty()) {
                Text(meta, style = MaterialTheme.typography.bodySmall, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        }
    }
}

private fun metaForAmenity(item: AmenityExample): String {
    val parts = mutableListOf<String>()
    if (item.distanceM != null) {
        parts += "~${item.distanceM.toInt()} m"
    }
    if (!item.openingHours.isNullOrBlank()) {
        parts += item.openingHours
    }
    return parts.joinToString(" • ")
}

private fun buildDetailMapPoints(feature: GeoJsonFeature): List<DetailMapPoint> {
    val points = mutableListOf(
        DetailMapPoint(
            id = "station",
            latitude = feature.latitude,
            longitude = feature.longitude,
            title = feature.properties.operatorName,
            isStation = true
        )
    )

    for ((idx, example) in feature.properties.amenityExamples.withIndex()) {
        val lat = example.lat ?: continue
        val lon = example.lon ?: continue
        points += DetailMapPoint(
            id = "amenity-$idx",
            latitude = lat,
            longitude = lon,
            title = example.name ?: example.category,
            isStation = false,
            amenityKey = "amenity_${example.category}"
        )
    }
    return points
}

private fun openUri(context: android.content.Context, url: String) {
    val intent = Intent(Intent.ACTION_VIEW, Uri.parse(url))
    context.startActivity(intent)
}
