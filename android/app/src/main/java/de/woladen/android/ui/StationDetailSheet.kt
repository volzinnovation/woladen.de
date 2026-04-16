package de.woladen.android.ui

import android.content.Intent
import android.net.Uri
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.outlined.ArrowBack
import androidx.compose.material.icons.outlined.Info
import androidx.compose.material.icons.outlined.NearMe
import androidx.compose.material.icons.outlined.Phone
import androidx.compose.material.icons.outlined.Star
import androidx.compose.material.icons.filled.Star
import androidx.compose.material3.Button
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedCard
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
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import de.woladen.android.model.AmenityExample
import de.woladen.android.model.AvailabilityStatus
import de.woladen.android.model.DetailRow
import de.woladen.android.model.GeoJsonFeature
import de.woladen.android.model.LiveDetailNote
import de.woladen.android.model.LiveEvseRow
import de.woladen.android.model.availabilityStatus
import de.woladen.android.model.displayPrice
import de.woladen.android.model.hasPrimaryDetailHighlights
import de.woladen.android.model.liveEvseRows
import de.woladen.android.model.occupancySourceLabel
import de.woladen.android.model.occupancySummaryLabel
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
                    Icon(Icons.AutoMirrored.Outlined.ArrowBack, contentDescription = "Zurück")
                }
            }

            Column(
                modifier = Modifier.padding(horizontal = 16.dp, vertical = 12.dp),
                verticalArrangement = Arrangement.spacedBy(12.dp)
            ) {
                Row(verticalAlignment = Alignment.Top, horizontalArrangement = Arrangement.spacedBy(12.dp)) {
                    Text(
                        text = feature.properties.operatorName,
                        style = MaterialTheme.typography.titleLarge,
                        maxLines = 2,
                        overflow = TextOverflow.Ellipsis,
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

                if (feature.hasPrimaryDetailHighlights) {
                    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                        if (feature.displayPrice.isNotBlank()) {
                            DetailChip(
                                text = feature.displayPrice,
                                symbol = "€"
                            )
                        }
                        if (feature.properties.openingHoursDisplay.isNotBlank()) {
                            DetailChip(
                                text = feature.properties.openingHoursDisplay,
                                symbol = "🕒"
                            )
                        }
                    }
                }

                Text(
                    "${feature.properties.address}, ${feature.properties.postcode} ${feature.properties.city}",
                    maxLines = 2,
                    overflow = TextOverflow.Ellipsis,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )

                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(10.dp)
                ) {
                    SummaryStatCard(
                        text = "${feature.properties.displayedMaxPowerKw.toInt()} kW max / ${feature.properties.chargingPointsCount} Ladepunkte",
                        modifier = Modifier.weight(1f)
                    )
                    feature.occupancySummaryLabel?.let { occupancy ->
                        SummaryStatCard(
                            text = occupancy,
                            modifier = Modifier.weight(1f),
                            tint = availabilityColor(feature.availabilityStatus)
                        )
                    }
                }

                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    Button(onClick = {
                        openUri(
                            context,
                            "https://www.google.com/maps/dir/?api=1&destination=${feature.latitude},${feature.longitude}"
                        )
                    }, modifier = Modifier
                        .weight(1f)
                        .heightIn(min = 52.dp)
                        .testTag("detail-google-nav-button")) {
                        Icon(Icons.Outlined.NearMe, contentDescription = null)
                        Text("Google", maxLines = 1, overflow = TextOverflow.Ellipsis)
                    }
                    OutlinedButton(onClick = {
                        openUri(context, "http://maps.apple.com/?daddr=${feature.latitude},${feature.longitude}")
                    }, modifier = Modifier
                        .weight(1f)
                        .heightIn(min = 52.dp)
                        .testTag("detail-system-nav-button")) {
                        Icon(Icons.Outlined.NearMe, contentDescription = null)
                        Text("Apple", maxLines = 1, overflow = TextOverflow.Ellipsis)
                    }
                    if (feature.properties.helpdeskPhone.isNotBlank()) {
                        OutlinedButton(onClick = {
                            val phoneNumber = feature.properties.helpdeskPhone.filter { it.isDigit() || it == '+' }
                            openUri(context, "tel:$phoneNumber")
                        }, modifier = Modifier
                            .weight(1f)
                            .heightIn(min = 52.dp)) {
                            Icon(Icons.Outlined.Phone, contentDescription = "Hilfe")
                            Text("Hilfe", maxLines = 1, overflow = TextOverflow.Ellipsis)
                        }
                    }
                }

                Text(
                    "${feature.properties.amenitiesTotal} ${formatAmenityCountLabel(feature.properties.amenitiesTotal)}",
                    style = MaterialTheme.typography.titleMedium
                )
                if (feature.properties.amenityExamples.isEmpty()) {
                    Text("Keine Details verfügbar.", color = MaterialTheme.colorScheme.onSurfaceVariant)
                } else {
                    for (item in feature.properties.amenityExamples) {
                        AmenityRow(item)
                    }
                }

                if (feature.liveEvseRows.isNotEmpty()) {
                    Text(liveSectionTitle(feature), style = MaterialTheme.typography.titleMedium)
                    for (row in feature.liveEvseRows) {
                        LiveEvseRowCard(row)
                    }
                }

                if (feature.properties.staticDetailRows.isNotEmpty() || feature.properties.detailSourceLabel != null) {
                    Text("Details", style = MaterialTheme.typography.titleMedium)
                    for (row in feature.properties.staticDetailRows) {
                        DetailInfoRow(row)
                    }
                    feature.properties.detailSourceLabel?.let { source ->
                        Text(
                            source,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }

                if (feature.liveEvseRows.isEmpty()) {
                    feature.occupancySourceLabel?.takeIf { it.isNotBlank() }?.let { source ->
                        Text(
                            source,
                            style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant
                        )
                    }
                }
            }
        }
    }
}

private fun formatAmenityCountLabel(count: Int): String =
    if (count == 1) "Angebot vor Ort" else "Angebote vor Ort"

@Composable
private fun SummaryStatCard(
    text: String,
    modifier: Modifier = Modifier,
    tint: Color = MaterialTheme.colorScheme.onSurface
) {
    OutlinedCard(modifier = modifier) {
        Column(
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            Row(
                horizontalArrangement = Arrangement.spacedBy(4.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Icon(Icons.Outlined.Info, contentDescription = null, tint = tint)
                Text(text, style = MaterialTheme.typography.bodyMedium, color = tint)
            }
        }
    }
}

@Composable
private fun DetailChip(text: String, symbol: String) {
    OutlinedCard {
        Row(
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 8.dp),
            horizontalArrangement = Arrangement.spacedBy(6.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            Text(symbol)
            Text(text, style = MaterialTheme.typography.bodySmall)
        }
    }
}

@Composable
private fun LiveEvseRowCard(row: LiveEvseRow) {
    OutlinedCard {
        Column(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp)
        ) {
            Row(verticalAlignment = Alignment.CenterVertically) {
                Text(
                    text = row.title,
                    style = MaterialTheme.typography.titleSmall,
                    modifier = Modifier.weight(1f)
                )
                StatusPill(status = row.status)
            }

            Row(verticalAlignment = Alignment.Top) {
                Text(
                    text = row.meta,
                    style = MaterialTheme.typography.bodySmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                    modifier = Modifier.weight(1f)
                )
                if (row.price.isNotBlank()) {
                    Text(
                        text = row.price,
                        style = MaterialTheme.typography.bodySmall,
                        color = Color(0xFF15803D)
                    )
                }
            }

            if (row.notes.isNotEmpty()) {
                Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                    for (note in row.notes) {
                        LiveNote(note)
                    }
                }
            }
        }
    }
}

@Composable
private fun LiveNote(note: LiveDetailNote) {
    Column(verticalArrangement = Arrangement.spacedBy(2.dp)) {
        Text(
            note.label,
            style = MaterialTheme.typography.labelSmall,
            color = MaterialTheme.colorScheme.onSurfaceVariant
        )
        Text(note.value, style = MaterialTheme.typography.bodySmall)
    }
}

@Composable
private fun StatusPill(status: AvailabilityStatus) {
    val color = availabilityColor(status)
    OutlinedCard {
        Text(
            text = status.label,
            color = color,
            style = MaterialTheme.typography.labelSmall,
            modifier = Modifier.padding(horizontal = 10.dp, vertical = 6.dp)
        )
    }
}

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

@Composable
private fun DetailInfoRow(row: DetailRow) {
    OutlinedCard {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 12.dp, vertical = 10.dp),
            horizontalArrangement = Arrangement.spacedBy(12.dp),
            verticalAlignment = Alignment.Top
        ) {
            Text(
                row.label,
                style = MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant,
                modifier = Modifier.weight(0.32f)
            )
            Text(
                row.value,
                style = MaterialTheme.typography.bodyMedium,
                modifier = Modifier.weight(0.68f)
            )
        }
    }
}

private fun availabilityColor(status: AvailabilityStatus): Color {
    return when (status) {
        AvailabilityStatus.FREE -> Color(0xFF0F766E)
        AvailabilityStatus.OCCUPIED -> Color(0xFFB45309)
        AvailabilityStatus.OUT_OF_ORDER -> Color(0xFFB91C1C)
        AvailabilityStatus.UNKNOWN -> Color.Gray
    }
}

private fun liveSectionTitle(feature: GeoJsonFeature): String {
    val provider = compactLiveProvider(feature.occupancySourceLabel)
    return when {
        provider.isNullOrBlank() -> "Live"
        provider == "lokale API" -> "Live von lokaler API"
        else -> "Live von $provider"
    }
}

private fun compactLiveProvider(sourceLabel: String?): String? {
    val candidate = sourceLabel
        ?.trim()
        ?.substringBefore(" • ")
        ?.trim()
        .orEmpty()
    if (candidate.isBlank()) return null
    if (candidate.startsWith("Live via ")) {
        val provider = candidate.removePrefix("Live via ").trim()
        return when {
            provider.isBlank() -> null
            provider == "lokaler API" -> "lokale API"
            else -> provider
        }
    }
    if (candidate.startsWith("Live-Stand") || candidate.startsWith("Stand ")) {
        return null
    }
    return candidate
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
