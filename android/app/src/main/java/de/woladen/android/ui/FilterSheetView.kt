package de.woladen.android.ui

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.CheckCircle
import androidx.compose.material.icons.outlined.RadioButtonUnchecked
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Slider
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.unit.dp
import de.woladen.android.model.FilterState
import de.woladen.android.model.OperatorEntry
import de.woladen.android.ui.components.AmenityIcon
import de.woladen.android.util.AmenityCatalog
import kotlin.math.roundToInt

@OptIn(ExperimentalMaterial3Api::class)
@Composable
fun FilterSheetView(
    filter: FilterState,
    operators: List<OperatorEntry>,
    availableAmenityKeys: List<String>,
    onDismiss: () -> Unit,
    onApply: (FilterState) -> Unit
) {
    var draftOperator by remember(filter) { mutableStateOf(filter.operatorName) }
    var draftMinPower by remember(filter) { mutableStateOf(filter.minPowerKw) }
    var draftAmenities by remember(filter) { mutableStateOf(filter.selectedAmenities.toMutableSet()) }
    var draftAmenityNameQuery by remember(filter) { mutableStateOf(filter.amenityNameQuery) }
    var operatorMenuExpanded by remember { mutableStateOf(false) }

    ModalBottomSheet(onDismissRequest = onDismiss) {
        Column(
            modifier = Modifier
                .padding(horizontal = 16.dp)
                .testTag("filter-sheet"),
            verticalArrangement = Arrangement.spacedBy(14.dp)
        ) {
            Row(
                modifier = Modifier.fillMaxWidth(),
                verticalAlignment = Alignment.CenterVertically,
                horizontalArrangement = Arrangement.SpaceBetween
            ) {
                Text("Filter", style = MaterialTheme.typography.titleLarge)
                Button(
                    onClick = {
                        onApply(
                            FilterState(
                                operatorName = draftOperator,
                                minPowerKw = draftMinPower,
                                selectedAmenities = draftAmenities,
                                amenityNameQuery = draftAmenityNameQuery
                            )
                        )
                    },
                    modifier = Modifier.testTag("filter-apply-button")
                ) {
                    Text("Anwenden")
                }
            }

            Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text("Betreiber", style = MaterialTheme.typography.titleSmall)
                OutlinedButton(onClick = { operatorMenuExpanded = true }) {
                    Text(if (draftOperator.isEmpty()) "Alle Betreiber" else draftOperator)
                }
                DropdownMenu(
                    expanded = operatorMenuExpanded,
                    onDismissRequest = { operatorMenuExpanded = false }
                ) {
                    DropdownMenuItem(
                        text = { Text("Alle Betreiber") },
                        onClick = {
                            draftOperator = ""
                            operatorMenuExpanded = false
                        }
                    )
                    for (entry in operators) {
                        DropdownMenuItem(
                            text = { Text("${entry.name} (${entry.stations})") },
                            onClick = {
                                draftOperator = entry.name
                                operatorMenuExpanded = false
                            }
                        )
                    }
                }
            }

            Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text("Name des Angebots vor Ort", style = MaterialTheme.typography.titleSmall)
                OutlinedTextField(
                    value = draftAmenityNameQuery,
                    onValueChange = { draftAmenityNameQuery = it },
                    placeholder = { Text("z. B. McDonald's") },
                    singleLine = true,
                    modifier = Modifier
                        .fillMaxWidth()
                        .testTag("filter-amenity-name-input")
                )
            }

            Column(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                Text("Min. Leistung", style = MaterialTheme.typography.titleSmall)
                Text("${draftMinPower.toInt()} kW")
                Slider(
                    value = draftMinPower.toFloat(),
                    onValueChange = { value ->
                        val snapped = ((value / 50f).roundToInt() * 50).coerceIn(50, 350)
                        draftMinPower = snapped.toDouble()
                    },
                    valueRange = 50f..350f
                )
            }

            Text("Angebote vor Ort", style = MaterialTheme.typography.titleSmall)
            LazyColumn(modifier = Modifier.weight(1f, fill = false)) {
                items(availableAmenityKeys) { key ->
                    val selected = draftAmenities.contains(key)
                    TextButton(
                        onClick = {
                            if (selected) draftAmenities.remove(key) else draftAmenities.add(key)
                        },
                        modifier = Modifier.fillMaxWidth()
                    ) {
                        Row(
                            modifier = Modifier.fillMaxWidth(),
                            verticalAlignment = Alignment.CenterVertically,
                            horizontalArrangement = Arrangement.spacedBy(8.dp)
                        ) {
                            AmenityIcon(
                                key = key,
                                contentDescription = null
                            )
                            Text(
                                AmenityCatalog.labelFor(key),
                                modifier = Modifier.weight(1f),
                                color = MaterialTheme.colorScheme.onSurface
                            )
                            Icon(
                                imageVector = if (selected) Icons.Outlined.CheckCircle else Icons.Outlined.RadioButtonUnchecked,
                                contentDescription = null
                            )
                        }
                    }
                }
            }

            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .padding(vertical = 12.dp),
                horizontalArrangement = Arrangement.spacedBy(8.dp)
            ) {
                OutlinedButton(
                    onClick = onDismiss,
                    modifier = Modifier.fillMaxWidth()
                ) {
                    Text("Abbrechen")
                }
            }
        }
    }
}
