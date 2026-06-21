package de.woladen.android.ui

import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.widthIn
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.outlined.CheckCircle
import androidx.compose.material.icons.outlined.Close
import androidx.compose.material.icons.outlined.RadioButtonUnchecked
import androidx.compose.material3.Button
import androidx.compose.material3.DropdownMenu
import androidx.compose.material3.DropdownMenuItem
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Slider
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.unit.dp
import androidx.compose.ui.window.Dialog
import de.woladen.android.R
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
    useWideDialog: Boolean = false,
    onDismiss: () -> Unit,
    onApply: (FilterState) -> Unit
) {
    var draftOperator by remember(filter) { mutableStateOf(filter.operatorName) }
    var draftMinPower by remember(filter) { mutableStateOf(filter.minPowerKw) }
    var draftAmenities by remember(filter) { mutableStateOf(filter.selectedAmenities.toMutableSet()) }
    var draftAmenityNameQuery by remember(filter) { mutableStateOf(filter.amenityNameQuery) }
    var draftAvailableOnly by remember(filter) { mutableStateOf(filter.availableOnly) }
    var draftCurrentlyOpenOnly by remember(filter) { mutableStateOf(filter.currentlyOpenOnly) }
    var operatorMenuExpanded by remember { mutableStateOf(false) }

    val applyDraft = {
        onApply(
            FilterState(
                operatorName = draftOperator,
                minPowerKw = draftMinPower,
                selectedAmenities = draftAmenities.toSet(),
                amenityNameQuery = draftAmenityNameQuery,
                availableOnly = draftAvailableOnly,
                currentlyOpenOnly = draftCurrentlyOpenOnly
            )
        )
    }

    if (useWideDialog) {
        Dialog(onDismissRequest = onDismiss) {
            Surface(
                modifier = Modifier
                    .fillMaxWidth(0.86f)
                    .widthIn(max = 760.dp)
                    .heightIn(max = 760.dp)
                    .testTag("filter-sheet"),
                shape = RoundedCornerShape(22.dp),
                tonalElevation = 8.dp,
                color = MaterialTheme.colorScheme.surface
            ) {
                FilterPanelContent(
                    operators = operators,
                    availableAmenityKeys = availableAmenityKeys,
                    draftOperator = draftOperator,
                    onOperatorChange = { draftOperator = it },
                    operatorMenuExpanded = operatorMenuExpanded,
                    onOperatorMenuExpandedChange = { operatorMenuExpanded = it },
                    draftAmenityNameQuery = draftAmenityNameQuery,
                    onAmenityNameQueryChange = { draftAmenityNameQuery = it },
                    draftAvailableOnly = draftAvailableOnly,
                    onAvailableOnlyChange = { draftAvailableOnly = it },
                    draftCurrentlyOpenOnly = draftCurrentlyOpenOnly,
                    onCurrentlyOpenOnlyChange = { draftCurrentlyOpenOnly = it },
                    draftMinPower = draftMinPower,
                    onMinPowerChange = { draftMinPower = it },
                    draftAmenities = draftAmenities,
                    useWideLayout = true,
                    onDismiss = onDismiss,
                    onApply = applyDraft
                )
            }
        }
    } else {
        ModalBottomSheet(onDismissRequest = onDismiss) {
            FilterPanelContent(
                operators = operators,
                availableAmenityKeys = availableAmenityKeys,
                draftOperator = draftOperator,
                onOperatorChange = { draftOperator = it },
                operatorMenuExpanded = operatorMenuExpanded,
                onOperatorMenuExpandedChange = { operatorMenuExpanded = it },
                draftAmenityNameQuery = draftAmenityNameQuery,
                onAmenityNameQueryChange = { draftAmenityNameQuery = it },
                draftAvailableOnly = draftAvailableOnly,
                onAvailableOnlyChange = { draftAvailableOnly = it },
                draftCurrentlyOpenOnly = draftCurrentlyOpenOnly,
                onCurrentlyOpenOnlyChange = { draftCurrentlyOpenOnly = it },
                draftMinPower = draftMinPower,
                onMinPowerChange = { draftMinPower = it },
                draftAmenities = draftAmenities,
                useWideLayout = false,
                onDismiss = onDismiss,
                onApply = applyDraft
            )
        }
    }
}

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun FilterPanelContent(
    operators: List<OperatorEntry>,
    availableAmenityKeys: List<String>,
    draftOperator: String,
    onOperatorChange: (String) -> Unit,
    operatorMenuExpanded: Boolean,
    onOperatorMenuExpandedChange: (Boolean) -> Unit,
    draftAmenityNameQuery: String,
    onAmenityNameQueryChange: (String) -> Unit,
    draftAvailableOnly: Boolean,
    onAvailableOnlyChange: (Boolean) -> Unit,
    draftCurrentlyOpenOnly: Boolean,
    onCurrentlyOpenOnlyChange: (Boolean) -> Unit,
    draftMinPower: Double,
    onMinPowerChange: (Double) -> Unit,
    draftAmenities: MutableSet<String>,
    useWideLayout: Boolean,
    onDismiss: () -> Unit,
    onApply: () -> Unit
) {
    Column {
        Row(
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 22.dp, vertical = 14.dp),
            verticalAlignment = Alignment.CenterVertically,
            horizontalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            Text(
                text = stringResource(R.string.i18n_filters_title),
                style = androidx.compose.material3.MaterialTheme.typography.titleLarge,
                modifier = Modifier.weight(1f)
            )
            TextButton(onClick = onDismiss) {
                Icon(Icons.Outlined.Close, contentDescription = stringResource(R.string.i18n_aria_closefilter))
            }
        }

        HorizontalDivider()

        Column(
            modifier = Modifier
                .weight(1f, fill = false)
                .verticalScroll(rememberScrollState())
                .padding(22.dp),
            verticalArrangement = Arrangement.spacedBy(20.dp)
        ) {
            if (useWideLayout) {
                Row(horizontalArrangement = Arrangement.spacedBy(22.dp)) {
                    OperatorControl(
                        value = draftOperator,
                        operators = operators,
                        expanded = operatorMenuExpanded,
                        onExpandedChange = onOperatorMenuExpandedChange,
                        onChange = onOperatorChange,
                        modifier = Modifier.weight(1f)
                    )
                    AmenityNameControl(
                        value = draftAmenityNameQuery,
                        onChange = onAmenityNameQueryChange,
                        modifier = Modifier.weight(1f)
                    )
                }
                Row(horizontalArrangement = Arrangement.spacedBy(22.dp)) {
                    ToggleCard(
                        checked = draftAvailableOnly,
                        title = stringResource(R.string.i18n_filters_availableonly),
                        note = stringResource(R.string.i18n_filters_availableonlynote),
                        onChange = onAvailableOnlyChange,
                        modifier = Modifier.weight(1f)
                    )
                    ToggleCard(
                        checked = draftCurrentlyOpenOnly,
                        title = stringResource(R.string.i18n_filters_currentlyopen),
                        note = stringResource(R.string.i18n_filters_currentlyopennote),
                        onChange = onCurrentlyOpenOnlyChange,
                        modifier = Modifier.weight(1f)
                    )
                }
            } else {
                OperatorControl(
                    value = draftOperator,
                    operators = operators,
                    expanded = operatorMenuExpanded,
                    onExpandedChange = onOperatorMenuExpandedChange,
                    onChange = onOperatorChange
                )
                AmenityNameControl(value = draftAmenityNameQuery, onChange = onAmenityNameQueryChange)
                ToggleCard(
                    checked = draftAvailableOnly,
                    title = stringResource(R.string.i18n_filters_availableonly),
                    note = stringResource(R.string.i18n_filters_availableonlynote),
                    onChange = onAvailableOnlyChange
                )
                ToggleCard(
                    checked = draftCurrentlyOpenOnly,
                    title = stringResource(R.string.i18n_filters_currentlyopen),
                    note = stringResource(R.string.i18n_filters_currentlyopennote),
                    onChange = onCurrentlyOpenOnlyChange
                )
            }

            PowerControl(value = draftMinPower, onChange = onMinPowerChange)

            Column(verticalArrangement = Arrangement.spacedBy(10.dp)) {
                Text(
                    text = stringResource(R.string.i18n_filters_amenities),
                    style = androidx.compose.material3.MaterialTheme.typography.titleSmall
                )
                FlowRow(
                    horizontalArrangement = Arrangement.spacedBy(10.dp),
                    verticalArrangement = Arrangement.spacedBy(10.dp)
                ) {
                    for (key in availableAmenityKeys) {
                        val selected = draftAmenities.contains(key)
                        AmenityTile(
                            key = key,
                            selected = selected,
                            onClick = {
                                if (selected) draftAmenities.remove(key) else draftAmenities.add(key)
                            }
                        )
                    }
                }
            }
        }

        HorizontalDivider()
        Button(
            onClick = onApply,
            modifier = Modifier
                .fillMaxWidth()
                .padding(horizontal = 22.dp, vertical = 14.dp)
                .testTag("filter-apply-button")
        ) {
            Text(stringResource(R.string.i18n_filters_apply))
        }
    }
}

@Composable
private fun OperatorControl(
    value: String,
    operators: List<OperatorEntry>,
    expanded: Boolean,
    onExpandedChange: (Boolean) -> Unit,
    onChange: (String) -> Unit,
    modifier: Modifier = Modifier
) {
    Column(modifier = modifier, verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(
            text = stringResource(R.string.i18n_filters_operator),
            style = androidx.compose.material3.MaterialTheme.typography.titleSmall
        )
        OutlinedButton(onClick = { onExpandedChange(true) }, modifier = Modifier.fillMaxWidth()) {
            Text(if (value.isEmpty()) stringResource(R.string.i18n_filters_alloperators) else value)
        }
        DropdownMenu(
            expanded = expanded,
            onDismissRequest = { onExpandedChange(false) }
        ) {
            DropdownMenuItem(
                text = { Text(stringResource(R.string.i18n_filters_alloperators)) },
                onClick = {
                    onChange("")
                    onExpandedChange(false)
                }
            )
            for (entry in operators) {
                DropdownMenuItem(
                    text = { Text("${entry.name} (${entry.stations})") },
                    onClick = {
                        onChange(entry.name)
                        onExpandedChange(false)
                    }
                )
            }
        }
    }
}

@Composable
private fun AmenityNameControl(
    value: String,
    onChange: (String) -> Unit,
    modifier: Modifier = Modifier
) {
    Column(modifier = modifier, verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(
            text = stringResource(R.string.i18n_filters_amenityname),
            style = androidx.compose.material3.MaterialTheme.typography.titleSmall
        )
        OutlinedTextField(
            value = value,
            onValueChange = onChange,
            placeholder = { Text(stringResource(R.string.i18n_filters_amenitynameplaceholder)) },
            singleLine = true,
            modifier = Modifier
                .fillMaxWidth()
                .testTag("filter-amenity-name-input")
        )
    }
}

@Composable
private fun ToggleCard(
    checked: Boolean,
    title: String,
    note: String,
    onChange: (Boolean) -> Unit,
    modifier: Modifier = Modifier
) {
    Row(
        modifier = modifier
            .heightIn(min = 96.dp)
            .background(MaterialTheme.colorScheme.surface, RoundedCornerShape(12.dp))
            .border(
                width = 1.dp,
                color = if (checked) {
                    MaterialTheme.colorScheme.primary.copy(alpha = 0.36f)
                } else {
                    MaterialTheme.colorScheme.outline
                },
                shape = RoundedCornerShape(12.dp)
            )
            .clickable { onChange(!checked) }
            .padding(14.dp),
        verticalAlignment = Alignment.Top,
        horizontalArrangement = Arrangement.spacedBy(12.dp)
    ) {
        Icon(
            imageVector = if (checked) Icons.Outlined.CheckCircle else Icons.Outlined.RadioButtonUnchecked,
            contentDescription = null,
            tint = if (checked) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant
        )
        Column(verticalArrangement = Arrangement.spacedBy(3.dp)) {
            Text(title, style = androidx.compose.material3.MaterialTheme.typography.titleSmall)
            Text(
                note,
                style = androidx.compose.material3.MaterialTheme.typography.bodySmall,
                color = MaterialTheme.colorScheme.onSurfaceVariant
            )
        }
    }
}

@Composable
private fun PowerControl(
    value: Double,
    onChange: (Double) -> Unit
) {
    Column(
        modifier = Modifier.widthIn(max = 360.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(
            text = stringResource(R.string.i18n_filters_minpower)
                .replace("{value}", value.toInt().toString()),
            style = androidx.compose.material3.MaterialTheme.typography.titleSmall
        )
        Slider(
            value = value.toFloat(),
            onValueChange = { next ->
                val snapped = ((next / 10f).roundToInt() * 10).coerceIn(0, 350)
                onChange(snapped.toDouble())
            },
            valueRange = 0f..350f
        )
        Row(modifier = Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween) {
            Text("0")
            Text("50")
            Text("150")
            Text("300+")
        }
    }
}

@Composable
private fun AmenityTile(
    key: String,
    selected: Boolean,
    onClick: () -> Unit
) {
    Column(
        modifier = Modifier
            .widthIn(min = 72.dp, max = 86.dp)
            .heightIn(min = 76.dp)
            .background(
                if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.12f) else Color.Transparent,
                RoundedCornerShape(12.dp)
            )
            .border(
                width = 1.dp,
                color = if (selected) MaterialTheme.colorScheme.primary.copy(alpha = 0.38f) else Color.Transparent,
                shape = RoundedCornerShape(12.dp)
            )
            .clickable(onClick = onClick)
            .padding(horizontal = 6.dp, vertical = 8.dp),
        horizontalAlignment = Alignment.CenterHorizontally,
        verticalArrangement = Arrangement.spacedBy(5.dp)
    ) {
        AmenityIcon(
            key = key,
            contentDescription = null
        )
        Text(
            text = AmenityCatalog.labelFor(key),
            style = androidx.compose.material3.MaterialTheme.typography.labelSmall,
            textAlign = TextAlign.Center,
            maxLines = 2
        )
    }
}
