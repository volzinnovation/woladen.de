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
import androidx.compose.ui.layout.Layout
import androidx.compose.ui.platform.testTag
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
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
    var draftOperators by remember(filter) { mutableStateOf(filter.normalizedOperatorNames) }
    var draftMinPower by remember(filter) { mutableStateOf(filter.minPowerKw) }
    var draftMinAmenityCount by remember(filter) { mutableStateOf(filter.minAmenityCount) }
    var draftAmenities by remember(filter) { mutableStateOf(filter.selectedAmenities.toMutableSet()) }
    var draftAmenityNameQuery by remember(filter) { mutableStateOf(filter.amenityNameQuery) }
    var draftAvailableOnly by remember(filter) { mutableStateOf(filter.availableOnly) }
    var draftCurrentlyOpenOnly by remember(filter) { mutableStateOf(filter.currentlyOpenOnly) }
    var draftRouteMaxDistanceKm by remember(filter) { mutableStateOf(filter.routeMaxDistanceFromLocationKm) }
    var operatorMenuExpanded by remember { mutableStateOf(false) }

    val applyDraft = {
        onApply(
            FilterState(
                selectedOperatorNames = draftOperators,
                minPowerKw = draftMinPower,
                minAmenityCount = draftMinAmenityCount,
                selectedAmenities = draftAmenities.toSet(),
                amenityNameQuery = draftAmenityNameQuery,
                availableOnly = draftAvailableOnly,
                currentlyOpenOnly = draftCurrentlyOpenOnly,
                routeMaxDistanceFromLocationKm = draftRouteMaxDistanceKm
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
                    draftOperators = draftOperators,
                    onOperatorToggle = { operatorName ->
                        draftOperators = if (draftOperators.contains(operatorName)) {
                            draftOperators - operatorName
                        } else {
                            draftOperators + operatorName
                        }
                    },
                    onOperatorsClear = { draftOperators = emptySet() },
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
                    draftMinAmenityCount = draftMinAmenityCount,
                    onMinAmenityCountChange = { draftMinAmenityCount = it },
                    draftRouteMaxDistanceKm = draftRouteMaxDistanceKm,
                    onRouteMaxDistanceChange = { draftRouteMaxDistanceKm = it },
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
                draftOperators = draftOperators,
                onOperatorToggle = { operatorName ->
                    draftOperators = if (draftOperators.contains(operatorName)) {
                        draftOperators - operatorName
                    } else {
                        draftOperators + operatorName
                    }
                },
                onOperatorsClear = { draftOperators = emptySet() },
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
                draftMinAmenityCount = draftMinAmenityCount,
                onMinAmenityCountChange = { draftMinAmenityCount = it },
                draftRouteMaxDistanceKm = draftRouteMaxDistanceKm,
                onRouteMaxDistanceChange = { draftRouteMaxDistanceKm = it },
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
    draftOperators: Set<String>,
    onOperatorToggle: (String) -> Unit,
    onOperatorsClear: () -> Unit,
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
    draftMinAmenityCount: Double,
    onMinAmenityCountChange: (Double) -> Unit,
    draftRouteMaxDistanceKm: Double?,
    onRouteMaxDistanceChange: (Double?) -> Unit,
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
                        selectedValues = draftOperators,
                        operators = operators,
                        expanded = operatorMenuExpanded,
                        onExpandedChange = onOperatorMenuExpandedChange,
                        onToggle = onOperatorToggle,
                        onClear = onOperatorsClear,
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
                    selectedValues = draftOperators,
                    operators = operators,
                    expanded = operatorMenuExpanded,
                    onExpandedChange = onOperatorMenuExpandedChange,
                    onToggle = onOperatorToggle,
                    onClear = onOperatorsClear
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
            AmenityCountControl(value = draftMinAmenityCount, onChange = onMinAmenityCountChange)
            RouteRangeControl(value = draftRouteMaxDistanceKm, onChange = onRouteMaxDistanceChange)

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
    selectedValues: Set<String>,
    operators: List<OperatorEntry>,
    expanded: Boolean,
    onExpandedChange: (Boolean) -> Unit,
    onToggle: (String) -> Unit,
    onClear: () -> Unit,
    modifier: Modifier = Modifier
) {
    val sortedOperators = operators.sortedWith(compareBy<OperatorEntry> { it.name.lowercase() }.thenBy { it.name })
    val selected = selectedValues.map { it.trim() }.filter { it.isNotBlank() }.toSet()
    Column(modifier = modifier, verticalArrangement = Arrangement.spacedBy(8.dp)) {
        Text(
            text = stringResource(R.string.i18n_filters_operator),
            style = androidx.compose.material3.MaterialTheme.typography.titleSmall
        )
        OutlinedButton(onClick = { onExpandedChange(true) }, modifier = Modifier.fillMaxWidth()) {
            Text(
                text = operatorSelectionLabel(selected, stringResource(R.string.i18n_filters_alloperators)),
                maxLines = 1,
                overflow = TextOverflow.Ellipsis
            )
        }
        DropdownMenu(
            expanded = expanded,
            onDismissRequest = { onExpandedChange(false) }
        ) {
            DropdownMenuItem(
                text = { Text(stringResource(R.string.i18n_filters_alloperators)) },
                onClick = {
                    onClear()
                    onExpandedChange(false)
                }
            )
            for (entry in sortedOperators) {
                val checked = entry.name in selected
                DropdownMenuItem(
                    text = { Text("${entry.name} (${entry.stations})") },
                    leadingIcon = {
                        Icon(
                            imageVector = if (checked) Icons.Outlined.CheckCircle else Icons.Outlined.RadioButtonUnchecked,
                            contentDescription = null
                        )
                    },
                    onClick = {
                        onToggle(entry.name)
                    }
                )
            }
        }
    }
}

private fun operatorSelectionLabel(selectedValues: Set<String>, allLabel: String): String {
    if (selectedValues.isEmpty()) return allLabel
    return selectedValues.sortedWith(compareBy<String> { it.lowercase() }.thenBy { it }).joinToString(" · ")
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
        SliderTickLabels(
            ticks = listOf(
                SliderTick(0f, "0"),
                SliderTick(50f, "50"),
                SliderTick(150f, "150"),
                SliderTick(300f, "300+")
            ),
            maxValue = 350f
        )
    }
}

@Composable
private fun AmenityCountControl(
    value: Double,
    onChange: (Double) -> Unit
) {
    Column(
        modifier = Modifier.widthIn(max = 360.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(
            text = stringResource(R.string.i18n_filters_minamenities)
                .replace("{value}", value.toInt().toString()),
            style = androidx.compose.material3.MaterialTheme.typography.titleSmall
        )
        Slider(
            value = value.toFloat(),
            onValueChange = { next ->
                onChange(next.roundToInt().coerceIn(0, 20).toDouble())
            },
            valueRange = 0f..20f,
            steps = 19
        )
        SliderTickLabels(
            ticks = listOf(
                SliderTick(0f, "0"),
                SliderTick(5f, "5"),
                SliderTick(10f, "10"),
                SliderTick(20f, "20+")
            ),
            maxValue = 20f
        )
    }
}

@Composable
private fun RouteRangeControl(
    value: Double?,
    onChange: (Double?) -> Unit
) {
    val selectedIndex = routeRangeIndex(value)
    Column(
        modifier = Modifier.widthIn(max = 360.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp)
    ) {
        Text(
            text = stringResource(R.string.i18n_filters_routerange)
                .replace("{value}", routeRangeLabel(value)),
            style = androidx.compose.material3.MaterialTheme.typography.titleSmall
        )
        Slider(
            value = selectedIndex.toFloat(),
            onValueChange = { next ->
                val snapped = next.roundToInt().coerceIn(0, ROUTE_RANGE_OPTIONS_KM.lastIndex)
                onChange(ROUTE_RANGE_OPTIONS_KM[snapped])
            },
            valueRange = 0f..ROUTE_RANGE_OPTIONS_KM.lastIndex.toFloat(),
            steps = (ROUTE_RANGE_OPTIONS_KM.size - 2).coerceAtLeast(0)
        )
        SliderTickLabels(
            ticks = listOf(
                SliderTick(0f, "50"),
                SliderTick(3f, "200"),
                SliderTick(7f, "400"),
                SliderTick(8f, "∞")
            ),
            maxValue = ROUTE_RANGE_OPTIONS_KM.lastIndex.toFloat()
        )
    }
}

private val ROUTE_RANGE_OPTIONS_KM = listOf<Double?>(50.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0, 400.0, null)

private fun routeRangeIndex(value: Double?): Int {
    if (value == null) return ROUTE_RANGE_OPTIONS_KM.lastIndex
    val match = ROUTE_RANGE_OPTIONS_KM.indexOfFirst { option -> option != null && option == value }
    if (match >= 0) return match
    return ROUTE_RANGE_OPTIONS_KM
        .withIndex()
        .filter { it.value != null }
        .minByOrNull { kotlin.math.abs((it.value ?: 0.0) - value) }
        ?.index ?: ROUTE_RANGE_OPTIONS_KM.lastIndex
}

private fun routeRangeLabel(value: Double?): String {
    return value?.let { "${it.roundToInt()} km" } ?: "∞"
}

private data class SliderTick(
    val value: Float,
    val label: String
)

@Composable
private fun SliderTickLabels(
    ticks: List<SliderTick>,
    maxValue: Float
) {
    Layout(
        modifier = Modifier
            .fillMaxWidth()
            .heightIn(min = 18.dp),
        content = {
            ticks.forEach { tick ->
                Text(
                    text = tick.label,
                    style = MaterialTheme.typography.labelSmall,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        }
    ) { measurables, constraints ->
        val placeables = measurables.map { measurable ->
            measurable.measure(constraints.copy(minWidth = 0, minHeight = 0))
        }
        val width = constraints.maxWidth
        val height = maxOf(
            constraints.minHeight,
            placeables.maxOfOrNull { it.height } ?: 0
        )

        layout(width, height) {
            placeables.forEachIndexed { index, placeable ->
                val fraction = (ticks[index].value / maxValue).coerceIn(0f, 1f)
                val targetX = (width * fraction - placeable.width / 2f)
                    .roundToInt()
                    .coerceIn(0, (width - placeable.width).coerceAtLeast(0))
                placeable.placeRelative(targetX, 0)
            }
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
            .widthIn(min = 92.dp, max = 112.dp)
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
            maxLines = 1,
            overflow = TextOverflow.Ellipsis
        )
    }
}
