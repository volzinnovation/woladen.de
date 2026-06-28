import SwiftUI

struct FilterSheetView: View {
    @Environment(\.dismiss) private var dismiss
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass

    @State private var draftFilter: FilterState
    @State private var isOperatorListExpanded = false
    let operators: [OperatorEntry]
    let availableAmenityKeys: [String]
    let onApply: (FilterState) -> Void

    init(filter: FilterState, operators: [OperatorEntry], availableAmenityKeys: [String], onApply: @escaping (FilterState) -> Void) {
        _draftFilter = State(initialValue: filter)
        self.operators = operators
        self.availableAmenityKeys = availableAmenityKeys
        self.onApply = onApply
    }

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            ScrollView {
                if horizontalSizeClass == .regular {
                    wideControls
                } else {
                    compactControls
                }
            }
            Divider()
            footer
        }
        .background(Color(.systemBackground))
    }

    private var header: some View {
        HStack {
            Text(String(localized: "filters.title"))
                .font(.title2.weight(.bold))
            Spacer()
            Button {
                dismiss()
            } label: {
                Image(systemName: "xmark")
                    .font(.headline.weight(.bold))
                    .foregroundStyle(StationVisualStyle.mutedForeground)
                    .frame(width: 40, height: 40)
                    .contentShape(Rectangle())
            }
            .buttonStyle(.plain)
            .accessibilityLabel(Text(String(localized: "aria.closeFilter")))
        }
        .padding(.horizontal, 22)
        .padding(.vertical, 14)
        .background(Color(.systemBackground))
    }

    private var wideControls: some View {
        VStack(alignment: .leading, spacing: 22) {
            HStack(alignment: .top, spacing: 22) {
                operatorControl
                amenityNameControl
            }

            HStack(alignment: .top, spacing: 22) {
                toggleCard(
                    isOn: $draftFilter.availableOnly,
                    title: String(localized: "filters.availableOnly"),
                    note: String(localized: "filters.availableOnlyNote")
                )
                toggleCard(
                    isOn: $draftFilter.currentlyOpenOnly,
                    title: String(localized: "filters.currentlyOpen"),
                    note: String(localized: "filters.currentlyOpenNote")
                )
            }

            powerControl
            amenityCountControl
            routeRangeControl
            amenitiesGrid
        }
        .padding(22)
    }

    private var compactControls: some View {
        VStack(alignment: .leading, spacing: 18) {
            operatorControl
            amenityNameControl
            toggleCard(
                isOn: $draftFilter.availableOnly,
                title: String(localized: "filters.availableOnly"),
                note: String(localized: "filters.availableOnlyNote")
            )
            toggleCard(
                isOn: $draftFilter.currentlyOpenOnly,
                title: String(localized: "filters.currentlyOpen"),
                note: String(localized: "filters.currentlyOpenNote")
            )
            powerControl
            amenityCountControl
            routeRangeControl
            amenitiesGrid
        }
        .padding(18)
    }

    private var operatorControl: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(String(localized: "filters.operator"))
                .font(.subheadline.weight(.semibold))

            DisclosureGroup(isExpanded: $isOperatorListExpanded) {
                ScrollView {
                    VStack(alignment: .leading, spacing: 6) {
                        operatorOptionRow(
                            title: String(localized: "filters.allOperators"),
                            isSelected: draftFilter.selectedOperatorNames.isEmpty,
                            action: { draftFilter.selectedOperatorNames = [] }
                        )
                        ForEach(sortedOperators) { entry in
                            operatorOptionRow(
                                title: "\(entry.name) (\(entry.stations))",
                                isSelected: draftFilter.selectedOperatorNames.contains(entry.name),
                                action: { toggleOperator(entry.name) }
                            )
                        }
                    }
                }
                .padding(.top, 8)
                .frame(maxHeight: 260, alignment: .top)
            } label: {
                Text(operatorSelectionLabel)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(.primary)
                    .lineLimit(1)
                    .truncationMode(.tail)
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 12)
            .background(StationVisualStyle.inputSurface, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var sortedOperators: [OperatorEntry] {
        operators.sorted {
            $0.name.localizedCaseInsensitiveCompare($1.name) == .orderedAscending
        }
    }

    private var operatorSelectionLabel: String {
        if draftFilter.selectedOperatorNames.isEmpty {
            return String(localized: "filters.allOperators")
        }
        return draftFilter.selectedOperatorNames
            .sorted { $0.localizedCaseInsensitiveCompare($1) == .orderedAscending }
            .joined(separator: " · ")
    }

    private func operatorOptionRow(title: String, isSelected: Bool, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            HStack(spacing: 10) {
                Image(systemName: isSelected ? "checkmark.circle.fill" : "circle")
                    .foregroundStyle(isSelected ? woladenBrandColor : Color.secondary)
                    .frame(width: 22)
                Text(title)
                    .font(.subheadline)
                    .foregroundStyle(.primary)
                    .lineLimit(1)
                    .truncationMode(.tail)
                Spacer(minLength: 0)
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
    }

    private func toggleOperator(_ name: String) {
        if draftFilter.selectedOperatorNames.contains(name) {
            draftFilter.selectedOperatorNames.remove(name)
        } else {
            draftFilter.selectedOperatorNames.insert(name)
        }
    }

    private var amenityNameControl: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(String(localized: "filters.amenityName"))
                .font(.subheadline.weight(.semibold))
            TextField(String(localized: "filters.amenityNamePlaceholder"), text: $draftFilter.amenityNameQuery)
                .textInputAutocapitalization(.words)
                .autocorrectionDisabled()
                .padding(.horizontal, 12)
                .padding(.vertical, 12)
                .background(StationVisualStyle.inputSurface, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
                .overlay {
                    RoundedRectangle(cornerRadius: 12, style: .continuous)
                        .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
                }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private func toggleCard(isOn: Binding<Bool>, title: String, note: String) -> some View {
        Button {
            isOn.wrappedValue.toggle()
        } label: {
            HStack(alignment: .top, spacing: 12) {
                Image(systemName: isOn.wrappedValue ? "checkmark.circle.fill" : "square")
                    .font(.title3.weight(.semibold))
                    .foregroundStyle(isOn.wrappedValue ? woladenBrandColor : Color.secondary)
                    .frame(width: 26)
                VStack(alignment: .leading, spacing: 3) {
                    Text(title)
                        .font(.headline)
                        .foregroundStyle(.primary)
                    Text(note)
                        .font(.subheadline)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
                Spacer(minLength: 0)
            }
            .padding(14)
            .frame(maxWidth: .infinity, minHeight: 96, alignment: .topLeading)
            .background(isOn.wrappedValue ? StationVisualStyle.selectedControlSurface : Color(.systemBackground), in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(isOn.wrappedValue ? StationVisualStyle.selectedControlBorder : StationVisualStyle.controlBorder, lineWidth: 1)
            }
        }
        .buttonStyle(.plain)
    }

    private var powerControl: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(minPowerLabel)
                .font(.headline)
            Slider(value: $draftFilter.minPowerKW, in: 0...350, step: 10)
                .tint(woladenBrandColor)
            SliderTickLabels(
                ticks: [
                    SliderTick(value: 0, label: "0"),
                    SliderTick(value: 50, label: "50"),
                    SliderTick(value: 150, label: "150"),
                    SliderTick(value: 300, label: "300+")
                ],
                maxValue: 350
            )
        }
        .frame(maxWidth: horizontalSizeClass == .regular ? 360 : .infinity, alignment: .leading)
    }

    private var amenityCountControl: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(minAmenityCountLabel)
                .font(.headline)
            Slider(value: $draftFilter.minAmenityCount, in: 0...20, step: 1)
                .tint(woladenBrandColor)
            SliderTickLabels(
                ticks: [
                    SliderTick(value: 0, label: "0"),
                    SliderTick(value: 5, label: "5"),
                    SliderTick(value: 10, label: "10"),
                    SliderTick(value: 20, label: "20+")
                ],
                maxValue: 20
            )
        }
        .frame(maxWidth: horizontalSizeClass == .regular ? 360 : .infinity, alignment: .leading)
    }

    private var routeRangeControl: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(routeRangeTitle)
                .font(.headline)
            Slider(
                value: Binding(
                    get: { Double(routeRangeIndex(for: draftFilter.routeMaxDistanceFromLocationKM)) },
                    set: { next in
                        let index = min(max(Int(next.rounded()), 0), routeRangeOptionsKM.count - 1)
                        draftFilter.routeMaxDistanceFromLocationKM = routeRangeOptionsKM[index]
                    }
                ),
                in: 0...Double(routeRangeOptionsKM.count - 1),
                step: 1
            )
            .tint(woladenBrandColor)
            SliderTickLabels(
                ticks: [
                    SliderTick(value: 0, label: "50"),
                    SliderTick(value: 3, label: "200"),
                    SliderTick(value: 7, label: "400"),
                    SliderTick(value: 8, label: "∞")
                ],
                maxValue: Double(routeRangeOptionsKM.count - 1)
            )
        }
        .frame(maxWidth: horizontalSizeClass == .regular ? 360 : .infinity, alignment: .leading)
    }

    private var amenitiesGrid: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(String(localized: "filters.amenities"))
                .font(.headline)
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 88), spacing: 10)], alignment: .leading, spacing: 10) {
                ForEach(availableAmenityKeys, id: \.self) { key in
                    amenityTile(for: key)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var footer: some View {
        Button {
            onApply(draftFilter)
            dismiss()
        } label: {
            Text(String(localized: "filters.apply"))
                .font(.headline)
                .frame(maxWidth: .infinity)
                .padding(.vertical, 14)
        }
        .buttonStyle(.plain)
        .foregroundStyle(.white)
        .background(woladenBrandColor, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
        .padding(.horizontal, 22)
        .padding(.vertical, 14)
        .background(Color(.systemBackground))
    }

    private var minPowerLabel: String {
        String(localized: "filters.minPower")
            .replacingOccurrences(of: "{value}", with: "\(Int(draftFilter.minPowerKW.rounded()))")
    }

    private var minAmenityCountLabel: String {
        String(localized: "filters.minAmenities")
            .replacingOccurrences(of: "{value}", with: "\(Int(draftFilter.minAmenityCount.rounded()))")
    }

    private var routeRangeTitle: String {
        String(localized: "filters.routeRange")
            .replacingOccurrences(of: "{value}", with: routeRangeValueLabel(draftFilter.routeMaxDistanceFromLocationKM))
    }

    private func amenityTile(for key: String) -> some View {
        let selected = draftFilter.selectedAmenities.contains(key)
        return Button {
            if selected {
                draftFilter.selectedAmenities.remove(key)
            } else {
                draftFilter.selectedAmenities.insert(key)
            }
        } label: {
            VStack(spacing: 5) {
                Image(systemName: AmenityCatalog.symbol(for: key))
                    .font(.title2)
                    .foregroundStyle(selected ? woladenBrandColor : Color.secondary)
                    .frame(width: 34, height: 34)
                Text(AmenityCatalog.label(for: key))
                    .font(.caption)
                    .foregroundStyle(.primary)
                    .multilineTextAlignment(.center)
                    .lineLimit(1)
                    .minimumScaleFactor(0.75)
            }
            .frame(maxWidth: .infinity, minHeight: 76)
            .padding(.horizontal, 4)
            .background(selected ? StationVisualStyle.selectedControlSurface : Color.clear, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(selected ? StationVisualStyle.selectedControlBorder : Color.clear, lineWidth: 1)
            }
        }
        .buttonStyle(.plain)
        .accessibilityLabel(Text(AmenityCatalog.label(for: key)))
    }
}

private let routeRangeOptionsKM: [Double?] = [50, 100, 150, 200, 250, 300, 350, 400, nil]

private func routeRangeIndex(for value: Double?) -> Int {
    guard let value else { return routeRangeOptionsKM.count - 1 }
    if let exactIndex = routeRangeOptionsKM.firstIndex(where: { $0 == value }) {
        return exactIndex
    }
    let finiteOptions = routeRangeOptionsKM.enumerated().compactMap { index, option -> (index: Int, value: Double)? in
        guard let option else { return nil }
        return (index, option)
    }
    return finiteOptions.min { lhs, rhs in
        abs(lhs.value - value) < abs(rhs.value - value)
    }?.index ?? routeRangeOptionsKM.count - 1
}

private func routeRangeValueLabel(_ value: Double?) -> String {
    guard let value else { return "∞" }
    return "\(Int(value.rounded())) km"
}

private struct SliderTick: Identifiable {
    let value: Double
    let label: String

    var id: Double { value }
}

private struct SliderTickLabels: View {
    let ticks: [SliderTick]
    let maxValue: Double

    var body: some View {
        GeometryReader { proxy in
            ZStack(alignment: .topLeading) {
                ForEach(ticks) { tick in
                    Text(tick.label)
                        .font(.caption.weight(.semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: 44)
                        .position(
                            x: tickPosition(for: tick, width: proxy.size.width),
                            y: 8
                        )
                }
            }
        }
        .frame(height: 18)
    }

    private func tickPosition(for tick: SliderTick, width: CGFloat) -> CGFloat {
        guard maxValue > 0 else { return 0 }
        let fraction = max(0, min(1, tick.value / maxValue))
        let labelHalfWidth: CGFloat = 22
        let rawX = width * fraction
        return min(max(rawX, labelHalfWidth), max(labelHalfWidth, width - labelHalfWidth))
    }
}
