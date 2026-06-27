import SwiftUI

struct FilterSheetView: View {
    @Environment(\.dismiss) private var dismiss
    @Environment(\.horizontalSizeClass) private var horizontalSizeClass

    @State private var draftFilter: FilterState
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
            amenitiesGrid
        }
        .padding(18)
    }

    private var operatorControl: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(String(localized: "filters.operator"))
                .font(.subheadline.weight(.semibold))
            Picker(String(localized: "filters.operator"), selection: $draftFilter.operatorName) {
                Text(String(localized: "filters.allOperators")).tag("")
                ForEach(operators) { entry in
                    Text("\(entry.name) (\(entry.stations))").tag(entry.name)
                }
            }
            .pickerStyle(.menu)
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.horizontal, 12)
            .padding(.vertical, 10)
            .background(StationVisualStyle.inputSurface, in: RoundedRectangle(cornerRadius: 12, style: .continuous))
            .overlay {
                RoundedRectangle(cornerRadius: 12, style: .continuous)
                    .stroke(StationVisualStyle.controlBorder, lineWidth: 1)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
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
            HStack {
                Text("0")
                Spacer()
                Text("50")
                Spacer()
                Text("150")
                Spacer()
                Text("300+")
            }
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
        }
        .frame(maxWidth: horizontalSizeClass == .regular ? 360 : .infinity, alignment: .leading)
    }

    private var amenityCountControl: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(minAmenityCountLabel)
                .font(.headline)
            Slider(value: $draftFilter.minAmenityCount, in: 0...20, step: 1)
                .tint(woladenBrandColor)
            HStack {
                Text("0")
                Spacer()
                Text("5")
                Spacer()
                Text("10")
                Spacer()
                Text("20+")
            }
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)
        }
        .frame(maxWidth: horizontalSizeClass == .regular ? 360 : .infinity, alignment: .leading)
    }

    private var amenitiesGrid: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(String(localized: "filters.amenities"))
                .font(.headline)
            LazyVGrid(columns: [GridItem(.adaptive(minimum: 72), spacing: 10)], alignment: .leading, spacing: 10) {
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
                    .lineLimit(2)
                    .minimumScaleFactor(0.8)
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
