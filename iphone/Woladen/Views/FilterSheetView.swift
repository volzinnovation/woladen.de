import SwiftUI

struct FilterSheetView: View {
    @Environment(\.dismiss) private var dismiss

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
        NavigationStack {
            Form {
                operatorSection
                amenityNameSection
                powerSection
                amenitiesSection
            }
            .navigationTitle(String(localized: "filters.title"))
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button(String(localized: "aria.closeFilter")) { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button(String(localized: "filters.apply")) {
                        onApply(draftFilter)
                        dismiss()
                    }
                }
            }
        }
    }

    private var operatorSection: some View {
        Section(String(localized: "filters.operator")) {
            Picker(String(localized: "filters.operator"), selection: $draftFilter.operatorName) {
                Text(String(localized: "filters.allOperators")).tag("")
                ForEach(operators) { entry in
                    Text("\(entry.name) (\(entry.stations))").tag(entry.name)
                }
            }
        }
    }

    private var powerSection: some View {
        Section(minPowerLabel) {
            VStack(alignment: .leading, spacing: 6) {
                Text(minPowerSummary)
                Slider(value: $draftFilter.minPowerKW, in: 50...350, step: 50)
            }
        }
    }

    private var amenityNameSection: some View {
        Section(String(localized: "filters.amenityName")) {
            TextField(String(localized: "filters.amenityNamePlaceholder"), text: $draftFilter.amenityNameQuery)
                .textInputAutocapitalization(.words)
                .autocorrectionDisabled()
        }
    }

    private var amenitiesSection: some View {
        Section(String(localized: "filters.amenities")) {
            ForEach(availableAmenityKeys, id: \.self) { key in
                amenityRow(for: key)
            }
        }
    }

    private var minPowerLabel: String {
        String(localized: "filters.minPower")
            .replacingOccurrences(of: "{value}", with: "\(Int(draftFilter.minPowerKW))")
    }

    private var minPowerSummary: String {
        String(localized: "filters.minPowerLabel")
            .replacingOccurrences(of: "{value}", with: "\(Int(draftFilter.minPowerKW))")
    }

    private func amenityRow(for key: String) -> some View {
        let selected = draftFilter.selectedAmenities.contains(key)
        return Button {
            if selected {
                draftFilter.selectedAmenities.remove(key)
            } else {
                draftFilter.selectedAmenities.insert(key)
            }
        } label: {
            HStack {
                Label(AmenityCatalog.label(for: key), systemImage: AmenityCatalog.symbol(for: key))
                Spacer()
                if selected {
                    Image(systemName: "checkmark.circle.fill")
                        .foregroundStyle(Color.accentColor)
                }
            }
        }
    }
}
