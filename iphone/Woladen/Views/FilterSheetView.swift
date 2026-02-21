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
                powerSection
                amenitiesSection
            }
            .navigationTitle("Filter")
            .toolbar {
                ToolbarItem(placement: .cancellationAction) {
                    Button("Abbrechen") { dismiss() }
                }
                ToolbarItem(placement: .confirmationAction) {
                    Button("Anwenden") {
                        onApply(draftFilter)
                        dismiss()
                    }
                }
            }
        }
    }

    private var operatorSection: some View {
        Section("Betreiber") {
            Picker("Betreiber", selection: $draftFilter.operatorName) {
                Text("Alle Betreiber").tag("")
                ForEach(operators) { entry in
                    Text("\(entry.name) (\(entry.stations))").tag(entry.name)
                }
            }
        }
    }

    private var powerSection: some View {
        Section("Min. Leistung") {
            VStack(alignment: .leading, spacing: 6) {
                Text("\(Int(draftFilter.minPowerKW)) kW")
                Slider(value: $draftFilter.minPowerKW, in: 50...350, step: 50)
            }
        }
    }

    private var amenitiesSection: some View {
        Section("Annehmlichkeiten") {
            ForEach(availableAmenityKeys, id: \.self) { key in
                amenityRow(for: key)
            }
        }
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
