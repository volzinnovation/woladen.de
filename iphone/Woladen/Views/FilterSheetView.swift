import SwiftUI

struct FilterSheetView: View {
    @ObservedObject var viewModel: ChargerViewModel
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Form {
                Section("Operator") {
                    Picker("Operator", selection: $viewModel.selectedOperator) {
                        Text("All operators").tag("")
                        ForEach(viewModel.operators) { op in
                            Text(op.name).tag(op.name)
                        }
                    }
                }

                Section("Minimum Power (\(Int(viewModel.minPowerKW.rounded())) kW)") {
                    Slider(value: $viewModel.minPowerKW, in: 50...400, step: 10)
                }

                Section("Required Amenities") {
                    if viewModel.availableAmenityKeys.isEmpty {
                        Text("No amenity keys available in current dataset.")
                            .foregroundStyle(.secondary)
                    } else {
                        ForEach(viewModel.availableAmenityKeys, id: \.self) { key in
                            Toggle(isOn: Binding(
                                get: { viewModel.selectedAmenityKeys.contains(key) },
                                set: { viewModel.setAmenity(key, enabled: $0) }
                            )) {
                                HStack(spacing: 8) {
                                    AmenityIconView(amenityKey: key, size: 18)
                                    Text(AmenityCatalog.label(for: key))
                                }
                            }
                        }
                    }
                }
            }
            .navigationTitle("Filters")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarLeading) {
                    Button("Reset") {
                        viewModel.resetFilters()
                    }
                }
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
    }
}
