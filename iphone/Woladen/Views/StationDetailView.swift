import SwiftUI
import MapKit

struct StationDetailView: View {
    @ObservedObject var viewModel: ChargerViewModel
    let station: ChargerStation

    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            ScrollView {
                VStack(alignment: .leading, spacing: 14) {
                    Text(station.operatorName)
                        .font(.title3.bold())

                    Text(fullAddress)
                        .foregroundStyle(.secondary)

                    HStack(spacing: 8) {
                        Label("\(Int(station.maxPowerKW.rounded())) kW", systemImage: "bolt.fill")
                        Label("\(station.amenitiesTotal) amenities", systemImage: "fork.knife")
                    }
                    .font(.subheadline)
                    .foregroundStyle(.secondary)

                    HStack(spacing: 10) {
                        Button {
                            viewModel.toggleFavorite(station)
                        } label: {
                            Label(
                                viewModel.isFavorite(station) ? "Favorited" : "Add Favorite",
                                systemImage: viewModel.isFavorite(station) ? "star.fill" : "star"
                            )
                        }
                        .buttonStyle(.borderedProminent)

                        Button {
                            openInAppleMaps(station)
                        } label: {
                            Label("Route", systemImage: "car.fill")
                        }
                        .buttonStyle(.bordered)
                    }

                    Divider()

                    if !station.sortedAmenityCounts.isEmpty {
                        Text("Nearby Amenity Counts")
                            .font(.headline)

                        ForEach(station.sortedAmenityCounts.prefix(12), id: \.key) { item in
                            HStack {
                                AmenityIconView(amenityKey: item.key, size: 16)
                                Text(AmenityCatalog.label(for: item.key))
                                Spacer()
                                Text("\(item.count)")
                                    .foregroundStyle(.secondary)
                            }
                            .font(.subheadline)
                        }
                    }

                    Divider()

                    Text("Nearby Place Details")
                        .font(.headline)

                    if station.amenityExamples.isEmpty {
                        Text("No amenity detail examples available.")
                            .foregroundStyle(.secondary)
                            .font(.subheadline)
                    } else {
                        ForEach(station.amenityExamples.prefix(20)) { example in
                            HStack(alignment: .top, spacing: 8) {
                                AmenityIconView(amenityKey: example.amenityKey, size: 20)
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(example.displayName)
                                        .font(.subheadline.weight(.semibold))
                                    Text(exampleMetaText(example))
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                }
                            }
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.vertical, 4)
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding()
            }
            .navigationTitle("Charger Details")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .navigationBarTrailing) {
                    Button("Done") {
                        dismiss()
                    }
                }
            }
        }
    }

    private var fullAddress: String {
        [station.address, station.postcode, station.city]
            .filter { !$0.isEmpty }
            .joined(separator: ", ")
    }

    private func exampleMetaText(_ example: AmenityExample) -> String {
        var parts: [String] = []
        parts.append(AmenityCatalog.label(for: example.amenityKey))
        if let distance = example.distanceMeters {
            parts.append("~\(distance)m")
        }
        if let opening = example.openingHours, !opening.isEmpty {
            parts.append(opening)
        }
        return parts.joined(separator: " • ")
    }

    private func openInAppleMaps(_ station: ChargerStation) {
        let placemark = MKPlacemark(coordinate: station.coordinate)
        let mapItem = MKMapItem(placemark: placemark)
        mapItem.name = station.operatorName
        mapItem.openInMaps(launchOptions: [MKLaunchOptionsDirectionsModeKey: MKLaunchOptionsDirectionsModeDriving])
    }
}
