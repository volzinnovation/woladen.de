import SwiftUI
import MapKit

struct MapTabView: View {
    @ObservedObject var viewModel: ChargerViewModel

    @State private var region = MKCoordinateRegion(
        center: CLLocationCoordinate2D(latitude: 51.1657, longitude: 10.4515),
        span: MKCoordinateSpan(latitudeDelta: 9.0, longitudeDelta: 9.0)
    )

    private var mapStations: [ChargerStation] {
        Array(viewModel.filteredStations.prefix(2000))
    }

    var body: some View {
        VStack(spacing: 10) {
            HStack(spacing: 10) {
                Button {
                    viewModel.isFilterSheetPresented = true
                } label: {
                    Label("Filters", systemImage: "line.3.horizontal.decrease.circle")
                }
                .buttonStyle(.borderedProminent)

                Button {
                    viewModel.requestLocation()
                    if let location = viewModel.locationManager.location {
                        withAnimation {
                            region.center = location.coordinate
                            region.span = MKCoordinateSpan(latitudeDelta: 0.25, longitudeDelta: 0.25)
                        }
                    }
                } label: {
                    Label("Locate", systemImage: "location.fill")
                }
                .buttonStyle(.bordered)

                Spacer()
            }
            .padding(.horizontal)

            Map(coordinateRegion: $region, interactionModes: .all, showsUserLocation: true, annotationItems: mapStations) { station in
                MapAnnotation(coordinate: station.coordinate) {
                    Button {
                        viewModel.selectedStation = station
                    } label: {
                        Circle()
                            .fill(markerColor(for: station))
                            .frame(width: 14, height: 14)
                            .overlay(
                                Circle()
                                    .stroke(Color.white, lineWidth: 1.5)
                            )
                    }
                    .buttonStyle(.plain)
                }
            }
            .clipShape(RoundedRectangle(cornerRadius: 14))
            .padding(.horizontal)

            VStack(alignment: .leading, spacing: 4) {
                Text("\(viewModel.filteredStations.count) chargers in current filter")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)

                if viewModel.filteredStations.count > mapStations.count {
                    Text("Map renders first \(mapStations.count) markers for performance.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .padding(.horizontal)
        }
        .navigationTitle("Woladen Map")
    }

    private func markerColor(for station: ChargerStation) -> Color {
        if station.amenitiesTotal >= 10 { return Color.green }
        if station.amenitiesTotal >= 5 { return Color.blue }
        if station.amenitiesTotal >= 1 { return Color.orange }
        return Color.gray
    }
}
