import SwiftUI
import MapKit

struct MapTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @Binding var showingFilter: Bool

    @State private var cameraPosition: MapCameraPosition = .region(
        MKCoordinateRegion(
            center: CLLocationCoordinate2D(latitude: 51.1657, longitude: 10.4515),
            span: MKCoordinateSpan(latitudeDelta: 7.5, longitudeDelta: 7.5)
        )
    )
    @State private var centerOnNextLocationUpdate = false
    @State private var hasCenteredInitialLocation = false
    @State private var lastQueriedCenter: CLLocationCoordinate2D?

    var body: some View {
        ZStack(alignment: .topTrailing) {
            Map(position: $cameraPosition) {
                ForEach(mapItems()) { feature in
                    Annotation("", coordinate: feature.coordinate) {
                        Button {
                            viewModel.selectedFeature = feature
                        } label: {
                            Circle()
                                .fill(color(for: viewModel.markerTint(for: feature)))
                                .frame(width: 16, height: 16)
                                .overlay(Circle().stroke(Color.white, lineWidth: 1.5))
                        }
                    }
                }

                if let current = locationService.currentLocation {
                    UserAnnotation()
                    Annotation("Mein Standort", coordinate: current.coordinate) {
                        Circle()
                            .fill(Color.blue)
                            .frame(width: 10, height: 10)
                            .overlay(Circle().stroke(Color.white, lineWidth: 1))
                    }
                }
            }
            .ignoresSafeArea()
            .onMapCameraChange(frequency: .onEnd) { context in
                let center = context.region.center
                guard shouldQuery(for: center) else { return }
                lastQueriedCenter = center
                viewModel.handleMapCenterChange(center)
            }

            HStack(spacing: 12) {
                Button {
                    centerOnNextLocationUpdate = true
                    locationService.requestSingleLocation()
                    if let current = locationService.currentLocation {
                        centerMap(on: current)
                    }
                } label: {
                    Image(systemName: "location.fill")
                        .font(.headline)
                        .padding(10)
                        .background(Color(.secondarySystemBackground), in: Circle())
                }

                Button {
                    showingFilter = true
                } label: {
                    Image(systemName: "line.3.horizontal.decrease.circle")
                        .font(.headline)
                        .padding(10)
                        .background(Color(.secondarySystemBackground), in: Circle())
                }
            }
            .padding(.trailing, 16)
            .padding(.top, 12)

            if viewModel.isLoading && viewModel.allFeatures.isEmpty {
                ProgressView("Lade Ladepunkte...")
                    .padding(12)
                    .background(Color(.systemBackground), in: RoundedRectangle(cornerRadius: 10))
                    .padding(.top, 12)
                    .padding(.leading, 12)
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
            }
        }
        .onChange(of: locationService.currentLocation) { _, newValue in
            guard let newValue else { return }
            if centerOnNextLocationUpdate || !hasCenteredInitialLocation {
                centerMap(on: newValue)
                hasCenteredInitialLocation = true
            }
        }
        .onAppear {
            if !hasCenteredInitialLocation {
                if let current = locationService.currentLocation {
                    centerMap(on: current)
                    hasCenteredInitialLocation = true
                } else {
                    centerOnNextLocationUpdate = true
                    locationService.requestSingleLocation()
                }
            }
        }
    }

    private func mapItems() -> [GeoJSONFeature] {
        viewModel.discoveredFeatures
    }

    private func centerMap(on location: CLLocation) {
        centerOnNextLocationUpdate = false
        cameraPosition = .region(
            MKCoordinateRegion(
                center: location.coordinate,
                span: MKCoordinateSpan(latitudeDelta: 0.12, longitudeDelta: 0.12)
            )
        )
        lastQueriedCenter = location.coordinate
        viewModel.handleMapCenterChange(location.coordinate)
    }

    private func shouldQuery(for center: CLLocationCoordinate2D) -> Bool {
        guard let lastQueriedCenter else { return true }
        let lhs = CLLocation(latitude: lastQueriedCenter.latitude, longitude: lastQueriedCenter.longitude)
        let rhs = CLLocation(latitude: center.latitude, longitude: center.longitude)
        return lhs.distance(from: rhs) > 250
    }

    private func color(for key: String) -> Color {
        switch key {
        case "gold": return Color.yellow
        case "silver": return Color.gray
        case "bronze": return Color.brown
        default: return Color.secondary
        }
    }
}
