import SwiftUI

struct StationListView: View {
    @ObservedObject var viewModel: ChargerViewModel
    let stations: [ChargerStation]
    let title: String
    let emptyText: String

    var body: some View {
        List {
            if stations.isEmpty {
                Text(emptyText)
                    .foregroundStyle(.secondary)
            } else {
                ForEach(stations) { station in
                    Button {
                        viewModel.selectedStation = station
                    } label: {
                        StationRowView(viewModel: viewModel, station: station)
                    }
                    .buttonStyle(.plain)
                }
            }
        }
        .listStyle(.plain)
        .navigationTitle(title)
        .toolbar {
            ToolbarItem(placement: .navigationBarTrailing) {
                Button {
                    viewModel.isFilterSheetPresented = true
                } label: {
                    Image(systemName: "line.3.horizontal.decrease.circle")
                }
            }
        }
    }
}

private struct StationRowView: View {
    @ObservedObject var viewModel: ChargerViewModel
    let station: ChargerStation

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                Text(station.operatorName)
                    .font(.headline)
                    .lineLimit(1)

                Spacer()

                if let distance = viewModel.distanceText(for: station) {
                    Text(distance)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }

            Text(fullAddress(for: station))
                .font(.subheadline)
                .foregroundStyle(.secondary)
                .lineLimit(2)

            HStack {
                Text("\(Int(station.maxPowerKW.rounded())) kW")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Text("•")
                    .foregroundStyle(.tertiary)
                Text("\(station.amenitiesTotal) amenities")
                    .font(.caption)
                    .foregroundStyle(.secondary)

                Spacer()

                if viewModel.isFavorite(station) {
                    Image(systemName: "star.fill")
                        .foregroundStyle(.yellow)
                        .font(.caption)
                }
            }
        }
        .padding(.vertical, 4)
    }

    private func fullAddress(for station: ChargerStation) -> String {
        let parts = [
            station.address,
            station.postcode,
            station.city,
        ].filter { !$0.isEmpty }
        return parts.joined(separator: ", ")
    }
}
