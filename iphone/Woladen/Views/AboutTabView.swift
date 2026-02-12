import SwiftUI

struct AboutTabView: View {
    @ObservedObject var viewModel: ChargerViewModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 14) {
                Text("woladen.de iPhone")
                    .font(.title2.bold())
                Text("Native SwiftUI port of the web experience.")
                    .foregroundStyle(.secondary)

                Divider()

                Text("Data Status")
                    .font(.headline)
                Text(viewModel.buildMetaText)
                    .font(.subheadline)
                    .foregroundStyle(.secondary)

                Divider()

                Text("Current Counts")
                    .font(.headline)
                HStack {
                    statChip(title: "All", value: "\(viewModel.allStations.count)")
                    statChip(title: "Filtered", value: "\(viewModel.filteredStations.count)")
                    statChip(title: "Favorites", value: "\(viewModel.favoriteStations.count)")
                }

                Divider()

                Text("Notes")
                    .font(.headline)
                Text("Filters and favorites are local to the device. Routing opens Apple Maps with driving directions.")
                    .font(.subheadline)
                    .foregroundStyle(.secondary)

                if let source = viewModel.buildMeta.sourceURL, let url = URL(string: source) {
                    Link(destination: url) {
                        Label("Open Data Source", systemImage: "arrow.up.right.square")
                    }
                    .padding(.top, 4)
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding()
        }
        .navigationTitle("Info")
    }

    private func statChip(title: String, value: String) -> some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(title)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value)
                .font(.headline)
        }
        .padding(.vertical, 8)
        .padding(.horizontal, 10)
        .background(.thinMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }
}
