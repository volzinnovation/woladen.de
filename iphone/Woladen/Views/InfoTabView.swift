import SwiftUI
import UIKit

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)

struct InfoTabView: View {
    private let websiteURL = URL(string: "https://woladen.de/")!
    private let githubURL = URL(string: "https://github.com/volzinnovation/woladen.de")!
    private let privacyPolicyURL = URL(string: "https://woladen.de/privacy.html")!
    private let imprintURL = URL(string: "https://woladen.de/imprint.html")!
    private let studiosURL = URL(string: "https://studios.moonshots.gmbh/")!
    private let geocoderURL = URL(string: "https://openrouteservice.org/dev/#/api-docs/geocode/autocomplete/get")!
    private let easterEggURL = URL(string: "https://hellmood.111mb.de//wake_up_16b_writeup.html")!
    private let contributors = [
        "Ramona Fleischer",
        "Johanna Thiele",
        "Greta Reutter",
        "Tara Golle",
        "Benedikt Schulz"
    ]

    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    var body: some View {
        List {
            Section(String(localized: "info.title")) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("woladen: \(String(localized: "seo.primaryTagline"))")
                        .font(.title3.weight(.semibold))
                    Text("\(String(localized: "seo.humanHook")) \(String(localized: "seo.timeLine"))")
                        .foregroundStyle(.secondary)
                    Text(String(localized: "seo.productMessage"))
                }
                .padding(.vertical, 2)
            }

            Section(String(localized: "info.legendTitle")) {
                Text(String(localized: "info.cardBackgroundTitle"))
                    .font(.headline)
                statusSwatch(color: StationVisualStyle.cardOneFreeLeft, border: StationVisualStyle.borderOneFreeLeft, text: String(localized: "info.legendOneFreeLeft"))
                statusSwatch(color: StationVisualStyle.cardOccupied, border: StationVisualStyle.borderOccupied, text: String(localized: "info.legendFullyOccupied"))
                statusSwatch(color: StationVisualStyle.cardOutOfOrder, border: StationVisualStyle.borderOutOfOrder, text: String(localized: "info.legendOutOfOrder"))
                statusSwatch(color: StationVisualStyle.cardOftenBroken, border: StationVisualStyle.borderOftenBroken, text: String(localized: "info.legendOftenBroken"))
                statusSwatch(color: StationVisualStyle.cardOftenOccupied, border: StationVisualStyle.borderOftenOccupied, text: String(localized: "info.legendOftenOccupied"))

                Text(String(localized: "info.mapMarkerTitle"))
                    .font(.headline)
                    .padding(.top, 6)
                legendRow(color: StationVisualStyle.amenityGold, text: String(localized: "info.legendGold"))
                legendRow(color: StationVisualStyle.amenitySilver, text: String(localized: "info.legendSilver"))
                legendRow(color: StationVisualStyle.amenityBronze, text: String(localized: "info.legendBronze"))
                legendRow(color: StationVisualStyle.amenityGrey, text: String(localized: "info.legendGrey"))
                favoriteLegendRow(text: String(localized: "info.legendFavorite"))
                markerOutOfOrderLegendRow(text: String(localized: "info.legendMarkerOutOfOrder"))
                markerFullyOccupiedLegendRow(text: String(localized: "info.legendMarkerFullyOccupied"))
            }

            Section(String(localized: "info.aboutTitle")) {
                Text(aboutText)
                if let raw = viewModel.infoSummary?.generatedAt, !raw.isEmpty {
                    Text(dataUpdatedText(for: raw, summary: viewModel.infoSummary))
                        .foregroundStyle(.secondary)
                }
            }

            Section(String(localized: "info.countriesTitle")) {
                if viewModel.isLoadingInfoSummary && viewModel.infoSummary == nil {
                    ProgressView(String(localized: "info.loadingCountries"))
                } else if let summary = viewModel.infoSummary, !summary.countries.isEmpty {
                    ForEach(summary.sortedCountries()) { country in
                        countryRow(country, summary: summary)
                    }
                } else {
                    Text(viewModel.infoSummaryError ?? String(localized: "info.countryLoadError"))
                        .foregroundStyle(.secondary)
                    Button(String(localized: "errors.reload")) {
                        viewModel.reloadInfoSummary()
                    }
                }
            }

            Section(String(localized: "info.dataSourcesTitle")) {
                if viewModel.isLoadingInfoSummary && viewModel.infoSummary == nil {
                    ProgressView(String(localized: "info.loadingSources"))
                } else if let summary = viewModel.infoSummary {
                    ForEach(summary.dataSourceLinks()) { source in
                        sourceLink(source)
                    }
                    Link(String(localized: "sources.geocoder"), destination: geocoderURL)
                    Link(String(localized: "sources.easterEgg"), destination: easterEggURL)
                } else {
                    Text(viewModel.infoSummaryError ?? String(localized: "info.sourceLoadError"))
                        .foregroundStyle(.secondary)
                }
            }

            Section(String(localized: "info.licensesTitle")) {
                Text(String(localized: "info.osmNote"))
                Link(
                    String(localized: "info.osmCopyright"),
                    destination: URL(string: "https://www.openstreetmap.org/copyright")!
                )
                Link(
                    String(localized: "info.odblLicense"),
                    destination: URL(string: "https://opendatacommons.org/licenses/odbl/1.0/")!
                )
            }

            Section(String(localized: "info.contactTitle")) {
                VStack(alignment: .leading, spacing: 6) {
                    Text(String(localized: "info.developedBy"))
                    Link(String(localized: "info.github"), destination: githubURL)
                    Text("\(String(localized: "info.distributedBy")) Moonshots Studios GmbH")
                    Link("woladen.de", destination: websiteURL)
                    Link("studios.moonshots.gmbh", destination: studiosURL)
                    Link(String(localized: "info.imprintLink"), destination: imprintURL)
                }
            }

            Section(String(localized: "info.contributorsTitle")) {
                Text(String(localized: "info.studentsGroup"))
                    .foregroundStyle(.secondary)
                ForEach(contributors, id: \.self) { contributor in
                    Text(contributor)
                }
            }

            Section(String(localized: "info.privacyTitle")) {
                Text(String(localized: "info.privacyBody"))
                Link(String(localized: "info.privacyLink"), destination: privacyPolicyURL)
            }

            Section(String(localized: "location.idleTitle")) {
                Text(locationStatusText)
                Button(locationActionTitle) {
                    handleLocationAction()
                }
            }
        }
        .task {
            viewModel.loadInfoSummaryIfNeeded()
        }
        .refreshable {
            viewModel.reloadInfoSummary()
        }
    }

    private func legendRow(color: Color, text: String) -> some View {
        HStack(spacing: 10) {
            Circle()
                .fill(color)
                .frame(width: 12, height: 12)
            Text(text)
        }
    }

    private func statusSwatch(color: Color, border: Color, text: String) -> some View {
        HStack(spacing: 10) {
            RoundedRectangle(cornerRadius: 5, style: .continuous)
                .fill(color)
                .frame(width: 22, height: 18)
                .overlay {
                    RoundedRectangle(cornerRadius: 5, style: .continuous)
                        .stroke(border, lineWidth: 1)
                }
            Text(text)
        }
    }

    private func markerOutOfOrderLegendRow(text: String) -> some View {
        HStack(spacing: 10) {
            ZStack {
                Circle()
                    .fill(StationVisualStyle.markerOutOfOrder)
                    .overlay(Circle().stroke(Color.white, lineWidth: 1.2))
                Image(systemName: "xmark")
                    .font(.system(size: 9, weight: .heavy))
                    .foregroundStyle(Color.white)
            }
            .frame(width: 18, height: 18)
            Text(text)
        }
    }

    private func markerFullyOccupiedLegendRow(text: String) -> some View {
        HStack(spacing: 10) {
            Circle()
                .fill(Color.white)
                .overlay(Circle().stroke(StationVisualStyle.markerFullyOccupied, lineWidth: 2))
                .frame(width: 18, height: 18)
            Text(text)
        }
    }

    private func favoriteLegendRow(text: String) -> some View {
        HStack(spacing: 10) {
            Image(systemName: "star.fill")
                .font(.system(size: 14, weight: .bold))
                .foregroundStyle(favoriteStarColor)
                .frame(width: 12, height: 12)
            Text(text)
        }
    }

    private func countryRow(_ country: OpenStaticCountry, summary: CatalogInfoSummary) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline, spacing: 10) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(country.localizedName())
                        .font(.body.weight(.semibold))
                    Text("(\(country.code))")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer(minLength: 12)
                Text(formatInteger(country.stationCount))
                    .font(.body.monospacedDigit())
                    .foregroundStyle(.secondary)
                    .accessibilityLabel("\(String(localized: "info.stations")): \(formatInteger(country.stationCount))")
            }

            let links = summary.countrySourceLinks(for: country.code)
            if links.isEmpty {
                Text(String(localized: "info.sourceUnknown"))
                    .font(.footnote)
                    .foregroundStyle(.secondary)
            } else {
                VStack(alignment: .leading, spacing: 4) {
                    ForEach(links) { source in
                        sourceLink(source)
                            .font(.footnote)
                    }
                }
            }
        }
        .padding(.vertical, 4)
    }

    @ViewBuilder
    private func sourceLink(_ source: InfoSourceLink) -> some View {
        if let url = source.url {
            Link(source.label, destination: url)
        } else {
            Text(source.label)
        }
    }

    private func formattedTimestamp(_ raw: String) -> String {
        if let date = iso8601WithFractional.date(from: raw) ?? iso8601.date(from: raw) {
            return dateFormatter.string(from: date)
        }
        return raw
    }

    private var aboutText: String {
        let stationCount = countText(
            summaryValue: viewModel.infoSummary?.stationCount,
            fallback: viewModel.allFeatures.count
        )
        let chargerCount = countText(
            summaryValue: viewModel.infoSummary?.chargerCount,
            fallback: viewModel.allFeatures.reduce(0) { total, feature in
                total + feature.properties.chargingPointsCount
            }
        )
        return [
            String(localized: "info.aboutIntro"),
            stationCount,
            String(localized: "info.aboutStationCountJoin"),
            chargerCount,
            String(localized: "info.aboutOutro")
        ].joined(separator: " ")
    }

    private func dataUpdatedText(for raw: String, summary: CatalogInfoSummary?) -> String {
        let countSuffix: String
        if let summary, summary.stationCount > 0, summary.chargerCount > 0 {
            countSuffix = String(localized: "info.countSuffix")
                .replacingOccurrences(of: "{stations}", with: formatInteger(summary.stationCount))
                .replacingOccurrences(of: "{chargers}", with: formatInteger(summary.chargerCount))
        } else {
            countSuffix = ""
        }
        return String(localized: "info.dataUpdated")
            .replacingOccurrences(of: "{date}", with: formattedTimestamp(raw))
            .replacingOccurrences(of: "{counts}", with: countSuffix)
    }

    private func countText(summaryValue: Int?, fallback: Int) -> String {
        if let summaryValue, summaryValue > 0 {
            return formatInteger(summaryValue)
        }
        if viewModel.isLoadingInfoSummary && viewModel.infoSummary == nil {
            return "..."
        }
        return formatInteger(fallback)
    }

    private func formatInteger(_ value: Int) -> String {
        NumberFormatter.localizedString(from: NSNumber(value: value), number: .decimal)
    }

    private var iso8601: ISO8601DateFormatter {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime]
        return formatter
    }

    private var iso8601WithFractional: ISO8601DateFormatter {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return formatter
    }

    private var dateFormatter: DateFormatter {
        let formatter = DateFormatter()
        formatter.locale = Locale.current
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }

    private var locationStatusText: String {
        switch locationService.authorizationStatus {
        case .authorizedAlways, .authorizedWhenInUse:
            return locationService.currentLocation == nil
                ? String(localized: "location.pendingMessage")
                : String(localized: "location.positionTitle")
        case .denied, .restricted:
            return String(localized: "location.settingsMessage")
        case .notDetermined:
            return String(localized: "location.idleMessage")
        @unknown default:
            return String(localized: "location.unknownMessage")
        }
    }

    private var locationActionTitle: String {
        switch locationService.authorizationStatus {
        case .denied, .restricted:
            return String(localized: "location.openSettings")
        case .notDetermined:
            return String(localized: "location.idleAction")
        default:
            return String(localized: "location.retry")
        }
    }

    private func handleLocationAction() {
        switch locationService.authorizationStatus {
        case .denied, .restricted:
            guard let url = URL(string: UIApplication.openSettingsURLString) else { return }
            UIApplication.shared.open(url)
        case .notDetermined:
            locationService.requestAuthorization()
        default:
            locationService.requestSingleLocation()
        }
    }

}
