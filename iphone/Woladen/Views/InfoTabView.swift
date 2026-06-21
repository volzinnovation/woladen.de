import SwiftUI

private let favoriteStarColor = Color(red: 245.0 / 255.0, green: 158.0 / 255.0, blue: 11.0 / 255.0)

struct InfoTabView: View {
    private let websiteURL = URL(string: "https://woladen.de/")!
    private let privacyPolicyURL = URL(string: "https://woladen.de/privacy.html")!
    private let imprintURL = URL(string: "https://woladen.de/imprint.html")!
    private let studiosURL = URL(string: "https://studios.moonshots.gmbh/")!
    private let mobilithekURL = URL(string: "https://mobilithek.info/")!
    private let mobilithekSourcesText = "Aktive Datenangebote von 800 Volt Technologies GmbH, ELU Mobility, EnBW mobility+ AG & Co. KG, Monta ApS, Qwello Deutschland GmbH, SMATRICS GmbH & Co KG, Smartlab Innovationsgesellschaft mbH, Tesla Germany GmbH, Wirelane GmbH, chargecloud GmbH, eRound, eliso GmbH und vaylens GmbH."

    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    var body: some View {
        List {
            Section(String(localized: "info.aboutTitle")) {
                Text(aboutText)
                if let info = viewModel.activeCatalogInfo {
                    Text(dataUpdatedText(for: info.manifest.generatedAt))
                        .foregroundStyle(.secondary)
                }
            }

            Section(String(localized: "info.legendTitle")) {
                legendRow(color: Color.yellow, text: String(localized: "info.legendGold"))
                legendRow(color: Color.gray, text: String(localized: "info.legendSilver"))
                legendRow(color: Color.brown, text: String(localized: "info.legendBronze"))
                legendRow(color: Color.secondary, text: String(localized: "info.legendGrey"))
                favoriteLegendRow(text: String(localized: "info.legendFavorite"))
            }

            Section(String(localized: "info.contactTitle")) {
                VStack(alignment: .leading, spacing: 6) {
                    Text(String(localized: "info.developedBy"))
                    Link(
                        String(localized: "info.github"),
                        destination: URL(string: "https://github.com/volzinnovation/woladen.de")!
                    )
                    Text("\(String(localized: "info.distributedBy")) Moonshots Studios GmbH")
                    Link("woladen.de", destination: websiteURL)
                    Link("studios.moonshots.gmbh", destination: studiosURL)
                    Link(String(localized: "info.imprintLink"), destination: imprintURL)
                }
            }

            Section(String(localized: "info.privacyTitle")) {
                Text(String(localized: "info.privacyBody"))
                Link(String(localized: "info.privacyLink"), destination: privacyPolicyURL)
            }

            Section(String(localized: "info.dataSourcesTitle")) {
                Link(
                    "BNetzA: Ladesäulenregister (Downloads und Formulare)",
                    destination: URL(string: "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html")!
                )
                Link("Mobilithek", destination: mobilithekURL)
                Text(mobilithekSourcesText)
                Link(
                    "OpenStreetMap",
                    destination: URL(string: "https://www.openstreetmap.org/")!
                )
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

            Section(String(localized: "location.idleTitle")) {
                Text(locationStatusText)
                Button(String(localized: "location.retry")) {
                    if locationService.authorizationStatus == .notDetermined {
                        locationService.requestAuthorization()
                    } else {
                        locationService.requestSingleLocation()
                    }
                }
            }

            Section("API-Katalog") {
                Text(viewModel.humanReadableCatalogSource())
                if let info = viewModel.activeCatalogInfo {
                    Text("Version: \(info.manifest.version)")
                    Text(
                        String(localized: "station.updated")
                            .replacingOccurrences(of: "{date}", with: formattedTimestamp(info.manifest.generatedAt))
                    )
                    Text("Schema: \(info.manifest.schema)")
                        .foregroundStyle(.secondary)
                }

                Button(String(localized: "errors.reload")) {
                    viewModel.reloadCatalogForCurrentContext(userLocation: locationService.currentLocation)
                }
            }

            Section("Hinweis für getrennte Updates") {
                Text("Code und Daten sind getrennt: Die App lädt den öffentlichen Katalog über die Live-EU-API und nutzt einen begrenzten lokalen Cache.")
                    .foregroundStyle(.secondary)
            }
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

    private func favoriteLegendRow(text: String) -> some View {
        HStack(spacing: 10) {
            Image(systemName: "star.fill")
                .font(.system(size: 14, weight: .bold))
                .foregroundStyle(favoriteStarColor)
                .frame(width: 12, height: 12)
            Text(text)
        }
    }

    private func formattedTimestamp(_ raw: String) -> String {
        if let date = iso8601WithFractional.date(from: raw) ?? iso8601.date(from: raw) {
            return dateFormatter.string(from: date)
        }
        return raw
    }

    private var aboutText: String {
        let stationCount = viewModel.allFeatures.count
        let chargerCount = viewModel.allFeatures.reduce(0) { total, feature in
            total + feature.properties.chargingPointsCount
        }
        return [
            String(localized: "info.aboutIntro"),
            "\(stationCount)",
            String(localized: "info.aboutStationCountJoin"),
            "\(chargerCount)",
            String(localized: "info.aboutOutro")
        ].joined(separator: " ")
    }

    private func dataUpdatedText(for raw: String) -> String {
        String(localized: "info.dataUpdated")
            .replacingOccurrences(of: "{date}", with: formattedTimestamp(raw))
            .replacingOccurrences(of: "{counts}", with: "")
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
            return String(localized: "location.positionTitle")
        case .denied, .restricted:
            return String(localized: "location.deniedTitle")
        case .notDetermined:
            return String(localized: "location.idleTitle")
        @unknown default:
            return String(localized: "location.unknownMessage")
        }
    }

}
