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
            Section("Über woladen.de") {
                Text("Finde Schnellladesäulen mit der besten Aufenthaltsqualität. Wir zeigen dir, wo es sich lohnt zu laden. Ohne Ladeweile.")
                if let info = viewModel.activeCatalogInfo {
                    Text("Datenstand: \(formattedTimestamp(info.manifest.generatedAt))")
                        .foregroundStyle(.secondary)
                }
            }

            Section("Legende") {
                legendRow(color: Color.yellow, text: ">10 Angebote vor Ort (Gold)")
                legendRow(color: Color.gray, text: ">5 Angebote vor Ort (Silber)")
                legendRow(color: Color.brown, text: ">1 Angebote vor Ort (Bronze)")
                legendRow(color: Color.secondary, text: "Keine Angebote vor Ort")
                favoriteLegendRow(text: "Favorit")
            }

            Section("Kontakt & Code") {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Entwickelt von Prof. Dr. Raphael Volz")
                    Text("Hochschule Pforzheim")
                    Link(
                        "GitHub Projekt",
                        destination: URL(string: "https://github.com/volzinnovation/woladen.de")!
                    )
                    Text("Die Moonshots Studios GmbH betreibt und vertreibt woladen.de und die begleitenden Apps für iPhone und Android.")
                    Link("woladen.de", destination: websiteURL)
                    Link("studios.moonshots.gmbh", destination: studiosURL)
                    Link("Impressum", destination: imprintURL)
                }
            }

            Section("Datenschutz") {
                Text("Standortzugriff ist optional. Wenn du ihn freigibst, wird er verwendet, um die Karte auf deine Umgebung zu fokussieren und nahe Schnelllader zu sortieren.")
                Text("Favoriten und der lokale API-Cache bleiben auf deinem Gerät.")
                Link("Datenschutzerklärung", destination: privacyPolicyURL)
            }

            Section("Datenquellen & Lizenzen") {
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
                Text("Kartendaten und POI-Daten © OpenStreetMap-Mitwirkende, verfügbar unter ODbL v1.0.")
                Link(
                    "OpenStreetMap: Copyright und Lizenzhinweise",
                    destination: URL(string: "https://www.openstreetmap.org/copyright")!
                )
                Link(
                    "ODbL v1.0: Vollständiger Lizenztext",
                    destination: URL(string: "https://opendatacommons.org/licenses/odbl/1.0/")!
                )
            }

            Section("Standort") {
                Text(locationStatusText)
                Button("Standort aktualisieren") {
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
                    Text("Erstellt am: \(formattedTimestamp(info.manifest.generatedAt))")
                    Text("Schema: \(info.manifest.schema)")
                        .foregroundStyle(.secondary)
                }

                Button("Katalog neu laden") {
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
            return deFormatter.string(from: date)
        }
        return raw
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

    private var deFormatter: DateFormatter {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "de_DE")
        formatter.dateStyle = .medium
        formatter.timeStyle = .short
        return formatter
    }

    private var locationStatusText: String {
        switch locationService.authorizationStatus {
        case .authorizedAlways, .authorizedWhenInUse:
            return "Standortzugriff erlaubt"
        case .denied, .restricted:
            return "Standortzugriff nicht erlaubt"
        case .notDetermined:
            return "Standortzugriff noch nicht entschieden"
        @unknown default:
            return "Standortstatus unbekannt"
        }
    }

}
