import SwiftUI
import UniformTypeIdentifiers

struct InfoTabView: View {
    private let websiteURL = URL(string: "https://woladen.de/")!
    private let privacyPolicyURL = URL(string: "https://woladen.de/privacy.html")!
    private let imprintURL = URL(string: "https://woladen.de/imprint.html")!
    private let studiosURL = URL(string: "https://studios.moonshots.gmbh/")!

    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @State private var showingImporter = false
    @State private var importMessage: String?
    @State private var importError: String?

    var body: some View {
        List {
            Section("Über woladen.de") {
                Text("Finde Schnellladesäulen mit der besten Aufenthaltsqualität. Wir zeigen dir, wo es sich lohnt zu laden. Ohne Ladeweile.")
                if let info = viewModel.activeBundleInfo {
                    Text("Datenstand: \(formattedTimestamp(info.manifest.generatedAt))")
                        .foregroundStyle(.secondary)
                }
            }

            Section("Legende") {
                legendRow(color: Color.yellow, text: ">10 Angebote vor Ort (Gold)")
                legendRow(color: Color.gray, text: ">5 Angebote vor Ort (Silber)")
                legendRow(color: Color.brown, text: ">1 Angebote vor Ort (Bronze)")
                legendRow(color: Color.secondary, text: "Keine Angebote vor Ort")
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
                Text("Favoriten und importierte Datenbundles bleiben auf deinem Gerät.")
                Link("Datenschutzerklärung", destination: privacyPolicyURL)
            }

            Section("Datenquellen & Lizenzen") {
                Link(
                    "BNetzA: Ladesäulenregister (Downloads und Formulare)",
                    destination: URL(string: "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/start.html")!
                )
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
                    locationService.requestSingleLocation()
                }
            }

            Section("Datenbundle") {
                Text(viewModel.humanReadableBundleSource())
                if let info = viewModel.activeBundleInfo {
                    Text("Version: \(info.manifest.version)")
                    Text("Erstellt am: \(formattedTimestamp(info.manifest.generatedAt))")
                }

                Button("Datenbundle importieren") {
                    showingImporter = true
                }

                Button("Installiertes Datenbundle entfernen", role: .destructive) {
                    do {
                        try DataBundleManager.shared.removeInstalledBundle()
                        viewModel.reloadDataAfterBundleUpdate(userLocation: locationService.currentLocation)
                        importMessage = "Installiertes Bundle entfernt. Baseline aktiv."
                        importError = nil
                    } catch {
                        importError = error.localizedDescription
                    }
                }
            }

            Section("Hinweis für getrennte Updates") {
                Text("Code und Daten sind getrennt: Die App enthält ein Baseline-Datenbundle. Optional kann ein neues Datenbundle als Ordner importiert werden (muss chargers_fast.geojson, operators.json und optional data_manifest.json enthalten).")
                    .foregroundStyle(.secondary)
            }

            if let importMessage {
                Section {
                    Text(importMessage)
                        .foregroundStyle(.green)
                }
            }

            if let importError {
                Section {
                    Text(importError)
                        .foregroundStyle(.red)
                }
            }
        }
        .fileImporter(
            isPresented: $showingImporter,
            allowedContentTypes: [.folder],
            allowsMultipleSelection: false
        ) { result in
            switch result {
            case .success(let urls):
                guard let url = urls.first else { return }
                importBundle(at: url)
            case .failure(let error):
                importError = error.localizedDescription
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

    private func importBundle(at url: URL) {
        let needsAccess = url.startAccessingSecurityScopedResource()
        defer {
            if needsAccess {
                url.stopAccessingSecurityScopedResource()
            }
        }

        do {
            try DataBundleManager.shared.installBundle(from: url)
            viewModel.reloadDataAfterBundleUpdate(userLocation: locationService.currentLocation)
            viewModel.applyFilters(userLocation: locationService.currentLocation)
            importMessage = "Datenbundle erfolgreich importiert."
            importError = nil
        } catch {
            importError = error.localizedDescription
        }
    }
}
