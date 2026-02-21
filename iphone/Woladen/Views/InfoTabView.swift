import SwiftUI
import UniformTypeIdentifiers

struct InfoTabView: View {
    @EnvironmentObject private var viewModel: AppViewModel
    @EnvironmentObject private var locationService: LocationService

    @State private var showingImporter = false
    @State private var importMessage: String?
    @State private var importError: String?

    var body: some View {
        List {
            Section("Woladen") {
                Text("Offline-first iPhone App fur Schnellladepunkte mit Aufenthaltsqualitat.")
                Text("Alle Basisdaten sind in der App enthalten und funktionieren ohne Internet.")
                    .foregroundStyle(.secondary)
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
                    Text("Generated: \(info.manifest.generatedAt)")
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

            Section("Hinweis fur getrennte Updates") {
                Text("Code und Daten sind getrennt: Die App enthalt ein Baseline-Datenbundle. Optional kann ein neues Datenbundle als Ordner importiert werden (muss chargers_fast.geojson, operators.json und optional data_manifest.json enthalten).")
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
        .navigationTitle("Info")
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
