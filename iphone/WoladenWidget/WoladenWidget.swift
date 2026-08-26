import SwiftUI
import WidgetKit
import CoreLocation

struct WoladenStationEntry: TimelineEntry {
    let date: Date
    let content: WoladenWidgetContent
}

struct WoladenStationProvider: TimelineProvider {
    func placeholder(in context: Context) -> WoladenStationEntry {
        WoladenStationEntry(date: Date(), content: previewContent)
    }

    func getSnapshot(in context: Context, completion: @escaping (WoladenStationEntry) -> Void) {
        completion(WoladenStationEntry(date: Date(), content: WoladenWidgetStateStore.currentContent() ?? previewContent))
    }

    func getTimeline(in context: Context, completion: @escaping (Timeline<WoladenStationEntry>) -> Void) {
        Task {
            let content = await refreshedContent()
            let refreshInterval: TimeInterval = content.mode == .trip ? 15 * 60 : 30 * 60
            completion(
                Timeline(
                    entries: [WoladenStationEntry(date: Date(), content: content)],
                    policy: .after(Date().addingTimeInterval(refreshInterval))
                )
            )
        }
    }

    private func refreshedContent() async -> WoladenWidgetContent {
        let mode = WoladenWidgetStateStore.loadMode()
        if mode == .trip {
            guard let saved = WoladenWidgetStateStore.loadTrip() else {
                return .empty(mode: .trip, state: .noTripTarget)
            }
            guard let target = saved.station else { return saved }
            do {
                guard let live = try await WoladenWidgetAPIClient().lookup(stationIDs: [target.stationID]).first else {
                    return saved
                }
                var refreshed = target
                refreshed.availabilityStatus = live.availabilityStatus
                refreshed.availableEVSEs = live.availableEVSEs
                refreshed.totalEVSEs = live.totalEVSEs
                if !live.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                    refreshed.priceDisplay = live.priceDisplay
                }
                refreshed.sourceUpdatedAt = woladenParseISO8601(live.sourceObservedAt) ?? woladenParseISO8601(live.fetchedAt)
                let content = WoladenWidgetContent(
                    mode: .trip,
                    state: .station,
                    station: refreshed,
                    generatedAt: Date(),
                    locationIsStale: false
                )
                WoladenWidgetStateStore.saveTrip(content)
                return content
            } catch {
                return WoladenWidgetContent(
                    mode: saved.mode,
                    state: saved.station == nil ? .networkUnavailable : saved.state,
                    station: saved.station,
                    generatedAt: saved.generatedAt,
                    locationIsStale: true
                )
            }
        }

        guard CLLocationManager.locationServicesEnabled() else {
            return planningFallback(state: .locationPermissionRequired)
        }
        guard let location = await WoladenWidgetLocationProvider.location() else {
            return planningFallback(state: .locationUnavailable)
        }
        do {
            guard let station = try await WoladenWidgetAPIClient()
                .nearestStations(location: location, filter: WoladenWidgetStateStore.loadFilter())
                .first else {
                let content = WoladenWidgetContent.empty(mode: .plan, state: .noMatch)
                WoladenWidgetStateStore.savePlanning(content)
                return content
            }
            let content = WoladenWidgetContent(
                mode: .plan,
                state: .station,
                station: station.snapshot(from: location),
                generatedAt: Date(),
                locationIsStale: false
            )
            WoladenWidgetStateStore.savePlanning(content)
            return content
        } catch {
            return planningFallback(state: .networkUnavailable)
        }
    }

    private func planningFallback(state: WoladenWidgetContent.State) -> WoladenWidgetContent {
        guard let saved = WoladenWidgetStateStore.loadPlanning(), saved.station != nil else {
            return .empty(mode: .plan, state: state)
        }
        return WoladenWidgetContent(
            mode: .plan,
            state: saved.state,
            station: saved.station,
            generatedAt: saved.generatedAt,
            locationIsStale: true
        )
    }

    private var previewContent: WoladenWidgetContent {
        WoladenWidgetContent(
            mode: .plan,
            state: .station,
            station: WoladenWidgetStation(
                stationID: "preview",
                operatorName: "Woladen",
                stationName: "Schnellladepark",
                city: "Berlin",
                address: "Musterstraße 1",
                latitude: 52.52,
                longitude: 13.405,
                maxPowerKW: 300,
                chargingPointCount: 12,
                availabilityStatus: "available",
                availableEVSEs: 7,
                totalEVSEs: 12,
                priceDisplay: "0,49 €/kWh",
                sourceUpdatedAt: Date(),
                distanceM: 1_800,
                eta: nil
            ),
            generatedAt: Date(),
            locationIsStale: false
        )
    }
}

@MainActor
private final class WoladenWidgetLocationProvider: NSObject, @preconcurrency CLLocationManagerDelegate {
    private let manager = CLLocationManager()
    private var continuation: CheckedContinuation<CLLocation?, Never>?

    static func location() async -> CLLocation? {
        let provider = WoladenWidgetLocationProvider()
        return await withCheckedContinuation { continuation in
            provider.continuation = continuation
            provider.manager.delegate = provider
            provider.manager.desiredAccuracy = kCLLocationAccuracyHundredMeters
            guard provider.manager.isAuthorizedForWidgetUpdates else {
                continuation.resume(returning: nil)
                provider.continuation = nil
                return
            }
            provider.manager.requestLocation()
            Task { @MainActor in
                try? await Task.sleep(for: .seconds(8))
                provider.finish(nil)
            }
        }
    }

    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        let recent = locations
            .filter { $0.horizontalAccuracy >= 0 && Date().timeIntervalSince($0.timestamp) < 5 * 60 }
            .min(by: { $0.horizontalAccuracy < $1.horizontalAccuracy })
        finish(recent)
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        finish(nil)
    }

    private func finish(_ location: CLLocation?) {
        continuation?.resume(returning: location)
        continuation = nil
        manager.stopUpdatingLocation()
    }
}

struct WoladenStationWidgetView: View {
    @Environment(\.widgetFamily) private var family
    let entry: WoladenStationEntry

    var body: some View {
        Group {
            if let station = entry.content.station {
                stationView(station)
            } else {
                emptyView
            }
        }
        .containerBackground(for: .widget) { Color(.systemBackground) }
        .widgetURL(entry.content.station?.deepLinkURL ?? URL(string: "woladen://\(entry.content.mode.rawValue)"))
    }

    private func stationView(_ station: WoladenWidgetStation) -> some View {
        VStack(alignment: .leading, spacing: family == .systemSmall ? 5 : 8) {
            HStack(spacing: 5) {
                Image(systemName: entry.content.mode == .trip ? "car.fill" : "location.fill")
                Text(entry.content.mode == .trip
                     ? String(localized: "widget.mode.trip", defaultValue: "Fahrtziel")
                     : String(localized: "widget.mode.plan", defaultValue: "In deiner Nähe"))
                Spacer(minLength: 2)
                if entry.content.locationIsStale {
                    Image(systemName: "clock.badge.exclamationmark")
                        .accessibilityLabel(String(localized: "widget.stale", defaultValue: "Stand möglicherweise veraltet"))
                }
            }
            .font(.caption.weight(.semibold))
            .foregroundStyle(.secondary)

            Text(station.displayName)
                .font(family == .systemSmall ? .headline : .title3.weight(.bold))
                .lineLimit(1)

            HStack(spacing: 5) {
                Image(systemName: statusSymbol(station.availabilityStatus))
                    .foregroundStyle(statusColor(station.availabilityStatus))
                Text(availabilityText(station))
                    .font(.subheadline.weight(.semibold))
                    .lineLimit(1)
            }

            if family == .systemMedium {
                HStack(spacing: 12) {
                    Label(distanceText(station.distanceM), systemImage: "location")
                    Label("\(Int(station.maxPowerKW.rounded())) kW", systemImage: "bolt.fill")
                    if let eta = station.eta {
                        Label(eta.formatted(date: .omitted, time: .shortened), systemImage: "clock")
                    }
                }
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(1)
            } else {
                Text("\(distanceText(station.distanceM)) · \(Int(station.maxPowerKW.rounded())) kW")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }

            if !station.priceDisplay.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                Text(station.priceDisplay)
                    .font(.caption.weight(.medium))
                    .lineLimit(1)
            }

            if let freshness = freshnessText(station) {
                Text(freshness)
                    .font(.caption2)
                    .foregroundStyle(isStale(station) ? .orange : .secondary)
                    .lineLimit(1)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var emptyView: some View {
        VStack(alignment: .leading, spacing: 8) {
            Image(systemName: emptySymbol)
                .font(.title2)
                .foregroundStyle(.secondary)
            Text(emptyTitle)
                .font(.headline)
            Text(emptyDetail)
                .font(.caption)
                .foregroundStyle(.secondary)
                .lineLimit(3)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private var emptySymbol: String {
        switch entry.content.state {
        case .locationPermissionRequired, .locationUnavailable: "location.slash"
        case .networkUnavailable: "wifi.slash"
        case .noMatch: "line.3.horizontal.decrease.circle"
        case .noTripTarget: "car"
        case .station: "bolt.fill"
        }
    }

    private var emptyTitle: String {
        switch entry.content.state {
        case .locationPermissionRequired, .locationUnavailable:
            String(localized: "widget.location.title", defaultValue: "Standort erforderlich")
        case .networkUnavailable:
            String(localized: "widget.network.title", defaultValue: "Keine Live-Daten")
        case .noMatch:
            String(localized: "widget.nomatch.title", defaultValue: "Keine passende Station")
        case .noTripTarget:
            String(localized: "widget.notarget.title", defaultValue: "Kein Fahrtziel")
        case .station:
            "Woladen"
        }
    }

    private var emptyDetail: String {
        switch entry.content.state {
        case .locationPermissionRequired, .locationUnavailable:
            String(localized: "widget.location.detail", defaultValue: "Woladen öffnen und Standortzugriff erlauben.")
        case .networkUnavailable:
            String(localized: "widget.network.detail", defaultValue: "Woladen zeigt wieder Daten, sobald die Verbindung steht.")
        case .noMatch:
            String(localized: "widget.nomatch.detail", defaultValue: "Passe die aktiven Filter in der App an.")
        case .noTripTarget:
            String(localized: "widget.notarget.detail", defaultValue: "Wähle in Woladen eine Station als Fahrtziel.")
        case .station:
            ""
        }
    }

    private func availabilityText(_ station: WoladenWidgetStation) -> String {
        if isStale(station) {
            return String(localized: "widget.status.stale", defaultValue: "Status veraltet")
        }
        if station.totalEVSEs > 0 {
            return "\(station.availableEVSEs)/\(station.totalEVSEs) \(String(localized: "availability.available", defaultValue: "frei"))"
        }
        switch station.availabilityStatus.lowercased() {
        case "available": return String(localized: "availability.available", defaultValue: "Verfügbar")
        case "occupied": return String(localized: "availability.occupied", defaultValue: "Belegt")
        case "out_of_service": return String(localized: "availability.outOfService", defaultValue: "Außer Betrieb")
        default: return String(localized: "availability.unknown", defaultValue: "Unbekannt")
        }
    }

    private func isStale(_ station: WoladenWidgetStation) -> Bool {
        entry.content.locationIsStale
            || station.sourceUpdatedAt.map { Date().timeIntervalSince($0) > 15 * 60 } == true
    }

    private func freshnessText(_ station: WoladenWidgetStation) -> String? {
        guard let date = station.sourceUpdatedAt else {
            return String(localized: "widget.updated.unknown", defaultValue: "Aktualisierung unbekannt")
        }
        let relative = date.formatted(.relative(presentation: .named, unitsStyle: .abbreviated))
        return String(localized: "widget.updated", defaultValue: "Aktualisiert {time}")
            .replacingOccurrences(of: "{time}", with: relative)
    }

    private func statusSymbol(_ status: String) -> String {
        switch status.lowercased() {
        case "available": "checkmark.circle.fill"
        case "occupied": "clock.fill"
        case "out_of_service": "exclamationmark.triangle.fill"
        default: "questionmark.circle.fill"
        }
    }

    private func statusColor(_ status: String) -> Color {
        switch status.lowercased() {
        case "available": .green
        case "occupied": .orange
        case "out_of_service": .red
        default: .secondary
        }
    }

    private func distanceText(_ meters: Double?) -> String {
        guard let meters else { return "–" }
        return Measurement(value: max(0, meters), unit: UnitLength.meters)
            .formatted(.measurement(width: .abbreviated, usage: .road))
    }
}

struct WoladenStationWidget: Widget {
    let kind = WoladenShared.widgetKind

    var body: some WidgetConfiguration {
        StaticConfiguration(kind: kind, provider: WoladenStationProvider()) { entry in
            WoladenStationWidgetView(entry: entry)
        }
        .configurationDisplayName(String(localized: "widget.name", defaultValue: "Woladen Station"))
        .description(String(localized: "widget.description", defaultValue: "Zeigt dein Fahrtziel oder die nächste passende Ladestation."))
        .supportedFamilies([.systemSmall, .systemMedium])
    }
}

@main
struct WoladenWidgetBundle: WidgetBundle {
    var body: some Widget {
        WoladenStationWidget()
    }
}
