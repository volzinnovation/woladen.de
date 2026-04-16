import Foundation
import CoreLocation

final class LocationService: NSObject, ObservableObject {
    @Published private(set) var authorizationStatus: CLAuthorizationStatus = .notDetermined
    @Published private(set) var currentLocation: CLLocation?
    @Published private(set) var lastError: String?

    private let manager = CLLocationManager()
    private let screenshotLocation = LocationService.resolveScreenshotLocation()
    private var requestedAlwaysUpgrade = false

    override init() {
        super.init()
        manager.delegate = self
        manager.desiredAccuracy = kCLLocationAccuracyNearestTenMeters
        manager.distanceFilter = 20
        if let screenshotLocation {
            authorizationStatus = .authorizedWhenInUse
            currentLocation = screenshotLocation
        } else {
            authorizationStatus = manager.authorizationStatus
        }
    }

    func activate() {
        if let screenshotLocation {
            authorizationStatus = .authorizedWhenInUse
            currentLocation = screenshotLocation
            lastError = nil
            return
        }

        authorizationStatus = manager.authorizationStatus
        switch authorizationStatus {
        case .notDetermined:
            manager.requestWhenInUseAuthorization()
        case .authorizedWhenInUse, .authorizedAlways:
            requestSingleLocation()
            startUpdates()
        default:
            stopUpdates()
        }
    }

    func requestAuthorization() {
        guard screenshotLocation == nil else { return }
        switch authorizationStatus {
        case .notDetermined:
            manager.requestWhenInUseAuthorization()
        case .authorizedWhenInUse:
            manager.requestAlwaysAuthorization()
        default:
            break
        }
    }

    func requestSingleLocation() {
        if let screenshotLocation {
            authorizationStatus = .authorizedWhenInUse
            currentLocation = screenshotLocation
            lastError = nil
            return
        }

        if authorizationStatus == .notDetermined {
            requestAuthorization()
            return
        }
        guard authorizationStatus == .authorizedWhenInUse || authorizationStatus == .authorizedAlways else {
            return
        }
        manager.requestLocation()
    }

    func startUpdates() {
        guard screenshotLocation == nil else { return }
        manager.startUpdatingLocation()
    }

    func stopUpdates() {
        guard screenshotLocation == nil else { return }
        manager.stopUpdatingLocation()
    }

    private static func resolveScreenshotLocation() -> CLLocation? {
        guard let rawLocation = ProcessInfo.processInfo.environment["WOLADEN_SCREENSHOT_LOCATION"] else {
            return nil
        }

        let parts = rawLocation
            .split(separator: ",", maxSplits: 1)
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }

        guard parts.count == 2,
              let latitude = Double(parts[0]),
              let longitude = Double(parts[1]) else {
            return nil
        }

        return CLLocation(latitude: latitude, longitude: longitude)
    }
}

extension LocationService: CLLocationManagerDelegate {
    func locationManagerDidChangeAuthorization(_ manager: CLLocationManager) {
        guard screenshotLocation == nil else { return }
        let status = manager.authorizationStatus
        DispatchQueue.main.async {
            self.authorizationStatus = status
            if status == .authorizedWhenInUse || status == .authorizedAlways {
                self.requestSingleLocation()
                self.startUpdates()
            } else {
                self.stopUpdates()
            }

            if status == .authorizedWhenInUse && !self.requestedAlwaysUpgrade {
                self.requestedAlwaysUpgrade = true
                manager.requestAlwaysAuthorization()
            }
        }
    }

    func locationManager(_ manager: CLLocationManager, didUpdateLocations locations: [CLLocation]) {
        guard screenshotLocation == nil else { return }
        if let first = locations.first {
            DispatchQueue.main.async {
                self.currentLocation = first
                self.lastError = nil
            }
        }
    }

    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
        guard screenshotLocation == nil else { return }
        DispatchQueue.main.async {
            self.lastError = error.localizedDescription
        }
    }
}
