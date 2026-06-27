import Foundation
import MapKit

@MainActor
final class PlaceSearchCompleter: NSObject, ObservableObject {
    @Published private(set) var completions: [MKLocalSearchCompletion] = []
    @Published private(set) var error: Error?

    private let completer = MKLocalSearchCompleter()
    private var lastQuery = ""

    override init() {
        super.init()
        completer.delegate = self
        completer.resultTypes = [.address, .pointOfInterest]
    }

    func update(query: String, region: MKCoordinateRegion?) {
        let trimmed = query.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.count >= 2 else {
            clear()
            return
        }
        if let region {
            completer.region = region
        }
        guard trimmed != lastQuery else { return }
        lastQuery = trimmed
        error = nil
        completer.queryFragment = trimmed
    }

    func clear() {
        lastQuery = ""
        completions = []
        error = nil
        completer.queryFragment = ""
    }
}

extension PlaceSearchCompleter: MKLocalSearchCompleterDelegate {
    nonisolated func completerDidUpdateResults(_ completer: MKLocalSearchCompleter) {
        let results = completer.results
        Task { @MainActor in
            self.error = nil
            self.completions = results
        }
    }

    nonisolated func completer(_ completer: MKLocalSearchCompleter, didFailWithError error: Error) {
        Task { @MainActor in
            self.error = error
            self.completions = []
        }
    }
}

