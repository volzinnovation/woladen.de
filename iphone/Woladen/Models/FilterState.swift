import Foundation

struct FilterState: Equatable {
    var operatorName: String = ""
    var minPowerKW: Double = 50
    var selectedAmenities: Set<String> = []

    var activeCount: Int {
        var count = 0
        if !operatorName.isEmpty { count += 1 }
        if minPowerKW > 50 { count += 1 }
        count += selectedAmenities.count
        return count
    }
}
