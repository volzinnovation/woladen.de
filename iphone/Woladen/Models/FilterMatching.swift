import Foundation

func normalizeAmenityNameQuery(_ value: String) -> String {
    let folded = value
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .folding(options: [.diacriticInsensitive, .widthInsensitive], locale: Locale(identifier: "en_US_POSIX"))
        .lowercased()
        .replacingOccurrences(of: "ß", with: "ss")

    return String(
        folded.unicodeScalars.filter { CharacterSet.alphanumerics.contains($0) }
    )
}

extension ChargerProperties {
    func matches(_ filterState: FilterState) -> Bool {
        if !filterState.operatorName.isEmpty && operatorName != filterState.operatorName {
            return false
        }
        if maxPowerKW < filterState.minPowerKW {
            return false
        }
        if !filterState.selectedAmenities.isEmpty {
            for key in filterState.selectedAmenities where (amenityCounts[key] ?? 0) <= 0 {
                return false
            }
        }
        return matchesAmenityNameQuery(filterState.amenityNameQuery)
    }

    func matchesAmenityNameQuery(_ query: String) -> Bool {
        let normalizedQuery = normalizeAmenityNameQuery(query)
        if normalizedQuery.isEmpty {
            return true
        }

        return amenityExamples.contains { example in
            guard let name = example.name else { return false }
            return normalizeAmenityNameQuery(name).contains(normalizedQuery)
        }
    }
}
