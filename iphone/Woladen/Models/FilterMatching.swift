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
    var hasAvailabilitySummary: Bool {
        occupancyTotalEVSEs > 0
    }

    var hasAvailableChargingPoint: Bool {
        occupancyTotalEVSEs > 0 && occupancyAvailableEVSEs > 0
    }

    func matches(_ filterState: FilterState) -> Bool {
        if !filterState.operatorName.isEmpty && operatorName != filterState.operatorName {
            return false
        }
        if maxPowerKW < filterState.minPowerKW {
            return false
        }
        if filterState.minAmenityCount > 0 && amenitiesTotal < Int(filterState.minAmenityCount.rounded()) {
            return false
        }
        if filterState.availableOnly && !hasAvailableChargingPoint {
            return false
        }
        if filterState.currentlyOpenOnly && !hasOpenAmenity {
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

    var hasOpenAmenity: Bool {
        amenityExamples.contains { isAmenityOpen($0.openingHours) }
    }
}

private let openingDayKeys = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]

private struct OpeningNowParts {
    let dayKey: String
    let previousDayKey: String
    let minuteOfDay: Int
}

private struct OpeningClause {
    let selectedDays: Set<String>?
    let mode: OpeningMode
    let ranges: [OpeningRange]
}

private enum OpeningMode {
    case open
    case closed
    case times
    case unknown
}

private struct OpeningRange {
    let start: Int
    let end: Int
    let openEnded: Bool
}

private func isAmenityOpen(_ openingHours: String?, now: Date = Date()) -> Bool {
    let normalized = (openingHours ?? "")
        .trimmingCharacters(in: .whitespacesAndNewlines)
        .replacingOccurrences(of: "\\s+", with: " ", options: .regularExpression)

    guard !normalized.isEmpty else { return false }
    if normalized.compare("24/7", options: .caseInsensitive) == .orderedSame { return true }
    if normalized.range(of: "^(?:off|closed)$", options: [.regularExpression, .caseInsensitive]) != nil {
        return false
    }
    if normalized.compare("open", options: .caseInsensitive) == .orderedSame { return true }

    let parts = openingNowParts(now: now)
    let clauses = normalized
        .split(separator: ";")
        .compactMap { parseOpeningClause(String($0)) }

    guard !clauses.isEmpty else { return false }

    var currentState: OpeningMode?
    for clause in clauses {
        if let state = openingState(for: clause, dayKey: parts.dayKey, minuteOfDay: parts.minuteOfDay, previousDay: false) {
            currentState = state
        }
    }
    if currentState == .open { return true }
    if currentState == .unknown { return false }

    for clause in clauses {
        if openingState(for: clause, dayKey: parts.previousDayKey, minuteOfDay: parts.minuteOfDay, previousDay: true) == .open {
            return true
        }
    }
    return false
}

private func openingNowParts(now: Date) -> OpeningNowParts {
    var calendar = Calendar(identifier: .gregorian)
    calendar.timeZone = TimeZone(identifier: "Europe/Berlin") ?? .current
    let weekday = calendar.component(.weekday, from: now)
    let dayIndex = (weekday + 5) % 7
    let hour = calendar.component(.hour, from: now)
    let minute = calendar.component(.minute, from: now)
    return OpeningNowParts(
        dayKey: openingDayKeys[dayIndex],
        previousDayKey: openingDayKeys[(dayIndex + 6) % 7],
        minuteOfDay: hour * 60 + minute
    )
}

private func parseOpeningClause(_ value: String) -> OpeningClause? {
    let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !trimmed.isEmpty else { return nil }

    let pattern = #"^((?:Mo|Tu|We|Th|Fr|Sa|Su)(?:\s*-\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?(?:\s*,\s*(?:Mo|Tu|We|Th|Fr|Sa|Su)(?:\s*-\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\s+(.+)$"#
    let regex = try? NSRegularExpression(pattern: pattern)
    let range = NSRange(trimmed.startIndex..<trimmed.endIndex, in: trimmed)
    let match = regex?.firstMatch(in: trimmed, range: range)
    let selector: String?
    let body: String
    if let match, match.numberOfRanges >= 3,
       let selectorRange = Range(match.range(at: 1), in: trimmed),
       let bodyRange = Range(match.range(at: 2), in: trimmed) {
        selector = String(trimmed[selectorRange])
        body = String(trimmed[bodyRange]).trimmingCharacters(in: .whitespacesAndNewlines)
    } else {
        selector = nil
        body = trimmed
    }

    if body.range(of: "^(?:off|closed)$", options: [.regularExpression, .caseInsensitive]) != nil {
        return OpeningClause(selectedDays: selectedOpeningDays(selector), mode: .closed, ranges: [])
    }
    if body.compare("open", options: .caseInsensitive) == .orderedSame {
        return OpeningClause(selectedDays: selectedOpeningDays(selector), mode: .open, ranges: [])
    }

    let ranges = body
        .split(separator: ",")
        .compactMap { parseOpeningRange(String($0)) }
    guard !ranges.isEmpty else {
        return OpeningClause(selectedDays: selectedOpeningDays(selector), mode: .unknown, ranges: [])
    }
    return OpeningClause(selectedDays: selectedOpeningDays(selector), mode: .times, ranges: ranges)
}

private func selectedOpeningDays(_ selector: String?) -> Set<String>? {
    guard let selector, !selector.isEmpty else { return nil }
    var selected = Set<String>()
    for rawPart in selector.split(separator: ",") {
        let part = rawPart.trimmingCharacters(in: .whitespacesAndNewlines)
        if part == "PH" { continue }
        if part.contains("-") {
            let bounds = part.split(separator: "-").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            guard bounds.count == 2,
                  let start = openingDayKeys.firstIndex(of: bounds[0]),
                  let end = openingDayKeys.firstIndex(of: bounds[1]) else { continue }
            for offset in 0..<openingDayKeys.count {
                let index = (start + offset) % openingDayKeys.count
                selected.insert(openingDayKeys[index])
                if index == end { break }
            }
        } else if openingDayKeys.contains(part) {
            selected.insert(part)
        }
    }
    return selected
}

private func parseOpeningRange(_ value: String) -> OpeningRange? {
    let compact = value.replacingOccurrences(of: "\\s+", with: "", options: .regularExpression)
    if compact.hasSuffix("+"), !compact.contains("-") {
        guard let start = parseOpeningMinute(String(compact.dropLast())) else { return nil }
        return OpeningRange(start: start, end: 24 * 60, openEnded: true)
    }

    let parts = compact.replacingOccurrences(of: "+", with: "").split(separator: "-")
    guard parts.count == 2,
          let start = parseOpeningMinute(String(parts[0])),
          let end = parseOpeningMinute(String(parts[1])) else { return nil }
    return OpeningRange(start: start, end: end, openEnded: false)
}

private func parseOpeningMinute(_ value: String) -> Int? {
    let pieces = value.split(separator: ":")
    guard pieces.count == 2,
          let hour = Int(pieces[0]),
          let minute = Int(pieces[1]),
          hour >= 0, hour <= 24,
          minute >= 0, minute <= 59,
          !(hour == 24 && minute != 0) else { return nil }
    return hour * 60 + minute
}

private func openingState(for clause: OpeningClause, dayKey: String, minuteOfDay: Int, previousDay: Bool) -> OpeningMode? {
    if let selectedDays = clause.selectedDays, !selectedDays.contains(dayKey) {
        return nil
    }
    switch clause.mode {
    case .closed:
        return previousDay ? nil : .closed
    case .open:
        return .open
    case .unknown:
        return previousDay ? nil : .unknown
    case .times:
        return clause.ranges.contains { isWithinOpeningRange($0, minuteOfDay: minuteOfDay, previousDay: previousDay) }
            ? .open
            : (previousDay ? nil : .closed)
    }
}

private func isWithinOpeningRange(_ range: OpeningRange, minuteOfDay: Int, previousDay: Bool) -> Bool {
    if range.openEnded {
        return previousDay ? minuteOfDay < 6 * 60 : minuteOfDay >= range.start
    }
    if range.start == range.end { return true }
    if range.start < range.end {
        return !previousDay && minuteOfDay >= range.start && minuteOfDay < range.end
    }
    return previousDay ? minuteOfDay < range.end : minuteOfDay >= range.start
}
