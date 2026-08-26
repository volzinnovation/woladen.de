import Foundation
import XCTest
@testable import Woladen

final class OpeningHoursStatusTests: XCTestCase {
    private let berlin = TimeZone(identifier: "Europe/Berlin")!

    func testWeekdayScheduleReportsOpenAndClosingTime() throws {
        let now = try date("2026-08-26T15:00:00Z") // Wednesday, 17:00 in Berlin.

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            "Mo-Fr 08:00-18:00",
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))

        XCTAssertEqual(evaluation.state, .open)
        XCTAssertEqual(evaluation.nextChange, try date("2026-08-26T16:00:00Z"))
    }

    func testWeekdayScheduleReportsClosedAndOpeningTime() throws {
        let now = try date("2026-08-26T05:00:00Z") // Wednesday, 07:00 in Berlin.

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            "Mo-Fr 08:00-18:00",
            now: now,
            timeZone: berlin,
            countryCode: "DEU"
        ))

        XCTAssertEqual(evaluation.state, .closed)
        XCTAssertEqual(evaluation.nextChange, try date("2026-08-26T06:00:00Z"))
    }

    func testSplitShiftFindsSameDayReopening() throws {
        let now = try date("2026-08-26T10:30:00Z") // Wednesday, 12:30 in Berlin.

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            "Mo-Fr 08:00-12:00,13:00-18:00",
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))

        XCTAssertEqual(evaluation.state, .closed)
        XCTAssertEqual(evaluation.nextChange, try date("2026-08-26T11:00:00Z"))
    }

    func testOvernightScheduleClosesAfterMidnight() throws {
        let now = try date("2026-08-28T23:00:00Z") // Saturday, 01:00 in Berlin.

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            "Fr 20:00-02:00",
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))

        XCTAssertEqual(evaluation.state, .open)
        XCTAssertEqual(evaluation.nextChange, try date("2026-08-29T00:00:00Z"))
    }

    func testDayScopedOpenRuleDoesNotCarryIntoFollowingDay() throws {
        let now = try date("2026-08-25T08:00:00Z") // Tuesday, 10:00 in Berlin.

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            "Mo open; Tu off",
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))

        XCTAssertEqual(evaluation.state, .closed)
    }

    func testPhotoScheduleIsInterpretedInsteadOfEchoed() throws {
        let raw = "Mo-Fr 06:00-20:00; Sa 07:00-18:00; Su 09:00-18:00; PH 09:00-18:00"
        let now = try date("2026-08-26T09:39:00Z") // Wednesday, 11:39 in Berlin.

        let detail = carPlayAmenityDetailText(
            distance: "43 m",
            openingHours: raw,
            countryCode: "DE",
            now: now,
            locale: Locale(identifier: "de_DE")
        )

        XCTAssertTrue(detail.hasPrefix("43 m · Jetzt geöffnet"), detail)
        XCTAssertTrue(detail.contains("Schließt um 20:00"), detail)
        XCTAssertFalse(detail.contains("Mo-Fr"), detail)
    }

    func testGermanPublicHolidayClauseOverridesWeekdayHours() throws {
        let raw = "Mo-Fr 06:00-20:00; Sa 07:00-18:00; Su 09:00-18:00; PH 09:00-18:00"
        let now = try date("2026-12-25T07:30:00Z") // Christmas Day, 08:30 in Berlin.

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            raw,
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))

        XCTAssertEqual(evaluation.state, .closed)
        XCTAssertEqual(evaluation.nextChange, try date("2026-12-25T08:00:00Z"))
    }

    func testRegionalPublicHolidayFallsBackWithoutSubdivisionContext() throws {
        let raw = "Mo-Fr 06:00-20:00; Sa 07:00-18:00; Su 09:00-18:00; PH 09:00-18:00"
        let now = try date("2026-10-31T07:30:00Z") // Reformation Day is regional in Germany.

        XCTAssertNil(woladenAmenityOpeningEvaluation(
            raw,
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))
        XCTAssertEqual(
            woladenAmenityOpeningDisplay(
                raw,
                now: now,
                timeZone: berlin,
                countryCode: "DE",
                locale: Locale(identifier: "de_DE")
            ),
            raw
        )
    }

    func testUnsupportedScheduleFallsBackToExactSourceText() throws {
        let raw = "sunrise-sunset; by appointment"
        let now = try date("2026-08-26T09:39:00Z")

        XCTAssertNil(woladenAmenityOpeningEvaluation(
            raw,
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))
        XCTAssertEqual(
            carPlayAmenityDetailText(
                distance: "125 m",
                openingHours: raw,
                countryCode: "DE",
                now: now,
                locale: Locale(identifier: "de_DE")
            ),
            "125 m · \(raw)"
        )
    }

    func testAlwaysOpenScheduleHasNoSyntheticClosingTime() throws {
        let now = try date("2026-08-26T09:39:00Z")

        let evaluation = try XCTUnwrap(woladenAmenityOpeningEvaluation(
            "24/7",
            now: now,
            timeZone: berlin,
            countryCode: "DE"
        ))

        XCTAssertEqual(evaluation.state, .open)
        XCTAssertNil(evaluation.nextChange)
        XCTAssertEqual(
            woladenAmenityOpeningDisplay(
                "24/7",
                now: now,
                timeZone: berlin,
                countryCode: "DE",
                locale: Locale(identifier: "de_DE")
            ),
            "Jetzt geöffnet"
        )
    }

    func testCountryCodeSelectsStationLocalTimeZone() {
        XCTAssertEqual(woladenOpeningTimeZone(countryCode: "DEU").identifier, "Europe/Berlin")
        XCTAssertEqual(woladenOpeningTimeZone(countryCode: "FI").identifier, "Europe/Helsinki")
    }

    private func date(_ value: String) throws -> Date {
        try XCTUnwrap(ISO8601DateFormatter().date(from: value))
    }
}
