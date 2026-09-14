package de.woladen.android.model

import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.Locale
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class OpeningHoursTest {
    private val berlin = ZoneId.of("Europe/Berlin")

    @Test
    fun weekdayScheduleReportsOpenAndClosingTime() {
        val evaluation = evaluateOpeningHours(
            "Mo-Fr 08:00-18:00",
            now = at("2026-08-26T17:00:00+02:00"),
            timeZone = berlin,
            countryCode = "DE"
        )

        assertNotNull(evaluation)
        assertEquals(OpeningState.OPEN, evaluation?.state)
        assertEquals(at("2026-08-26T18:00:00+02:00").toInstant(), evaluation?.nextChange?.toInstant())
    }

    @Test
    fun weekdayScheduleReportsClosedAndOpeningTime() {
        val evaluation = evaluateOpeningHours(
            "Mo-Fr 08:00-18:00",
            now = at("2026-08-26T07:00:00+02:00"),
            timeZone = berlin,
            countryCode = "DEU"
        )

        assertEquals(OpeningState.CLOSED, evaluation?.state)
        assertEquals(at("2026-08-26T08:00:00+02:00").toInstant(), evaluation?.nextChange?.toInstant())
    }

    @Test
    fun splitShiftFindsSameDayReopening() {
        val evaluation = evaluateOpeningHours(
            "Mo-Fr 08:00-12:00,13:00-18:00",
            now = at("2026-08-26T12:30:00+02:00"),
            timeZone = berlin,
            countryCode = "DE"
        )

        assertEquals(OpeningState.CLOSED, evaluation?.state)
        assertEquals(at("2026-08-26T13:00:00+02:00").toInstant(), evaluation?.nextChange?.toInstant())
    }

    @Test
    fun overnightScheduleClosesAfterMidnight() {
        val evaluation = evaluateOpeningHours(
            "Fr 20:00-02:00",
            now = at("2026-08-29T01:00:00+02:00"),
            timeZone = berlin,
            countryCode = "DE"
        )

        assertEquals(OpeningState.OPEN, evaluation?.state)
        assertEquals(at("2026-08-29T02:00:00+02:00").toInstant(), evaluation?.nextChange?.toInstant())
    }

    @Test
    fun dayScopedOpenRuleDoesNotCarryIntoFollowingDay() {
        val evaluation = evaluateOpeningHours(
            "Mo open; Tu off",
            now = at("2026-08-25T10:00:00+02:00"),
            timeZone = berlin,
            countryCode = "DE"
        )

        assertEquals(OpeningState.CLOSED, evaluation?.state)
    }

    @Test
    fun germanPublicHolidayClauseOverridesWeekdayHours() {
        val raw = "Mo-Fr 06:00-20:00; Sa 07:00-18:00; Su 09:00-18:00; PH 09:00-18:00"
        val evaluation = evaluateOpeningHours(
            raw,
            now = at("2026-12-25T08:30:00+01:00"),
            timeZone = berlin,
            countryCode = "DE"
        )

        assertEquals(OpeningState.CLOSED, evaluation?.state)
        assertEquals(at("2026-12-25T09:00:00+01:00").toInstant(), evaluation?.nextChange?.toInstant())
    }

    @Test
    fun regionalPublicHolidayFallsBackToSourceText() {
        val raw = "Mo-Fr 06:00-20:00; Sa 07:00-18:00; Su 09:00-18:00; PH 09:00-18:00"
        val now = at("2026-10-31T08:30:00+01:00")

        assertNull(evaluateOpeningHours(raw, now = now, timeZone = berlin, countryCode = "DE"))
        assertEquals(raw, formatAmenityOpeningHours(raw, now = now, timeZone = berlin, countryCode = "DE"))
    }

    @Test
    fun unsupportedScheduleFallsBackToExactSourceText() {
        val raw = "sunrise-sunset; by appointment"
        val now = at("2026-08-26T11:39:00+02:00")

        assertNull(evaluateOpeningHours(raw, now = now, timeZone = berlin, countryCode = "DE"))
        assertEquals(raw, formatAmenityOpeningHours(raw, now = now, timeZone = berlin, countryCode = "DE"))
    }

    @Test
    fun alwaysOpenScheduleHasNoSyntheticClosingTime() {
        val now = at("2026-08-26T11:39:00+02:00")
        val evaluation = evaluateOpeningHours("24/7", now = now, timeZone = berlin, countryCode = "DE")

        assertEquals(OpeningState.OPEN, evaluation?.state)
        assertNull(evaluation?.nextChange)
        assertTrue(formatAmenityOpeningHours("24/7", now = now, timeZone = berlin, countryCode = "DE", locale = Locale.ENGLISH)!!.startsWith("Open now"))
    }

    @Test
    fun countryCodeSelectsStationLocalTimeZone() {
        assertEquals(ZoneId.of("Europe/Berlin"), openingTimeZone("DEU"))
        assertEquals(ZoneId.of("Europe/Helsinki"), openingTimeZone("FI"))
    }

    @Test
    fun amenityFilterUsesStationCountryAndInterpretedHours() {
        val now = at("2026-08-26T17:00:00+02:00")
        assertTrue(isAmenityOpen("Mo-Fr 08:00-18:00", now = now, countryCode = "DE"))
        assertFalse(isAmenityOpen("Mo-Fr 08:00-18:00", now = at("2026-08-26T19:00:00+02:00"), countryCode = "DE"))
    }

    private fun at(value: String): ZonedDateTime = ZonedDateTime.parse(value)
}
