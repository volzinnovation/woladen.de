package de.woladen.android.car

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class WoladenNearbyScreenLocationTest {
    @Test
    fun rejectsStaleOrInaccurateCarLocation() {
        val now = 1_000_000L
        assertFalse(isUsableCarLocation(now - 6 * 60 * 1_000L, 50f, now))
        assertFalse(isUsableCarLocation(now - 60_000L, 300f, now))
    }

    @Test
    fun acceptsFreshAccurateCarLocation() {
        val now = 1_000_000L
        assertTrue(isUsableCarLocation(now - 60_000L, 100f, now))
    }
}
