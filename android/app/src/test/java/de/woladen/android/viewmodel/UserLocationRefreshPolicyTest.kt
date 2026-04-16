package de.woladen.android.viewmodel

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class UserLocationRefreshPolicyTest {

    @Test
    fun refreshes_when_no_previous_user_center_exists() {
        assertTrue(
            shouldRefreshUserLocation(
                lastCenter = null,
                latitude = 48.8947,
                longitude = 8.7044
            )
        )
    }

    @Test
    fun skips_small_user_location_changes_without_force() {
        assertFalse(
            shouldRefreshUserLocation(
                lastCenter = 48.8947 to 8.7044,
                latitude = 48.8950,
                longitude = 8.7046
            )
        )
    }

    @Test
    fun refreshes_when_forced_even_without_movement() {
        assertTrue(
            shouldRefreshUserLocation(
                lastCenter = 48.8947 to 8.7044,
                latitude = 48.8947,
                longitude = 8.7044,
                force = true
            )
        )
    }
}
