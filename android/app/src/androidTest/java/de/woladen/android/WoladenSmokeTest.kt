package de.woladen.android

import android.Manifest
import android.os.SystemClock
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import androidx.test.rule.GrantPermissionRule
import androidx.test.uiautomator.By
import androidx.test.uiautomator.BySelector
import androidx.test.uiautomator.UiDevice
import androidx.test.uiautomator.UiObject2
import androidx.test.uiautomator.Until
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class WoladenSmokeTest {

    @get:Rule(order = 0)
    val permissionRule: GrantPermissionRule = GrantPermissionRule.grant(
        Manifest.permission.ACCESS_FINE_LOCATION,
        Manifest.permission.ACCESS_COARSE_LOCATION
    )

    private val device: UiDevice = UiDevice.getInstance(InstrumentationRegistry.getInstrumentation())

    @Before
    fun launchApp() {
        dismissKeyguardAndSystemPanels()
        device.pressHome()
        val launchOutput = runCatching {
            device.executeShellCommand("am start -W -n $PACKAGE_NAME/.MainActivity")
        }.getOrDefault("")
        if (launchOutput.contains("Error:", ignoreCase = true)) {
            throw AssertionError("Failed to launch app: $launchOutput")
        }
        val packageVisible = device.wait(Until.hasObject(By.pkg(PACKAGE_NAME)), 30_000)
        if (!packageVisible) {
            throw AssertionError("App did not reach foreground package=$PACKAGE_NAME")
        }
        clickByResIfPresent("tab-list", 4_000)
        device.waitForIdle()
    }

    @Test
    fun smoke_all_primary_features_including_station_detail_sheet() {
        waitByRes("station-row", 90_000)

        clickByRes("list-filter-button")
        waitByTextContains("Betreiber", 20_000)
        dismissSheetWithBack()

        clickByRes("tab-map")
        clickByRes("map-location-button")
        clickByRes("map-filter-button")
        waitByTextContains("Betreiber", 20_000)
        dismissSheetWithBack()

        clickByRes("tab-list")
        clickFirstByRes("station-row")

        waitByRes("detail-favorite-button", 20_000)
        clickByRes("detail-favorite-button")
        clickByRes("detail-close-button")

        clickByRes("tab-favorites")
        waitByRes("favorites-row", 20_000)

        clickByRes("tab-info")
        waitByRes("info-root", 20_000)
        clickByResIfPresent("info-location-refresh-button", 3_000)

        clickByRes("tab-list")
        waitByRes("station-row", 20_000)
    }

    @Test
    fun regression_repeated_map_taps_remain_responsive_and_detail_still_opens() {
        waitByRes("station-row", 90_000)
        clickByRes("tab-map")
        waitByRes("map-filter-button", 20_000)

        repeat(8) {
            tapCenter()
            tapCenter()
            SystemClock.sleep(120)
        }

        clickByRes("map-filter-button")
        waitByTextContains("Betreiber", 20_000)
        dismissSheetWithBack()

        clickByRes("tab-list")
        waitByRes("station-row", 20_000)
        clickFirstByRes("station-row")
        waitByRes("detail-close-button", 20_000)
        clickByRes("detail-close-button")
    }

    @Test
    fun stationDetailSheet_actions_and_content_are_accessible() {
        waitByRes("station-row", 90_000)
        clickFirstByRes("station-row")

        waitByRes("detail-favorite-button", 20_000)
        waitByRes("detail-close-button", 10_000)
        clickByRes("detail-favorite-button")
        clickByRes("detail-close-button")
    }

    private fun clickByRes(tag: String) {
        waitByRes(tag, 20_000).click()
        device.waitForIdle()
    }

    private fun clickFirstByRes(tag: String) {
        val objects = findObjectsByTag(tag)
        if (objects.isEmpty()) {
            throw AssertionError("No UI object found for tag=$tag")
        }
        objects.first().click()
        device.waitForIdle()
    }

    private fun tapCenter() {
        val x = device.displayWidth / 2
        val y = device.displayHeight / 2
        device.click(x, y)
        device.waitForIdle()
    }

    private fun dismissSheetWithBack() {
        device.pressBack()
        device.waitForIdle()
    }

    private fun clickByResIfPresent(tag: String, timeoutMs: Long): Boolean {
        val target = waitObjectOrNull(selectorsForTag(tag), timeoutMs) ?: return false
        target.click()
        device.waitForIdle()
        return true
    }

    private fun waitByRes(tag: String, timeoutMs: Long): UiObject2 {
        return waitObject(selectorsForTag(tag), timeoutMs)
    }

    private fun waitByTextContains(text: String, timeoutMs: Long): UiObject2 {
        return waitObject(listOf(By.textContains(text)), timeoutMs)
    }

    private fun waitObject(selectors: List<BySelector>, timeoutMs: Long): UiObject2 {
        return waitObjectOrNull(selectors, timeoutMs)
            ?: throw AssertionError("UI element not found for selectors: $selectors")
    }

    private fun waitObjectOrNull(selectors: List<BySelector>, timeoutMs: Long): UiObject2? {
        val deadline = SystemClock.uptimeMillis() + timeoutMs
        while (SystemClock.uptimeMillis() < deadline) {
            for (selector in selectors) {
                val match = device.findObject(selector)
                if (match != null) {
                    return match
                }
            }
            SystemClock.sleep(250)
        }
        return null
    }

    private fun selectorsForTag(tag: String): List<BySelector> {
        val selectors = mutableListOf(
            By.res(PACKAGE_NAME, tag),
            By.res(tag)
        )
        fallbackTextForTag(tag)?.let { selectors += By.text(it) }
        fallbackDescForTag(tag)?.let { selectors += By.desc(it) }
        return selectors
    }

    private fun findObjectsByTag(tag: String): List<UiObject2> {
        val found = linkedMapOf<Int, UiObject2>()
        for (selector in selectorsForTag(tag)) {
            for (obj in device.findObjects(selector)) {
                found[System.identityHashCode(obj)] = obj
            }
        }
        return found.values.toList()
    }

    private fun fallbackTextForTag(tag: String): String? {
        return when (tag) {
            "tab-list" -> "Liste"
            "tab-map" -> "Karte"
            "tab-favorites" -> "Favoriten"
            "tab-info" -> "Info"
            "filter-apply-button" -> "Anwenden"
            "detail-google-nav-button" -> "Google Navi"
            "detail-system-nav-button" -> "System Navi"
            "info-location-refresh-button" -> "Standort aktualisieren"
            else -> null
        }
    }

    private fun fallbackDescForTag(tag: String): String? {
        return when (tag) {
            "map-location-button" -> "Standort"
            "map-filter-button", "list-filter-button" -> "Filter"
            "detail-favorite-button" -> "Favorit"
            "detail-close-button" -> "Zurück"
            else -> null
        }
    }

    private fun dismissKeyguardAndSystemPanels() {
        runCatching { device.wakeUp() }
        runCatching { device.executeShellCommand("input keyevent KEYCODE_WAKEUP") }
        runCatching { device.executeShellCommand("wm dismiss-keyguard") }
        runCatching { device.executeShellCommand("input swipe 500 2200 500 350 250") }
        runCatching { device.executeShellCommand("input keyevent 82") }
        runCatching { device.executeShellCommand("input keyevent KEYCODE_MENU") }
        runCatching { device.executeShellCommand("cmd statusbar collapse") }
        runCatching { device.executeShellCommand("cmd statusbar collapse") }
    }

    companion object {
        private const val PACKAGE_NAME = "de.woladen.android"
    }
}
