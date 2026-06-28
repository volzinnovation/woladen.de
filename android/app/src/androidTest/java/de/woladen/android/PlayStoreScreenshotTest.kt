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
import java.io.File
import kotlin.math.min
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class PlayStoreScreenshotTest {

    @get:Rule(order = 0)
    val permissionRule: GrantPermissionRule = GrantPermissionRule.grant(
        Manifest.permission.ACCESS_FINE_LOCATION,
        Manifest.permission.ACCESS_COARSE_LOCATION
    )

    private val device: UiDevice = UiDevice.getInstance(InstrumentationRegistry.getInstrumentation())
    private lateinit var outputDir: File

    @Before
    fun launchApp() {
        outputDir = File(
            "/sdcard/Download/play-store-screenshots"
        ).resolve(
            deviceProfileLabel()
        )
        device.executeShellCommand("rm -rf ${outputDir.absolutePath}")
        device.executeShellCommand("mkdir -p ${outputDir.absolutePath}")

        dismissKeyguardAndSystemPanels()
        setStoreOrientation()

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
        acceptLocationPromptIfPresent(10_000)
        clickByResIfPresent("tab-list", 4_000)
        ensureNearbyChargersVisible()
        waitByRes("station-row", 30_000)
    }

    @After
    fun cleanup() {
        runCatching { device.unfreezeRotation() }
    }

    @Test
    fun capturePlayStoreScreens() {
        capture("01-list")

        clickFirstByRes("station-row")
        waitByRes("detail-favorite-button", 20_000)
        capture("02-detail")

        clickByRes("detail-favorite-button")
        SystemClock.sleep(500)
        clickByRes("detail-close-button")
        waitByRes("station-row", 20_000)

        clickByRes("tab-map")
        waitByRes("map-filter-button", 20_000)
        SystemClock.sleep(4_000)
        capture("03-map")

        clickByRes("tab-favorites")
        waitByRes("favorites-row", 20_000)
        capture("04-favorites")

        clickByRes("tab-route")
        waitByRes("route-root", 20_000)
        SystemClock.sleep(1_000)
        capture("05-route")

        clickByRes("tab-info")
        waitByRes("info-root", 20_000)
        SystemClock.sleep(1_000)
        capture("06-info")
    }

    private fun capture(name: String) {
        device.waitForIdle()
        SystemClock.sleep(750)
        val file = File(outputDir, "$name.png")
        val command = "screencap -p ${file.absolutePath}"
        val output = runCatching {
            device.executeShellCommand(command)
        }.getOrDefault("")
        val lsOutput = runCatching {
            device.executeShellCommand("ls -l ${file.absolutePath}")
        }.getOrDefault("")
        if (lsOutput.contains("No such file", ignoreCase = true)) {
            throw AssertionError(
                "Failed to capture screenshot: ${file.absolutePath}. Shell output: $output"
            )
        }
    }

    private fun deviceProfileLabel(): String {
        val smallestEdge = min(device.displayWidth, device.displayHeight)
        return if (smallestEdge >= 1400) "tablet-landscape" else "phone-portrait"
    }

    private fun setStoreOrientation() {
        runCatching { device.unfreezeRotation() }
        val smallestEdge = min(device.displayWidth, device.displayHeight)
        if (smallestEdge >= 1400) {
            runCatching { device.setOrientationRight() }
        } else {
            runCatching { device.setOrientationNatural() }
        }
        device.waitForIdle()
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

    private fun clickByResIfPresent(tag: String, timeoutMs: Long): Boolean {
        val target = waitObjectOrNull(selectorsForTag(tag), timeoutMs) ?: return false
        target.click()
        device.waitForIdle()
        return true
    }

    private fun ensureNearbyChargersVisible() {
        if (waitObjectOrNull(selectorsForTag("station-row"), 2_000) != null) {
            return
        }

        clickByRes("tab-map")
        waitByRes("map-location-button", 20_000)
        clickByRes("map-location-button")
        if (acceptLocationPromptIfPresent(10_000)) {
            SystemClock.sleep(500)
            clickByRes("map-location-button")
        }
        SystemClock.sleep(5_000)
        clickByRes("tab-list")
    }

    private fun waitByRes(tag: String, timeoutMs: Long): UiObject2 {
        return waitObject(selectorsForTag(tag), timeoutMs)
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

    private fun acceptLocationPromptIfPresent(timeoutMs: Long): Boolean {
        val selectors = listOf(
            By.res("com.android.permissioncontroller", "permission_allow_foreground_only_button"),
            By.res("com.google.android.permissioncontroller", "permission_allow_foreground_only_button"),
            By.textContains("While using the app"),
            By.textContains("Nur während der Nutzung"),
            By.textContains("Beim Verwenden der App")
        )
        val button = waitObjectOrNull(selectors, timeoutMs) ?: return false
        button.click()
        device.waitForIdle()
        return true
    }

    private fun selectorsForTag(tag: String): List<BySelector> {
        val selectors = mutableListOf(
            By.res(PACKAGE_NAME, tag),
            By.res(tag)
        )
        fallbackTextForTag(tag).forEach { selectors += By.text(it) }
        fallbackDescForTag(tag).forEach { description ->
            selectors += By.desc(description)
            selectors += By.descContains(description)
        }
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

    private fun fallbackTextForTag(tag: String): List<String> {
        return when (tag) {
            "tab-list" -> listOf("Liste", "List")
            "tab-map" -> listOf("Karte", "Map")
            "tab-route" -> listOf("Route")
            "tab-favorites" -> listOf("Favoriten", "Favorites")
            "tab-info" -> listOf("Info")
            "detail-google-nav-button" -> listOf("Google Navi")
            "detail-system-nav-button" -> listOf("System Navi")
            else -> emptyList()
        }
    }

    private fun fallbackDescForTag(tag: String): List<String> {
        return when (tag) {
            "map-location-button" -> listOf("Standort", "Locate")
            "map-filter-button", "list-filter-button" -> listOf("Filter")
            "detail-favorite-button" -> listOf(
                "Favorit",
                "Favorit speichern",
                "Favorit entfernen",
                "Save favorite",
                "Remove favorite"
            )
            "detail-close-button" -> listOf("Zurück", "Details schließen", "Close details")
            else -> emptyList()
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
