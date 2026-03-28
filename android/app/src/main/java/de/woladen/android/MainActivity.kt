package de.woladen.android

import android.Manifest
import android.content.pm.ApplicationInfo
import android.os.Build
import android.os.Bundle
import android.view.WindowManager
import androidx.activity.ComponentActivity
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.compose.setContent
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.viewModels
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.remember
import androidx.core.view.WindowCompat
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.ui.WoladenTheme
import de.woladen.android.ui.WoladenAppScreen
import de.woladen.android.viewmodel.AppViewModel

class MainActivity : ComponentActivity() {
    private val viewModel: AppViewModel by viewModels()

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        WindowCompat.setDecorFitsSystemWindows(window, true)

        val isDebuggable = (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
        if (isDebuggable) {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O_MR1) {
                setShowWhenLocked(true)
                setTurnScreenOn(true)
            } else {
                @Suppress("DEPRECATION")
                window.addFlags(
                    WindowManager.LayoutParams.FLAG_SHOW_WHEN_LOCKED or
                        WindowManager.LayoutParams.FLAG_TURN_SCREEN_ON
                )
            }
        }

        setContent {
            val locationService = remember { LocationService(applicationContext) }
            val favoritesStore = remember { FavoritesStore(applicationContext) }

            val permissionLauncher = rememberLauncherForActivityResult(
                contract = ActivityResultContracts.RequestMultiplePermissions()
            ) { result ->
                val granted = result.values.any { it }
                locationService.onPermissionResult(granted)
            }

            fun requestLocationPermission() {
                permissionLauncher.launch(
                    arrayOf(
                        Manifest.permission.ACCESS_FINE_LOCATION,
                        Manifest.permission.ACCESS_COARSE_LOCATION
                    )
                )
            }

            LaunchedEffect(Unit) {
                locationService.refreshAuthorization()
                viewModel.load(locationService.currentLocation)
            }

            LaunchedEffect(viewModel.allFeatures.size) {
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }

            LaunchedEffect(locationService.currentLocation) {
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }

            DisposableEffect(Unit) {
                onDispose {
                    locationService.stopUpdates()
                }
            }

            WoladenTheme {
                WoladenAppScreen(
                    viewModel = viewModel,
                    locationService = locationService,
                    favoritesStore = favoritesStore,
                    onRequestLocationPermission = { requestLocationPermission() }
                )
            }
        }
    }
}
