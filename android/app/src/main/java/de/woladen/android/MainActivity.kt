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
import androidx.core.view.WindowCompat
import de.woladen.android.service.LocationService
import de.woladen.android.store.FavoritesStore
import de.woladen.android.ui.WoladenTheme
import de.woladen.android.ui.WoladenAppScreen
import de.woladen.android.viewmodel.AppViewModel
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleEventObserver

class MainActivity : ComponentActivity() {
    private val viewModel: AppViewModel by viewModels()
    private lateinit var locationService: LocationService
    private lateinit var favoritesStore: FavoritesStore

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        WindowCompat.setDecorFitsSystemWindows(window, true)
        locationService = LocationService(applicationContext)
        favoritesStore = FavoritesStore(applicationContext)

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
                locationService.activate()
                viewModel.load(locationService.currentLocation)
            }

            LaunchedEffect(viewModel.allFeatures.size) {
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }

            LaunchedEffect(locationService.currentLocation) {
                viewModel.seedFromInitialUserLocation(locationService.currentLocation)
            }

            DisposableEffect(locationService) {
                val observer = LifecycleEventObserver { _, event ->
                    if (event == Lifecycle.Event.ON_START) {
                        locationService.activate()
                    }
                }
                lifecycle.addObserver(observer)
                onDispose {
                    lifecycle.removeObserver(observer)
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
