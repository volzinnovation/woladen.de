package de.woladen.android.service

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.pm.PackageManager
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.os.Bundle
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.core.content.ContextCompat

enum class LocationAuthorizationStatus {
    NOT_DETERMINED,
    DENIED,
    AUTHORIZED_WHEN_IN_USE
}

class LocationService(private val context: Context) {
    private val locationManager = context.getSystemService(Context.LOCATION_SERVICE) as LocationManager
    private val prefs = context.getSharedPreferences("woladen", Context.MODE_PRIVATE)
    private val askedKey = "location_permission_requested"

    var authorizationStatus: LocationAuthorizationStatus by mutableStateOf(LocationAuthorizationStatus.NOT_DETERMINED)
        private set

    var currentLocation: Location? by mutableStateOf(null)
        private set

    var lastError: String? by mutableStateOf(null)
        private set

    private val updateListener = object : LocationListener {
        override fun onLocationChanged(location: Location) {
            currentLocation = location
            lastError = null
        }

        override fun onProviderEnabled(provider: String) = Unit

        override fun onProviderDisabled(provider: String) = Unit

        @Deprecated("Deprecated in Java")
        override fun onStatusChanged(provider: String?, status: Int, extras: Bundle?) = Unit
    }

    fun activate() {
        refreshAuthorization()
        if (authorizationStatus == LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE) {
            requestSingleLocation()
            startUpdates()
        } else {
            stopUpdates()
        }
    }

    fun refreshAuthorization() {
        authorizationStatus = if (hasLocationPermission()) {
            LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE
        } else {
            if (prefs.getBoolean(askedKey, false)) {
                LocationAuthorizationStatus.DENIED
            } else {
                LocationAuthorizationStatus.NOT_DETERMINED
            }
        }
    }

    fun onPermissionResult(granted: Boolean) {
        prefs.edit().putBoolean(askedKey, true).apply()
        authorizationStatus = if (granted) {
            LocationAuthorizationStatus.AUTHORIZED_WHEN_IN_USE
        } else {
            LocationAuthorizationStatus.DENIED
        }
        if (granted) {
            requestSingleLocation()
            startUpdates()
        } else {
            stopUpdates()
        }
    }

    @SuppressLint("MissingPermission")
    fun requestSingleLocation() {
        if (!hasLocationPermission()) {
            refreshAuthorization()
            return
        }
        runCatching { locationManager.removeUpdates(updateListener) }

        val providers = locationManager.getProviders(true)
        val lastKnown = providers
            .mapNotNull { provider -> runCatching { locationManager.getLastKnownLocation(provider) }.getOrNull() }
            .maxByOrNull { it.time }

        if (lastKnown != null) {
            currentLocation = lastKnown
            lastError = null
        }

        for (provider in providers) {
            runCatching {
                locationManager.requestLocationUpdates(provider, 0L, 0f, updateListener)
            }.onFailure {
                lastError = it.localizedMessage
            }
        }
    }

    @SuppressLint("MissingPermission")
    fun startUpdates() {
        if (!hasLocationPermission()) {
            refreshAuthorization()
            return
        }
        runCatching { locationManager.removeUpdates(updateListener) }
        val providers = locationManager.getProviders(true)
        for (provider in providers) {
            runCatching {
                locationManager.requestLocationUpdates(provider, 5_000L, 20f, updateListener)
            }.onFailure {
                lastError = it.localizedMessage
            }
        }
    }

    fun stopUpdates() {
        runCatching {
            locationManager.removeUpdates(updateListener)
        }
    }

    private fun hasLocationPermission(): Boolean {
        val fine = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED
        val coarse = ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_COARSE_LOCATION) == PackageManager.PERMISSION_GRANTED
        return fine || coarse
    }
}
