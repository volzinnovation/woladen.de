package de.woladen.android.car

import android.content.Intent
import android.content.pm.ApplicationInfo
import androidx.car.app.CarAppService
import androidx.car.app.Session
import androidx.car.app.SessionInfo
import androidx.car.app.validation.HostValidator

/** Android Auto entry point for the charging POI experience. */
class WoladenCarAppService : CarAppService() {
    override fun createHostValidator(): HostValidator {
        val isDebuggable = applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE != 0
        return if (isDebuggable) {
            HostValidator.ALLOW_ALL_HOSTS_VALIDATOR
        } else {
            HostValidator.Builder(this)
                .addAllowedHosts(androidx.car.app.R.array.hosts_allowlist_sample)
                .build()
        }
    }

    override fun onCreateSession(sessionInfo: SessionInfo): Session = WoladenCarSession()
}

private class WoladenCarSession : Session() {
    override fun onCreateScreen(intent: Intent): androidx.car.app.Screen = WoladenNearbyScreen(carContext)
}
