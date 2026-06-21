package de.woladen.android.app

import android.app.Application
import org.maplibre.android.MapLibre
import de.woladen.android.util.AppStrings

class WoladenApplication : Application() {
    override fun onCreate() {
        super.onCreate()
        AppStrings.initialize(this)
        MapLibre.getInstance(this)
    }
}
