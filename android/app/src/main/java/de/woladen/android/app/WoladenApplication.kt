package de.woladen.android.app

import android.app.Application
import org.maplibre.android.MapLibre
import de.woladen.android.repository.ChargerRepository
import de.woladen.android.service.LiveApiClient
import de.woladen.android.store.FavoritesStore
import de.woladen.android.store.FilterStateStore
import de.woladen.android.util.AppStrings

class WoladenApplication : Application() {
    val liveApiClient: LiveApiClient by lazy { LiveApiClient() }
    val chargerRepository: ChargerRepository by lazy { ChargerRepository(liveApiClient) }
    val favoritesStore: FavoritesStore by lazy { FavoritesStore(this) }
    val filterStateStore: FilterStateStore by lazy { FilterStateStore(this) }

    override fun onCreate() {
        super.onCreate()
        AppStrings.initialize(this)
        MapLibre.getInstance(this)
    }
}
