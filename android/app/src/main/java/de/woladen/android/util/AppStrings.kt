package de.woladen.android.util

import android.content.Context
import androidx.annotation.StringRes
import de.woladen.android.R

object AppStrings {
    @Volatile
    private var context: Context? = null

    fun initialize(context: Context) {
        this.context = context.applicationContext
    }

    fun isInitialized(): Boolean = context != null

    fun get(@StringRes id: Int): String {
        return context?.getString(id) ?: fallback(id)
    }

    fun get(@StringRes id: Int, replacements: Map<String, String>): String {
        return replacements.entries.fold(get(id)) { text, entry ->
            text.replace("{${entry.key}}", entry.value)
        }
    }

    fun count(@StringRes id: Int, count: Int): String {
        return get(id, mapOf("count" to count.toString()))
    }

    private fun fallback(@StringRes id: Int): String {
        return when (id) {
            R.string.i18n_availability_available -> "{count} free"
            R.string.i18n_availability_free -> "Free"
            R.string.i18n_availability_occupied -> "Occupied"
            R.string.i18n_availability_occupiedcount -> "{count} occupied"
            R.string.i18n_availability_out_of_order -> "Out of order"
            R.string.i18n_availability_outofordercount -> "{count} out of order"
            R.string.i18n_availability_summaryunknown -> "Occupancy unknown"
            R.string.i18n_availability_unknown -> "Unknown"
            R.string.i18n_availability_unknowncount -> "{count} unknown"
            R.string.i18n_common_no -> "No"
            R.string.i18n_common_yes -> "Yes"
            R.string.i18n_errors_catalogmessage -> "No network connection. Sorry, live search will not work until this device is online."
            R.string.i18n_errors_catalogtitle -> "Charging points could not be loaded"
            R.string.i18n_errors_dataload -> "Error loading data."
            R.string.i18n_errors_reload -> "Reload"
            R.string.i18n_staticdetails_access -> "Access"
            R.string.i18n_staticdetails_connectors -> "Connectors"
            R.string.i18n_staticdetails_currenttype -> "Current type"
            R.string.i18n_staticdetails_energy -> "Energy"
            R.string.i18n_staticdetails_notrenewable -> "Not marked as renewable"
            R.string.i18n_staticdetails_payment -> "Payment"
            R.string.i18n_staticdetails_renewable -> "100% renewable"
            R.string.i18n_staticdetails_service -> "Service"
            R.string.i18n_staticdetails_sockets -> "{count} sockets"
            R.string.i18n_station_detailssource -> "Details via {source} · updated {date}"
            R.string.i18n_station_detailssourceonly -> "Details via {source}"
            R.string.i18n_station_evse -> "Charging point {index}"
            R.string.i18n_station_live -> "Live"
            R.string.i18n_station_livedataavailable -> "Live data available"
            R.string.i18n_station_livevia -> "Live via {source}"
            R.string.i18n_station_liveviaupdated -> "Live via {source} · updated {date}"
            R.string.i18n_station_nextslot -> "Next slot"
            R.string.i18n_station_stationstatus -> "Station status"
            R.string.i18n_station_supplementalstatus -> "Supplemental status"
            R.string.i18n_station_updated -> "Updated {date}"
            else -> ""
        }
    }
}
