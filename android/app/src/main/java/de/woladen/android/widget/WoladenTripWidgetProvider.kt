package de.woladen.android.widget

import android.app.PendingIntent
import android.appwidget.AppWidgetManager
import android.appwidget.AppWidgetProvider
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.widget.RemoteViews
import de.woladen.android.MainActivity
import de.woladen.android.R
import de.woladen.android.app.WoladenApplication
import de.woladen.android.model.TripEtaEstimator
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Locale

/** Compact home-screen entry point for the active trip or route planner. */
class WoladenTripWidgetProvider : AppWidgetProvider() {
    override fun onUpdate(context: Context, manager: AppWidgetManager, appWidgetIds: IntArray) {
        appWidgetIds.forEach { update(context, manager, it) }
    }

    companion object {
        fun refresh(context: Context) {
            val manager = AppWidgetManager.getInstance(context)
            val component = ComponentName(context, WoladenTripWidgetProvider::class.java)
            manager.getAppWidgetIds(component).forEach { update(context, manager, it) }
        }

        private fun update(context: Context, manager: AppWidgetManager, id: Int) {
            val tripStore = (context.applicationContext as WoladenApplication).tripStore
            val plan = tripStore.activePlan
            val views = RemoteViews(context.packageName, R.layout.widget_trip).apply {
                setTextViewText(R.id.widget_title, plan?.name ?: context.getString(R.string.i18n_widget_plan_trip))
                setTextViewText(
                    R.id.widget_detail,
                    plan?.let { active ->
                        val eta = TripEtaEstimator.estimate(active, 0)
                        val next = active.nextStop?.let { stop ->
                            context.getString(R.string.i18n_widget_next_stop, stop.stationName.ifBlank { stop.operatorName })
                        } ?: context.getString(R.string.i18n_widget_destination, active.route.destination.label)
                        "$next · ETA ${formatEta(eta.destinationArrivalEpochMs)} · SOC ${eta.projectedArrivalSocPercent.toInt()}%"
                    } ?: context.getString(R.string.i18n_widget_open_planner)
                )
                setOnClickPendingIntent(
                    R.id.widget_root,
                    PendingIntent.getActivity(
                        context,
                        id,
                        Intent(Intent.ACTION_VIEW, Uri.parse("woladen://trip"), context, MainActivity::class.java),
                        PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
                    )
                )
            }
            manager.updateAppWidget(id, views)
        }

        private fun formatEta(epochMillis: Long): String =
            SimpleDateFormat("HH:mm", Locale.getDefault()).format(Date(epochMillis))
    }
}
