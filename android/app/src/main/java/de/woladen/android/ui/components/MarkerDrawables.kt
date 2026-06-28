package de.woladen.android.ui.components

import android.content.Context
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.Path
import android.graphics.drawable.BitmapDrawable
import android.graphics.drawable.Drawable
import androidx.annotation.ColorInt
import kotlin.math.PI
import kotlin.math.cos
import kotlin.math.sin

fun createCircleMarkerDrawable(
    context: Context,
    @ColorInt fillColor: Int,
    diameterDp: Float,
    strokeDp: Float = 1.5f,
    @ColorInt strokeColor: Int = Color.WHITE
): Drawable {
    val density = context.resources.displayMetrics.density
    val diameterPx = (diameterDp * density).toInt().coerceAtLeast(2)
    val strokePx = strokeDp * density
    val bitmap = Bitmap.createBitmap(diameterPx, diameterPx, Bitmap.Config.ARGB_8888)
    val canvas = Canvas(bitmap)

    val fillPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = fillColor
        style = Paint.Style.FILL
    }

    val strokePaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = strokeColor
        style = Paint.Style.STROKE
        strokeWidth = strokePx
    }

    val center = diameterPx / 2f
    val radius = center - strokePx

    canvas.drawCircle(center, center, radius, fillPaint)
    canvas.drawCircle(center, center, radius, strokePaint)

    return BitmapDrawable(context.resources, bitmap)
}

fun createFavoriteMarkerDrawable(
    context: Context,
    diameterDp: Float = 28f
): Drawable {
    val density = context.resources.displayMetrics.density
    val diameterPx = (diameterDp * density).toInt().coerceAtLeast(2)
    val bitmap = Bitmap.createBitmap(diameterPx, diameterPx, Bitmap.Config.ARGB_8888)
    val canvas = Canvas(bitmap)
    val center = diameterPx / 2f
    val outerRadius = center * 0.82f
    val innerRadius = outerRadius * 0.46f
    val path = Path()

    for (index in 0 until 10) {
        val angle = -PI / 2.0 + index * PI / 5.0
        val radius = if (index % 2 == 0) outerRadius else innerRadius
        val x = center + cos(angle).toFloat() * radius
        val y = center + sin(angle).toFloat() * radius
        if (index == 0) {
            path.moveTo(x, y)
        } else {
            path.lineTo(x, y)
        }
    }
    path.close()

    val fillPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.rgb(245, 158, 11)
        style = Paint.Style.FILL
    }
    val strokePaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        color = Color.WHITE
        style = Paint.Style.STROKE
        strokeWidth = 1.5f * density
    }

    canvas.drawPath(path, fillPaint)
    canvas.drawPath(path, strokePaint)

    return BitmapDrawable(context.resources, bitmap)
}

fun markerColorForKey(key: String): Int {
    return when (key) {
        "gold" -> Color.rgb(245, 158, 11)
        "silver" -> Color.rgb(148, 163, 184)
        "bronze" -> Color.rgb(180, 83, 9)
        else -> Color.rgb(14, 165, 233)
    }
}
