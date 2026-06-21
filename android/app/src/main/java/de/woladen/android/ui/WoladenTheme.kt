package de.woladen.android.ui

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.sp

private val WoladenPrimary = Color(0xFF0F766E)
private val WoladenPrimaryDark = Color(0xFF5EEAD4)
private val LightColors = lightColorScheme(
    primary = WoladenPrimary,
    onPrimary = Color.White,
    background = Color(0xFFF8FAFC),
    onBackground = Color(0xFF1E293B),
    surface = Color.White,
    onSurface = Color(0xFF1E293B),
    surfaceVariant = Color(0xFFF1F5F9),
    onSurfaceVariant = Color(0xFF64748B),
    outline = Color(0xFFE2E8F0)
)
private val DarkColors = darkColorScheme(
    primary = WoladenPrimaryDark,
    onPrimary = Color(0xFF042F2E),
    background = Color(0xFF0B1820),
    onBackground = Color(0xFFE2E8F0),
    surface = Color(0xFF09161E),
    onSurface = Color(0xFFE2E8F0),
    surfaceVariant = Color(0xFF10232D),
    onSurfaceVariant = Color(0xFF94A3B8),
    outline = Color(0xFF334155)
)
private val BaseTypography = Typography()
private val WoladenTypography = Typography(
    displayLarge = BaseTypography.displayLarge.copy(fontSize = 62.sp, lineHeight = 70.sp),
    displayMedium = BaseTypography.displayMedium.copy(fontSize = 50.sp, lineHeight = 58.sp),
    displaySmall = BaseTypography.displaySmall.copy(fontSize = 42.sp, lineHeight = 50.sp),
    headlineLarge = BaseTypography.headlineLarge.copy(fontSize = 38.sp, lineHeight = 46.sp),
    headlineMedium = BaseTypography.headlineMedium.copy(fontSize = 34.sp, lineHeight = 42.sp),
    headlineSmall = BaseTypography.headlineSmall.copy(fontSize = 30.sp, lineHeight = 38.sp),
    titleLarge = BaseTypography.titleLarge.copy(fontSize = 26.sp, lineHeight = 34.sp),
    titleMedium = BaseTypography.titleMedium.copy(fontSize = 20.sp, lineHeight = 28.sp),
    titleSmall = BaseTypography.titleSmall.copy(fontSize = 18.sp, lineHeight = 26.sp),
    bodyLarge = BaseTypography.bodyLarge.copy(fontSize = 19.sp, lineHeight = 28.sp),
    bodyMedium = BaseTypography.bodyMedium.copy(fontSize = 18.sp, lineHeight = 27.sp),
    bodySmall = BaseTypography.bodySmall.copy(fontSize = 16.sp, lineHeight = 24.sp),
    labelLarge = BaseTypography.labelLarge.copy(fontSize = 18.sp, lineHeight = 26.sp),
    labelMedium = BaseTypography.labelMedium.copy(fontSize = 17.sp, lineHeight = 24.sp),
    labelSmall = BaseTypography.labelSmall.copy(fontSize = 16.sp, lineHeight = 23.sp)
)

@Composable
fun WoladenTheme(content: @Composable () -> Unit) {
    MaterialTheme(
        colorScheme = if (androidx.compose.foundation.isSystemInDarkTheme()) DarkColors else LightColors,
        typography = WoladenTypography,
        content = content
    )
}
