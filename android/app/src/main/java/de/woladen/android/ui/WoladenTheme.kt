package de.woladen.android.ui

import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Typography
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.unit.sp

private val LightColors = lightColorScheme()
private val DarkColors = darkColorScheme()
private val BaseTypography = Typography()
private val WoladenTypography = Typography(
    displayLarge = BaseTypography.displayLarge.copy(fontSize = 60.sp, lineHeight = 68.sp),
    displayMedium = BaseTypography.displayMedium.copy(fontSize = 48.sp, lineHeight = 56.sp),
    displaySmall = BaseTypography.displaySmall.copy(fontSize = 40.sp, lineHeight = 48.sp),
    headlineLarge = BaseTypography.headlineLarge.copy(fontSize = 36.sp, lineHeight = 44.sp),
    headlineMedium = BaseTypography.headlineMedium.copy(fontSize = 32.sp, lineHeight = 40.sp),
    headlineSmall = BaseTypography.headlineSmall.copy(fontSize = 28.sp, lineHeight = 36.sp),
    titleLarge = BaseTypography.titleLarge.copy(fontSize = 24.sp, lineHeight = 32.sp),
    titleMedium = BaseTypography.titleMedium.copy(fontSize = 18.sp, lineHeight = 26.sp),
    titleSmall = BaseTypography.titleSmall.copy(fontSize = 17.sp, lineHeight = 24.sp),
    bodyLarge = BaseTypography.bodyLarge.copy(fontSize = 18.sp, lineHeight = 26.sp),
    bodyMedium = BaseTypography.bodyMedium.copy(fontSize = 17.sp, lineHeight = 25.sp),
    bodySmall = BaseTypography.bodySmall.copy(fontSize = 15.sp, lineHeight = 22.sp),
    labelLarge = BaseTypography.labelLarge.copy(fontSize = 16.sp, lineHeight = 22.sp),
    labelMedium = BaseTypography.labelMedium.copy(fontSize = 15.sp, lineHeight = 21.sp),
    labelSmall = BaseTypography.labelSmall.copy(fontSize = 14.sp, lineHeight = 20.sp)
)

@Composable
fun WoladenTheme(content: @Composable () -> Unit) {
    MaterialTheme(
        colorScheme = if (androidx.compose.foundation.isSystemInDarkTheme()) DarkColors else LightColors,
        typography = WoladenTypography,
        content = content
    )
}
