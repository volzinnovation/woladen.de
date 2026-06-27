package de.woladen.android.store

import android.content.Context
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import org.json.JSONObject
import java.time.Instant
import java.util.Locale

const val FAVORITE_CATEGORY_UNCATEGORIZED = "__uncategorized__"
const val FAVORITE_FILTER_ALL = "__all__"

private const val FAVORITE_METADATA_VERSION = 2
private const val MAX_FAVORITE_CATEGORY_LENGTH = 48
private const val MAX_FAVORITE_CATEGORIES = 12

data class FavoriteItem(
    val stationId: String,
    val categories: List<String>,
    val createdAt: String,
    val updatedAt: String,
    val source: String
)

class FavoritesStore(context: Context) {
    private val preferences = context.getSharedPreferences("woladen", Context.MODE_PRIVATE)
    private val defaultsKey = "woladen_favorites_v2"
    private val legacyDefaultsKey = "woladen_favorites"

    var items: Map<String, FavoriteItem> by mutableStateOf(loadItems())
        private set

    var favorites: Set<String> by mutableStateOf(items.keys)
        private set

    fun toggle(stationId: String) {
        val id = normalizeStationId(stationId)
        if (id.isBlank()) return
        val next = items.toMutableMap()
        if (next.containsKey(id)) {
            next.remove(id)
        } else {
            next[id] = createFavoriteItem(id, source = "manual")
        }
        publish(next)
        saveItems()
    }

    fun remove(stationId: String) {
        val id = normalizeStationId(stationId)
        if (!favorites.contains(id)) return
        val next = items.toMutableMap()
        next.remove(id)
        publish(next)
        saveItems()
    }

    fun isFavorite(stationId: String): Boolean {
        return favorites.contains(normalizeStationId(stationId))
    }

    fun categoriesFor(stationId: String): List<String> {
        return items[normalizeStationId(stationId)]?.categories.orEmpty()
    }

    fun sortedCategories(): List<String> {
        val categories = mutableListOf<String>()
        val seen = linkedSetOf<String>()
        for (item in items.values) {
            for (category in normalizeCategories(item.categories, knownCategories = categories)) {
                val key = categoryKey(category)
                if (seen.add(key)) {
                    categories += category
                }
            }
        }
        return categories.sortedWith(String.CASE_INSENSITIVE_ORDER)
    }

    fun addCategory(stationId: String, category: String, source: String = "manual") {
        val id = normalizeStationId(stationId)
        val label = normalizeCategoryLabel(category)
        if (id.isBlank() || label.isBlank()) return
        val next = items.toMutableMap()
        val existing = next[id] ?: createFavoriteItem(id, source)
        val knownCategories = sortedCategories()
        next[id] = existing.copy(
            categories = normalizeCategories(existing.categories + label, knownCategories = knownCategories),
            updatedAt = nowIso(),
            source = normalizeSource(existing.source, fallback = source)
        )
        publish(next)
        saveItems()
    }

    fun removeCategory(stationId: String, category: String) {
        val id = normalizeStationId(stationId)
        val key = categoryKey(category)
        val existing = items[id] ?: return
        val next = items.toMutableMap()
        next[id] = existing.copy(
            categories = existing.categories.filter { categoryKey(it) != key },
            updatedAt = nowIso()
        )
        publish(next)
        saveItems()
    }

    fun addRouteFavorites(stationIds: List<String>, category: String) {
        val label = normalizeCategoryLabel(category)
        if (label.isBlank()) return
        val next = items.toMutableMap()
        val knownCategories = sortedCategories()
        for (rawStationId in stationIds) {
            val stationId = normalizeStationId(rawStationId)
            if (stationId.isBlank()) continue
            val existing = next[stationId] ?: createFavoriteItem(stationId, source = "route")
            next[stationId] = existing.copy(
                categories = normalizeCategories(existing.categories + label, knownCategories = knownCategories),
                updatedAt = nowIso(),
                source = normalizeSource(existing.source, fallback = "route")
            )
        }
        publish(next)
        saveItems()
    }

    fun categorySuggestions(query: String, excluding: List<String>): List<String> {
        val excludedKeys = excluding.mapTo(linkedSetOf(), ::categoryKey)
        val normalizedQuery = categoryKey(query)
        val categories = sortedCategories().filter { categoryKey(it) !in excludedKeys }
        if (normalizedQuery.isBlank()) {
            return categories.take(6)
        }
        val prefix = categories.filter { categoryKey(it).startsWith(normalizedQuery) }
        val substring = categories.filter {
            val key = categoryKey(it)
            !key.startsWith(normalizedQuery) && key.contains(normalizedQuery)
        }
        return (prefix + substring).take(6)
    }

    fun favoriteIdsMatching(filter: String): Set<String> {
        return when (filter) {
            FAVORITE_FILTER_ALL -> favorites
            FAVORITE_CATEGORY_UNCATEGORIZED -> items.values
                .filter { it.categories.isEmpty() }
                .mapTo(linkedSetOf()) { it.stationId }
            else -> items.values
                .filter { item -> item.categories.any { categoryKey(it) == filter } }
                .mapTo(linkedSetOf()) { it.stationId }
        }
    }

    fun countMatching(filter: String): Int {
        return favoriteIdsMatching(filter).size
    }

    private fun loadItems(): Map<String, FavoriteItem> {
        preferences.getString(defaultsKey, null)?.let { raw ->
            parseItems(raw)?.let { return it }
        }

        val timestamp = nowIso()
        return preferences.getStringSet(legacyDefaultsKey, emptySet())
            .orEmpty()
            .map(::normalizeStationId)
            .filter { it.isNotBlank() }
            .distinct()
            .associateWith { stationId ->
                FavoriteItem(
                    stationId = stationId,
                    categories = emptyList(),
                    createdAt = timestamp,
                    updatedAt = timestamp,
                    source = "migration"
                )
            }
    }

    private fun saveItems() {
        val payload = JSONObject()
        payload.put("version", FAVORITE_METADATA_VERSION)
        val jsonItems = JSONObject()
        val knownCategories = sortedCategories()
        for ((stationId, item) in items.toSortedMap()) {
            jsonItems.put(stationId, JSONObject().apply {
                put("station_id", stationId)
                put("categories", org.json.JSONArray(normalizeCategories(item.categories, knownCategories)))
                put("created_at", item.createdAt)
                put("updated_at", item.updatedAt)
                put("source", normalizeSource(item.source))
            })
        }
        payload.put("items", jsonItems)
        preferences.edit().putString(defaultsKey, payload.toString()).apply()
    }

    private fun publish(next: Map<String, FavoriteItem>) {
        items = next.toMap()
        favorites = items.keys
    }

    private fun createFavoriteItem(stationId: String, source: String): FavoriteItem {
        val timestamp = nowIso()
        return FavoriteItem(
            stationId = stationId,
            categories = emptyList(),
            createdAt = timestamp,
            updatedAt = timestamp,
            source = normalizeSource(source)
        )
    }

    private fun parseItems(raw: String): Map<String, FavoriteItem>? {
        return runCatching {
            val payload = JSONObject(raw)
            if (payload.optInt("version") != FAVORITE_METADATA_VERSION) {
                return@runCatching null
            }
            val sourceItems = payload.optJSONObject("items") ?: return@runCatching emptyMap()
            val result = linkedMapOf<String, FavoriteItem>()
            val knownCategories = mutableListOf<String>()
            val keys = sourceItems.keys()
            while (keys.hasNext()) {
                val key = keys.next()
                val item = sourceItems.optJSONObject(key) ?: continue
                val stationId = normalizeStationId(item.optString("station_id", key))
                if (stationId.isBlank()) continue
                val categories = normalizeCategories(
                    (0 until item.optJSONArray("categories")?.length().orZero())
                        .mapNotNull { index -> item.optJSONArray("categories")?.optString(index) },
                    knownCategories = knownCategories
                )
                knownCategories += categories
                val createdAt = cleanTimestamp(item.optString("created_at"), fallback = nowIso())
                result[stationId] = FavoriteItem(
                    stationId = stationId,
                    categories = categories,
                    createdAt = createdAt,
                    updatedAt = cleanTimestamp(item.optString("updated_at"), fallback = createdAt),
                    source = normalizeSource(item.optString("source"))
                )
            }
            result
        }.getOrNull()
    }
}

fun normalizeCategoryLabel(value: String): String {
    return value
        .replace("\\s+".toRegex(), " ")
        .trim()
        .take(MAX_FAVORITE_CATEGORY_LENGTH)
}

fun categoryKey(value: String): String {
    return normalizeCategoryLabel(value).lowercase(Locale.ROOT)
}

private fun normalizeStationId(value: String): String = value.trim()

private fun normalizeCategories(value: List<String>, knownCategories: List<String> = emptyList()): List<String> {
    val displayByKey = linkedMapOf<String, String>()
    for (category in knownCategories) {
        val label = normalizeCategoryLabel(category)
        val key = categoryKey(label)
        if (label.isNotBlank() && key !in displayByKey) {
            displayByKey[key] = label
        }
    }

    val categories = mutableListOf<String>()
    val seen = linkedSetOf<String>()
    for (item in value) {
        val label = normalizeCategoryLabel(item)
        val key = categoryKey(label)
        if (label.isBlank() || !seen.add(key)) continue
        categories += displayByKey[key] ?: label
    }
    return categories.take(MAX_FAVORITE_CATEGORIES)
}

private fun normalizeSource(value: String, fallback: String = "manual"): String {
    val source = value.trim()
    return if (source in setOf("manual", "route", "migration")) source else fallback
}

private fun cleanTimestamp(value: String, fallback: String): String {
    val text = value.trim()
    if (text.isBlank()) return fallback
    return runCatching { Instant.parse(text) }.fold(
        onSuccess = { text },
        onFailure = { fallback }
    )
}

private fun nowIso(): String = Instant.now().toString()

private fun Int?.orZero(): Int = this ?: 0
