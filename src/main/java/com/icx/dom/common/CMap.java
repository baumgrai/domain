package com.icx.dom.common;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Map helpers
 * 
 * @author baumgrai
 */
public abstract class CMap {

	// -------------------------------------------------------------------------
	// Primitives
	// -------------------------------------------------------------------------

	/**
	 * Check if map is null or empty
	 * 
	 * @param map
	 *            map
	 * 
	 * @return true if map is null or empty, false otherwise
	 */
	public static boolean isEmpty(Map<?, ?> map) {
		return (map == null || map.isEmpty());
	}

	// -------------------------------------------------------------------------
	// Create maps
	// -------------------------------------------------------------------------

	/**
	 * Create and initialize generic map (HashMap)
	 * 
	 * @param firstKey
	 *            first key
	 * @param firstValue
	 *            first value
	 * @param furtherKeysAndValues
	 *            key and value objects in alternating order (key1, value1, key2, value2, ...)
	 * 
	 * @return map with given key/value pairs
	 */
	@SuppressWarnings("unchecked")
	public static <K, V> Map<K, V> newMap(K firstKey, V firstValue, Object... furtherKeysAndValues) {

		Map<K, V> map = new HashMap<>();
		map.put(firstKey, firstValue);
		for (int i = 0; i < furtherKeysAndValues.length; i += 2) {
			map.put((K) furtherKeysAndValues[i], (V) furtherKeysAndValues[i + 1]);
		}

		return map;
	}

	/**
	 * Create and initialize generic sorted map (TreeMap)
	 * 
	 * @param firstKey
	 *            first key
	 * @param firstValue
	 *            first value
	 * @param furtherKeysAndValues
	 *            key and value objects in alternating order (key1, value1, key2, value2, ...)
	 * 
	 * @return map with given key/value pairs
	 */
	@SuppressWarnings("unchecked")
	public static <K, V> SortedMap<K, V> newSortedMap(K firstKey, V firstValue, Object... furtherKeysAndValues) {

		SortedMap<K, V> map = new TreeMap<>();
		map.put(firstKey, firstValue);
		for (int i = 0; i < furtherKeysAndValues.length; i += 2) {
			map.put((K) furtherKeysAndValues[i], (V) furtherKeysAndValues[i + 1]);
		}

		return map;
	}

	// -------------------------------------------------------------------------
	// Conversion
	// -------------------------------------------------------------------------

	/**
	 * Make keys in {@code Map<String, Object>} map uppercase
	 * 
	 * @param map
	 *            map
	 */
	public static <V> void upperCaseKeysInMap(Map<String, V> map) {

		if (map == null) {
			return;
		}

		Map<String, V> tmp = new TreeMap<>(map);

		map.clear();
		tmp.forEach((k, v) -> map.put(k.toUpperCase(), v));
	}

}
