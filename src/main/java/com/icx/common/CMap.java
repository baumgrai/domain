package com.icx.common.base;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

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
	 * @param <K>
	 *            key type
	 * @param <V>
	 *            value type
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
	 * @param <K>
	 *            key type
	 * @param <V>
	 *            value type
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
	 * Convert list of records (key/value maps) to csv string
	 * 
	 * @param records
	 *            list of maps (representing database table records)
	 * @param columns
	 *            list of entries where key is column name and value column header name for csv
	 * @param emptyValue
	 *            string to insert as field content if value of a column is empty
	 * 
	 * @return csv string
	 */
	public static String listOfMapsToCsv(List<Map<String, String>> records, List<Entry<String, String>> columns, String emptyValue) {

		if (records == null || records.isEmpty()) {
			return "";
		}

		// Get fields of all records (no duplicates)
		Set<String> fields = new HashSet<>();
		for (Map<String, String> record : records) {
			for (String field : record.keySet()) {
				fields.add(field != null ? field : "");
			}
		}

		// Build ordered field list and CSV-headline
		StringBuilder csv = new StringBuilder();
		List<String> orderedFields = new ArrayList<>();
		if (columns != null) {
			for (Entry<String, String> col : columns) {
				if (fields.contains(col.getKey())) {
					orderedFields.add(col.getKey());
					String value = (col.getValue() != null ? col.getValue() : "");
					csv.append(value.replace(";", ",").replace("\n", " ") + ";");
				}
			}
		}
		else {
			orderedFields.addAll(fields);
			Collections.sort(orderedFields);
			for (String field : orderedFields) {
				csv.append(field.replace(";", ",").replace("\n", " ") + ";");
			}
		}

		csv.append("\n");

		// Insert CSV-records
		for (Map<String, String> record : records) {
			for (String field : orderedFields) {
				if (record.get(field) != null) {
					csv.append(record.get(field).replace(";", ",").replace("\n", " ") + ";");
				}
				else {
					csv.append(emptyValue + ";");
				}
			}

			csv.append("\n");
		}

		return csv.toString();
	}

}
