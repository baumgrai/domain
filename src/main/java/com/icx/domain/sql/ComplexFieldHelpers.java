package com.icx.domain.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CCollection;
import com.icx.common.CList;
import com.icx.common.CMap;
import com.icx.common.CReflection;
import com.icx.common.Common;

/**
 * Helpers for conversion of collections and maps to/from associated rows of entry tables
 * <p>
 * Fields of domain classes which contain collections or maps are represented in database as separate 'entry' tables unlike fields containing simple values which will be represented as table columns.
 * Methods of this class perform conversion between these representations.
 * <p>
 * Collections supported as fields of domain objects may contain alternatively strings, numbers (Integer, Long, Float, Double), enum values and other simple objects, which can be constructed using a
 * constructor with one string argument. The same is valid for keys and values of maps supported as fields of domain objects. Collection elements and map values may also be themselves collections or
 * maps of simple elements. But map keys may only be simple elements.
 * 
 * @author baumgrai
 */
public abstract class ComplexFieldHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(ComplexFieldHelpers.class);

	private static final String NULL = "(null)";

	// -------------------------------------------------------------------------
	// Convert elements of collections or keys and values of maps to values stored in column
	// -------------------------------------------------------------------------

	// Collection -> comma separated string, caring for null elements
	private static <T> String collection2String(Collection<T> collection) {

		if (CCollection.isEmpty(collection)) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		for (T element : collection) {
			sb.append((element != null ? element.toString() : NULL) + ",");
		}

		return sb.substring(0, sb.length() - 1);
	}

	// Map -> semicolon/equal-sign separated string, caring for null keys and values
	private static <K, V> String map2String(Map<K, V> map) {

		if (CMap.isEmpty(map)) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		for (Entry<K, V> e : map.entrySet()) {

			K k = e.getKey();
			V v = e.getValue();

			sb.append((k != null ? k.toString() : NULL) + "=");
			sb.append((v != null ? v.toString() : NULL) + ";");
		}

		return sb.substring(0, sb.length() - 1);
	}

	// Collection element or value of map entry -> value to store in database table column
	// Method will only be used for elements of collections or keys or values of maps - so here lists of lists, lists of maps, maps with lists or maps as keys or values will be handled
	static Object element2ColumnValue(Object element) {

		if (element instanceof Collection) { // Elements of collection or values of map are collections
			return collection2String((Collection<?>) element);
		}
		else if (element instanceof Map) { // Elements of collection or values of map are maps
			return map2String((Map<?, ?>) element);
		}
		else { // Elements of collection or values of map are simple values
			return element;
		}
	}

	// -------------------------------------------------------------------------
	// Convert values stored in column to elements of collections or keys and values of maps
	// -------------------------------------------------------------------------

	// String -> object, used in string2Collection() and string2Map() to allow converting collections and maps of objects which are not strings; works for enum objects, strings and objects which can
	// be constructed using a constructor with one string argument
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T> T string2SimpleObject(Class<? extends T> objectClass, String s) {

		objectClass = (Class<? extends T>) CReflection.getBoxingWrapperType(objectClass);

		if (s == null) {
			return null;
		}
		else if (objectClass == String.class) {
			return (T) s;
		}
		else {
			try {
				if (Enum.class.isAssignableFrom(objectClass)) {
					return (T) Enum.valueOf((Class<? extends Enum>) objectClass, s);
				}
				else if (objectClass == Integer.class) {
					return (T) Integer.valueOf(s);
				}
				else if (objectClass == Long.class) {
					return (T) Long.valueOf(s);
				}
				else if (objectClass == Float.class) {
					return (T) Float.valueOf(s);
				}
				else if (objectClass == Double.class) {
					return (T) Double.valueOf(s);
				}
				else {
					Constructor<?> constructor = objectClass.getConstructor(String.class);
					return (T) constructor.newInstance(s);
				}
			}
			catch (IllegalArgumentException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
				log.error("SQL: String '{}' cannot be converted to type '{}'! ({})", s, objectClass.getName(), objectClass, ex);
				return null;
			}
		}
	}

	// Comma separated string -> collection - elements of collections will be converted from string to object of element type
	private static <T> Collection<T> string2Collection(Class<? extends Collection<T>> collectionClass, Class<? extends T> elementClass, String collectionAsString) {

		try {
			Collection<T> collection = (collectionClass.isInterface() ? CReflection.newCollection(collectionClass) : collectionClass.getDeclaredConstructor().newInstance());
			if (collectionAsString != null) {
				for (String element : collectionAsString.split("\\,")) {
					collection.add(objectsEqual(element, NULL) ? null : string2SimpleObject(elementClass, element));
				}
			}

			return collection;
		}
		catch (Exception ex) {
			log.error("SQL: {} occurred trying to create collection of type '{}': {}", ex.getClass().getSimpleName(), collectionClass.getName(), ex);
			return Collections.emptyList();
		}
	}

	// Comma/equal-sign separated string -> map - keys and values of map will be converted from string to object of key and value type
	private static <K, V> Map<K, V> string2Map(Class<? extends Map<K, V>> mapClass, Class<? extends K> keyClass, Class<? extends V> valueClass, String mapAsString) {

		try {
			Map<K, V> map = (mapClass.isInterface() ? CReflection.newMap(mapClass) : mapClass.getDeclaredConstructor().newInstance());

			if (mapAsString != null) {
				for (String entry : mapAsString.split("\\;")) {

					String[] keyValue = entry.split("\\=", 2);
					String key = (objectsEqual(keyValue[0], NULL) ? null : keyValue[0]);
					String value = (keyValue.length > 1 ? objectsEqual(keyValue[1], NULL) ? null : keyValue[1] : "");

					map.put(string2SimpleObject(keyClass, key), string2SimpleObject(valueClass, value));
				}
			}

			return map;
		}
		catch (Exception ex) {
			log.error("SQL: {} occurred trying to create map of type '{}': {}", ex.getClass().getSimpleName(), mapClass.getName(), ex);
			return Collections.emptyMap();
		}
	}

	// Value stored in database table column -> collection element or value of map entry
	// Method will only be used for elements of collections or keys or values of maps - so here lists of lists, lists of maps, maps with lists or maps as keys or values will be handled
	@SuppressWarnings("unchecked")
	static Object columnValue2Element(Type elementType, Object columnValue) {

		if (elementType instanceof ParameterizedType) {

			// Element of collection or value of map is collection or map too -> this collection or map is stored as string in database and may only contain simple objects
			ParameterizedType parameterizedType = (ParameterizedType) elementType;
			Class<?> rawType = (Class<?>) parameterizedType.getRawType();

			if (Collection.class.isAssignableFrom(rawType)) { // Collection

				Class<? extends Collection<Object>> collectionClass = (Class<? extends Collection<Object>>) rawType;
				Class<? extends Object> elementClass = (Class<?>) parameterizedType.getActualTypeArguments()[0];

				return string2Collection(collectionClass, elementClass, (String) columnValue);
			}
			else { // Map
				Class<? extends Map<Object, Object>> mapClass = (Class<? extends Map<Object, Object>>) rawType;
				Class<? extends Object> keyClass = (Class<? extends Object>) parameterizedType.getActualTypeArguments()[0];
				Class<? extends Object> valueClass = (Class<? extends Object>) parameterizedType.getActualTypeArguments()[1];

				return string2Map(mapClass, keyClass, valueClass, (String) columnValue);
			}
		}
		else {
			// Element of collection or value of map is simple object
			return columnValue;
		}
	}

	// -------------------------------------------------------------------------
	// Conversion from/to collection or map to/from list of entry records as stored in database
	// -------------------------------------------------------------------------

	// Convert collection to entry records (to INSERT INTO entry table)
	static List<SortedMap<String, Object>> collection2EntryRecords(String mainTableRefIdColumnName, long objId, Collection<?> collection) {

		List<SortedMap<String, Object>> entryRecords = new ArrayList<>();

		if (CCollection.isEmpty(collection)) {
			return entryRecords;
		}

		int order = 0;
		Iterator<?> it = collection.iterator();
		while (it.hasNext()) {

			SortedMap<String, Object> entryRecord = new TreeMap<>();
			entryRecords.add(entryRecord);

			entryRecord.put(mainTableRefIdColumnName, objId);
			entryRecord.put(Const.ELEMENT_COL, element2ColumnValue(it.next()));
			if (collection instanceof List) { // Care for element order on lists
				entryRecord.put(Const.ORDER_COL, order++);
			}
		}

		return entryRecords;
	}

	// Convert records of entry table to collection
	@SuppressWarnings("unchecked")
	static Collection<Object> entryRecords2Collection(ParameterizedType genericFieldType, List<SortedMap<String, Object>> entryRecords) {

		Collection<Object> collection = CReflection.newCollection((Class<? extends Collection<Object>>) genericFieldType.getRawType());

		if (CList.isEmpty(entryRecords)) {
			return collection;
		}

		Type elementType = genericFieldType.getActualTypeArguments()[0];
		for (SortedMap<String, Object> entryRecord : entryRecords) {
			collection.add(columnValue2Element(elementType, entryRecord.get(Const.ELEMENT_COL))); // Element of collection can be a collection or map itself
		}

		return collection;
	}

	// Convert key value map to entry records (to INSERT INTO entry table)
	static List<SortedMap<String, Object>> map2EntryRecords(String mainTableRefIdColumnName, long objId, Map<?, ?> map) {

		List<SortedMap<String, Object>> entryRecords = new ArrayList<>();

		if (CMap.isEmpty(map)) {
			return entryRecords;
		}

		Iterator<?> it = map.entrySet().iterator();
		while (it.hasNext()) {

			SortedMap<String, Object> entryRecord = new TreeMap<>();
			entryRecords.add(entryRecord);

			Entry<?, ?> keyValuePair = (Entry<?, ?>) it.next();

			entryRecord.put(mainTableRefIdColumnName, objId);
			entryRecord.put(Const.KEY_COL, keyValuePair.getKey()); // Keys may not be complex objects
			entryRecord.put(Const.VALUE_COL, element2ColumnValue(keyValuePair.getValue()));
		}

		return entryRecords;
	}

	// Convert records of entry table to map
	@SuppressWarnings("unchecked")
	static Map<Object, Object> entryRecords2Map(ParameterizedType genericFieldType, List<SortedMap<String, Object>> entryRecords) {

		Map<Object, Object> map = CReflection.newMap((Class<? extends Map<Object, Object>>) genericFieldType.getRawType());

		if (CList.isEmpty(entryRecords)) {
			return map;
		}

		Type valueType = genericFieldType.getActualTypeArguments()[1];
		for (SortedMap<String, Object> entryRecord : entryRecords) {
			map.put(entryRecord.get(Const.KEY_COL), columnValue2Element(valueType, entryRecord.get(Const.VALUE_COL)));
		}

		return map;
	}

}
