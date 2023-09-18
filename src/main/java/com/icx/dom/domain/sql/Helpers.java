package com.icx.dom.domain.sql;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.AbstractMap.SimpleEntry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CCollection;
import com.icx.dom.common.CList;
import com.icx.dom.common.CLog;
import com.icx.dom.common.CMap;
import com.icx.dom.common.Common;
import com.icx.dom.common.Reflection;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainObject;

public abstract class Helpers extends Common {

	static final Logger log = LoggerFactory.getLogger(Helpers.class);

	private static final String NULL = "(null)";

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

	private static Object element2ColumnValue(Object element) {

		if (element instanceof Collection) {
			return collection2String((Collection<?>) element);
		}
		else if (element instanceof Map) {
			return map2String((Map<?, ?>) element);
		}
		else {
			return field2ColumnValue(element);
		}
	}

	// String -> object, used in string2Collection() and string2Map() to allow converting collections and maps of objects which are not strings; works for enum objects, strings and objects which can
	// be constructed using a constructor with one string argument
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static <T> T string2SimpleObject(Class<? extends T> objectClass, String s) {

		objectClass = (Class<? extends T>) Reflection.getBoxingWrapperType(objectClass);

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

	// Comma separated string -> collection
	private static <T> Collection<T> string2Collection(Class<? extends Collection<T>> collectionClass, Class<? extends T> elementClass, String collectionAsString) {

		try {
			Collection<T> collection = (collectionClass.isInterface() ? Reflection.newCollection(collectionClass) : collectionClass.getDeclaredConstructor().newInstance());
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

	// Comma/equal-sign separated string -> map
	private static <K, V> Map<K, V> string2Map(Class<? extends Map<K, V>> mapClass, Class<? extends K> keyClass, Class<? extends V> valueClass, String mapAsString) {

		try {
			Map<K, V> map = (mapClass.isInterface() ? Reflection.newMap(mapClass) : mapClass.getDeclaredConstructor().newInstance());

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

	// Method will only be used for elements of collections or keys or values of maps - so here lists of lists, lists of maps, maps with lists or maps as keys or values will be handled
	@SuppressWarnings("unchecked")
	private static Object columnValue2Element(Type elementType, Object columnValue) {

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
			return column2FieldValue((Class<?>) elementType, columnValue);
		}
	}

	// -------------------------------------------------------------------------
	// Conversion from/to field to/from column value
	// -------------------------------------------------------------------------

	// Convert field value to value to store in database
	static Object field2ColumnValue(Object fieldValue) {

		if (fieldValue instanceof Enum) {
			return fieldValue.toString();
		}
		else if (fieldValue instanceof BigInteger) {
			return ((BigInteger) fieldValue).longValue();
		}
		else if (fieldValue instanceof BigDecimal) {
			return ((BigDecimal) fieldValue).doubleValue();
		}
		else if (fieldValue instanceof File) {
			return ((File) fieldValue).getPath();
		}
		else {
			return fieldValue;
		}
	}

	// Convert value retrieved from database to value for field
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T> T column2FieldValue(Class<? extends T> fieldType, Object columnValue) {

		if (fieldType == null) {
			return (T) columnValue;
		}

		try {
			if (columnValue == null) {
				return null;
			}
			else if (fieldType.isAssignableFrom(columnValue.getClass()) || Reflection.getBoxingWrapperType(fieldType).isAssignableFrom(columnValue.getClass())) {
				return (T) columnValue;
			}
			else if (fieldType == BigInteger.class) {
				return (T) BigInteger.valueOf((long) columnValue);
			}
			else if (fieldType == BigDecimal.class) {

				Double d = (double) columnValue;
				if (d % 1.0 == 0 && d < Long.MAX_VALUE) { // Avoid artifacts BigDecimal@4 -> BigDecimal@4.0
					return (T) BigDecimal.valueOf(d.longValue());
				}
				else {
					return (T) BigDecimal.valueOf(d);
				}
			}
			else if (Enum.class.isAssignableFrom(fieldType)) {
				return (T) Enum.valueOf((Class<? extends Enum>) fieldType, (String) columnValue);
			}
			else if (File.class.isAssignableFrom(fieldType)) {
				return (T) new File((String) columnValue);
			}
			else {
				return (T) columnValue;
			}
		}
		catch (IllegalArgumentException iaex) {
			log.error("SQL: Column value {} cannot be converted to enum type '{}'! ({})", CLog.forAnalyticLogging(columnValue), ((Class<? extends Enum>) fieldType).getName(), iaex.getMessage());
		}
		catch (Exception ex) {
			log.error("SQL: Column value {} cannot be converted to  '{}'! ({})", CLog.forAnalyticLogging(columnValue), fieldType.getName(), ex);
		}

		return null;
	}

	// -------------------------------------------------------------------------
	// Conversion from/to collection or map to/from list of entry records like stored in database
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

		Collection<Object> collection = Reflection.newCollection((Class<? extends Collection<Object>>) genericFieldType.getRawType());

		if (CList.isEmpty(entryRecords)) {
			return collection;
		}

		for (SortedMap<String, Object> entryRecord : entryRecords) {

			Type elementType = genericFieldType.getActualTypeArguments()[0];
			Object element = columnValue2Element(elementType, entryRecord.get(Const.ELEMENT_COL)); // Element of collection can be a collection or map itself

			collection.add(element);
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
			entryRecord.put(Const.KEY_COL, field2ColumnValue(keyValuePair.getKey())); // Keys may not be complex objects
			entryRecord.put(Const.VALUE_COL, element2ColumnValue(keyValuePair.getValue()));
		}

		return entryRecords;
	}

	// Convert records of entry table to map
	@SuppressWarnings("unchecked")
	static Map<Object, Object> entryRecords2Map(ParameterizedType genericFieldType, List<SortedMap<String, Object>> entryRecords) {

		Map<Object, Object> map = Reflection.newMap((Class<? extends Map<Object, Object>>) genericFieldType.getRawType());

		if (CList.isEmpty(entryRecords)) {
			return map;
		}

		for (SortedMap<String, Object> entryRecord : entryRecords) {

			Type keyType = genericFieldType.getActualTypeArguments()[0];
			Object key = column2FieldValue((Class<?>) keyType, entryRecord.get(Const.KEY_COL)); // Keys may not be complex objects

			Type valueType = genericFieldType.getActualTypeArguments()[1];
			Object value = columnValue2Element(valueType, entryRecord.get(Const.VALUE_COL)); // Value of map can be a collection or map itself

			map.put(key, value);
		}

		return map;
	}

	// -------------------------------------------------------------------------
	// General helpers
	// -------------------------------------------------------------------------

	static Class<?> requiredJdbcTypeFor(Class<?> fieldClass) {

		if (fieldClass == BigInteger.class) {
			return Long.class;
		}
		else if (fieldClass == BigDecimal.class) {
			return Double.class;
		}
		else if (Enum.class.isAssignableFrom(fieldClass)) {
			return String.class;
		}
		else if (File.class.isAssignableFrom(fieldClass)) {
			return String.class;
		}
		else {
			return Reflection.getBoxingWrapperType(fieldClass);
		}
	}

	// Create object with given id - only used for exclusive selection methods
	static final synchronized <T extends DomainObject> T createWithId(final Class<T> domainObjectClass, long id) {

		T obj = DomainController.instantiate(domainObjectClass);
		if (obj != null) {

			obj.registerById(id);

			if (log.isDebugEnabled()) {
				log.debug("DC: Created {}.", obj.name());
			}
		}

		return obj;
	}

	// Count new and changed objects grouped by object domain classes (for logging only)
	static <T extends SqlDomainObject> Set<Entry<String, Integer>> groupCountsByDomainClassName(Set<T> objects) {

		return objects.stream().collect(Collectors.groupingBy(Object::getClass)).entrySet().stream().map(e -> new SimpleEntry<>(e.getKey().getSimpleName(), e.getValue().size()))
				.collect(Collectors.toSet());
	}

	// Build "(<ids>)" lists with at maximum 1000 ids (Oracle limit for # of elements in WHERE IN (...) clause = 1000)
	static List<String> buildMax1000IdsLists(Set<Long> ids) {

		List<String> idStringLists = new ArrayList<>();
		if (ids == null || ids.isEmpty()) {
			return idStringLists;
		}

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (long id : ids) {

			if (i % 1000 != 0) {
				sb.append(",");
			}

			sb.append(id);

			if (i % 1000 == 999) {
				idStringLists.add(sb.toString());
				sb.setLength(0);
			}

			i++;
		}

		if (sb.length() > 0) {
			idStringLists.add(sb.toString());
		}

		return idStringLists;
	}

	// Build string list of elements for WHERE clause (of DELETE statement)
	static String buildElementList(Set<Object> elements) {

		StringBuilder sb = new StringBuilder();
		sb.append("(");

		for (Object element : elements) {

			element = Helpers.field2ColumnValue(element);
			if (element instanceof String) {
				sb.append("'" + element + "'");
			}
			else {
				sb.append(element);
			}

			sb.append(",");
		}

		sb.replace(sb.length() - 1, sb.length(), ")");

		return sb.toString();
	}

}
