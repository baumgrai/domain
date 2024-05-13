package com.icx.domain.sql;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CCollection;
import com.icx.common.CList;
import com.icx.common.CMap;
import com.icx.common.CReflection;
import com.icx.common.Common;
import com.icx.jdbc.SqlDb;
import com.icx.jdbc.SqlDbException;

/**
 * Helpers for conversion of collections and maps to and from 'entry' tables and helpers to convert collections and maps to and from string representation.
 * <p>
 * Collections and maps of 'first level' will be represented in persistence database as so called 'entry' tables, which contain one 'entry' record per element of collection or entry of map.
 * <p>
 * Collections and maps of 'second level' - means as elements of collections or values of maps themselves - are also supported, but only if their elements or keys/values can simply be converted to and
 * from string representation using {@code toString()} for conversion of value to string and {@code valueOf()} method or constructor with String argument for rebuilding value from string.
 * 
 * @author baumgrai
 */
public abstract class ComplexFieldHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(ComplexFieldHelpers.class);

	private static final String NULL = "(null)";
	public static final long INITIAL_ORDER_INCREMENT = 0x100000000L;

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
	private static Object element2ColumnValue(Object element) {

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

	// String constructor cache
	private static Map<Class<?>, Constructor<?>> stringConstructorMap = new HashMap<>();

	// String -> object, used in string2Collection() and string2Map() to allow converting collections and maps of objects which are not strings; works for enum objects, strings, numbers and objects
	// which can be constructed using a constructor with one string argument
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
				else if (objectClass == Short.class) {
					return (T) Short.valueOf(s);
				}
				else if (objectClass == Integer.class) {
					return (T) Integer.valueOf(s);
				}
				else if (objectClass == Long.class) {
					return (T) Long.valueOf(s);
				}
				else if (objectClass == Double.class) {
					return (T) Double.valueOf(s);
				}
				else {
					Constructor<?> constructor = null;
					if (stringConstructorMap.containsKey(objectClass)) {
						constructor = stringConstructorMap.get(objectClass);
					}
					else {
						constructor = objectClass.getConstructor(String.class);
						stringConstructorMap.put(objectClass, constructor);
					}
					return (T) constructor.newInstance(s);
				}
			}
			catch (IllegalArgumentException | NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | InvocationTargetException ex) {
				log.error("SDC: String '{}' cannot be converted to type '{}'! ({})", s, objectClass.getName(), objectClass, ex);
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
			log.error("SDC: {} occurred trying to create collection of type '{}': {}", ex.getClass().getSimpleName(), collectionClass.getName(), ex);
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
			log.error("SDC: {} occurred trying to create map of type '{}': {}", ex.getClass().getSimpleName(), mapClass.getName(), ex);
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

		long order = INITIAL_ORDER_INCREMENT;
		Iterator<?> it = collection.iterator();
		while (it.hasNext()) {

			SortedMap<String, Object> entryRecord = new TreeMap<>();
			entryRecords.add(entryRecord);

			entryRecord.put(mainTableRefIdColumnName, objId);
			entryRecord.put(Const.ELEMENT_COL, element2ColumnValue(it.next()));
			if (collection instanceof List) { // Care for element order on lists
				entryRecord.put(Const.ORDER_COL, order);
				order += INITIAL_ORDER_INCREMENT;
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

	// Convert single entry of map to key/value map to UPDATE appropriate records in entry table
	static SortedMap<String, Object> entryValue2ColumnValueMap(Object value) {
		return CMap.newSortedMap(Const.VALUE_COL, element2ColumnValue(value));
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

	// -------------------------------------------------------------------------
	// Update entry tables for maps and sets
	// -------------------------------------------------------------------------

	// DELETE, INSERT and/or UPDATE entry records representing map on change of map
	static void updateEntriesForMap(Map<?, ?> oldMap, Map<?, ?> newMap, SqlDomainController sdc, Connection cn, String entryTableName, String refIdColumnName, SqlDomainObject object)
			throws SQLException, SqlDbException {

		Set<Object> mapKeysToRemove = new HashSet<>();
		Map<Object, Object> mapEntriesToInsert = new HashMap<>();
		Map<Object, Object> mapEntriesToChange = new HashMap<>();
		boolean hasOldMapNullKey = false;

		// Determine map entries which do not exist anymore
		for (Object oldKey : oldMap.keySet()) {
			if (!newMap.containsKey(oldKey)) {
				if (oldKey == null) {
					hasOldMapNullKey = true;
				}
				else {
					mapKeysToRemove.add(oldKey);
				}
			}
		}

		// Determine new and changed map entries
		for (Entry<?, ?> newMapEntry : newMap.entrySet()) {
			Object newKey = newMapEntry.getKey();
			Object newValue = newMapEntry.getValue();

			if (!oldMap.containsKey(newKey)) {
				mapEntriesToInsert.put(newKey, newValue);
			}
			else if (!objectsEqual(oldMap.get(newKey), newValue)) {
				mapEntriesToChange.put(newKey, newValue);
			}
		}

		// Delete entry records for removed map entries
		if (!mapKeysToRemove.isEmpty()) {
			// Multiple deletes with lists of max 1000 elements (Oracle limitation)
			// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ENTRY_KEY IN <keys of entries to remove>
			for (String keyList : Helpers.buildStringLists(mapKeysToRemove, 1000)) {
				SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + " IN (" + keyList + ")");
			}
		}

		if (hasOldMapNullKey) {
			// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ENTRY_KEY IS NULL
			SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + " IS NULL");
		}

		// Insert entry records for new map entries
		if (!mapEntriesToInsert.isEmpty()) {
			// (batch) INSERT INTO <entry table> (<object reference column>, ENTRY_KEY, ENTRY_VALUE) VALUES (<objectid>, <converted key>, <converted value>)
			sdc.sqlDb.insertInto(cn, entryTableName, map2EntryRecords(refIdColumnName, object.getId(), mapEntriesToInsert));
		}

		// Update entry records for changed map entries
		for (Entry<Object, Object> entry : mapEntriesToChange.entrySet()) {
			Object key = entry.getKey();

			// UPDATE <entry table> SET ENTRY_VALUE=<entry value> WHERE <object reference column>=<objectid> AND ENTRY_KEY=<entry key>
			sdc.sqlDb.update(cn, entryTableName, entryValue2ColumnValueMap(entry.getValue()),
					refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + "=" + (key instanceof String || key instanceof Enum ? "'" + key + "'" : key));
		}
	}

	// DELETE and/or INSERT entry records representing set on change of set
	static void updateEntriesForSet(Set<?> oldSet, Set<?> newSet, SqlDomainController sdc, Connection cn, String entryTableName, String refIdColumnName, SqlDomainObject object)
			throws SQLException, SqlDbException {

		Set<Object> elementsToRemove = new HashSet<>();
		Set<Object> elementsToInsert = new HashSet<>();
		boolean hasNullElement = false;

		// Determine elements which do not exist anymore
		for (Object oldElement : oldSet) {
			if (!newSet.contains(oldElement)) {
				if (oldElement == null) {
					hasNullElement = true;
				}
				else {
					elementsToRemove.add(oldElement);
				}
			}
		}

		// Determine new elements (null element is no exception from default handling on insertInto())
		for (Object newElement : newSet) {
			if (!oldSet.contains(newElement)) {
				elementsToInsert.add(newElement);
			}
		}

		// Delete entry records for removed elements
		if (!elementsToRemove.isEmpty()) {
			// Multiple deletes with lists of max 1000 elements (Oracle limitation)
			// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ENTRY_KEY IN <keys of entries to remove>
			for (String elementList : Helpers.buildStringLists(elementsToRemove, 1000)) {
				SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.ELEMENT_COL + " IN (" + elementList + ")");
			}
		}

		if (hasNullElement) {
			// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ELEMENT IS NULL
			SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.ELEMENT_COL + " IS NULL");
		}

		// Insert entry records for new elements
		if (!elementsToInsert.isEmpty()) {
			// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT) VALUES (<objectid>, <set element>)
			sdc.sqlDb.insertInto(cn, entryTableName, collection2EntryRecords(refIdColumnName, object.getId(), elementsToInsert));
		}
	}

	// -------------------------------------------------------------------------
	// Update entry tables for lists (and arrays)
	// -------------------------------------------------------------------------

	// Information to change persistence records for changed list with minimal effort
	static class ListChangeInfo {

		// Members
		List<Long> orderedOrderNumbers = null;

		Set<Long> orderNumbersOfElementsToRemove = new HashSet<>();
		SortedMap<Long, Object> elementToInsertByOrderNumberMap = new TreeMap<>();

		List<Long> orderNumbersOfCommonElementsInOldOrder = new ArrayList<>();
		List<Long> orderNumbersOfCommonElementsInNewOrder = new ArrayList<>();
		List<List<Long>> orderNumberPermutationCyclesOfCommonElements = new ArrayList<>();

		private static long buildNewOrderNumber(List<Long> orderNumbers, int indexOfNewElement) throws DenseOrderNumbersException {

			if (indexOfNewElement == orderNumbers.size()) {
				return orderNumbers.get(orderNumbers.size() - 1) + INITIAL_ORDER_INCREMENT; // Last order number plus initial order number increment
			}
			else {
				long orderNumberOfPrecedingElement = (indexOfNewElement == 0 ? 0 : orderNumbers.get(indexOfNewElement - 1));
				long currentOrderNumberAtIndex = orderNumbers.get(indexOfNewElement);

				if (currentOrderNumberAtIndex == orderNumberOfPrecedingElement + 1) { // Order numbers are dense at index where new element is to insert -> new element cannot be inserted
					throw new DenseOrderNumbersException(indexOfNewElement);
				}
				else {
					return (orderNumberOfPrecedingElement + currentOrderNumberAtIndex) / 2; // Arithmetic mean of surrounding order numbers
				}
			}
		}

		// Constructor
		public ListChangeInfo(
				List<?> oldList,
				List<?> newList,
				List<Long> oldOrderedOrderNumbers) throws DenseOrderNumbersException {

			if (oldList.size() != oldOrderedOrderNumbers.size()) {
				throw new IllegalArgumentException("List of order numbers has not the same size as existing list of elements: (" + oldOrderedOrderNumbers.size() + "/" + oldList.size() + ")");
			}

			this.orderedOrderNumbers = new ArrayList<>(oldOrderedOrderNumbers);

			// Collect indexes of elements to remove and collect common elements - care for doublets and their order
			List<Object> newListClone = new ArrayList<>(newList);
			for (int o = 0; o < oldList.size(); o++) {
				Object element = oldList.get(o);

				if (newListClone.contains(element)) {
					newListClone.remove(element);
					orderNumbersOfCommonElementsInOldOrder.add(oldOrderedOrderNumbers.get(o));
				}
				else {
					orderNumbersOfElementsToRemove.add(orderedOrderNumbers.get(o));
				}
			}
			orderedOrderNumbers.removeAll(orderNumbersOfElementsToRemove);

			// Collect indexes of elements to insert and collect common elements in possibly changed order
			List<Object> oldListClone = new ArrayList<>(oldList);
			for (int n = 0; n < newList.size(); n++) {
				Object element = newList.get(n);

				if (oldListClone.contains(element)) {
					int o = oldListClone.indexOf(element);
					oldListClone.set(o, new Object()); // Replace element by non-findable object
					long orderNumber = oldOrderedOrderNumbers.get(o);
					orderNumbersOfCommonElementsInNewOrder.add(orderNumber);
					// orderNumbers.set(n, orderNumber);
				}
				else {
					long newOrderNumberAtIndex = buildNewOrderNumber(orderedOrderNumbers, n);
					elementToInsertByOrderNumberMap.put(newOrderNumberAtIndex, element);
					orderedOrderNumbers.add(n, newOrderNumberAtIndex);
				}
			}

			// Collect index mapping for elements which were only shifted in list (common in both lists)
			Set<Integer> usedIndexes = new HashSet<>();
			int startIndex = 0;

			// Find all permutation cycles of element order
			do {
				usedIndexes.add(startIndex);

				long orderNumberOfElement = orderNumbersOfCommonElementsInOldOrder.get(startIndex);
				int newIndex = orderNumbersOfCommonElementsInNewOrder.indexOf(orderNumberOfElement);

				if (!objectsEqual(newIndex, startIndex)) {

					// Start new permutation cycle with order number of element with start index
					List<Long> permutationCycle = new ArrayList<>();
					orderNumberPermutationCyclesOfCommonElements.add(permutationCycle);
					permutationCycle.add(orderNumberOfElement);

					// Complete permutation cycle
					do {
						// Add order number of element with new index to permutation cycle and continue building permutation cycle with element having new index
						orderNumberOfElement = orderNumbersOfCommonElementsInOldOrder.get(newIndex);
						permutationCycle.add(orderNumberOfElement);
						usedIndexes.add(newIndex);
						newIndex = orderNumbersOfCommonElementsInNewOrder.indexOf(orderNumberOfElement);

					} while (newIndex != startIndex);
				}

				// Find start index for next permutation cycle (index not contained in any of the already built permutation cycles)
				startIndex++;
				while (usedIndexes.contains(startIndex)) {
					startIndex++;
				}

			} while (startIndex < orderNumbersOfCommonElementsInOldOrder.size());
		}

		@Override
		public String toString() {
			return "\t" + orderNumbersOfElementsToRemove + "\n\t" + elementToInsertByOrderNumberMap + "\n\t" + orderNumberPermutationCyclesOfCommonElements;
		}
	}

	// public static void main(String[] args) throws DenseOrderNumbersException {
	// new ListChangeInfo(CList.newList(0, 0), CList.newList(0), CList.newList(1L, 2L));
	// }

	// Exception to internally signal that persistence entries have to be rebuild from scratch because any new element cannot be inserted at its index position due to density of oder numbers
	@SuppressWarnings("serial")
	private static class DenseOrderNumbersException extends Exception {

		final int index;

		DenseOrderNumbersException(
				int index) {

			this.index = index;
		}

		@Override
		public String toString() {
			return "Index: " + index;
		}
	}

	// DELETE, INSERT and UPDATE entry records representing list on changes in list
	static void updateEntriesForList(List<?> oldList, List<?> newList, SqlDomainController sdc, Connection cn, String entryTableName, String refIdColumnName, long objectId,
			Field listField /* for logging only */) throws SQLException, SqlDbException {

		if (log.isTraceEnabled()) {
			log.trace("SDC: Old list: {}", oldList);
			log.trace("SDC: New list: {}", newList);
		}

		if (objectsEqual(oldList, newList)) {
			;
		}
		else if (Collections.disjoint(oldList, newList)) { // Lists have no elements in common

			if (!CList.isEmpty(oldList)) {

				// Remove all entry records for list elements of old list (which are not contained in new list)
				SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + objectId);

				// Reset order number cache
				sdc.setOrderedListOrderNumbers(entryTableName, objectId, new ArrayList<>());
			}

			if (!CList.isEmpty(newList)) {

				// Insert entry records for all list elements of new list (which were not contained in old list)
				List<SortedMap<String, Object>> entryRecords = collection2EntryRecords(refIdColumnName, objectId, newList);
				sdc.sqlDb.insertInto(cn, entryTableName, entryRecords);

				// Update order number cache for new built list
				sdc.setOrderedListOrderNumbers(entryTableName, objectId, entryRecords.stream().map(r -> ((Number) r.get(Const.ORDER_COL)).longValue()).collect(Collectors.toList()));
			}
		}
		else { // Lists have any elements in common
			try {
				// Get (ordered) list of order numbers of currently persisted (old) list - SELECT from database if not already cached
				List<Long> orderedOrderNumbers = sdc.getOrderedListOrderNumbers(entryTableName, objectId);
				// if (!oldList.isEmpty() && orderedOrderNumbers.isEmpty()) {
				// List<String> columnNames = CList.newList(entryTableName + "." + Const.ELEMENT_COL, entryTableName + "." + Const.ORDER_COL);
				// orderedOrderNumbers = sdc.sqlDb.selectFrom(cn, entryTableName, columnNames, refIdColumnName + "=" + objectId, Const.ORDER_COL, -1, null).stream()
				// .map(r -> ((Number) r.get(Const.ORDER_COL)).longValue()).collect(Collectors.toList());
				// }

				// Collect infos to update persisted list incrementally and build list of order numbers for entry records of new list
				ListChangeInfo listChangeInfo = new ListChangeInfo(oldList, newList, orderedOrderNumbers);

				// Update order number cache for this list
				sdc.setOrderedListOrderNumbers(entryTableName, objectId, listChangeInfo.orderedOrderNumbers);

				// Delete entry records for elements not contained in new list anymore
				// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ELEMENT_ORDER IN <orders of elements to remove>
				for (String tmpOrderNumberStringList : Helpers.buildStringLists(listChangeInfo.orderNumbersOfElementsToRemove, 1000)) {
					SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + objectId + " AND " + Const.ORDER_COL + " IN (" + tmpOrderNumberStringList + ")");
				}

				// Build and insert entry records for of new elements
				List<SortedMap<String, Object>> entryRecordsToInsert = new ArrayList<>();
				for (Entry<Long, Object> entry : listChangeInfo.elementToInsertByOrderNumberMap.entrySet()) {

					SortedMap<String, Object> entryRecord = new TreeMap<>();
					entryRecord.put(refIdColumnName, objectId);
					entryRecord.put(Const.ORDER_COL, entry.getKey());
					entryRecord.put(Const.ELEMENT_COL, element2ColumnValue(entry.getValue()));

					entryRecordsToInsert.add(entryRecord);
				}

				sdc.sqlDb.insertInto(cn, entryTableName, entryRecordsToInsert);

				// Update order numbers (shift cyclicly) of permutated common elements
				for (List<Long> cycle : listChangeInfo.orderNumberPermutationCyclesOfCommonElements) {

					sdc.sqlDb.update(cn, entryTableName, CMap.newMap(Const.ORDER_COL, 0L), refIdColumnName + "=" + objectId + " AND " + Const.ORDER_COL + "=" + cycle.get(cycle.size() - 1));
					for (int index = cycle.size() - 2; index >= 0; index--) {
						sdc.sqlDb.update(cn, entryTableName, CMap.newMap(Const.ORDER_COL, cycle.get(index + 1)), refIdColumnName + "=" + objectId + " AND " + Const.ORDER_COL + "=" + cycle.get(index));
					}
					sdc.sqlDb.update(cn, entryTableName, CMap.newMap(Const.ORDER_COL, cycle.get(0)), refIdColumnName + "=" + objectId + " AND " + Const.ORDER_COL + "=0");
				}
			}
			catch (DenseOrderNumbersException e) { // Any new element cannot be inserted because order numbers are dense at inserting index!

				log.info("SDC: List order numbers are dense at index {}! Cannot directly persist changed list '{}'. Persist list again from scratch.", e.index, listField.getName());

				// Remove all existing entries (which represent old list) and insert all new entries (which represent new list)
				SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + objectId);
				List<SortedMap<String, Object>> entryRecords = collection2EntryRecords(refIdColumnName, objectId, newList);
				sdc.sqlDb.insertInto(cn, entryTableName, entryRecords);

				// Update order number cache for newly built list
				sdc.setOrderedListOrderNumbers(entryTableName, objectId, entryRecords.stream().map(r -> ((Number) r.get(Const.ORDER_COL)).longValue()).collect(Collectors.toList()));
			}
		}
	}

	// DELETE, INSERT and/or UPDATE entry records representing array on changes of array
	// TODO: Optimize algorithm like for lists
	static int updateEntriesForArray(Object newArray, SqlDomainController sdc, Connection cn, String entryTableName, String refIdColumnName, long objectId) throws SQLException, SqlDbException {

		int length = Array.getLength(newArray);
		List<Object> listOfArrayElements = new ArrayList<>();
		for (int i = 0; i < length; i++) {
			listOfArrayElements.add(Array.get(newArray, i));
		}

		// DELETE FROM <entry table> WHERE <object reference column>=<objectid>
		SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + objectId);
		// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT, ELEMENT_ORDER) VALUES (<objectid>, <array element>, <order>)
		sdc.sqlDb.insertInto(cn, entryTableName, collection2EntryRecords(refIdColumnName, objectId, listOfArrayElements));

		return length;
	}

	// -------------------
	// Test...
	// -------------------

	// // Generate order numbers for elements of list - make large gaps between consecutive order numbers to allow insertion of elements without changing order numbers of existing elements each time
	// private static List<Long> generateOrderNumbers(List<?> list) {
	//
	// List<Long> orderNumbers = new ArrayList<>();
	// long orderNumber = INITIAL_ORDER_INCREMENT;
	// for (int o = 0; o < list.size(); o++, orderNumber += INITIAL_ORDER_INCREMENT) {
	// orderNumbers.add(orderNumber);
	// }
	//
	// return orderNumbers;
	// }
	//
	// public static void main(String[] args) {
	//
	// // Old list
	//
	// List<String> oldList = CList.newList("a", "b", "b", "c", "d");
	// List<Long> orderedOrderNumbers = generateOrderNumbers(oldList);
	//
	// SortedMap<Long, String> oldListPersistenceMap = new TreeMap<>();
	// for (int o = 0; o < oldList.size(); o++) {
	// oldListPersistenceMap.put(orderedOrderNumbers.get(o), oldList.get(o));
	// }
	// oldList = new ArrayList<>(oldListPersistenceMap.values());
	// System.out.println(oldList);
	// System.out.println(oldListPersistenceMap);
	//
	// // New list
	//
	// List<String> newList = CList.newList("f", "b", "a", "e", "c", "g");
	// System.out.println(newList);
	//
	// // Collect change infos
	//
	// ListChangeInfo listChangeInfo;
	// try {
	// listChangeInfo = collectListChangeInfos(oldList, newList, orderedOrderNumbers);
	// System.out.println(listChangeInfo);
	//
	// // Build new persistence map
	//
	// SortedMap<Long, String> newListPersistenceMap = new TreeMap<>(oldListPersistenceMap);
	// for (Long orderNumber : listChangeInfo.orderNumbersOfElementsToRemove) {
	// newListPersistenceMap.remove(orderNumber);
	// }
	// System.out.println(newListPersistenceMap);
	// for (Entry<Long, Object> entry : listChangeInfo.elementsToInsertByNewOrderNumberMap.entrySet()) {
	// newListPersistenceMap.put(entry.getKey(), (String) entry.getValue());
	// }
	// System.out.println(newListPersistenceMap);
	// for (List<Long> cycle : listChangeInfo.orderNumberPermutationCyclesOfCommonElements) {
	//
	// int index = 0;
	// long firstOrderNumber = cycle.get(index);
	// String element = oldListPersistenceMap.get(firstOrderNumber);
	// newListPersistenceMap.put(0L, element);
	//
	// while (index < cycle.size() - 1) {
	// long orderNumber = cycle.get(++index);
	// newListPersistenceMap.put(orderNumber, element);
	// element = oldListPersistenceMap.get(orderNumber);
	// }
	//
	// newListPersistenceMap.remove(0L);
	// newListPersistenceMap.put(firstOrderNumber, element);
	// }
	// System.out.println(newListPersistenceMap);
	//
	// // Rebuild new list
	//
	// List<Object> rebuiltNewList = new ArrayList<>(newListPersistenceMap.values());
	// System.out.println(rebuiltNewList);
	// }
	// catch (DenseOrderNumbersException e) {
	// }
	// }

}
