package com.icx.domain.sql;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
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

	// Get order numbers of elements to remove from list and collect common elements in old order
	private static Set<Long> collectOrderNumbersOfElementsToRemove(List<?> oldList, List<?> newList, List<Long> orderNumbers, List<Object> commonElementsInOldOrder) {

		// Collect indexes of elements to remove from highest to lowest index (to avoid shifting indexes on removing order numbers) - care for doublets
		List<Object> newListClone = new ArrayList<>(newList);
		List<Integer> indexesOfElementsToRemoveInReverseOrder = new ArrayList<>();
		for (int o = oldList.size() - 1; o >= 0; o--) {
			Object element = oldList.get(o);

			if (!newListClone.contains(element)) {
				indexesOfElementsToRemoveInReverseOrder.add(o);
			}
			else {
				newListClone.remove(element);
				commonElementsInOldOrder.add(0, element);
			}
		}

		// Build list of order numbers of elements to remove and update order number list accordingly - from highest to lowest index
		Set<Long> orderNumbersOfElementsToRemove = new HashSet<>();
		for (int index : indexesOfElementsToRemoveInReverseOrder) {
			orderNumbersOfElementsToRemove.add(orderNumbers.remove(index));
		}

		return orderNumbersOfElementsToRemove;
	}

	// Generate new order numbers for elements to insert and collect common elements in new order
	// TODO: Handle case where order numbers are dense and no further number can be inserted between two consecutive numbers
	private static Map<Long, Object> assignNewOrderNumbersToElementsToInsert(List<?> oldList, List<?> newList, List<Long> orderNumbers, List<Object> commonElementsInNewOrder) {

		// Collect indexes of elements to insert - from lowest to highest index
		List<Object> oldListClone = new ArrayList<>(oldList);
		SortedSet<Integer> indexesOfElementsToInsert = new TreeSet<>();
		for (int n = 0; n < newList.size(); n++) {
			Object element = newList.get(n);

			if (!oldListClone.contains(element)) {
				indexesOfElementsToInsert.add(n);
			}
			else {
				oldListClone.remove(element);
				commonElementsInNewOrder.add(element);
			}
		}

		// Build map of new order numbers and elements to insert and update order number list accordingly
		Map<Long, Object> elementsToInsertByOrderNumberMap = new HashMap<>();
		for (int index : indexesOfElementsToInsert) {

			// Generate new order number for element to insert
			Long orderNumberForIndex = -1L;
			if (index == 0) { // Half of first order number
				orderNumberForIndex = orderNumbers.get(0) / 2;
			}
			else if (index < orderNumbers.size()) { // Arithmetic mean of surrounding order numbers
				orderNumberForIndex = (orderNumbers.get(index - 1) + orderNumbers.get(index)) / 2;
			}
			else { // Last order number plus initial order number increment
				orderNumberForIndex = orderNumbers.get(orderNumbers.size() - 1) + INITIAL_ORDER_INCREMENT;
			}

			// Add order number/element entry and adapt order number list
			elementsToInsertByOrderNumberMap.put(orderNumberForIndex, newList.get(index));
			orderNumbers.add(index, orderNumberForIndex);
		}

		return elementsToInsertByOrderNumberMap;
	}

	// Get index of object in list caring for doublets (replace object in list for found index by unknown object to avoid finding same index again for object doublet)
	private static int retrieveIndexOfObject(List<Object> list, Object object) {

		int index = list.indexOf(object);
		list.remove(index);
		list.add(index, new Object());

		return index;
	}

	// Build lists of order numbers of circularly permuted common elements (ignoring identical permutation)
	private static List<List<Long>> getOrderNumberPermutationCyclesOfCommonElements(List<Object> commonElementsInOldOrder, List<Object> commonElementsInNewOrder, List<Long> orderNumbersInOldOrder) {

		List<List<Long>> orderPermutationCycles = new ArrayList<>();
		List<Integer> usedIndexes = new ArrayList<>();
		int startIndex = 0;
		int oldIndex = 0;
		int newIndex = 0;

		// Find all permutation cycles of element order
		while (startIndex < commonElementsInOldOrder.size()) {

			// Start new permutation cycle with order number of element with start index
			List<Long> orderPermutationCycle = new ArrayList<>();
			orderPermutationCycles.add(orderPermutationCycle);
			orderPermutationCycle.add(orderNumbersInOldOrder.get(startIndex));
			usedIndexes.add(startIndex);

			// Complete permutation cycle
			oldIndex = startIndex;
			do {
				// Retrieve new index of element having given index
				Object element = commonElementsInOldOrder.get(oldIndex);
				newIndex = retrieveIndexOfObject(commonElementsInNewOrder, element);

				if (newIndex != startIndex) {

					// Add order number of element with new index to permutation cycle and continue building permutation cycle with element having new index
					orderPermutationCycle.add(orderNumbersInOldOrder.get(newIndex));
					usedIndexes.add(newIndex);
					oldIndex = newIndex;
				}
			} while (newIndex != startIndex);

			// Find start index for next permutation cycle (index not contained in any of the already built permutation cycles)
			startIndex++;
			while (startIndex < commonElementsInOldOrder.size() && usedIndexes.contains(startIndex)) {
				startIndex++;
			}
		}

		// Remove one-elemet cycles (identical positions of elements in new and old list)
		Iterator<List<Long>> it = orderPermutationCycles.iterator();
		while (it.hasNext()) {
			if (it.next().size() == 1) {
				it.remove();
			}
		}

		return orderPermutationCycles;
	}

	// Information to change persistence records for changed list with minimal effort
	static class ListChangeInfo {

		Set<Long> orderNumbersOfElementsToRemove = new HashSet<>();
		Map<Long, Object> elementsToInsertByNewOrderNumberMap = new HashMap<>();
		List<List<Long>> orderNumberPermutationCyclesOfCommonElements = new ArrayList<>();

		@Override
		public String toString() {
			return "\t" + orderNumbersOfElementsToRemove + "\n\t" + elementsToInsertByNewOrderNumberMap + "\n\t" + orderNumberPermutationCyclesOfCommonElements;
		}
	}

	// Collect list change information
	private static ListChangeInfo collectListChangeInfos(List<?> oldList, List<?> newList, List<Long> orderedOrderNumbers) {

		ListChangeInfo listChangeInfo = new ListChangeInfo();

		// Collect order numbers of elements not contained in new list anymore and collect common elements in old order
		List<Object> commonElementsInOldOrder = new ArrayList<>();
		listChangeInfo.orderNumbersOfElementsToRemove = collectOrderNumbersOfElementsToRemove(oldList, newList, orderedOrderNumbers, commonElementsInOldOrder);

		// Store order numbers of common elements in old order (order numbers of removed elements are removed from original oder number list)
		List<Long> orderNumbersOfCommonElementsInOldOrder = new ArrayList<>(orderedOrderNumbers);

		// Collect indexes of new elements, build their order numbers and insert these order numbers in list of ordered order numbers. Also collect common elements in new order.
		List<Object> commonElementsInNewOrder = new ArrayList<>();
		listChangeInfo.elementsToInsertByNewOrderNumberMap = assignNewOrderNumbersToElementsToInsert(oldList, newList, orderedOrderNumbers, commonElementsInNewOrder);

		// Collect order number mapping for elements which were only shifted in list
		listChangeInfo.orderNumberPermutationCyclesOfCommonElements = getOrderNumberPermutationCyclesOfCommonElements(commonElementsInOldOrder, commonElementsInNewOrder,
				orderNumbersOfCommonElementsInOldOrder);

		return listChangeInfo;
	}

	// Generate order numbers for elements of list - make large gaps between consecutive order numbers to allow insertion of elements without changing order numbers of existing elements each time
	private static List<Long> generateOrderNumbers(List<?> list) {

		List<Long> orderNumbers = new ArrayList<>();
		long orderNumber = INITIAL_ORDER_INCREMENT;
		for (int o = 0; o < list.size(); o++, orderNumber += INITIAL_ORDER_INCREMENT) {
			orderNumbers.add(orderNumber);
		}

		return orderNumbers;
	}

	public static void main(String[] args) {

		// Old list

		List<String> oldList = CList.newList("a", "b", "b", "c", "d");
		List<Long> orderedOrderNumbers = generateOrderNumbers(oldList);

		SortedMap<Long, String> oldListPersistenceMap = new TreeMap<>();
		for (int o = 0; o < oldList.size(); o++) {
			oldListPersistenceMap.put(orderedOrderNumbers.get(o), oldList.get(o));
		}
		oldList = new ArrayList<>(oldListPersistenceMap.values());
		System.out.println(oldList);
		System.out.println(oldListPersistenceMap);

		// New list

		List<String> newList = CList.newList("f", "b", "a", "e", "c", "g");
		System.out.println(newList);

		// Collect change infos

		ListChangeInfo listChangeInfo = collectListChangeInfos(oldList, newList, orderedOrderNumbers);
		System.out.println(listChangeInfo);

		// Build new persistence map

		SortedMap<Long, String> newListPersistenceMap = new TreeMap<>(oldListPersistenceMap);
		for (Long orderNumber : listChangeInfo.orderNumbersOfElementsToRemove) {
			newListPersistenceMap.remove(orderNumber);
		}
		System.out.println(newListPersistenceMap);
		for (Entry<Long, Object> entry : listChangeInfo.elementsToInsertByNewOrderNumberMap.entrySet()) {
			newListPersistenceMap.put(entry.getKey(), (String) entry.getValue());
		}
		System.out.println(newListPersistenceMap);
		for (List<Long> cycle : listChangeInfo.orderNumberPermutationCyclesOfCommonElements) {

			int index = 0;
			long firstOrderNumber = cycle.get(index);
			String element = oldListPersistenceMap.get(firstOrderNumber);
			newListPersistenceMap.put(0L, element);

			while (index < cycle.size() - 1) {
				long orderNumber = cycle.get(++index);
				newListPersistenceMap.put(orderNumber, element);
				element = oldListPersistenceMap.get(orderNumber);
			}

			newListPersistenceMap.remove(0L);
			newListPersistenceMap.put(firstOrderNumber, element);
		}
		System.out.println(newListPersistenceMap);

		// Rebuild new list

		List<Object> rebuiltNewList = new ArrayList<>(newListPersistenceMap.values());
		System.out.println(rebuiltNewList);
	}

	// DELETE, INSERT and UPDATE entry records representing list on changes in list
	static void updateEntriesForList(List<?> oldList, List<?> newList, SqlDomainController sdc, Connection cn, String entryTableName, String refIdColumnName, SqlDomainObject object)
			throws SQLException, SqlDbException {

		if (log.isTraceEnabled()) {
			log.trace("SDC: Old list: {}", oldList);
			log.trace("SDC: New list: {}", newList);
		}

		// Handle cases if one of the lists is empty or lists have disjoint elements
		boolean wasDone = false;
		if (!CList.isEmpty(oldList) && (CList.isEmpty(newList) || Collections.disjoint(oldList, newList))) {
			// DELETE FROM <entry table> WHERE <object reference column>=<objectid>
			SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId());
			wasDone = true;
		}

		if (!CList.isEmpty(newList) && (CList.isEmpty(oldList) || Collections.disjoint(oldList, newList))) {
			// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT, ELEMENT_ORDER) VALUES (<objectid>, <list element>, <order>)
			sdc.sqlDb.insertInto(cn, entryTableName, collection2EntryRecords(refIdColumnName, object.getId(), newList));
			wasDone = true;
		}

		// Handle case if there are common elements in both lists
		if (!wasDone) {

			// Get (ordered) list of order numbers of currently persisted (old) list
			// TODO: Store order number list locally
			String qualifiedOrderColumnName = entryTableName + "." + Const.ORDER_COL;
			List<Long> orderedOrderNumbers = sdc.sqlDb.selectFrom(cn, entryTableName, qualifiedOrderColumnName, null, Const.ORDER_COL, -1, null).stream()
					.map(r -> ((Number) r.get(Const.ORDER_COL)).longValue()).collect(Collectors.toList());

			// Collect infos to update persisted list incrementally
			ListChangeInfo listChangeInfo = collectListChangeInfos(oldList, newList, orderedOrderNumbers);

			// Delete elements not contained in new list anymore
			// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ELEMENT_ORDER IN <orders of elements to remove>
			for (String tmpOrderNumberStringList : Helpers.buildStringLists(listChangeInfo.orderNumbersOfElementsToRemove, 1000)) {
				SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.ORDER_COL + " IN (" + tmpOrderNumberStringList + ")");
			}

			// Build entry records for persistence of new elements
			List<SortedMap<String, Object>> entryRecordsToInsert = new ArrayList<>();
			for (Entry<Long, Object> entry : listChangeInfo.elementsToInsertByNewOrderNumberMap.entrySet()) {
				SortedMap<String, Object> entryRecord = new TreeMap<>();
				entryRecord.put(refIdColumnName, object.getId());
				entryRecord.put(Const.ORDER_COL, entry.getKey());
				entryRecord.put(Const.ELEMENT_COL, element2ColumnValue(entry.getValue()));
				entryRecordsToInsert.add(entryRecord);
			}

			// Insert entry records for new elements
			// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT, ELEMENT_ORDER) VALUES (<objectid>, <list element>, <order>)
			sdc.sqlDb.insertInto(cn, entryTableName, entryRecordsToInsert);

			// Update order numbers (shift cyclicly) of common elements in both lists
			// UPDATE <entry table> SET ELEMENT_ORDER=<new order number> WHERE <object reference column>=<objectid> AND ELEMENT_ORDER=<old order number>
			for (List<Long> cycle : listChangeInfo.orderNumberPermutationCyclesOfCommonElements) {
				sdc.sqlDb.update(cn, entryTableName, CMap.newMap(Const.ORDER_COL, 0L), refIdColumnName + "=" + object.getId() + " AND " + Const.ORDER_COL + "=" + cycle.get(cycle.size() - 1));
				for (int index = cycle.size() - 2; index >= 0; index--) {
					sdc.sqlDb.update(cn, entryTableName, CMap.newMap(Const.ORDER_COL, cycle.get(index + 1)),
							refIdColumnName + "=" + object.getId() + " AND " + Const.ORDER_COL + "=" + cycle.get(index));
				}
				sdc.sqlDb.update(cn, entryTableName, CMap.newMap(Const.ORDER_COL, cycle.get(0)), refIdColumnName + "=" + object.getId() + " AND " + Const.ORDER_COL + "=0");
			}
		}
	}

	// DELETE, INSERT and/or UPDATE entry records representing array on changes of array
	// TODO: Optimize algorithm like for lists
	static int updateEntriesForArray(Object newArray, SqlDomainController sdc, Connection cn, String entryTableName, String refIdColumnName, SqlDomainObject object)
			throws SQLException, SqlDbException {

		int length = Array.getLength(newArray);
		List<Object> listOfArrayElements = new ArrayList<>();
		for (int i = 0; i < length; i++) {
			listOfArrayElements.add(Array.get(newArray, i));
		}

		// DELETE FROM <entry table> WHERE <object reference column>=<objectid>
		SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId());
		// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT, ELEMENT_ORDER) VALUES (<objectid>, <array element>, <order>)
		sdc.sqlDb.insertInto(cn, entryTableName, collection2EntryRecords(refIdColumnName, object.getId(), listOfArrayElements));

		return length;
	}

}
