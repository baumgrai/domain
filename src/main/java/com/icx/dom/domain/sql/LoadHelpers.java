package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CCollection;
import com.icx.dom.common.CList;
import com.icx.dom.common.CLog;
import com.icx.dom.common.CMap;
import com.icx.dom.common.Common;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

public abstract class LoadAndSaveHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(LoadAndSaveHelpers.class);

	// -------------------------------------------------------------------------
	// SELECT helpers
	// -------------------------------------------------------------------------

	private static class SelectDescription {

		String joinedTableExpression = null;

		List<String> allColumnNames = new ArrayList<>();
		List<Class<?>> allFieldTypes = new ArrayList<>();

		String orderByClause = null;
	}

	// Build joined table expression for all inherited domain classes and also build name list of columns to retrieve
	private static <S extends SqlDomainObject> SelectDescription buildSelectDescriptionFor(Class<S> objectDomainClass) {

		SelectDescription sd = new SelectDescription();

		// Build table and column expression for object domain class

		SqlDbTable objectTable = SqlRegistry.getTableFor(objectDomainClass);

		sd.joinedTableExpression = objectTable.name;
		sd.allColumnNames.addAll(objectTable.columns.stream().map(c -> objectTable.name + "." + c.name).collect(Collectors.toList()));
		sd.allFieldTypes.addAll(objectTable.columns.stream().map(SqlRegistry::getRequiredJdbcTypeFor).collect(Collectors.toList()));

		// Extend table and column expression for inherited domain classes

		Predicate<Column> isNonStandardColumnPredicate = c -> !objectsEqual(c.name, Const.ID_COL) && !objectsEqual(c.name, Const.DOMAIN_CLASS_COL);

		Class<? extends DomainObject> derivedDomainClass = Registry.getSuperclass(objectDomainClass);
		while (derivedDomainClass != SqlDomainObject.class) {

			SqlDbTable inheritedTable = SqlRegistry.getTableFor(derivedDomainClass);

			sd.joinedTableExpression += " JOIN " + inheritedTable.name + " ON " + inheritedTable.name + ".ID=" + objectTable.name + ".ID";
			sd.allColumnNames.addAll(inheritedTable.columns.stream().filter(isNonStandardColumnPredicate).map(c -> inheritedTable.name + "." + c.name).collect(Collectors.toList()));
			sd.allFieldTypes.addAll(inheritedTable.columns.stream().filter(isNonStandardColumnPredicate).map(SqlRegistry::getRequiredJdbcTypeFor).collect(Collectors.toList()));

			derivedDomainClass = Registry.getSuperclass(derivedDomainClass);
		}

		return sd;
	}

	// Build joined table expression for entry tables (storing collections and maps)
	private static SelectDescription buildSelectDescriptionForEntriesOf(String baseTableExpression, String objectTableName, Field complexField) {

		SelectDescription sde = new SelectDescription();

		// Build table clause - join entry table and main object table

		String entryTableName = SqlRegistry.getEntryTableFor(complexField).name;
		String refIdColumnName = SqlRegistry.getMainTableRefIdColumnFor(complexField).name;

		sde.joinedTableExpression = entryTableName + " JOIN " + baseTableExpression + " ON " + entryTableName + "." + refIdColumnName + "=" + objectTableName + ".ID";

		// Build column clause for entry records

		// Column referencing main table for domain class
		sde.allColumnNames.add(refIdColumnName);
		sde.allFieldTypes.add(Long.class);

		Class<?> fieldClass = complexField.getType();
		ParameterizedType genericFieldType = ((ParameterizedType) complexField.getGenericType());

		if (Collection.class.isAssignableFrom(fieldClass)) {

			// Column for elements of collection
			sde.allColumnNames.add(Const.ELEMENT_COL);
			Type elementType = genericFieldType.getActualTypeArguments()[0];
			if (elementType instanceof ParameterizedType) {
				elementType = String.class; // Collections and maps as elements of a collection are stored as strings in database
			}
			else {
				elementType = Helpers.requiredJdbcTypeFor((Class<?>) elementType);
			}
			sde.allFieldTypes.add((Class<?>) elementType);

			if (List.class.isAssignableFrom(fieldClass)) {

				// Column for list order and ORDER BY clause
				sde.allColumnNames.add(Const.ORDER_COL);
				sde.allFieldTypes.add(Integer.class);
				sde.orderByClause = Const.ORDER_COL;
			}
		}
		else {
			// Column for keys of map
			sde.allColumnNames.add(Const.KEY_COL);
			Type keyType = genericFieldType.getActualTypeArguments()[0];
			keyType = Helpers.requiredJdbcTypeFor((Class<?>) keyType);
			sde.allFieldTypes.add((Class<?>) keyType); // Keys may not be complex objects

			// Column for values of map
			sde.allColumnNames.add(Const.VALUE_COL);
			Type valueType = genericFieldType.getActualTypeArguments()[1];
			if (valueType instanceof ParameterizedType) {
				valueType = String.class; // Collections and maps as values of a map are stored as strings in database
			}
			else {
				valueType = Helpers.requiredJdbcTypeFor((Class<?>) valueType);
			}
			sde.allFieldTypes.add((Class<?>) valueType);
		}

		return sde;
	}

	// Load object records for one object domain class - means one record per object, containing data of all tables associated with object domain class according inheritance
	// e.g. class Racebike extends Bike -> tables [ DOM_BIKE, DOM_RACEBIKE ])
	static <S extends SqlDomainObject> Map<Long, SortedMap<String, Object>> retrieveRecordsFor(Connection cn, int limit, Class<S> objectDomainClass, String whereClause) {

		Map<Long, SortedMap<String, Object>> loadedRecordMap = new HashMap<>();

		try {
			// First load main object records

			// Build select description for object records
			SelectDescription sd = buildSelectDescriptionFor(objectDomainClass);

			// SELECT object records
			List<SortedMap<String, Object>> loadedRecords = SqlDomainController.sqlDb.selectFrom(cn, sd.joinedTableExpression, sd.allColumnNames, whereClause, null, limit, null, sd.allFieldTypes);
			if (CList.isEmpty(loadedRecords)) {
				return loadedRecordMap;
			}

			// Build up loaded records by id map
			for (SortedMap<String, Object> rec : loadedRecords) {

				// Check if record is a 'real' (not derived) object record (this should not be necessary)
				String actualObjectDomainClassName = (String) rec.get(Const.DOMAIN_CLASS_COL);
				if (objectsEqual(actualObjectDomainClassName, objectDomainClass.getSimpleName())) {

					long objectId = (long) rec.get(Const.ID_COL);
					loadedRecordMap.put(objectId, rec);
				}
				else {
					log.info("SDC: Loaded record {} for object domain class '{}' is not a 'real' object record - actual object domain class is '{}'", CLog.forAnalyticLogging(rec),
							objectDomainClass.getSimpleName(), actualObjectDomainClassName);
				}
			}

			// Second load entry records for all complex (table related) fields and assign them to main object records using entry table name as key

			String objectTableName = SqlRegistry.getTableFor(objectDomainClass).name;

			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(objectDomainClass)) {

				// For all table related fields...
				for (Field complexField : Registry.getComplexFields(domainClass)) {

					// Build table expression, column names and order by clause to SELECT entry records
					SelectDescription sde = buildSelectDescriptionForEntriesOf(sd.joinedTableExpression, objectTableName, complexField);

					List<SortedMap<String, Object>> loadedEntryRecords = new ArrayList<>();
					if (limit > 0) {

						// If # of loaded object records is limited SELECT only entry records for actually loaded object records
						List<String> idsLists = Helpers.buildMax1000IdsLists(loadedRecordMap.keySet());
						for (String idsList : idsLists) {
							String limitWhereClause = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "") + objectTableName + ".ID IN (" + idsList + ")";
							loadedEntryRecords.addAll(SqlDomainController.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, limitWhereClause, sde.orderByClause, sde.allFieldTypes));
						}
					}
					else {
						// SELECT all entry records
						loadedEntryRecords.addAll(SqlDomainController.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, sde.allFieldTypes));
					}

					// Group selected entry records
					Map<Long, List<SortedMap<String, Object>>> entryRecordsByObjectIdMap = new HashMap<>();
					String refIdColumnName = SqlRegistry.getMainTableRefIdColumnFor(complexField).name;

					for (SortedMap<String, Object> entryRecord : loadedEntryRecords) {

						// Ignore entry record if main object record (referenced by object id) is not present
						long objectId = (long) entryRecord.get(refIdColumnName);
						if (!loadedRecordMap.containsKey(objectId)) {
							log.warn("SDC: Object {}@{} was not loaded before (trying) updating collection or map field '{}'", domainClass.getSimpleName(), objectId, complexField.getName());
							continue;
						}

						// Add entry record to entry records for object
						entryRecordsByObjectIdMap.computeIfAbsent(objectId, m -> new ArrayList<>()).add(entryRecord);
					}

					// Put collection or map defined by entry records to loaded object record with entry table name as key
					String entryTableName = SqlRegistry.getEntryTableFor(complexField).name;
					ParameterizedType genericFieldType = ((ParameterizedType) complexField.getGenericType());

					if (Collection.class.isAssignableFrom(complexField.getType())) { // Collection

						for (long objectId : entryRecordsByObjectIdMap.keySet()) {
							loadedRecordMap.get(objectId).put(entryTableName, ComplexFieldHelpers.entryRecords2Collection(genericFieldType, entryRecordsByObjectIdMap.get(objectId)));
						}
					}
					else { // Map
						for (long objectId : entryRecordsByObjectIdMap.keySet()) {
							loadedRecordMap.get(objectId).put(entryTableName, ComplexFieldHelpers.entryRecords2Map(genericFieldType, entryRecordsByObjectIdMap.get(objectId)));
						}
					}
				}
			}
		}
		catch (SQLException | SqlDbException e) {

			// Method is indirectly used in Java functional interface (as part of select supplier) and therefore may not throw exceptions

			log.error("SDC: {} loading objects of domain class '{}' from database: {}", e.getClass().getSimpleName(), objectDomainClass.getName(), e.getMessage());
		}

		return loadedRecordMap;
	}

	// -------------------------------------------------------------------------
	// INSERT and UPDATE helpers
	// -------------------------------------------------------------------------

	// Collect changed field values (in respect to object record) or all field values if object is still unsaved
	static Map<Class<? extends DomainObject>, Map<Field, Object>> collectFieldChanges(SqlDomainObject object) {

		long id = object.getId();
		Class<? extends SqlDomainObject> cls = object.getClass();

		Map<Class<? extends DomainObject>, Map<Field, Object>> fieldChangesMapByDomainClassMap = new HashMap<>();

		// Try to find object record
		SortedMap<String, Object> objectRecord = SqlDomainController.recordMap.get(cls).get(id);
		if (objectRecord == null) {

			// New object: add { field , field value } entry to changes map for all registered fields (there is no conversion necessary here - field values will be collected as they are)
			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(cls)) {
				for (Field field : Registry.getRegisteredFields(domainClass)) {
					fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(field, object.getFieldValue(field));
				}
			}
		}
		else {
			// Check fields and add { field , field value } entries to changes map if value was changed (conversion is only necessary for check of equality of field and column value)
			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(cls)) {

				// Data fields
				for (Field dataField : Registry.getDataFields(domainClass)) {

					Object fieldValue = object.getFieldValue(dataField);
					Object columnValue = objectRecord.get(SqlRegistry.getColumnFor(dataField).name);

					if (!logicallyEqual(fieldValue, Helpers.column2FieldValue(dataField.getType(), columnValue))) {
						fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(dataField, fieldValue);
					}
				}

				// Reference fields
				for (Field refField : Registry.getReferenceFields(domainClass)) {

					DomainObject refObj = (DomainObject) object.getFieldValue(refField);
					Long refObjIdFromField = (refObj == null ? null : refObj.getId());
					Long refObjIdFromColumn = (Long) objectRecord.get(SqlRegistry.getColumnFor(refField).name);

					if (!objectsEqual(refObjIdFromField, refObjIdFromColumn)) {
						fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(refField, refObj);
					}
				}

				// Element collection and key/value map fields
				for (Field complexField : Registry.getComplexFields(domainClass)) {

					SqlDbTable entryTable = SqlRegistry.getEntryTableFor(complexField);

					if (Collection.class.isAssignableFrom(complexField.getType())) { // Collection

						Collection<?> fieldCollection = (Collection<?>) object.getFieldValue(complexField);
						Collection<?> columnCollection = (Collection<?>) objectRecord.get(entryTable.name);

						if (!objectsEqual(fieldCollection, columnCollection)) {
							fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(complexField, fieldCollection);
						}
					}
					else { // Map
						Map<?, ?> fieldMap = (Map<?, ?>) object.getFieldValue(complexField);
						Map<?, ?> columnMap = (Map<?, ?>) objectRecord.get(entryTable.name);

						if (!objectsEqual(fieldMap, columnMap)) {
							fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(complexField, fieldMap);
						}
					}
				}
			}
		}

		return fieldChangesMapByDomainClassMap;
	}

	// Build column value map for SQL INSERT or UPDATE from field changes map
	static SortedMap<String, Object> fieldChangesMap2ColumnValueMap(Map<Field, Object> fieldChangesMap, SqlDomainObject object) {

		SortedMap<String, Object> columnValueMap = new TreeMap<>();

		if (CMap.isEmpty(fieldChangesMap)) {
			return columnValueMap;
		}

		for (Entry<Field, Object> entry : fieldChangesMap.entrySet()) {

			// Handle only column related fields here
			Field field = entry.getKey();
			if (!Registry.isDataField(field) && !Registry.isReferenceField(field)) {
				continue;
			}

			// Build column/value entry for data or reference field
			Column column = SqlRegistry.getColumnFor(field);
			if (Registry.isReferenceField(field)) { // Reference field

				DomainObject refObj = (DomainObject) entry.getValue();
				columnValueMap.put(column.name, refObj == null ? null : refObj.getId());
			}
			else { // Data field
				Object columnValue = null;
				Object fieldValue = entry.getValue();

				if (fieldValue instanceof String && ((String) fieldValue).length() > column.maxlen) {

					log.warn("SDO: Value '{}' exceeds maximum size {} of column '{}' for object {}! Truncate before saving object...", fieldValue, column.maxlen, column.name, object.name());
					object.setFieldWarning(field, "CONTENT_TRUNCATED_IN_DATABASE");

					columnValue = ((String) fieldValue).substring(0, column.maxlen);
				}
				else {
					columnValue = Helpers.field2ColumnValue(fieldValue); // Convert object field value to appropriate value to set in table column (Enum, BigInteger, BigDecimal, File)
				}

				columnValueMap.put(column.name, columnValue);
			}
		}

		return columnValueMap;
	}

	// DELETE, UPDATE or/and INSERT entry records (reflecting element collection or key/value map fields) and update object record
	static void updateEntryTables(Connection cn, Map<Field, Object> fieldChangesMap, SortedMap<String, Object> objectRecord, SqlDomainObject object) throws SqlDbException, SQLException {

		if (CMap.isEmpty(fieldChangesMap)) {
			return;
		}

		// For all changed complex fields...
		for (Field complexField : fieldChangesMap.keySet()) {

			// Ignore changes of column related fields here
			if (!Registry.isComplexField(complexField)) {
				continue;
			}

			// Get entry table and column referencing domain object
			String entryTableName = SqlRegistry.getEntryTableFor(complexField).name;
			String refIdColumnName = SqlRegistry.getMainTableRefIdColumnFor(complexField).name;

			// Perform database operations
			boolean isMap = Map.class.isAssignableFrom(complexField.getType());
			try {
				if (isMap) {

					// Map field -> get existing map from object record and new map from field
					Map<?, ?> oldMap = (Map<?, ?>) objectRecord.computeIfAbsent(entryTableName, m -> new HashMap<>());
					Map<?, ?> newMap = (Map<?, ?>) fieldChangesMap.get(complexField);

					// Ignore unchanged map
					if (objectsEqual(oldMap, newMap)) {
						continue;
					}

					// Determine map entries which do not exist anymore
					Set<Object> mapKeysToRemove = new HashSet<>();
					boolean hasOldMapNullKey = false;
					for (Entry<?, ?> oldMapEntry : oldMap.entrySet()) {

						if (!newMap.containsKey(oldMapEntry.getKey())) {
							if (oldMapEntry.getKey() == null) {
								hasOldMapNullKey = true;
							}
							else {
								mapKeysToRemove.add(oldMapEntry.getKey());
							}
						}
					}

					// DELETE entry records for non-existing map entries
					if (!mapKeysToRemove.isEmpty()) {
						// TODO: Multiple DELETES with lists of max 1000 elements (Oracle)
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + " IN " + Helpers.buildElementList(mapKeysToRemove));
					}

					if (hasOldMapNullKey) {
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + " IS NULL");
					}

					// Determine new and changes map entries
					Map<Object, Object> mapEntriesToInsert = new HashMap<>();
					Map<Object, Object> mapEntriesToChange = new HashMap<>();
					for (Entry<?, ?> newMapEntry : newMap.entrySet()) {

						if (!oldMap.containsKey(newMapEntry.getKey())) {
							mapEntriesToInsert.put(newMapEntry.getKey(), newMapEntry.getValue());
						}
						else if (!objectsEqual(oldMap.get(newMapEntry.getKey()), newMapEntry.getValue())) {
							mapEntriesToChange.put(newMapEntry.getKey(), newMapEntry.getValue());
						}
					}

					// INSERT new records for new map entries
					if (!mapEntriesToInsert.isEmpty()) {
						SqlDomainController.sqlDb.insertInto(cn, entryTableName, ComplexFieldHelpers.map2EntryRecords(refIdColumnName, object.getId(), mapEntriesToInsert));
					}

					// UPDATE records for changed map entries
					if (!mapEntriesToChange.isEmpty()) {
						for (Object key : mapEntriesToChange.keySet()) {

							SortedMap<String, Object> valueMap = new TreeMap<>();
							valueMap.put(Const.VALUE_COL, ComplexFieldHelpers.element2ColumnValue(mapEntriesToChange.get(key)));

							key = Helpers.field2ColumnValue(key);

							SqlDomainController.sqlDb.update(cn, entryTableName, valueMap,
									refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + "=" + (key instanceof String ? "'" + key + "'" : key));
						}
					}

					// Update object record - store map itself instead of entry records
					objectRecord.put(entryTableName, newMap);
				}
				else if (Set.class.isAssignableFrom(complexField.getType())) {

					// Set field -> get existing set from object record and new set from field
					Set<?> oldSet = (Set<?>) objectRecord.computeIfAbsent(entryTableName, s -> new HashSet<>());
					Set<?> newSet = (Set<?>) fieldChangesMap.get(complexField);

					// Ignore unchanged set
					if (objectsEqual(oldSet, newSet)) {
						continue;
					}

					// Determine elements which do not exist anymore
					Set<Object> elementsToRemove = new HashSet<>();
					boolean hasNullElement = false;
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

					// DELETE entry records for non-existing elements
					if (!elementsToRemove.isEmpty()) {
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND ELEMENT IN " + Helpers.buildElementList(elementsToRemove));
					}
					if (hasNullElement) {
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND ELEMENT IS NULL");
					}

					// Determine new elements (null element is no exception from default handling on insertInto())
					Set<Object> elementsToInsert = new HashSet<>();
					for (Object newElement : newSet) {
						if (!oldSet.contains(newElement)) {
							elementsToInsert.add(newElement);
						}
					}

					// INSERT new records for new elements
					if (!elementsToInsert.isEmpty()) {
						SqlDomainController.sqlDb.insertInto(cn, entryTableName, ComplexFieldHelpers.collection2EntryRecords(refIdColumnName, object.getId(), elementsToInsert));
					}

					// Update object record - store set itself instead of entry records
					objectRecord.put(entryTableName, newSet);
				}
				else { // List
						// List field -> get existing list from object record and new list from field
					List<?> oldList = (List<?>) objectRecord.computeIfAbsent(entryTableName, l -> new ArrayList<>());
					List<?> newList = (List<?>) fieldChangesMap.get(complexField);

					// Ignore unchanged list
					if (objectsEqual(oldList, newList)) {
						continue;
					}

					// DELETE old entry records and INSERT new ones (to simplify operation on list where order-only changes must be reflected)
					SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId());
					SqlDomainController.sqlDb.insertInto(cn, entryTableName, ComplexFieldHelpers.collection2EntryRecords(refIdColumnName, object.getId(), newList));

					// Update object record - store list itself instead of entry records
					objectRecord.put(entryTableName, newList);
				}
			}
			catch (SQLException sqlex) {

				log.error("SDO: Exception on updating entry table '{}' for {} field '{}' of object '{}'", entryTableName, (isMap ? "map" : "collection"), complexField.getName(), object.name());
				object.setFieldError(complexField, "Entries for this " + (isMap ? "map" : "collection") + " field could not be updated in database");

				throw sqlex;
			}
		}
	}

	// -------------------------------------------------------------------------
	// Check unsaved changes
	// -------------------------------------------------------------------------

	// Check if object has unsaved reference changes
	private static void checkUnsavedReferenceChanges(SqlDomainObject obj, Field refField, String foreignKeyColumnName, Long refObjectIdFromDatabase) {

		DomainObject refObj = (DomainObject) obj.getFieldValue(refField);
		Long refObjectIdFromLocalObjectRecord = (Long) SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).get(foreignKeyColumnName);

		if (refObj == null && refObjectIdFromLocalObjectRecord != null) {

			log.warn("SDC: Reference field '{}' of object '{}' has unsaved null reference which will be overridden by reference to '{}@{}' from database!", refField.getName(), obj.name(),
					refField.getType().getSimpleName(), refObjectIdFromDatabase);

			obj.setFieldWarning(refField, "Discarded unsaved changed null reference of field '" + refField.getName() + "' on loading object from database");
		}
		else if (refObj != null && refObj.getId() != refObjectIdFromLocalObjectRecord) {

			if (refObjectIdFromDatabase != null) {
				log.warn("SDC: Referenced field '{}' of object '{}' has unsaved changed reference to '{}@{}' which will be overridden by reference to '{}@{}' from database!", refField.getName(),
						obj.name(), refField.getType().getSimpleName(), refObj.getId(), refField.getType().getSimpleName(), refObjectIdFromDatabase);
			}
			else {
				log.warn("SDC: Referenced field '{}' of object '{}' has unsaved changed reference to '{}@{}' which will be overridden by null reference from database!", refField.getName(), obj.name(),
						refField.getType().getSimpleName(), refObj.getId());
			}

			obj.setFieldWarning(refField, "Discarded unsaved changed reference to " + DomainObject.name(refObj) + " of field '" + refField.getName() + "' on loading object from database");
		}
	}

	// Check if object has unsaved value changes
	private static void checkUnsavedValueChanges(SqlDomainObject obj, Field dataField, String columnName, Object columnValueFromDatabase) {

		Object fieldValue = obj.getFieldValue(dataField);
		Object columnValueFromObjectRecord = SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).get(columnName);
		Object fieldValueFromObjectRecord = Helpers.column2FieldValue(dataField.getType(), columnValueFromObjectRecord);

		if (!objectsEqual(fieldValue, fieldValueFromObjectRecord)) {

			log.warn("SDC: Data field '{}' of object '{}' has unsaved changed value {} which will be overridden by value {} from database!", dataField.getName(), obj.name(),
					CLog.forSecretLogging(dataField.getName(), fieldValue), CLog.forSecretLogging(dataField.getName(), columnValueFromDatabase));

			obj.setFieldWarning(dataField,
					"Discarded unsaved changed value " + CLog.forSecretLogging(dataField.getName(), fieldValue) + " of field '" + dataField.getName() + "' on loading object from database");
		}
	}

	// Check if object has unsaved changes in a collection or map
	private static void checkUnsavedComplexFieldChanges(SqlDomainObject obj, Field complexField, String entryTableName, Object collectionOrMapFromObject, boolean isCollection) {

		if (isCollection) { // Collection

			Collection<?> colFromObjRecord = (Collection<?>) SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);

			if (!objectsEqual(collectionOrMapFromObject, colFromObjRecord)) {

				log.warn("SDC: Collection field '{}' of object '{}' {} has unsaved changes and will be overridden by collection {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, colFromObjRecord);

				obj.setFieldWarning(complexField,
						"Discarded unsaved changed collection " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading collection from database");
			}
		}
		else { // Map
			Map<?, ?> mapFromObjRecord = (Map<?, ?>) SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);

			if (!objectsEqual(collectionOrMapFromObject, mapFromObjRecord)) {

				log.warn("SDC: Key/value map field '{}' of object '{}' {} has unsaved changes and will be overridden by map {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, mapFromObjRecord);

				obj.setFieldWarning(complexField, "Discarded unsaved changed key/value map " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading map from database");
			}

		}
	}

	// -------------------------------------------------------------------------
	// Build objects from loaded records
	// -------------------------------------------------------------------------

	// Unresolved reference description
	static class UnresolvedReference<S extends SqlDomainObject> {

		S obj;
		Field refField;
		Class<S> refDomainClass;
		long refObjectId;

		public UnresolvedReference(
				S obj,
				Field refField,
				long refObjectId) {

			this.obj = obj;
			this.refField = refField;
			this.refDomainClass = Registry.getReferencedDomainClass(refField);
			this.refObjectId = refObjectId;
		}
	}

	// Assign changed data in record from database to corresponding fields of domain object - check for unsaved changes before which will be discarded
	@SuppressWarnings("unchecked")
	private static <S extends SqlDomainObject> void assignDataToObject(S obj, boolean isNew, SortedMap<String, Object> databaseChangesMap, Set<S> objectsWhereReferencesChanged,
			Set<UnresolvedReference<S>> unresolvedReferences) {

		// Changed field predicates
		Predicate<Field> hasValueChangedPredicate = (f -> SqlRegistry.getColumnFor(f) != null && databaseChangesMap.containsKey(SqlRegistry.getColumnFor(f).name));
		Predicate<Field> hasEntriesChangedPredicate = (f -> SqlRegistry.getEntryTableFor(f) != null && databaseChangesMap.containsKey(SqlRegistry.getEntryTableFor(f).name));

		// Assign loaded data to fields for all inherited domain classes of domain object
		for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(obj.getClass())) {

			// Reference fields: assign new references immediately if referenced object exists - otherwise collect unresolved references
			for (Field refField : Registry.getReferenceFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				String foreignKeyColumnName = SqlRegistry.getColumnFor(refField).name;
				Long refObjectIdFromDatabase = (Long) databaseChangesMap.get(foreignKeyColumnName);

				if (!isNew) {
					checkUnsavedReferenceChanges(obj, refField, foreignKeyColumnName, refObjectIdFromDatabase /* only for logging */);
				}

				if (refObjectIdFromDatabase == null) { // Null reference
					obj.setFieldValue(refField, null);
				}
				else { // Object reference
					SqlDomainObject newRefObject = DomainController.find((Class<? extends SqlDomainObject>) refField.getType(), refObjectIdFromDatabase);
					if (newRefObject != null) { // Referenced object is already loaded
						obj.setFieldValue(refField, newRefObject);
					}
					else { // Referenced object is still not loaded (data horizon or circular reference) -> unresolved reference
						obj.setFieldValue(refField, null);
						unresolvedReferences.add(new UnresolvedReference<>(obj, refField, refObjectIdFromDatabase));
					}
				}

				objectsWhereReferencesChanged.add(obj); // Collect objects where references changed for subsequent update of accumulations
			}

			// Data fields: assign - potentially converted - values
			for (Field dataField : Registry.getDataFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				String columnName = SqlRegistry.getColumnFor(dataField).name;
				Object columnValueFromDatabase = databaseChangesMap.get(columnName);
				Object fieldValueFromDatabase = Helpers.column2FieldValue(dataField.getType(), columnValueFromDatabase);

				if (!isNew) {
					checkUnsavedValueChanges(obj, dataField, columnName, columnValueFromDatabase /* only for logging */);
				}

				obj.setFieldValue(dataField, fieldValueFromDatabase);
			}

			// Table related fields: convert entry records to collections or maps and set appropriate field values of object
			for (Field complexField : Registry.getComplexFields(domainClass).stream().filter(hasEntriesChangedPredicate).collect(Collectors.toList())) {

				String entryTableName = SqlRegistry.getEntryTableFor(complexField).name;
				boolean isCollection = Collection.class.isAssignableFrom(complexField.getType());
				Object collectionOrMapFromFieldValue = obj.getFieldValue(complexField);

				if (!isNew) {
					checkUnsavedComplexFieldChanges(obj, complexField, entryTableName, collectionOrMapFromFieldValue, isCollection);
				}

				if (isCollection) { // Collection

					Collection<Object> colFromDatabase = (Collection<Object>) databaseChangesMap.get(entryTableName);
					Collection<Object> colFromObject = (Collection<Object>) collectionOrMapFromFieldValue;

					colFromObject.clear();
					colFromObject.addAll(colFromDatabase);
				}
				else { // Map
					Map<Object, Object> mapFromDatabase = (Map<Object, Object>) databaseChangesMap.get(entryTableName);
					Map<Object, Object> mapFromObject = (Map<Object, Object>) collectionOrMapFromFieldValue;

					mapFromObject.clear();
					mapFromObject.putAll(mapFromDatabase);
				}
			}
		}

		// Reflect database changes in local object record - do this not until all checks for unsaved changes were done
		if (isNew) {
			SqlDomainController.recordMap.get(obj.getClass()).put(obj.getId(), databaseChangesMap);
		}
		else {
			SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).putAll(databaseChangesMap);
		}
	}

	// Update local object records, instantiate new objects and assign data to all new or changed objects. Collect objects having changed and/or still unresolved references
	static <S extends SqlDomainObject> boolean buildObjectsFromLoadedRecords(Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap, Set<S> loadedObjects,
			Set<S> objectsWhereReferencesChanged, Set<UnresolvedReference<S>> unresolvedReferences) {

		if (!CMap.isEmpty(loadedRecordsMap)) {
			log.info("SDC: Build objects from loaded records...");
		}

		Set<S> newObjects = new HashSet<>();
		Set<S> changedObjects = new HashSet<>();

		// Finalize loaded objects in order of parent/child relationship (to avoid unnecessary unresolved references)
		for (Class<? extends DomainObject> odc : Registry.getRegisteredObjectDomainClasses().stream().filter(loadedRecordsMap::containsKey).collect(Collectors.toList())) {
			@SuppressWarnings("unchecked")
			Class<S> objectDomainClass = (Class<S>) odc;

			// Get column names of table for subsequent logging
			List<String> columnNames = SqlRegistry.getTableFor(objectDomainClass).columns.stream().map(c -> c.name).collect(Collectors.toList());

			// Handle loaded object records: instantiate non-existing objects and assign loaded data to new and changed objects
			for (Entry<Long, SortedMap<String, Object>> entry : loadedRecordsMap.get(odc).entrySet()) {

				// Loaded object record
				long id = entry.getKey();
				SortedMap<String, Object> loadedRecord = entry.getValue();

				// Try to find registered domain object for loaded object record
				S obj = DomainController.find(objectDomainClass, id);
				if (obj == null) { // Object is newly loaded

					// Newly loaded domain object: instantiate, register and initialize object
					obj = DomainController.instantiate(objectDomainClass);
					if (obj == null) {
						log.error("SDC: Object {}@{} could not be instantiated on loading object record from database", objectDomainClass.getName(), id);
					}
					else {
						obj.registerById(id);
						obj.setIsStored();
						obj.lastModifiedInDb = ((LocalDateTime) loadedRecord.get(Const.LAST_MODIFIED_COL));

						// Assign loaded data to corresponding fields of domain object, collect unresolved references and objects where references were changed
						assignDataToObject(obj, true, loadedRecord, objectsWhereReferencesChanged, unresolvedReferences);

						if (log.isTraceEnabled()) {
							log.trace("SDC: Loaded new object '{}': {}", obj.name(), JdbcHelpers.forLoggingSqlResult(loadedRecord, columnNames));
						}

						newObjects.add(obj);
					}
				}
				else { // Object is already registered

					// Get current object record
					SortedMap<String, Object> objectRecord = SqlDomainController.recordMap.get(objectDomainClass).get(id);

					// Collect changes (we assume that differences between object and database values found here can only be caused by changes in database made by another domain controller instance)
					SortedMap<String, Object> databaseChangesMap = new TreeMap<>();
					for (Entry<String, Object> loadedRecordEntry : loadedRecord.entrySet()) {

						String col = loadedRecordEntry.getKey();
						Object value = loadedRecordEntry.getValue();

						// Add column/value entry to changes map if current and loaded values differ - ignore last modified column; consider only logical changes
						if (!objectsEqual(col, Const.LAST_MODIFIED_COL) && !logicallyEqual(value, objectRecord.get(col))) {

							if (log.isDebugEnabled()) {
								log.debug("SDC: Column {}: loaded value {} differs from current value {}", col, CLog.forSecretLogging(col, value), CLog.forSecretLogging(col, objectRecord.get(col)));
							}

							databaseChangesMap.put(col, value);
						}
					}

					// Check if object was changed in database and assign changes in this case
					if (!databaseChangesMap.isEmpty()) {

						log.info("SDC: Loaded record for '{}@{}' differs from current record. New values: {}", objectDomainClass.getSimpleName(), id,
								JdbcHelpers.forLoggingSqlResult(databaseChangesMap, columnNames));

						if (log.isTraceEnabled()) {
							log.info("SDC: Current object record: {}", JdbcHelpers.forLoggingSqlResult(objectRecord, columnNames));
						}

						// Consider changed last modification date if any logical change was detected
						databaseChangesMap.put(Const.LAST_MODIFIED_COL, loadedRecord.get(Const.LAST_MODIFIED_COL));

						// Assign changed data to corresponding fields of domain object, collect unresolved references and objects where references were changed
						assignDataToObject(obj, false, databaseChangesMap, objectsWhereReferencesChanged, unresolvedReferences);

						changedObjects.add(obj);
					}
				}

				loadedObjects.add(obj);
			}
		}

		log.info("SDC: Loaded: #'s of new objects: {}, #'s of changed objects: {}", Helpers.groupCountsByDomainClassName(newObjects), Helpers.groupCountsByDomainClassName(changedObjects));

		return (!changedObjects.isEmpty() || !newObjects.isEmpty()); // true if any changes in database were detected
	}

	// -------------------------------------------------------------------------
	// Unresolved references
	// -------------------------------------------------------------------------

	// Determine object domain class of (missing) object given by id and derived domain class by loading object record
	@SuppressWarnings("unchecked")
	private static <S extends SqlDomainObject> Class<S> determineObjectDomainClassOfMissingObject(Connection cn, Class<S> domainClass, long id) {

		log.info("SDC: Domain class '{}' is not an object domain class -> determine object domain class for missing object(s).", domainClass.getSimpleName());

		// Determine object domain class by retrieving domain class name from selected referenced object record
		try {
			String refTableName = SqlRegistry.getTableFor(domainClass).name;
			List<SortedMap<String, Object>> records = SqlDomainController.sqlDb.selectFrom(cn, refTableName, Const.DOMAIN_CLASS_COL, "ID=" + id, null, CList.newList(String.class));
			if (records.isEmpty()) {
				log.error("No record found for object {}@{} which is referenced by child object and therefore should exist", domainClass.getSimpleName(), id);
			}
			else {
				String objectDomainClassName = (String) records.get(0).get(Const.DOMAIN_CLASS_COL); // Assume JDBC type of column is String for String field
				log.info("SDC: Object domain class for referenced domain class '{}' is: '{}'", domainClass.getSimpleName(), objectDomainClassName);
				return (Class<S>) DomainController.getDomainClassByName(objectDomainClassName);
			}
		}
		catch (SQLException | SqlDbException e) {
			log.error("SDC: Exception determining object domain class for domain class '{}'", domainClass, e);
		}

		return null;
	}

	// Load records of objects which were not loaded initially (because they are out of data horizon) but which are referenced by initially loaded objects
	static <S extends SqlDomainObject> Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadMissingObjectRecords(Connection cn, Set<UnresolvedReference<S>> unresolvedReferences) {

		// Build up map containing id's of all missing objects ordered by domain object classes

		Map<Class<S>, Set<Long>> missingObjectsMap = new HashMap<>();
		for (UnresolvedReference<S> ur : unresolvedReferences) {

			// Use object domain class of referenced object as key - this may be not the referenced domain class itself (e.g.: referenced domain class is Car but object domain class is Sportscar)
			Map<Class<S>, Class<S>> cacheMap = new HashMap<>();
			Class<S> refObjectDomainClass = null;

			if (Registry.isObjectDomainClass(ur.refDomainClass)) {
				refObjectDomainClass = ur.refDomainClass;
			}
			else if (cacheMap.containsKey(ur.refDomainClass)) {
				refObjectDomainClass = cacheMap.get(ur.refDomainClass);
			}
			else {
				refObjectDomainClass = determineObjectDomainClassOfMissingObject(cn, ur.refDomainClass, ur.refObjectId);
				cacheMap.put(ur.refDomainClass, refObjectDomainClass);
			}

			// Collect missing objects by object domain classes - before check if missing object was already loaded (this happens on circular references: at the end all objects were loaded but not all
			// references could be resolved during loading)
			DomainObject obj = DomainController.find(refObjectDomainClass, ur.refObjectId);
			if (obj == null) {
				missingObjectsMap.computeIfAbsent(refObjectDomainClass, c -> new HashSet<>()).add(ur.refObjectId);
			}
			else if (log.isDebugEnabled()) {
				log.debug("SDC: Missing object {} was already loaded after detecting unresolved reference and do not have to be loaded again (circular reference)", obj.name());
			}
		}

		// Load missing object records

		Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedMissingRecordsMap = new HashMap<>();
		for (Entry<Class<S>, Set<Long>> entry : missingObjectsMap.entrySet()) {
			Class<S> objectDomainClass = entry.getKey();

			Set<Long> missingObjectIds = missingObjectsMap.get(objectDomainClass);
			log.info("SDC: Load {} missing '{}' object(s){}", missingObjectIds.size(), objectDomainClass.getSimpleName(), (missingObjectIds.size() <= 32 ? " " + missingObjectIds : "..."));

			// Build WHERE clause(s) with IDs and load missing objects
			Map<Long, SortedMap<String, Object>> collectedRecordMap = new HashMap<>();
			List<String> idsLists = Helpers.buildMax1000IdsLists(entry.getValue());
			for (String idsList : idsLists) {
				collectedRecordMap.putAll(retrieveRecordsFor(cn, 0, objectDomainClass, SqlRegistry.getTableFor(objectDomainClass).name + ".ID IN (" + idsList + ")"));
			}

			loadedMissingRecordsMap.put(objectDomainClass, collectedRecordMap);
		}

		return loadedMissingRecordsMap;
	}

	// Resolve collected unresolved references by now loaded parent object
	static <S extends SqlDomainObject> void resolveUnresolvedReferences(Set<UnresolvedReference<S>> unresolvedReferences) {

		for (UnresolvedReference<S> ur : unresolvedReferences) {

			// Find parent object
			Class<S> referencedDomainClass = Registry.getReferencedDomainClass(ur.refField);
			SqlDomainObject parentObj = DomainController.find(referencedDomainClass, ur.refObjectId);

			if (parentObj == null) {
				log.error("SDC: Referenced object {}@{} for resolving unresolved reference not found (but should exist)", referencedDomainClass.getSimpleName(), ur.refObjectId);
			}
			else {
				if (log.isTraceEnabled()) {
					log.trace("SDC: Resolve reference of '{}' to parent '{}' after deferred loading of parent", DomainObject.name(ur.obj), DomainObject.name(parentObj));
				}

				// Set object's reference to parent object
				ur.obj.setFieldValue(ur.refField, parentObj);
			}
		}

		if (!CCollection.isEmpty(unresolvedReferences)) {
			log.info("SDC: Resolved {} references which remained unresolved during last load cycle.", unresolvedReferences.size());
		}
	}

}
