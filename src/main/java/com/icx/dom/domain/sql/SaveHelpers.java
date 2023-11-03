package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
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

import com.icx.dom.common.CList;
import com.icx.dom.common.CLog;
import com.icx.dom.common.CMap;
import com.icx.dom.common.Common;
import com.icx.dom.common.Reflection;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

public abstract class SaveHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(SaveHelpers.class);

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	// Collect changed fields in respect to object record or all fields if object is still not stored for one of the object's domain classes
	// Note: Collected field values in changes map are NOT converted to column values here
	static Map<Field, Object> getFieldChangesForDomainClass(SqlRegistry sqlRegistry, SqlDomainObject object, SortedMap<String, Object> objectRecord, Class<? extends SqlDomainObject> domainClass) {

		// Try to find object record
		Map<Field, Object> fieldChangesMap = new HashMap<>();
		if (objectRecord == null) {

			// New object: add { field , field value } entry to changes map for all registered fields (there is no conversion necessary here - field values will be collected as they are)
			for (Field field : sqlRegistry.getRegisteredFields(domainClass)) {
				fieldChangesMap.put(field, object.getFieldValue(field));
			}
		}
		else {

			// Data fields
			for (Field dataField : sqlRegistry.getDataFields(domainClass)) {
				Object fieldValue = object.getFieldValue(dataField);
				Object columnValue = objectRecord.get(sqlRegistry.getColumnFor(dataField).name);

				if (!logicallyEqual(fieldValue, Helpers.column2FieldValue(dataField.getType(), columnValue))) {
					fieldChangesMap.put(dataField, fieldValue);
				}
			}

			// Reference fields
			for (Field refField : sqlRegistry.getReferenceFields(domainClass)) {
				SqlDomainObject parentObject = (SqlDomainObject) object.getFieldValue(refField);
				Long refObjIdFromField = (parentObject == null ? null : parentObject.getId());
				Long refObjIdFromColumn = (Long) objectRecord.get(sqlRegistry.getColumnFor(refField).name);

				if (!objectsEqual(refObjIdFromField, refObjIdFromColumn)) {
					fieldChangesMap.put(refField, parentObject);
				}
			}

			// Element collection and key/value map fields
			for (Field complexField : sqlRegistry.getComplexFields(domainClass)) {
				SqlDbTable entryTable = sqlRegistry.getEntryTableFor(complexField);

				if (Collection.class.isAssignableFrom(complexField.getType())) { // Collection
					Collection<?> fieldCollection = (Collection<?>) object.getFieldValue(complexField);
					Collection<?> columnCollection = (Collection<?>) objectRecord.get(entryTable.name);

					if (!objectsEqual(fieldCollection, columnCollection)) {
						fieldChangesMap.put(complexField, fieldCollection);
					}
				}
				else { // Map
					Map<?, ?> fieldMap = (Map<?, ?>) object.getFieldValue(complexField);
					Map<?, ?> columnMap = (Map<?, ?>) objectRecord.get(entryTable.name);

					if (!objectsEqual(fieldMap, columnMap)) {
						fieldChangesMap.put(complexField, fieldMap);
					}
				}
			}
		}

		return fieldChangesMap;
	}

	// Build column value map for SQL INSERT or UPDATE from field changes map - consider only fields where columns are associated with - ignore table related fields
	// Note: Type conversion to column values will be done here
	static SortedMap<String, Object> fieldChangesMap2ColumnValueMap(SqlRegistry sqlRegistry, Map<Field, Object> fieldChangesMap, SqlDomainObject object /* used only on error */) {

		SortedMap<String, Object> columnValueMap = new TreeMap<>();
		if (CMap.isEmpty(fieldChangesMap)) {
			return columnValueMap;
		}

		// Consider only column related fields (no complex, table related fields)
		for (Field field : fieldChangesMap.keySet().stream().filter(f -> Registry.isDataField(f) || sqlRegistry.isReferenceField(f)).collect(Collectors.toList())) {

			// Build column/value entry for data or reference field
			Column column = sqlRegistry.getColumnFor(field);
			if (sqlRegistry.isReferenceField(field)) { // Reference field

				// Assign referenced object's id
				SqlDomainObject parentObject = (SqlDomainObject) fieldChangesMap.get(field);
				columnValueMap.put(column.name, parentObject == null ? null : parentObject.getId());
			}
			else { // Data field
				Object columnValue = null;
				Object fieldValue = fieldChangesMap.get(field);

				// Assign field value
				if (fieldValue instanceof String && ((String) fieldValue).length() > column.maxlen) { // String field exceeds text column size
					log.warn("SDO: Value '{}' exceeds maximum size {} of column '{}' for object {}! Truncate before saving object...", fieldValue, column.maxlen, column.name, object.name());
					object.setFieldWarning(field, "CONTENT_TRUNCATED_IN_DATABASE");
					columnValue = ((String) fieldValue).substring(0, column.maxlen);
				}
				else {
					// Convert field value to appropriate value to set in table column (Enum, BigInteger, BigDecimal, File)
					columnValue = Helpers.field2ColumnValue(fieldValue);
				}
				columnValueMap.put(column.name, columnValue);
			}
		}

		return columnValueMap;
	}

	// DELETE, UPDATE or/and INSERT entry records reflecting table related collection or map fields (complex fields) and update object record - ignore column related fields here
	private static void updateEntryTables(Connection cn, SqlDomainController sdc, Map<Field, Object> fieldChangesMap, SortedMap<String, Object> objectRecord, SqlDomainObject object)
			throws SqlDbException, SQLException {

		if (CMap.isEmpty(fieldChangesMap)) {
			return;
		}

		// Consider complex, table related fields...
		for (Field complexField : fieldChangesMap.keySet().stream().filter(f -> sdc.registry.isComplexField(f)).collect(Collectors.toList())) {
			String entryTableName = ((SqlRegistry) sdc.registry).getEntryTableFor(complexField).name;
			String refIdColumnName = ((SqlRegistry) sdc.registry).getMainTableRefIdColumnFor(complexField).name;

			boolean isMap = Map.class.isAssignableFrom(complexField.getType());
			boolean isList = List.class.isAssignableFrom(complexField.getType());
			boolean isSet = Set.class.isAssignableFrom(complexField.getType());

			Object complexColumnObject = objectRecord.computeIfAbsent(entryTableName, (isMap ? m -> new HashMap<>() : isList ? m -> new ArrayList<>() : isSet ? m -> new HashSet<>() : null));
			Object complexFieldObject = fieldChangesMap.get(complexField);

			// Ignore unchanged complex object
			if (objectsEqual(complexFieldObject, complexColumnObject)) {
				continue;
			}

			// DELETE, UPDATE and/or INSERT entry records for maps, sets and lists
			try {
				if (isMap) {
					Map<?, ?> oldMap = (Map<?, ?>) complexColumnObject;
					Map<?, ?> newMap = (Map<?, ?>) complexFieldObject;

					Set<Object> mapKeysToRemove = new HashSet<>();
					Map<Object, Object> mapEntriesToInsert = new HashMap<>();
					Map<Object, Object> mapEntriesToChange = new HashMap<>();
					boolean hasOldMapNullKey = false;

					// Determine map entries which do not exist anymore
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

					// Determine new and changed map entries
					for (Entry<?, ?> newMapEntry : newMap.entrySet()) {
						if (!oldMap.containsKey(newMapEntry.getKey())) {
							mapEntriesToInsert.put(newMapEntry.getKey(), newMapEntry.getValue());
						}
						else if (!objectsEqual(oldMap.get(newMapEntry.getKey()), newMapEntry.getValue())) {
							mapEntriesToChange.put(newMapEntry.getKey(), newMapEntry.getValue());
						}
					}

					// Delete entry records for removed map entries
					if (!mapKeysToRemove.isEmpty()) {
						// TODO: Multiple DELETES with lists of max 1000 elements (Oracle)
						// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ENTRY_KEY IN <keys of entries to remove>
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + " IN " + Helpers.buildElementList(mapKeysToRemove));
					}

					if (hasOldMapNullKey) {
						// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ENTRY_KEY IS NULL
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + " IS NULL");
					}

					// Insert entry records for new map entries
					if (!mapEntriesToInsert.isEmpty()) {
						// (batch) INSERT INTO <entry table> (<object reference column>, ENTRY_KEY, ENTRY_VALUE) VALUES (<objectid>, <converted key>, <converted value>)
						sdc.sqlDb.insertInto(cn, entryTableName, ComplexFieldHelpers.map2EntryRecords(refIdColumnName, object.getId(), mapEntriesToInsert));
					}

					// Update entry records for changed map entries
					for (Object key : mapEntriesToChange.keySet()) {
						SortedMap<String, Object> updateMap = CMap.newSortedMap(Const.VALUE_COL, ComplexFieldHelpers.element2ColumnValue(mapEntriesToChange.get(key)));
						Object columnKey = Helpers.field2ColumnValue(key);

						// UPDATE <entry table> SET ENTRY_VALUE=<converted entry value> WHERE <object reference column>=<objectid> AND ENTRY_KEY=<converted entry key>
						sdc.sqlDb.update(cn, entryTableName, updateMap,
								refIdColumnName + "=" + object.getId() + " AND " + Const.KEY_COL + "=" + (columnKey instanceof String ? "'" + columnKey + "'" : columnKey));
					}
				}
				else if (isSet) {
					Set<?> oldSet = (Set<?>) complexColumnObject;
					Set<?> newSet = (Set<?>) complexFieldObject;

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
						// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ELEMENT IN <elements to remove>
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND ELEMENT IN " + Helpers.buildElementList(elementsToRemove));
					}

					if (hasNullElement) {
						// DELETE FROM <entry table> WHERE <object reference column>=<objectid> AND ELEMENT IS NULL
						SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId() + " AND ELEMENT IS NULL");
					}

					// Insert entry records for new elements
					if (!elementsToInsert.isEmpty()) {
						// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT) VALUES (<objectid>, <converted set element>)
						sdc.sqlDb.insertInto(cn, entryTableName, ComplexFieldHelpers.collection2EntryRecords(refIdColumnName, object.getId(), elementsToInsert));
					}
				}
				else if (isList) { // List
					// DELETE FROM <entry table> WHERE <object reference column>=<objectid>
					SqlDb.deleteFrom(cn, entryTableName, refIdColumnName + "=" + object.getId());
					// (batch) INSERT INTO <entry table> (<object reference column>, ELEMENT, ELEMENT_ORDER) VALUES (<objectid>, <converted list element>, <order>)
					sdc.sqlDb.insertInto(cn, entryTableName, ComplexFieldHelpers.collection2EntryRecords(refIdColumnName, object.getId(), (List<?>) complexFieldObject));
				}
				else {
					log.error("SDO: Value of field '{}' is of unsupported type '{}'!", complexField.getName(), complexField.getType().getName());
				}

				// Update object record by new complex object
				objectRecord.put(entryTableName, complexFieldObject);
			}
			catch (SQLException sqlex) {
				log.error("SDO: Exception on updating entry table '{}' for {} field '{}' of object '{}'", entryTableName, (isMap ? "map" : "collection"), complexField.getName(), object.name());
				object.setFieldError(complexField, "Entries for this " + (isMap ? "map" : "collection") + " field could not be updated in database");
				throw sqlex;
			}
		}
	}

	// Store referenced (parent) objects which are not already stored in database before saving this (child) object or reset references to parent objects temporarily and collect these objects here
	// to store them after saving this object. This is necessary to avoid database exception on INSERT records with non-existing references in foreign key columns.
	// Note: If reference is nullable, parent object may also reference this object directly or indirectly (circular reference). In this case an infinite loop would occur on saving parent object
	// before saving object. Therefore nullable references will temporarily reset to allow saving child object, and parent objects will be saved after saving child object with restoring references.
	private static Map<Field, SqlDomainObject> storeOrCollectUnstoredParentObjects(Connection cn, SqlDomainController sdc, SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass,
			List<SqlDomainObject> objectsToCheckForCircularReference) throws SQLException, SqlDbException {

		// For all reference fields of (derived) domain class...
		int stackSize = objectsToCheckForCircularReference.size();
		Map<Field, SqlDomainObject> unstoredParentObjectMap = new HashMap<>();
		for (Field refField : sdc.registry.getReferenceFields(domainClass)) {

			// Get parent object, do nothing if reference is null or parent object is already stored
			SqlDomainObject parentObject = (SqlDomainObject) obj.getFieldValue(refField);
			if (parentObject == null || parentObject.isStored) {
				continue;
			}

			// Re-register meanwhile unregistered parent object
			if (!sdc.isRegistered(parentObject)) { // Parent object was unregistered before saving this object (during synchronization due to data horizon condition)
				log.warn("SDO: {}Parent {} is not registered anymore (probably out of data horizon) - reregister object", CLog.tabs(stackSize), DomainObject.name(parentObject));
				sdc.reregister(parentObject);
			}

			// Decide if parent object is to store after saving this object (NULLable reference) or if it can be saved now (NOT NULLable reference)
			if (((SqlRegistry) sdc.registry).getColumnFor(refField).isNullable) {

				// Collect parent object for subsequent storing and reset reference to allow saving this object
				unstoredParentObjectMap.put(refField, parentObject);
				obj.setFieldValue(refField, null);
				log.info("SDO: {}Collected unstored parent object {} for subsequent storing", CLog.tabs(stackSize), DomainObject.name(parentObject));
			}
			else {
				// Save parent object before saving object
				log.info("SDO: {}Store parent object", CLog.tabs(stackSize));
				try {
					save(cn, sdc, parentObject, objectsToCheckForCircularReference);
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}INSERT failed! Object {} cannot be saved because parent object {} could not be stored!", CLog.tabs(stackSize), obj.name(), DomainObject.name(parentObject));
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());
					obj.setFieldError(refField, "REFERENCED_OBJECT_COULD_NOT_BE_STORED");

					// Restore references which were reset before save error occurred
					for (Field rf : unstoredParentObjectMap.keySet()) {
						obj.setFieldValue(rf, unstoredParentObjectMap.get(rf)); // Use setter without throw clause because we know the values here
					}

					throw sqlex; // Throw exception to force ROLL BACK of complete transaction
				}
			}
		}

		return unstoredParentObjectMap;
	}

	// Store collected parent (parent) objects (on nullable references) after saving this (child) object and restore (UPDATE) references in database
	private static SortedMap<String, Object> storeCollectedParentObjectsAndRestoreReferences(Connection cn, SqlDomainController sdc, SqlDomainObject obj,
			Map<Field, SqlDomainObject> collectedParentObjectMap, List<SqlDomainObject> objectsToCheckForCircularReference) throws SqlDbException {

		int stackSize = objectsToCheckForCircularReference.size();
		SortedMap<String, Object> restoredReferencesCVMap = new TreeMap<>();

		// For all parent objects which could not be stored before saving this object...
		for (Entry<Field, SqlDomainObject> entry : collectedParentObjectMap.entrySet()) {
			Field refField = entry.getKey();
			SqlDomainObject parentObject = entry.getValue();

			// Ignore if meanwhile unregistered
			if (!sdc.isRegistered(parentObject)) {// Parent object was deleted during saving this object
				log.warn("SDO: {}Parent {} is not registered anymore!", CLog.tabs(stackSize), DomainObject.name(parentObject));
				continue;
			}

			// Store parent object if not done meanwhile
			if (!parentObject.isStored) {
				log.info("SDO: {}Store parent object {} after saving this object {}", CLog.tabs(stackSize), DomainObject.name(parentObject), obj.name());
				try {
					save(cn, sdc, parentObject, objectsToCheckForCircularReference);
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}INSERT failed! Unsaved parent object {} of object to save {} cannot not be saved!", CLog.tabs(stackSize), DomainObject.name(parentObject), obj.name());
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());
					obj.setFieldError(refField, "REFERENCED_OBJECT_COULD_NOT_BE_SAVED");
				}
			}

			// If storing was successful re-establish reference to parent object and UPDATE reference in database (reference was reset before to allow saving child object)
			if (parentObject.isStored) {
				obj.setFieldValue(refField, parentObject); // Use setter without throw clause because we know the values here
				SortedMap<String, Object> columnValueMapForOneReference = CMap.newSortedMap(((SqlRegistry) sdc.registry).getColumnFor(refField).name, parentObject.getId());
				try {
					String referencingTableName = ((SqlRegistry) sdc.registry).getTableFor(sdc.registry.getCastedDeclaringDomainClass(refField)).name;
					// UPDATE <referencing table> SET <foreign key column>=<refrenced objectid> WHERE ID=<object id>
					sdc.sqlDb.update(cn, referencingTableName, columnValueMapForOneReference, Const.ID_COL + "=" + obj.getId());
					restoredReferencesCVMap.putAll(columnValueMapForOneReference); // Store change to subsequently update object record
					log.info("SDO: {}Restored reference '{}' to {} for {}", CLog.tabs(stackSize), refField.getName(), DomainObject.name(parentObject), obj.name());
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}UPDATE failed! Exception on restoring reference field '{}' for object {}", CLog.tabs(stackSize), Reflection.qualifiedName(refField), obj.name());
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());
					obj.currentException = sqlex;
					obj.setFieldError(refField, sqlex.getMessage());
				}
			}
		}

		return restoredReferencesCVMap;
	}

	// Try to UPDATE every field of domain object separately after 'normal' UPDATE containing all changed fields failed
	private static void tryAnalyticUpdate(Connection cn, SqlDomainController sdc, SqlDomainObject obj, SqlDbTable table, SortedMap<String, Object> columnValueMap) throws SqlDbException {

		SortedMap<String, Object> oneCVMap = new TreeMap<>();
		obj.currentException = null;

		Iterator<String> it = columnValueMap.keySet().iterator();
		while (it.hasNext()) {
			String columnName = it.next();
			Object columnValue = columnValueMap.get(columnName);

			oneCVMap.clear();
			oneCVMap.put(columnName, columnValue);
			try {
				// Update one column
				// UPDATE <table> SET <column>=<column value> WHERE ID=<objectid>
				sdc.sqlDb.update(cn, table.name, oneCVMap, Const.ID_COL + "=" + obj.getId());
			}
			catch (SQLException sqlex) {
				log.error("SDO: UPDATE failed by exception! Column '{}' cannot be updated to {} for object {}", columnName, CLog.forAnalyticLogging(oneCVMap.get(columnName)), obj.name());
				log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());
				obj.currentException = sqlex;
				Field field = ((SqlRegistry) sdc.registry).getFieldFor(table.findColumnByName(columnName));
				if (field != null) {
					obj.setFieldError(field, "CANNOT_UPDATE_COLUMN - " + sqlex.getMessage());
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Save object to database
	// -------------------------------------------------------------------------

	// Save object in one transaction to database
	static synchronized void save(Connection cn, SqlDomainController sdc, SqlDomainObject obj, List<SqlDomainObject> objectsToCheckForCircularReference) throws SQLException, SqlDbException {

		// Check if object was already stored within current (recursive) save operation
		int stackSize = objectsToCheckForCircularReference.size();
		if (objectsToCheckForCircularReference.contains(obj)) {
			log.info("SDO: {}Object {} was already stored or tried to store within this recursive save operation (detected circular reference)", CLog.tabs(stackSize), obj.name());
			return;
		}
		else {
			objectsToCheckForCircularReference.add(obj);
		}

		if (log.isDebugEnabled()) {
			log.debug("SDO: {}Save{} object {}", CLog.tabs(stackSize), (obj.isStored ? "" : " new"), obj.name());
		}

		// Update accumulations for pending reference changes (only for convenience here) and reset object's exception and field errors/warnings which will be detected if saving fails
		sdc.updateAccumulationsOfParentObjects(obj);
		obj.clearErrors();

		// Get domain classes of object and create or retrieve object record
		List<Class<? extends SqlDomainObject>> domainClasses = sdc.registry.getDomainClassesFor(obj.getClass()); // INSERT from bottom to top level domain class (foreign keys for inheritance)
		SortedMap<String, Object> objectRecord = null;
		if (!obj.isStored) { // New object
			obj.lastModifiedInDb = LocalDateTime.now();
			objectRecord = new TreeMap<>(); // Create object record
			sdc.recordMap.get(obj.getClass()).put(obj.getId(), objectRecord);
		}
		else { // Existing object
			domainClasses = CList.reverse(domainClasses); // UPDATE from top to bottom to allow check if 'last modified' field of base class has to be changed
			objectRecord = sdc.recordMap.get(obj.getClass()).get(obj.getId()); // Existing object record
		}

		// INSERT or UPDATE records in tables associated with domain classes of object
		Map<Field, SqlDomainObject> collectedParentObjectMap = new HashMap<>();
		boolean wasChanged = false;
		for (Class<? extends SqlDomainObject> domainClass : domainClasses) {
			SqlDbTable table = ((SqlRegistry) sdc.registry).getTableFor(domainClass);

			// Save un-stored parent objects (on non nullable reference) or reset parent/child reference and collect parent objects here to save them after saving this object (on nullable reference)
			collectedParentObjectMap.putAll(storeOrCollectUnstoredParentObjects(cn, sdc, obj, domainClass, objectsToCheckForCircularReference));

			// Get field changes for domain class and build column value map for INSERT or UPDATE
			Map<Field, Object> fieldChangesMap = getFieldChangesForDomainClass(((SqlRegistry) sdc.registry), obj, objectRecord, domainClass);
			SortedMap<String, Object> columnValueMap = fieldChangesMap2ColumnValueMap(((SqlRegistry) sdc.registry), fieldChangesMap, obj);

			if (!obj.isStored) { // INSERT

				// Add standard columns
				columnValueMap.put(Const.ID_COL, obj.getId());
				columnValueMap.put(Const.DOMAIN_CLASS_COL, obj.getClass().getSimpleName()); // to identify object domain class also from records of tables related to non-domain object classes
				if (sdc.registry.isBaseDomainClass(domainClass)) {
					columnValueMap.put(Const.LAST_MODIFIED_COL, obj.lastModifiedInDb);
				}

				try {
					// Insert record into table associated to this domain class
					// Note: During INSERT values in column/value map will potentially be converted to JDBC specific type, so that data types after INSERT and after SELECT are the same
					// INSERT INTO <table of current object class> (columns) VALUES (column values)
					sdc.sqlDb.insertInto(cn, table.name, columnValueMap);
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}INSERT failed by exception! Object {} cannot be saved in table '{}'", CLog.tabs(stackSize), obj.name(), table.name);
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

					// Set exception and field errors on constraint violation(s) (check violations in ascending order of severity to have the most critical ones assigned to field(s)
					obj.currentException = sqlex;
					sdc.hasColumnSizeViolations(obj, domainClass);
					sdc.hasUniqueConstraintViolations(obj, domainClass);
					sdc.hasNotNullConstraintViolations(obj, domainClass);

					// Restore parent/child references which were reset before trying to save object
					for (Field refField : collectedParentObjectMap.keySet()) {
						obj.setFieldValue(refField, collectedParentObjectMap.get(refField));
					}

					// ROLL BACK whole transaction
					cn.rollback();
					log.warn("SDO: Whole save transaction rolled back!");
					throw sqlex;
				}
			}
			else { // UPDATE

				// Check if any changes exists in any of the domain classes
				if (!columnValueMap.isEmpty()) {
					wasChanged = true;
				}

				// Update 'last modified' field (of bottom domain class) if any changes where detected
				if (sdc.registry.isBaseDomainClass(domainClass) && wasChanged) {
					obj.lastModifiedInDb = LocalDateTime.now();
					columnValueMap.put(Const.LAST_MODIFIED_COL, obj.lastModifiedInDb);
				}

				// UPDATE record in table associated to this domain class if there are changes for current domain class
				if (!columnValueMap.isEmpty()) {
					try {
						// UPDATE <table> SET <names of changed columns>=<converted field values> WHERE ID=<objectid>
						long count = sdc.sqlDb.update(cn, table.name, columnValueMap, Const.ID_COL + "=" + obj.getId());
						if (count == 0) {
							throw new SqlDbException("Object '" + obj + "' could not be saved because record for this object does not exist in table '" + table.name + "'");
						}
					}
					catch (SQLException sqlex) {
						log.error("SDO: {}UPDATE failed by {}! Not all changed fields can be saved for object {}!\nTry to update columns separetely...", CLog.tabs(stackSize),
								sqlex.getClass().getSimpleName(), obj.name());
						log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

						// On error try to UPDATE columns separately - so all but one (or more) columns can be updated
						tryAnalyticUpdate(cn, sdc, obj, table, columnValueMap);
					}
				}
			}

			// Update local object record
			objectRecord.putAll(columnValueMap);

			// Handle table related fields (collections and maps): Delete old entry records, update changed map entries and insert new entries
			updateEntryTables(cn, sdc, fieldChangesMap, objectRecord, obj);
		}

		// Mark new object as stored - do this not until all records have been inserted but before saving parent objects which are not yet stored
		if (!obj.isStored) {
			obj.isStored = true;
		}

		// Save collected parent objects (on nullable foreign key columns) after saving this object and restore (UPDATE) references in database
		if (!collectedParentObjectMap.isEmpty()) {
			objectRecord.putAll(storeCollectedParentObjectsAndRestoreReferences(cn, sdc, obj, collectedParentObjectMap, objectsToCheckForCircularReference));
		}
		if (log.isDebugEnabled()) {
			log.debug("SDO: {}Object {} saved", CLog.tabs(stackSize), obj.name());
		}
	}
}
