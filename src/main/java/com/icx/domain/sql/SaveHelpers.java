package com.icx.domain.sql;

import java.io.File;
import java.lang.reflect.Array;
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
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CList;
import com.icx.common.CLog;
import com.icx.common.CMap;
import com.icx.common.CReflection;
import com.icx.common.Common;
import com.icx.domain.DomainObject;
import com.icx.domain.Registry;
import com.icx.jdbc.SqlDbException;
import com.icx.jdbc.SqlDbTable;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;

/**
 * Helpers for saving domain objects to database
 * 
 * @author baumgrai
 */
public abstract class SaveHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(SaveHelpers.class);

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	// Collect changed fields in respect to object record or all fields if object is still not stored for one of the object's domain classes
	// Note: Collected field values in changes map are NOT converted to column values here
	static Map<Field, Object> getFieldChangesForDomainClass(SqlDomainController sdc, SqlDomainObject object, SortedMap<String, Object> objectRecord, Class<? extends SqlDomainObject> domainClass) {

		SqlRegistry sqlRegistry = sdc.getSqlRegistry();

		// Try to find object record
		Map<Field, Object> fieldChangesMap = new HashMap<>();
		if (CMap.isEmpty(objectRecord)) {

			// New object: add { field , field value } entry to changes map for all data and reference fields (there is no conversion necessary here - field values will be collected as they are)
			for (Field field : sqlRegistry.getDataAndReferenceFields(domainClass)) {
				fieldChangesMap.put(field, object.getFieldValue(field));
			}

			// Add { field, complex field value } entry to changes map for all complex fields where any entry exists in array, collection or map
			for (Field complexField : sqlRegistry.getComplexFields(domainClass)) {

				if (complexField.getType().isArray()) { // Array
					Object fieldArray = object.getFieldValue(complexField);
					if (!logicallyEqual(fieldArray, null)) {
						fieldChangesMap.put(complexField, fieldArray);
					}
				}
				else if (Collection.class.isAssignableFrom(complexField.getType())) { // Collection
					Collection<?> fieldCollection = (Collection<?>) object.getFieldValue(complexField);
					if (!logicallyEqual(fieldCollection, null)) {
						fieldChangesMap.put(complexField, fieldCollection);
					}
				}
				else { // Map
					Map<?, ?> fieldMap = (Map<?, ?>) object.getFieldValue(complexField);
					if (!logicallyEqual(fieldMap, null)) {
						fieldChangesMap.put(complexField, fieldMap);
					}
				}
			}
		}
		else {
			// Data fields
			for (Field dataField : sqlRegistry.getDataFields(domainClass)) {
				Object fieldValue = object.getFieldValue(dataField);
				Object columnValue = objectRecord.get(sqlRegistry.getColumnFor(dataField).name);

				if (!objectsEqual(fieldValue, columnValue)) {
					fieldChangesMap.put(dataField, fieldValue);
				}
			}

			// Reference fields
			for (Field refField : sqlRegistry.getReferenceFields(domainClass)) {

				SqlDomainObject parentObject = (SqlDomainObject) object.getFieldValue(refField);
				Long refObjIdFromField = (parentObject != null ? parentObject.getId() : null);
				Number refObjIdFromColumnNumber = (Number) objectRecord.get(sqlRegistry.getColumnFor(refField).name);
				Long refObjIdFromColumn = (refObjIdFromColumnNumber != null ? refObjIdFromColumnNumber.longValue() : null);

				if (!objectsEqual(refObjIdFromField, refObjIdFromColumn)) {
					fieldChangesMap.put(refField, parentObject);
				}
			}

			// Element collection and key/value map fields
			for (Field complexField : sqlRegistry.getComplexFields(domainClass)) {
				SqlDbTable entryTable = sqlRegistry.getEntryTableFor(complexField);

				if (complexField.getType().isArray()) { // Array
					Object fieldArray = object.getFieldValue(complexField);
					Object columnArray = objectRecord.get(entryTable.name);

					if (!logicallyEqual(fieldArray, columnArray)) {
						fieldChangesMap.put(complexField, fieldArray);
					}
				}
				else if (Collection.class.isAssignableFrom(complexField.getType())) { // Collection
					Collection<?> fieldCollection = (Collection<?>) object.getFieldValue(complexField);
					Collection<?> columnCollection = (Collection<?>) objectRecord.get(entryTable.name);

					if (!logicallyEqual(fieldCollection, columnCollection)) {
						fieldChangesMap.put(complexField, fieldCollection);
					}
				}
				else { // Map
					Map<?, ?> fieldMap = (Map<?, ?>) object.getFieldValue(complexField);
					Map<?, ?> columnMap = (Map<?, ?>) objectRecord.get(entryTable.name);

					if (!logicallyEqual(fieldMap, columnMap)) {
						fieldChangesMap.put(complexField, fieldMap);
					}
				}
			}
		}

		return fieldChangesMap;
	}

	// Build column value map for SQL INSERT or UPDATE from field changes map - consider only fields where columns are associated with - ignore table related fields
	// Note: Type conversion to column values will be done here
	static SortedMap<String, Object> fieldChangesMap2ColumnValueMap(SqlDomainController sdc, Map<Field, Object> fieldChangesMap, SqlDomainObject obj /* used only on error */) {

		SortedMap<String, Object> columnValueMap = new TreeMap<>();
		if (CMap.isEmpty(fieldChangesMap)) {
			return columnValueMap;
		}

		SqlRegistry sqlRegistry = (SqlRegistry) sdc.getRegistry();

		// Consider only column related fields (no complex, table related fields)
		for (Field field : fieldChangesMap.keySet().stream().filter(f -> Registry.isDataField(f) || sqlRegistry.isReferenceField(f)).collect(Collectors.toList())) {

			// Build column/value entry for data or reference field
			SqlDbColumn column = sqlRegistry.getColumnFor(field);
			if (sqlRegistry.isReferenceField(field)) { // Reference field

				// Assign referenced object's id
				SqlDomainObject parentObject = (SqlDomainObject) fieldChangesMap.get(field);
				columnValueMap.put(column.name, parentObject == null ? null : parentObject.getId());
			}
			else { // Data field
				Object columnValue = null;
				Object fieldValue = fieldChangesMap.get(field);

				// Assign field value
				if (fieldValue instanceof String && ((String) fieldValue).length() > column.maxlen) { // Truncate if string field exceeds text column size

					log.warn("SDC: Value '{}' exceeds maximum size {} of column '{}' for object {}! Truncate before saving object...", CLog.forSecretLogging(field, fieldValue), column.maxlen,
							column.name, obj.name());

					obj.setFieldWarning(field, "CONTENT_TRUNCATED_IN_DATABASE");
					columnValue = ((String) fieldValue).substring(0, column.maxlen);
				}
				else if (fieldValue instanceof File) {
					columnValue = Helpers.buildFileByteEntry((File) fieldValue, column.name);
				}
				else {
					columnValue = fieldValue;
				}

				columnValueMap.put(column.name, columnValue);
			}
		}

		return columnValueMap;
	}

	// DELETE, UPDATE or/and INSERT entry records reflecting table related collection or map fields (complex fields) and update object record - ignore column related fields here
	private static void updateEntryTable(Connection cn, SqlDomainController sdc, Field complexField, Object newComplexValue, SortedMap<String, Object> objectRecord, SqlDomainObject object)
			throws SqlDbException, SQLException {

		// Consider complex, table related fields...
		String entryTableName = sdc.getSqlRegistry().getEntryTableFor(complexField).name;
		String refIdColumnName = sdc.getSqlRegistry().getMainTableRefIdColumnFor(complexField).name;
		boolean isSorted = false;

		// DELETE, UPDATE and/or INSERT entry records for maps, sets, lists and arrays
		try {
			if (Map.class.isAssignableFrom(complexField.getType())) {
				isSorted = SortedMap.class.isAssignableFrom(complexField.getType());

				// DELETE, INSERT and/or UPDATE entry records representing map on changes in map
				Map<?, ?> oldMap = (Map<?, ?>) objectRecord.computeIfAbsent(entryTableName, m -> new HashMap<>());
				Map<?, ?> newMap = (Map<?, ?>) newComplexValue;
				ComplexFieldHelpers.updateEntriesForMap(oldMap, newMap, sdc, cn, entryTableName, refIdColumnName, object);

				// Update object record by new map
				objectRecord.put(entryTableName, isSorted ? new TreeMap<>(newMap) : new HashMap<>(newMap));
			}
			else if (Set.class.isAssignableFrom(complexField.getType())) {
				isSorted = SortedSet.class.isAssignableFrom(complexField.getType());

				// DELETE and/or UPDATE entry records representing set on changes in set
				Set<?> oldSet = (Set<?>) objectRecord.computeIfAbsent(entryTableName, m -> new HashSet<>());
				Set<?> newSet = (Set<?>) newComplexValue;
				ComplexFieldHelpers.updateEntriesForSet(oldSet, newSet, sdc, cn, entryTableName, refIdColumnName, object);

				// Update object record by new set
				objectRecord.put(entryTableName, isSorted ? new TreeSet<>(newSet) : new HashSet<>(newSet));
			}
			else if (List.class.isAssignableFrom(complexField.getType())) {

				// DELETE, INSERT and UPDATE entry records representing list on changes in list
				List<?> oldList = (List<?>) objectRecord.computeIfAbsent(entryTableName, m -> new ArrayList<>());
				List<?> newList = (List<?>) newComplexValue;
				ComplexFieldHelpers.updateEntriesForList(oldList, newList, sdc, cn, entryTableName, refIdColumnName, object.getId(), complexField);

				// Update object record by new list
				objectRecord.put(entryTableName, new ArrayList<>(newList));
			}
			else if (complexField.getType().isArray()) {

				// DELETE, INSERT and/or UPDATE entry records representing array on changes of array
				Object newArray = newComplexValue;
				int length = ComplexFieldHelpers.updateEntriesForArray(newArray, sdc, cn, entryTableName, refIdColumnName, object.getId());

				// Update object record by new array
				Object recordArray = Array.newInstance(complexField.getType().getComponentType(), length);
				System.arraycopy(newArray, 0, recordArray, 0, length);
				objectRecord.put(entryTableName, recordArray);
			}
			else {
				log.error("SDC: Value of field '{}' is of unsupported type '{}'!", complexField.getName(), complexField.getType().getName());
			}
		}
		catch (SQLException sqlex) {
			boolean isMap = Map.class.isAssignableFrom(complexField.getType());
			log.error("SDC: Exception on updating entry table '{}' for {} field '{}' of object '{}'", entryTableName, (isMap ? "map" : "collection"), complexField.getName(), object.name());
			object.setFieldError(complexField, "Entries for this " + (isMap ? "map" : "collection") + " field could not be updated in database");
			throw sqlex;
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
		for (Field refField : sdc.getRegistry().getReferenceFields(domainClass)) {

			// Get parent object, do nothing if reference is null or parent object is already stored
			SqlDomainObject parentObject = (SqlDomainObject) obj.getFieldValue(refField);
			if (parentObject == null || parentObject.isStored) {
				continue;
			}

			// Re-register meanwhile unregistered parent object
			if (!sdc.isRegistered(parentObject) && parentObject.getId() > 0) { // Parent object was unregistered before saving this object (during synchronization due to data horizon condition)
				log.warn("SDC: {}Parent {} is not registered anymore (probably out of data horizon) - reregister object", CLog.tabs(stackSize), DomainObject.name(parentObject));
				sdc.reregister(parentObject);
			}

			// Decide if parent object is to store after saving this object (NULLable reference) or if it can be saved now (NOT NULLable reference)
			if (sdc.getSqlRegistry().getColumnFor(refField).isNullable) {

				// Collect parent object for subsequent storing and reset reference to allow saving this object
				unstoredParentObjectMap.put(refField, parentObject);
				obj.setFieldValue(refField, null);
				if (log.isDebugEnabled()) {
					log.debug("SDC: {}Collected unstored parent object {} for subsequent storing", CLog.tabs(stackSize), DomainObject.name(parentObject));
				}
			}
			else {
				// Save parent object before saving object
				if (log.isDebugEnabled()) {
					log.debug("SDC: {}Store parent object", CLog.tabs(stackSize));
				}
				try {
					save(cn, sdc, parentObject, objectsToCheckForCircularReference);
				}
				catch (SQLException sqlex) {
					log.error("SDC: {}INSERT failed! Object {} cannot be saved because parent object {} could not be stored!", CLog.tabs(stackSize), obj.name(), DomainObject.name(parentObject));
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
			if (!sdc.isRegistered(parentObject) && parentObject.getId() > 0) {// Parent object was deleted during saving this object
				log.warn("SDC: {}Parent {} is not registered anymore!", CLog.tabs(stackSize), DomainObject.name(parentObject));
				continue;
			}

			// Store parent object if not done meanwhile
			if (!parentObject.isStored) {
				if (log.isDebugEnabled()) {
					log.debug("SDC: {}Store parent object {} after saving this object {}", CLog.tabs(stackSize), DomainObject.name(parentObject), obj.name());
				}
				try {
					save(cn, sdc, parentObject, objectsToCheckForCircularReference);
				}
				catch (SQLException sqlex) {
					log.error("SDC: {}INSERT failed! Unsaved parent object {} of object to save {} cannot not be saved!", CLog.tabs(stackSize), DomainObject.name(parentObject), obj.name());
					obj.setFieldError(refField, "REFERENCED_OBJECT_COULD_NOT_BE_SAVED");
				}
			}

			// If storing was successful re-establish reference to parent object and UPDATE reference in database (reference was reset before to allow saving child object)
			if (parentObject.isStored) {
				obj.setFieldValue(refField, parentObject); // Use setter without throw clause because we know the values here
				SortedMap<String, Object> columnValueMapForOneReference = CMap.newSortedMap(sdc.getSqlRegistry().getColumnFor(refField).name, parentObject.getId());
				try {
					String referencingTableName = sdc.getSqlRegistry().getTableFor(sdc.getRegistry().getCastedDeclaringDomainClass(refField)).name;
					// UPDATE <referencing table> SET <foreign key column>=<refrenced objectid> WHERE ID=<object id>
					sdc.sqlDb.update(cn, referencingTableName, columnValueMapForOneReference, Const.ID_COL + "=" + obj.getId());
					restoredReferencesCVMap.putAll(columnValueMapForOneReference); // Store change to subsequently update object record
					if (log.isTraceEnabled()) {
						log.trace("SDC: {}Restored reference '{}' to {} for {}", CLog.tabs(stackSize), refField.getName(), DomainObject.name(parentObject), obj.name());
					}
				}
				catch (SQLException sqlex) {
					log.error("SDC: {}UPDATE failed! Exception on restoring reference field '{}' for object {}", CLog.tabs(stackSize), CReflection.qualifiedName(refField), obj.name());
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

			// Build UPDATE statement for one column only
			String columnName = it.next();
			Object fieldValue = columnValueMap.get(columnName);

			String whereClause = Const.ID_COL + "=" + obj.getId();
			oneCVMap.clear();
			oneCVMap.put(columnName, fieldValue);
			try {
				// Update column
				sdc.sqlDb.update(cn, table.name, oneCVMap, whereClause);
			}
			catch (SQLException sqlex) {
				log.error("SDC: UPDATE failed by exception! Column '{}' cannot be updated to {} for object {}", columnName, CLog.forSecretLogging(table.name, columnName, oneCVMap.get(columnName)),
						obj.name());
				obj.currentException = sqlex;
				Field field = sdc.getSqlRegistry().getFieldFor(table.findColumnByName(columnName));
				if (field != null) {
					obj.setFieldError(field, "CANNOT_UPDATE_COLUMN - " + sqlex.getMessage());

					// Reset field and column value in column value map to original values
					try {
						List<SortedMap<String, Object>> results = sdc.sqlDb.selectFrom(cn, table.name, columnName, Const.ID_COL + "=" + obj.getId(), null, 0, null);
						fieldValue = results.get(0).get(columnName);
						obj.setFieldValue(field, fieldValue);
						columnValueMap.put(columnName, fieldValue);
					}
					catch (SQLException | SqlDbException e) {
						log.error("SDC: SELECT failed by exception! '{}.{}' cannot be read for object {}", table.name, columnName, obj.name());
					}
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Save object to database
	// -------------------------------------------------------------------------

	// Save object in one transaction to database
	static boolean save(Connection cn, SqlDomainController sdc, SqlDomainObject obj, List<SqlDomainObject> objectsToCheckForCircularReference) throws SQLException, SqlDbException {

		// Register object in object store before (initial) saving if not already done
		int stackSize = objectsToCheckForCircularReference.size();
		if (!sdc.isRegistered(obj) && obj.getId() == 0) {
			log.debug("SDC: {}Object {} was not registered before - register now", CLog.tabs(stackSize), obj.name());
			sdc.register(obj);
		}

		// Check if object was already stored within current (recursive) save operation
		if (objectsToCheckForCircularReference.contains(obj)) {
			if (log.isDebugEnabled()) {
				log.debug("SDC: {}Object {} was already stored or tried to store within this recursive save operation (detected circular reference)", CLog.tabs(stackSize), obj.name());
			}
			return false;
		}
		else {
			objectsToCheckForCircularReference.add(obj);
		}

		if (log.isTraceEnabled()) {
			log.trace("SDC: {}Save{} object {}", CLog.tabs(stackSize), (obj.isStored ? "" : " new"), obj.name());
		}

		// Update accumulations for pending reference changes (only for convenience here) and reset object's exception and field errors/warnings which will be detected if saving fails
		sdc.updateAccumulationsOfParentObjects(obj);
		obj.clearErrors();

		// Get domain classes of object and create or retrieve object record
		List<Class<? extends SqlDomainObject>> domainClasses = sdc.getRegistry().getDomainClassesFor(obj.getClass()); // INSERT from bottom to top level domain class (foreign keys for inheritance)
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
			SqlDbTable table = sdc.getSqlRegistry().getTableFor(domainClass);

			// Save un-stored parent objects (on non nullable reference) or reset parent/child reference and collect parent objects here to save them after saving this object (on nullable reference)
			collectedParentObjectMap.putAll(storeOrCollectUnstoredParentObjects(cn, sdc, obj, domainClass, objectsToCheckForCircularReference));

			// Get field changes for domain class
			Map<Field, Object> fieldChangesForDomainClassMap = getFieldChangesForDomainClass(sdc, obj, objectRecord, domainClass);
			if (!fieldChangesForDomainClassMap.isEmpty()) {
				wasChanged = true;
			}

			// Build column value map for INSERT or UPDATE - ignore changes of complex fields which are not associated with a column (but with an 'entry' table)
			SortedMap<String, Object> columnValueMap = fieldChangesMap2ColumnValueMap(sdc, fieldChangesForDomainClassMap, obj);

			if (!obj.isStored) { // INSERT

				// Do not display secret field in trace log!
				// log.trace("SDC: Field/value map of new object for domain class {}: {}", domainClass.getSimpleName(), fieldChangesForDomainClassMap);

				// Add standard columns
				columnValueMap.put(Const.ID_COL, obj.getId());
				columnValueMap.put(Const.DOMAIN_CLASS_COL, obj.getClass().getSimpleName()); // to identify object domain class also from records of tables related to non-domain object classes
				if (sdc.getRegistry().isBaseDomainClass(domainClass)) {
					columnValueMap.put(Const.LAST_MODIFIED_COL, obj.lastModifiedInDb);
				}

				try {
					// Insert record into table associated to this domain class
					// Note: During INSERT values in column/value map will potentially be converted to JDBC specific type, so that data types after INSERT and after SELECT are the same
					// INSERT INTO <table of current object class> (columns) VALUES (column values)
					sdc.sqlDb.insertInto(cn, table.name, columnValueMap);
				}
				catch (SQLException sqlex) {

					if (obj.getClass().getSimpleName().contains("InProgress")) {
						// On trying to insert temporary in-progress records assume duplicate key exception here and suppress error messages and error handling because this is not an error case
						// (in-progress records protect associated objects from multiple parallel access)
						if (log.isDebugEnabled()) {
							log.debug(
									"SDC: {}@{} is currently in use by another instance and therefore cannot be allocated excusively! ('InProgress' object could not be inserted due to unique constraint for 'id' column)",
									(obj.getClass().getEnclosingClass() != null ? obj.getClass().getEnclosingClass().getSimpleName() : ""), obj.getId());
						}
					}
					else {
						log.error("SDC: {}INSERT failed by exception! Object {} cannot be saved in table '{}'", CLog.tabs(stackSize), obj.name(), table.name);

						// Set exception and field errors on constraint violation(s) (check violations in ascending order of severity to have the most critical ones assigned to field(s)
						obj.currentException = sqlex;
						sdc.hasColumnSizeViolations(obj, domainClass);
						sdc.hasUniqueConstraintViolations(obj, domainClass);
						sdc.hasNotNullConstraintViolations(obj, domainClass);

						// Restore parent/child references which were reset before trying to save object
						for (Field refField : collectedParentObjectMap.keySet()) {
							obj.setFieldValue(refField, collectedParentObjectMap.get(refField));
						}
					}

					throw sqlex;
				}
			}
			else { // UPDATE
					// Do not display secret field in trace log!
					// log.trace("SDC: Field changes detected for domain class {}: {}", domainClass.getSimpleName(), fieldChangesForDomainClassMap);

				// Update 'last modified' field (of bottom domain class) if any changes where detected
				if (sdc.getRegistry().isBaseDomainClass(domainClass) && wasChanged) {
					obj.lastModifiedInDb = LocalDateTime.now();
					columnValueMap.put(Const.LAST_MODIFIED_COL, obj.lastModifiedInDb);
				}

				// UPDATE record in table associated to this domain class if there are changes for current domain class
				if (!columnValueMap.isEmpty()) {
					try {
						// UPDATE <table> SET <names of changed columns>=<converted field values> WHERE ID=<objectid>
						long count = sdc.sqlDb.update(cn, table.name, columnValueMap, Const.ID_COL + "=" + obj.getId());
						if (count == 0) {
							log.warn("Object '{}' could not be saved because it was meanwhile deleted by another thread/instance (record for this object does not exist anymore in table '{}')", obj,
									table.name);
							if (sdc.isRegistered(obj)) {
								sdc.unregister(obj);
							}
							return false;
						}
					}
					catch (SQLException sqlex) {
						log.error("SDC: {}UPDATE failed by {}: {}! Not all changed fields can be saved for object {}! Try to update columns separately...", CLog.tabs(stackSize),
								sqlex.getClass().getSimpleName(), sqlex.getMessage(), obj.name());

						// On error try to UPDATE columns separately - so all but one (or more) columns can actually be updated
						tryAnalyticUpdate(cn, sdc, obj, table, columnValueMap);
					}
				}
			}

			// Update local object record by changes of data and reference fields
			// Note: Object record contains field values as they are (but in encrypted form if they are 'secret' values). Necessary conversion for storing values in database will be made in
			// SqlDb::assigneValue() and will not be reflected in object record.
			objectRecord.putAll(columnValueMap);

			// Handle table related fields (collections and maps): Delete old entry records, update changed map entries and insert new entries
			for (Field complexField : fieldChangesForDomainClassMap.keySet().stream().filter(f -> sdc.getRegistry().isComplexField(f)).collect(Collectors.toList())) {
				updateEntryTable(cn, sdc, complexField, fieldChangesForDomainClassMap.get(complexField), objectRecord, obj);
			}
		}

		// Mark new object as stored - do this not until all records have been inserted but before saving parent objects which are not yet stored
		if (!obj.isStored) {
			obj.isStored = true;
		}

		// Save collected parent objects (on nullable foreign key columns) after saving this object and restore (UPDATE) references in database
		if (!collectedParentObjectMap.isEmpty()) {
			objectRecord.putAll(storeCollectedParentObjectsAndRestoreReferences(cn, sdc, obj, collectedParentObjectMap, objectsToCheckForCircularReference));
		}

		return wasChanged;
	}
}
