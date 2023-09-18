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
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

public abstract class LoadHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(LoadHelpers.class);

	// Determine object domain class of (missing) object given by id and derived domain class by loading object record
	@SuppressWarnings("unchecked")
	private static <S extends SqlDomainObject> Class<S> determineObjectDomainClassOfMissingObject(Connection cn, Class<S> domainClass, long id) {

		log.info("SDC: Domain class '{}' is not an object domain class -> determine object domain class for missing object(s).", domainClass.getSimpleName());

		// Determine object domain class by retrieving domain class name from selected referenced object record
		try {
			String refTableName = SqlRegistry.getTableFor(domainClass).name;
			List<SortedMap<String, Object>> records = SqlDomainController.sqlDb.selectFrom(cn, refTableName, SqlDomainObject.DOMAIN_CLASS_COL, "ID=" + id, null, CList.newList(String.class));
			if (records.isEmpty()) {
				log.error("No record found for object {}@{} which is referenced by child object and therefore should exist", domainClass.getSimpleName(), id);
			}
			else {
				String objectDomainClassName = (String) records.get(0).get(SqlDomainObject.DOMAIN_CLASS_COL); // Assume JDBC type of column is String for String field
				log.info("SDC: Object domain class for referenced domain class '{}' is: '{}'", domainClass.getSimpleName(), objectDomainClassName);
				return (Class<S>) DomainController.getDomainClassByName(objectDomainClassName);
			}
		}
		catch (SQLException | SqlDbException e) {
			log.error("SDC: Exception determining object domain class for domain class '{}'", domainClass, e);
		}

		return null;
	}

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

		Predicate<Column> isNonStandardColumnPredicate = c -> !objectsEqual(c.name, SqlDomainObject.ID_COL) && !objectsEqual(c.name, SqlDomainObject.DOMAIN_CLASS_COL);

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
	private static SelectDescription buildSelectDescriptionForEntriesOf(String baseTableExpression, SqlDbTable mainObjectTable, Field complexField) {

		SelectDescription sde = new SelectDescription();

		// Build table clause - join entry table and main object table

		SqlDbTable entryTable = SqlRegistry.getEntryTableFor(complexField);
		Column mainTableRefIdColumn = SqlRegistry.getMainTableRefIdColumnFor(complexField);

		sde.joinedTableExpression = entryTable.name + " JOIN " + baseTableExpression + " ON " + entryTable.name + "." + mainTableRefIdColumn.name + "=" + mainObjectTable.name + ".ID";

		// Build column clause for entry records

		// Column referencing main table for domain class
		sde.allColumnNames.add(mainTableRefIdColumn.name);
		sde.allFieldTypes.add(Long.class);

		Class<?> fieldClass = complexField.getType();
		ParameterizedType genericFieldType = ((ParameterizedType) complexField.getGenericType());

		if (Collection.class.isAssignableFrom(fieldClass)) {

			// Column for elements of collection
			sde.allColumnNames.add(SqlDomainObject.ELEMENT_COL);
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
				sde.allColumnNames.add(SqlDomainObject.ORDER_COL);
				sde.allFieldTypes.add(Integer.class);
				sde.orderByClause = SqlDomainObject.ORDER_COL;
			}
		}
		else {
			// Column for keys of map
			sde.allColumnNames.add(SqlDomainObject.KEY_COL);
			Type keyType = genericFieldType.getActualTypeArguments()[0];
			keyType = Helpers.requiredJdbcTypeFor((Class<?>) keyType);
			sde.allFieldTypes.add((Class<?>) keyType); // Keys may not be complex objects

			// Column for values of map
			sde.allColumnNames.add(SqlDomainObject.VALUE_COL);
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
	static <S extends SqlDomainObject> Map<Long, SortedMap<String, Object>> retrieveRecordsForObjectDomainClass(Connection cn, int limit, Class<S> objectDomainClass, String whereClause) {

		Map<Long, SortedMap<String, Object>> loadedRecordMap = new HashMap<>();

		try {
			// First load main object records

			// Build select description for object records
			SelectDescription sd = buildSelectDescriptionFor(objectDomainClass);

			// SELECT object records and return empty map if no (matching) object found in database
			List<SortedMap<String, Object>> loadedRecords = SqlDomainController.sqlDb.selectFrom(cn, sd.joinedTableExpression, sd.allColumnNames, whereClause, null, limit, null, sd.allFieldTypes);
			if (CList.isEmpty(loadedRecords)) {
				return loadedRecordMap;
			}

			// Build up loaded records by id map
			for (SortedMap<String, Object> rec : loadedRecords) {

				// Check if record is a 'real' (not derived) object record (this should not be necessary)
				String actualObjectDomainClassName = (String) rec.get(SqlDomainObject.DOMAIN_CLASS_COL);
				if (objectsEqual(actualObjectDomainClassName, objectDomainClass.getSimpleName())) {

					long objectId = (long) rec.get(SqlDomainObject.ID_COL);
					loadedRecordMap.put(objectId, rec);
				}
				else {
					log.info("SDC: Loaded record {} for object domain class '{}' is not a 'real' object record - actual object domain class is '{}'", CLog.forAnalyticLogging(rec),
							objectDomainClass.getSimpleName(), actualObjectDomainClassName);
				}
			}

			// Second load entry records for all complex (table related) fields and assign them to main object records using entry table name as key

			SqlDbTable objectTable = SqlRegistry.getTableFor(objectDomainClass);

			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(objectDomainClass)) {
				for (Field complexField : Registry.getComplexFields(domainClass)) {

					// Build table expression, column names and order by clause to SELECT entry records
					SelectDescription sde = buildSelectDescriptionForEntriesOf(sd.joinedTableExpression, objectTable, complexField);

					List<SortedMap<String, Object>> loadedEntryRecords = new ArrayList<>();
					if (limit > 0) {

						// If # of loaded object records is limited SELECT only entry records for actually loaded object records
						List<String> idsLists = Helpers.buildMax1000IdsLists(loadedRecordMap.keySet());
						for (String idsList : idsLists) {
							String localWhereClause = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "") + objectTable.name + ".ID IN (" + idsList + ")";
							loadedEntryRecords.addAll(SqlDomainController.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, localWhereClause, sde.orderByClause, sde.allFieldTypes));
						}
					}
					else {
						// SELECT all entry records
						loadedEntryRecords.addAll(SqlDomainController.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, sde.allFieldTypes));
					}

					// Get entry table and column referencing id of main object record for complex (collection or map) field
					String entryTableName = SqlRegistry.getEntryTableFor(complexField).name;
					String refIdColumnName = SqlRegistry.getMainTableRefIdColumnFor(complexField).name;

					// Add records of entry table to related object records using entry table name as key
					for (SortedMap<String, Object> entryRecord : loadedEntryRecords) {

						// Get object id and check if object record is present
						long objectId = (long) entryRecord.get(refIdColumnName);
						if (!loadedRecordMap.containsKey(objectId)) {
							log.warn("SDC: Object {}@{} was not loaded before (trying) updating collection or map field '{}'", domainClass.getSimpleName(), objectId, complexField.getName());
							continue;
						}

						// Add entry record to record list
						@SuppressWarnings("unchecked")
						List<SortedMap<String, Object>> entryRecords = (List<SortedMap<String, Object>>) loadedRecordMap.get(objectId).computeIfAbsent(entryTableName, t -> new ArrayList<>());
						entryRecords.add(entryRecord);
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
	@SuppressWarnings("unchecked")
	private static void checkUnsavedComplexFieldChanges(SqlDomainObject obj, Field complexField, String entryTableName, Object collectionOrMapFromObject, boolean isCollection) {

		ParameterizedType genericFieldType = (ParameterizedType) complexField.getGenericType();

		if (isCollection) { // Collection

			Collection<?> colFromObjRecord = Helpers.entryRecords2Collection(genericFieldType,
					(List<SortedMap<String, Object>>) SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName));

			if (!objectsEqual(collectionOrMapFromObject, colFromObjRecord)) {

				log.warn("SDC: Collection field '{}' of object '{}' {} has unsaved changes and will be overridden by collection {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, colFromObjRecord);

				obj.setFieldWarning(complexField,
						"Discarded unsaved changed collection " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading collection from database");
			}
		}
		else { // Map
			Map<?, ?> mapFromObjRecord = Helpers.entryRecords2Map(genericFieldType,
					(List<SortedMap<String, Object>>) SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName));

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
				ParameterizedType genericFieldType = (ParameterizedType) complexField.getGenericType();
				boolean isCollection = Collection.class.isAssignableFrom(complexField.getType());
				Object collectionOrMapFromFieldValue = obj.getFieldValue(complexField);

				if (!isNew) {
					checkUnsavedComplexFieldChanges(obj, complexField, entryTableName, collectionOrMapFromFieldValue, isCollection);
				}

				if (isCollection) { // Collection

					Collection<?> colFromDatabase = Helpers.entryRecords2Collection(genericFieldType, (List<SortedMap<String, Object>>) databaseChangesMap.get(entryTableName));
					Collection<Object> colFromObject = (Collection<Object>) collectionOrMapFromFieldValue;

					colFromObject.clear();
					colFromObject.addAll(colFromDatabase);
				}
				else { // Map
					Map<?, ?> mapFromDatabase = Helpers.entryRecords2Map(genericFieldType, (List<SortedMap<String, Object>>) databaseChangesMap.get(entryTableName));
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
						obj.lastModifiedInDb = ((LocalDateTime) loadedRecord.get(SqlDomainObject.LAST_MODIFIED_COL));

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
						if (!objectsEqual(col, SqlDomainObject.LAST_MODIFIED_COL) && !logicallyEqual(value, objectRecord.get(col))) {

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
						databaseChangesMap.put(SqlDomainObject.LAST_MODIFIED_COL, loadedRecord.get(SqlDomainObject.LAST_MODIFIED_COL));

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
				collectedRecordMap.putAll(retrieveRecordsForObjectDomainClass(cn, 0, objectDomainClass, SqlRegistry.getTableFor(objectDomainClass).name + ".ID IN (" + idsList + ")"));
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
