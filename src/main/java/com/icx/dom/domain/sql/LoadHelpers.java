package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CCollection;
import com.icx.dom.common.CDateTime;
import com.icx.dom.common.CList;
import com.icx.dom.common.CLog;
import com.icx.dom.common.CMap;
import com.icx.dom.common.Common;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

/**
 * Helpers for loading objects from database
 * 
 * @author RainerBaumgärtel
 */
public abstract class LoadHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(LoadHelpers.class);

	// -------------------------------------------------------------------------
	// SELECT helpers
	// -------------------------------------------------------------------------

	// Information to build SELCT statement for loading records from database
	private static class SelectDescription {

		String joinedTableExpression = null;
		List<String> allColumnNames = new ArrayList<>();
		List<Class<?>> allFieldTypes = new ArrayList<>();
		String orderByClause = null;
	}

	// Build joined table expression for all inherited domain classes and also build name list of columns to retrieve
	private static SelectDescription buildSelectDescriptionForMainObjectRecords(SqlRegistry sqlRegistry, Class<? extends SqlDomainObject> objectDomainClass) {

		// Build table and column expression for object domain class
		SelectDescription sd = new SelectDescription();
		SqlDbTable objectTable = sqlRegistry.getTableFor(objectDomainClass);
		sd.joinedTableExpression = objectTable.name;
		sd.allColumnNames.addAll(objectTable.columns.stream().map(c -> objectTable.name + "." + c.name).collect(Collectors.toList()));
		sd.allFieldTypes.addAll(objectTable.columns.stream().map(sqlRegistry::getRequiredJdbcTypeFor).collect(Collectors.toList()));

		// Extend table and column expression for inherited domain classes
		Predicate<Column> isNonStandardColumnPredicate = c -> !objectsEqual(c.name, Const.ID_COL) && !objectsEqual(c.name, Const.DOMAIN_CLASS_COL);
		Class<? extends SqlDomainObject> derivedDomainClass = sqlRegistry.getCastedSuperclass(objectDomainClass);
		while (derivedDomainClass != SqlDomainObject.class) {

			SqlDbTable inheritedTable = sqlRegistry.getTableFor(derivedDomainClass);
			sd.joinedTableExpression += " JOIN " + inheritedTable.name + " ON " + inheritedTable.name + ".ID=" + objectTable.name + ".ID";
			sd.allColumnNames.addAll(inheritedTable.columns.stream().filter(isNonStandardColumnPredicate).map(c -> inheritedTable.name + "." + c.name).collect(Collectors.toList()));
			sd.allFieldTypes.addAll(inheritedTable.columns.stream().filter(isNonStandardColumnPredicate).map(sqlRegistry::getRequiredJdbcTypeFor).collect(Collectors.toList()));

			derivedDomainClass = sqlRegistry.getCastedSuperclass(derivedDomainClass);
		}
		return sd;
	}

	// Build joined table expression for entry tables (storing collections and maps)
	private static SelectDescription buildSelectDescriptionForEntryRecords(SqlRegistry sqlRegistry, String baseTableExpression, String objectTableName, Field complexField) {

		// Build table and column clause for entry records - join entry table and main object table
		SelectDescription sde = new SelectDescription();
		String entryTableName = sqlRegistry.getEntryTableFor(complexField).name;
		String refIdColumnName = sqlRegistry.getMainTableRefIdColumnFor(complexField).name;
		sde.joinedTableExpression = entryTableName + " JOIN " + baseTableExpression + " ON " + entryTableName + "." + refIdColumnName + "=" + objectTableName + ".ID";
		sde.allColumnNames.add(refIdColumnName); // Column referencing main table for domain class
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
	static Map<Long, SortedMap<String, Object>> retrieveRecordsFromDatabase(Connection cn, SqlDomainController sdc, int limit, Class<? extends SqlDomainObject> objectDomainClass, String whereClause) {

		Map<Long, SortedMap<String, Object>> loadedRecordMap = new HashMap<>();
		try {

			// Load (main) object records and build up loaded records by id map
			SelectDescription sd = buildSelectDescriptionForMainObjectRecords(((SqlRegistry) sdc.registry), objectDomainClass);
			List<SortedMap<String, Object>> loadedRecords = sdc.sqlDb.selectFrom(cn, sd.joinedTableExpression, sd.allColumnNames, whereClause, null, limit, null, sd.allFieldTypes);
			if (CList.isEmpty(loadedRecords)) {
				return loadedRecordMap;
			}
			for (SortedMap<String, Object> rec : loadedRecords) {
				loadedRecordMap.put((long) rec.get(Const.ID_COL), rec);
			}

			// Load entry records for all complex (table related) fields and assign them to main object records using entry table name as key
			String objectTableName = ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).name;
			for (Class<?> domainClass : sdc.registry.getDomainClassesFor(objectDomainClass)) {

				// For all table related fields...
				for (Field complexField : sdc.registry.getComplexFields(sdc.registry.castDomainClass(domainClass))) {

					// Build table expression, column names and order by clause to SELECT entry records
					SelectDescription sde = buildSelectDescriptionForEntryRecords(((SqlRegistry) sdc.registry), sd.joinedTableExpression, objectTableName, complexField);

					// Load entry records
					List<SortedMap<String, Object>> loadedEntryRecords = new ArrayList<>();
					if (limit > 0) {

						// If # of loaded object records is limited SELECT only entry records for actually loaded object records
						List<String> idsLists = Helpers.buildMax1000IdsLists(loadedRecordMap.keySet());
						for (String idsList : idsLists) {
							String limitWhereClause = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "") + objectTableName + ".ID IN (" + idsList + ")";
							loadedEntryRecords.addAll(sdc.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, limitWhereClause, sde.orderByClause, sde.allFieldTypes));
						}
					}
					else {
						// SELECT all entry records
						loadedEntryRecords.addAll(sdc.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, sde.allFieldTypes));
					}

					// Group entry records by objects where they belong to
					Map<Long, List<SortedMap<String, Object>>> entryRecordsByObjectIdMap = new HashMap<>();
					String refIdColumnName = ((SqlRegistry) sdc.registry).getMainTableRefIdColumnFor(complexField).name;
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

					// Build collection or map from entry records and add it to loaded object record with entry table name as key
					String entryTableName = ((SqlRegistry) sdc.registry).getEntryTableFor(complexField).name;
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
		catch (SQLException | SqlDbException e) { // Method is indirectly used in Java functional interface (as part of select supplier) and therefore may not throw exceptions
			log.error("SDC: {} loading objects of domain class '{}' from database: {}", e.getClass().getSimpleName(), objectDomainClass.getName(), e.getMessage());
		}
		return loadedRecordMap;
	}

	// -------------------------------------------------------------------------
	// SELECT suppliers
	// -------------------------------------------------------------------------

	// Select supplier for loading all objects from database - considering data horizon for data horizon controlled domain classes
	@SafeVarargs
	static final Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> selectAll(Connection cn, SqlDomainController sdc,
			Class<? extends SqlDomainObject>... domainClassesToExclude) {

		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		// Build database specific datetime string for data horizon
		String dataHorizon = String.format(sdc.sqlDb.getDbType().dateTemplate(), sdc.getCurrentDataHorizon().format(DateTimeFormatter.ofPattern(CDateTime.DATETIME_MS_FORMAT)));

		// Load objects records for all registered object domain classes - consider data horizon if specified
		for (Class<? extends SqlDomainObject> objectDomainClass : sdc.registry.getRegisteredObjectDomainClasses()) {
			if (domainClassesToExclude != null && Stream.of(domainClassesToExclude).anyMatch(objectDomainClass::isAssignableFrom)) { // Ignore objects of excluded domain classes
				continue;
			}

			// For data horizon controlled object domain classes build WHERE clause for data horizon control
			String whereClause = (sdc.registry.isDataHorizonControlled(objectDomainClass) ? Const.LAST_MODIFIED_COL + ">=" + dataHorizon : null);

			// Retrieve object records for one object domain class by SELECTing from database
			Map<Long, SortedMap<String, Object>> loadedRecordsMap = retrieveRecordsFromDatabase(cn, sdc, 0, objectDomainClass, whereClause);
			if (!CMap.isEmpty(loadedRecordsMap)) {
				loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);
			}
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// Select supplier for loading objects of given object domain class which records in database match given WHERE clause (usage in application needs knowledge about Java -> SQL mapping)
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> select(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> objectDomainClass,
			String whereClause, int maxCount) {

		// Try to SELECT object records FOR UPDATE
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = LoadHelpers.retrieveRecordsFromDatabase(cn, sdc, maxCount, objectDomainClass, whereClause);
		if (CMap.isEmpty(loadedRecordsMap)) {
			return Collections.emptyMap();
		}

		// Build map to return from supplier method
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);
		return loadedRecordsMapByDomainClassMap;
	}

	// Select supplier used for synchronization if multiple instances access one database and have to process distinct objects (like orders)
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> selectExclusively(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> objectDomainClass,
			Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount) {

		// Build WHERE clause
		String objectTableName = ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).name;
		String inProgressTableName = ((SqlRegistry) sdc.registry).getTableFor(inProgressClass).name;
		whereClause = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "") + objectTableName + ".ID NOT IN (SELECT ID FROM " + inProgressTableName + ")";

		// SELECT object records
		Map<Long, SortedMap<String, Object>> rawRecordsMap = LoadHelpers.retrieveRecordsFromDatabase(cn, sdc, maxCount, objectDomainClass, whereClause);
		if (CMap.isEmpty(rawRecordsMap)) {
			return Collections.emptyMap();
		}

		// Try to INSERT in-progress record with same id as records found (works only for one in-progress record per record found because of UNIQUE constraint for ID field) - provide only records
		// where in-progress record could be inserted
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = new HashMap<>();
		for (Entry<Long, SortedMap<String, Object>> entry : rawRecordsMap.entrySet()) {

			long id = entry.getKey();
			try {
				SqlDomainObject inProgressObject = sdc.createWithId(inProgressClass, id);
				sdc.save(inProgressObject);
				loadedRecordsMap.put(id, entry.getValue());
			}
			catch (SQLException sqlex) {
				log.info("SDC: {} record with id {} is already in progress (by another instance)", objectDomainClass.getSimpleName(), id);
			}
			catch (SqlDbException sqldbex) {
				log.error("SDC: {} occurred trying to INSERT {} record", sqldbex, inProgressClass.getSimpleName());
			}
			catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
				log.error("SDC: {} occurred trying to create in-progress object ({})", ex, inProgressClass.getSimpleName()); // Do not throw - selectExcusively() is used within lambda expression
			}
		}

		// Build map to return from supplier method
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);

		return loadedRecordsMapByDomainClassMap;
	}

	// SELECT record(s) for specific object from database and build domain object record for this object - returns empty map if object could not be loaded
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> selectObjectRecord(Connection cn, SqlDomainController sdc, SqlDomainObject obj) {

		String idWhereClause = ((SqlRegistry) sdc.registry).getTableFor(obj.getClass()).name + "." + Const.ID_COL + "=" + obj.getId();
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		Map<Long, SortedMap<String, Object>> loadedRecordsMap = LoadHelpers.retrieveRecordsFromDatabase(cn, sdc, 0, obj.getClass(), idWhereClause);
		if (!loadedRecordsMap.isEmpty()) {
			loadedRecordsMapByDomainClassMap.put(obj.getClass(), loadedRecordsMap);
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// -------------------------------------------------------------------------
	// Build objects from loaded records
	// -------------------------------------------------------------------------

	// Check if object has unsaved reference changes
	private static void assignFieldWarningOnUnsavedReferenceChange(SqlDomainController sdc, SqlDomainObject obj, Field refField, String foreignKeyColumnName, Long parentObjectIdFromDatabase) {

		SqlDomainObject parentObj = (SqlDomainObject) obj.getFieldValue(refField);
		Long parentObjectIdFromLocalObjectRecord = (Long) sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(foreignKeyColumnName);

		if (parentObj == null && parentObjectIdFromLocalObjectRecord != null) {
			log.warn("SDC: Reference field '{}' of object '{}' has unsaved null reference which will be overridden by reference to '{}@{}' from database!", refField.getName(), obj.name(),
					refField.getType().getSimpleName(), parentObjectIdFromDatabase);
			obj.setFieldWarning(refField, "Discarded unsaved changed null reference of field '" + refField.getName() + "' on loading object from database");
		}
		else if (parentObj != null && parentObj.getId() != parentObjectIdFromLocalObjectRecord) {
			if (parentObjectIdFromDatabase != null) {
				log.warn("SDC: Referenced field '{}' of object '{}' has unsaved changed reference to '{}@{}' which will be overridden by reference to '{}@{}' from database!", refField.getName(),
						obj.name(), refField.getType().getSimpleName(), parentObj.getId(), refField.getType().getSimpleName(), parentObjectIdFromDatabase);
			}
			else {
				log.warn("SDC: Referenced field '{}' of object '{}' has unsaved changed reference to '{}@{}' which will be overridden by null reference from database!", refField.getName(), obj.name(),
						refField.getType().getSimpleName(), parentObj.getId());
			}
			obj.setFieldWarning(refField, "Discarded unsaved changed reference to " + DomainObject.name(parentObj) + " of field '" + refField.getName() + "' on loading object from database");
		}
	}

	// Check if object has unsaved value changes
	private static void assignFieldWarningOnUnsavedValueChange(SqlDomainController sdc, SqlDomainObject obj, Field dataField, String columnName, Object columnValueFromDatabase) {

		Object fieldValue = obj.getFieldValue(dataField);
		Object columnValueFromObjectRecord = sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(columnName);
		Object fieldValueFromObjectRecord = Helpers.column2FieldValue(dataField.getType(), columnValueFromObjectRecord);

		if (!objectsEqual(fieldValue, fieldValueFromObjectRecord)) {
			log.warn("SDC: Data field '{}' of object '{}' has unsaved changed value {} which will be overridden by value {} from database!", dataField.getName(), obj.name(),
					CLog.forSecretLogging(dataField.getName(), fieldValue), CLog.forSecretLogging(dataField.getName(), columnValueFromDatabase));
			obj.setFieldWarning(dataField,
					"Discarded unsaved changed value " + CLog.forSecretLogging(dataField.getName(), fieldValue) + " of field '" + dataField.getName() + "' on loading object from database");
		}
	}

	// Check if object has unsaved changes in a collection or map
	private static void assignFieldWarningOnUnsavedComplexFieldChange(SqlDomainController sdc, SqlDomainObject obj, Field complexField, String entryTableName, Object collectionOrMapFromObject,
			boolean isCollection) {

		if (isCollection) { // Collection
			Collection<?> colFromObjRecord = (Collection<?>) sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);
			if (!objectsEqual(collectionOrMapFromObject, colFromObjRecord)) {
				log.warn("SDC: Collection field '{}' of object '{}' {} has unsaved changes and will be overridden by collection {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, colFromObjRecord);
				obj.setFieldWarning(complexField,
						"Discarded unsaved changed collection " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading collection from database");
			}
		}
		else { // Map
			Map<?, ?> mapFromObjRecord = (Map<?, ?>) sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);
			if (!objectsEqual(collectionOrMapFromObject, mapFromObjRecord)) {
				log.warn("SDC: Key/value map field '{}' of object '{}' {} has unsaved changes and will be overridden by map {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, mapFromObjRecord);
				obj.setFieldWarning(complexField, "Discarded unsaved changed key/value map " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading map from database");
			}
		}
	}

	// Assign changed data in record from database to corresponding fields of domain object - check for unsaved changes before, which will then be discarded
	@SuppressWarnings("unchecked")
	private static void assignDataToDomainObject(SqlDomainController sdc, SqlDomainObject obj, boolean isNew, SortedMap<String, Object> databaseChangesMap,
			Set<SqlDomainObject> objectsWhereReferencesChanged) {

		// Changed field predicates
		Predicate<Field> hasValueChangedPredicate = (f -> databaseChangesMap.containsKey(((SqlRegistry) sdc.registry).getColumnFor(f).name));
		Predicate<Field> hasEntriesChangedPredicate = (f -> databaseChangesMap.containsKey(((SqlRegistry) sdc.registry).getEntryTableFor(f).name));

		// Assign loaded data to fields for all inherited domain classes of domain object
		for (Class<? extends SqlDomainObject> domainClass : sdc.registry.getDomainClassesFor(obj.getClass())) {

			// Reference fields: assign new references immediately if referenced object exists - otherwise collect unresolved references
			for (Field refField : sdc.registry.getReferenceFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {
				String foreignKeyColumnName = ((SqlRegistry) sdc.registry).getColumnFor(refField).name;
				Long parentObjectIdFromDatabase = (Long) databaseChangesMap.get(foreignKeyColumnName);

				if (!isNew) {
					assignFieldWarningOnUnsavedReferenceChange(sdc, obj, refField, foreignKeyColumnName, parentObjectIdFromDatabase);
				}

				if (parentObjectIdFromDatabase == null) { // Null reference
					obj.setFieldValue(refField, null);
				}
				else { // Object reference
					SqlDomainObject newParentObject = sdc.find(sdc.registry.getCastedReferencedDomainClass(refField), parentObjectIdFromDatabase);
					if (newParentObject != null) { // Referenced object is already loaded
						obj.setFieldValue(refField, newParentObject);
					}
					else { // Referenced object is still not loaded (data horizon or circular reference) -> unresolved reference
						obj.setFieldValue(refField, null); // Temporarily reset reference - this will be set to referenced object after loading this
					}
				}
				objectsWhereReferencesChanged.add(obj); // Collect objects where references changed for subsequent update of accumulations
			}

			// Data fields: assign - potentially converted - values
			for (Field dataField : sdc.registry.getDataFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {
				String columnName = ((SqlRegistry) sdc.registry).getColumnFor(dataField).name;
				Object columnValueFromDatabase = databaseChangesMap.get(columnName);

				if (!isNew) {
					assignFieldWarningOnUnsavedValueChange(sdc, obj, dataField, columnName, columnValueFromDatabase /* only for logging */);
				}
				obj.setFieldValue(dataField, Helpers.column2FieldValue(dataField.getType(), columnValueFromDatabase));
			}

			// Complex (table related) fields: set field values of object to collection or map (conversion from entry table record was already done directly after loading entry records)
			for (Field complexField : sdc.registry.getComplexFields(domainClass).stream().filter(hasEntriesChangedPredicate).collect(Collectors.toList())) {
				String entryTableName = ((SqlRegistry) sdc.registry).getEntryTableFor(complexField).name;
				boolean isCollection = Collection.class.isAssignableFrom(complexField.getType());
				Object collectionOrMapFromFieldValue = obj.getFieldValue(complexField);

				if (!isNew) {
					assignFieldWarningOnUnsavedComplexFieldChange(sdc, obj, complexField, entryTableName, collectionOrMapFromFieldValue, isCollection);
				}

				if (isCollection) { // Collection
					Collection<Object> colFromObject = (Collection<Object>) collectionOrMapFromFieldValue;
					colFromObject.clear();
					colFromObject.addAll((Collection<Object>) databaseChangesMap.get(entryTableName));
				}
				else { // Map
					Map<Object, Object> mapFromObject = (Map<Object, Object>) collectionOrMapFromFieldValue;
					mapFromObject.clear();
					mapFromObject.putAll((Map<Object, Object>) databaseChangesMap.get(entryTableName));
				}
			}
		}

		// Reflect database changes in local object record - do this not until all checks for unsaved changes were done
		if (isNew) {
			sdc.recordMap.get(obj.getClass()).put(obj.getId(), databaseChangesMap);
		}
		else {
			sdc.recordMap.get(obj.getClass()).get(obj.getId()).putAll(databaseChangesMap);
		}
	}

	// Update local object records, instantiate new objects and assign data to all new or changed objects. Collect objects having changed and/or still unresolved references
	static boolean buildObjectsFromLoadedRecords(SqlDomainController sdc, Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap,
			Set<SqlDomainObject> loadedObjects, Set<SqlDomainObject> objectsWhereReferencesChanged, Set<UnresolvedReference> unresolvedReferences)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		if (!CMap.isEmpty(loadedRecordsMap)) {
			log.info("SDC: Build objects from loaded records...");
		}

		Set<SqlDomainObject> newObjects = new HashSet<>(); // Only for logging
		Set<SqlDomainObject> changedObjects = new HashSet<>();

		// Filter object domain classes where records were loaded for and handle loaded objects in order of parent/child relationship to avoid unnecessary unresolved references
		for (Class<? extends SqlDomainObject> objectDomainClass : sdc.registry.getRegisteredObjectDomainClasses().stream().filter(loadedRecordsMap::containsKey).collect(Collectors.toList())) {
			List<String> columnNames = ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).columns.stream().map(c -> c.name).collect(Collectors.toList()); // For logging only

			// Handle loaded object records: instantiate new objects and assign loaded data to new and changed objects
			for (Entry<Long, SortedMap<String, Object>> entry : loadedRecordsMap.get(objectDomainClass).entrySet()) {
				long id = entry.getKey();
				SortedMap<String, Object> loadedRecord = entry.getValue();
				boolean isNew = false;

				// Try to find registered domain object for loaded object record
				SqlDomainObject obj = sdc.find(objectDomainClass, id);
				if (obj == null) { // Object is new
					isNew = true;

					// Instantiate, register and initialize object
					obj = sdc.instantiate(objectDomainClass);
					sdc.registerById(obj, id);
					obj.setIsStored();
					obj.lastModifiedInDb = ((LocalDateTime) loadedRecord.get(Const.LAST_MODIFIED_COL));
				}

				// Find 'unresolved references' where parent object in database is not yet (loaded and) registered
				unresolvedReferences.addAll(findUnresolvedReferencesOnLoad(sdc, obj, loadedRecord));

				// Build map with changes in database in respect to current object
				SortedMap<String, Object> databaseChangesMap = new TreeMap<>();
				if (isNew) {
					databaseChangesMap = loadedRecord;
					newObjects.add(obj);
				}
				else { // Object is already registered

					// Collect changes (we assume that differences between object and database values found here can only be caused by changes in database made by another domain controller instance)
					SortedMap<String, Object> objectRecord = sdc.recordMap.get(objectDomainClass).get(id); // Current object record
					for (String col : loadedRecord.keySet()) {
						Object oldValue = objectRecord.get(col);
						Object newValue = loadedRecord.get(col);

						// Add column/value entry to changes map if current and loaded values differ - ignore last modified column; consider only logical changes
						if (!objectsEqual(col, Const.LAST_MODIFIED_COL) && !logicallyEqual(oldValue, newValue)) {
							if (log.isDebugEnabled()) {
								log.debug("SDC: Column {}: loaded value {} differs from current value {}", col, CLog.forSecretLogging(col, newValue), CLog.forSecretLogging(col, oldValue));
							}
							databaseChangesMap.put(col, newValue);
						}
					}

					// Check if object was changed in database
					if (!databaseChangesMap.isEmpty()) {
						log.info("SDC: Loaded record for '{}@{}' differs from current record. New values: {}", objectDomainClass.getSimpleName(), id,
								JdbcHelpers.forLoggingSqlResult(databaseChangesMap, columnNames));
						if (log.isTraceEnabled()) {
							log.info("SDC: Current object record: {}", JdbcHelpers.forLoggingSqlResult(objectRecord, columnNames));
						}

						databaseChangesMap.put(Const.LAST_MODIFIED_COL, loadedRecord.get(Const.LAST_MODIFIED_COL)); // Change last modification date if any logical change was detected
						changedObjects.add(obj);
					}
				}

				// Assign loaded data to corresponding fields of domain object and collect objects where references were changed
				assignDataToDomainObject(sdc, obj, isNew, databaseChangesMap, objectsWhereReferencesChanged);
				if (log.isTraceEnabled()) {
					log.trace("SDC: Loaded {}object '{}': {}", (isNew ? "new " : ""), obj.name(), JdbcHelpers.forLoggingSqlResult(loadedRecord, columnNames));
				}
				loadedObjects.add(obj);
			}
		}

		log.info("SDC: Loaded: #'s of new objects: {}, #'s of changed objects: {} (#'s of total loaded objects: {})", Helpers.groupCountsByDomainClassName(newObjects),
				Helpers.groupCountsByDomainClassName(changedObjects), Helpers.groupCountsByDomainClassName(loadedObjects));

		return (!changedObjects.isEmpty() || !newObjects.isEmpty()); // true if any changes in database were detected
	}

	// -------------------------------------------------------------------------
	// Unresolved references
	// -------------------------------------------------------------------------

	// Unresolved reference description
	static class UnresolvedReference {

		SqlDomainObject obj;
		Field refField;
		Class<? extends SqlDomainObject> parentDomainClass;
		long parentObjectId;

		public UnresolvedReference(
				SqlDomainController sdc,
				SqlDomainObject obj,
				Field refField,
				long parentObjectId) {

			this.obj = obj;
			this.refField = refField;
			this.parentDomainClass = sdc.registry.getCastedReferencedDomainClass(refField);
			this.parentObjectId = parentObjectId;
		}
	}

	// Check if object(s) referenced by this object in database are already (loaded and) registered and collect unresolved reference where this is not the case. Also re-register parent object(s) if
	// parent/child relation did not change but current parent object was unregistered
	private static <S extends SqlDomainObject> Set<UnresolvedReference> findUnresolvedReferencesOnLoad(SqlDomainController sdc, S obj, SortedMap<String, Object> loadedRecord) {

		Set<UnresolvedReference> unresolvedReferences = new HashSet<>();
		for (Class<? extends SqlDomainObject> domainClass : sdc.registry.getDomainClassesFor(obj.getClass())) {

			// Reference fields: assign new references immediately if referenced object exists - otherwise collect unresolved references
			for (Field refField : sdc.registry.getReferenceFields(domainClass)) {

				String foreignKeyColumnName = ((SqlRegistry) sdc.registry).getColumnFor(refField).name;
				Long parentObjectIdFromDatabase = (Long) loadedRecord.get(foreignKeyColumnName);
				SqlDomainObject parentObjectInHeap = (SqlDomainObject) obj.getFieldValue(refField);

				if (parentObjectIdFromDatabase != null) { // Parent object is defined in database

					// Check if parent object is already registered
					SqlDomainObject parentObjectFromDatabase = sdc.find(sdc.registry.getCastedReferencedDomainClass(refField), parentObjectIdFromDatabase);
					if (parentObjectFromDatabase == null) { // Parent object is not registered

						if (parentObjectInHeap != null && parentObjectInHeap.getId() == parentObjectIdFromDatabase) { // Parent object in heap equals parent object in database (but is not registered)
							// Atypical case: referenced object was unregistered (on synchronization by data horizon condition) when it was still not referenced - reference was established afterwards

							// Re-register parent object (using known id)
							log.warn("SDC: {} was unregistered before parent/child relation was established -> reregister object which now is parent of {}", parentObjectInHeap, obj);
							sdc.reregister(parentObjectInHeap);
						}
						else { // Parent/child relation changed in database (but new parent object is not just yet registered)
								// Collect unresolved reference to parent object (which will later be resolved by loading parent object from database)
							log.info("SDC: Parent {}@{} in database of {} is not yet registered - collect unresolved reference and later resolve it by loading parent object",
									domainClass.getSimpleName(), parentObjectIdFromDatabase, obj);
							unresolvedReferences.add(new UnresolvedReference(sdc, obj, refField, parentObjectIdFromDatabase));
						}
					}
					else { // Parent object from database is already registered
							// Nothing to do here - reference will later be changed if parent object in database differs from parent object in heap
					}
				}
				else { // No parent object (null reference) in database
						// Nothing to do here - reference will later be set to null if it is not already null
				}
			}
		}

		return unresolvedReferences;
	}

	// Determine object domain class of (missing) object given by id and derived domain class by loading object record
	private static Class<? extends SqlDomainObject> determineObjectDomainClass(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> domainClass, long id)
			throws SQLException, SqlDbException {

		log.info("SDC: Domain class '{}' is not an object domain class -> determine object domain class for missing object(s).", domainClass.getSimpleName());

		// Determine object domain class of missing referenced object by retrieving domain class name from loaded record for object record
		String tableName = ((SqlRegistry) sdc.registry).getTableFor(domainClass).name;
		List<SortedMap<String, Object>> records = sdc.sqlDb.selectFrom(cn, tableName, Const.DOMAIN_CLASS_COL, "ID=" + id, null, CList.newList(String.class));
		if (records.isEmpty()) {
			log.error("No record found for object {}@{} which is referenced and therefore should exist", domainClass.getSimpleName(), id);
			throw new SqlDbException("Could not determine referenced " + domainClass.getSimpleName() + "@" + id + " object's object domain class");
		}
		else {
			String objectDomainClassName = (String) records.get(0).get(Const.DOMAIN_CLASS_COL); // Assume JDBC type of column is String for String field
			log.info("SDC: Object domain class for {}@{} is: '{}'", domainClass.getSimpleName(), id, objectDomainClassName);
			return sdc.getDomainClassByName(objectDomainClassName);
		}
	}

	// Load records of objects which were not yet loaded but which are referenced by loaded objects
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadMissingObjects(Connection cn, SqlDomainController sdc, Set<UnresolvedReference> unresolvedReferences)
			throws SQLException, SqlDbException {

		// Collect missing objects - check if missing object was already loaded before (this happens on circular references)
		Map<Class<? extends SqlDomainObject>, Set<Long>> missingObjectIdsMap = new HashMap<>();
		for (UnresolvedReference ur : unresolvedReferences) {

			SqlDomainObject obj = sdc.find(ur.parentDomainClass, ur.parentObjectId);
			if (obj != null) {
				log.info("SDC: Missing object {} was already loaded after detecting unresolved reference and do not have to be loaded again (circular reference)", obj.name());
			}
			else {
				Class<? extends SqlDomainObject> objectDomainClass = ur.parentDomainClass;
				if (!sdc.registry.isObjectDomainClass(ur.parentDomainClass)) {
					objectDomainClass = determineObjectDomainClass(cn, sdc, ur.parentDomainClass, ur.parentObjectId);
				}
				missingObjectIdsMap.computeIfAbsent(objectDomainClass, l -> new HashSet<>()).add(ur.parentObjectId);
			}
		}

		// Load missing object records
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedMissingRecordsMap = new HashMap<>();
		for (Entry<Class<? extends SqlDomainObject>, Set<Long>> missingObjectsEntry : missingObjectIdsMap.entrySet()) {

			Class<? extends SqlDomainObject> objectDomainClass = missingObjectsEntry.getKey();
			Set<Long> missingObjectIds = missingObjectsEntry.getValue();
			log.info("SDC: Load {} missing '{}' object(s){}", missingObjectIds.size(), objectDomainClass.getSimpleName(), (missingObjectIds.size() <= 32 ? " " + missingObjectIds : "..."));

			// Build WHERE clause(s) with IDs and load missing objects
			Map<Long, SortedMap<String, Object>> collectedRecordMap = new HashMap<>();
			for (String idsList : Helpers.buildMax1000IdsLists(missingObjectIds)) {
				collectedRecordMap.putAll(retrieveRecordsFromDatabase(cn, sdc, 0, objectDomainClass, ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).name + ".ID IN (" + idsList + ")"));
			}
			loadedMissingRecordsMap.put(objectDomainClass, collectedRecordMap);
		}

		return loadedMissingRecordsMap;
	}

	// Resolve collected unresolved references by now loaded parent object
	static void resolveUnresolvedReferences(SqlDomainController sdc, Set<UnresolvedReference> unresolvedReferences) {

		for (UnresolvedReference ur : unresolvedReferences) {

			SqlDomainObject parentObj = sdc.find(ur.parentDomainClass, ur.parentObjectId);
			if (parentObj == null) {
				log.error("SDC: Referenced object {}@{} for resolving unresolved reference not found (but should exist)", ur.parentDomainClass.getSimpleName(), ur.parentObjectId);
			}
			else {
				ur.obj.setFieldValue(ur.refField, parentObj);
				if (log.isTraceEnabled()) {
					log.trace("SDC: Resolve reference of '{}' to parent '{}' after deferred loading of parent", DomainObject.name(ur.obj), DomainObject.name(parentObj));
				}
			}
		}
		if (!CCollection.isEmpty(unresolvedReferences)) {
			log.info("SDC: Resolved {} references which remained unresolved during last load cycle.", unresolvedReferences.size());
		}
	}

	// -------------------------------------------------------------------------
	// Load objects assuring referential integrity
	// -------------------------------------------------------------------------

	// Load objects from database using SELECT supplier, finalize these objects and load and load missing referenced objects in a loop to ensure referential integrity.
	static boolean loadAssuringReferentialIntegrity(SqlDomainController sdc, Function<Connection, Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>>> select,
			Set<SqlDomainObject> loadedObjects) throws SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, SqlDbException {

		// Get database connection from pool
		try (SqlConnection sqlcn = SqlConnection.open(sdc.sqlDb.pool, true)) {

			// Initially load object records using given select-supplier
			Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap = select.apply(sqlcn.cn);

			// Instantiate newly loaded objects, assign changed data and references to objects, collect initially unresolved references
			Set<SqlDomainObject> objectsWhereReferencesChanged = new HashSet<>();
			Set<UnresolvedReference> unresolvedReferences = new HashSet<>();
			boolean hasChanges = buildObjectsFromLoadedRecords(sdc, loadedRecordsMap, loadedObjects, objectsWhereReferencesChanged, unresolvedReferences);

			// Cyclicly load and instantiate missing referenced objects and detect unresolved references on these objects
			int c = 1;
			Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> missingRecordsMap;
			Set<UnresolvedReference> furtherUnresolvedReferences = new HashSet<>();
			while (!unresolvedReferences.isEmpty()) {

				log.info("SDC: There were in total {} unresolved reference(s) of {} objects referenced by {} objects detected in {}. load cycle", unresolvedReferences.size(),
						unresolvedReferences.stream().map(ur -> ur.refField.getType().getSimpleName()).distinct().collect(Collectors.toList()),
						unresolvedReferences.stream().map(ur -> ur.obj.getClass().getSimpleName()).distinct().collect(Collectors.toList()), c++);

				// Load and instantiate missing objects of unresolved references
				missingRecordsMap = loadMissingObjects(sqlcn.cn, sdc, unresolvedReferences);

				// Add loaded records of missed object to total loaded object records
				for (Entry<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> entry : missingRecordsMap.entrySet()) {
					loadedRecordsMap.computeIfAbsent(entry.getKey(), odc -> new HashMap<>()).putAll(entry.getValue());
				}

				// Initialize missed objects and determine further unresolved references
				furtherUnresolvedReferences.clear();
				hasChanges |= buildObjectsFromLoadedRecords(sdc, missingRecordsMap, loadedObjects, objectsWhereReferencesChanged, furtherUnresolvedReferences);

				// Resolve unresolved references of last cycle after missing objects were instantiated
				resolveUnresolvedReferences(sdc, unresolvedReferences);
				unresolvedReferences = furtherUnresolvedReferences;
			}

			// Update accumulations of all objects which are referenced by any of the objects where references changed
			objectsWhereReferencesChanged.forEach(o -> sdc.updateAccumulationsOfParentObjects(o));

			return hasChanges;
		}
	}

}
