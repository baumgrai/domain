package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
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

import com.icx.common.base.CCollection;
import com.icx.common.base.CDateTime;
import com.icx.common.base.CList;
import com.icx.common.base.CLog;
import com.icx.common.base.CMap;
import com.icx.common.base.Common;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

/**
 * Helpers for loading domain objects from database
 * 
 * @author baumgrai
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
		String orderByClause = null;
	}

	// Build joined table expression for all inherited domain classes and also build name list of columns to retrieve
	private static SelectDescription buildSelectDescriptionForMainObjectRecords(SqlRegistry sqlRegistry, Class<? extends SqlDomainObject> objectDomainClass) {

		// Build table and column expression for object domain class
		SelectDescription sd = new SelectDescription();
		SqlDbTable objectTable = sqlRegistry.getTableFor(objectDomainClass);
		sd.joinedTableExpression = objectTable.name;
		sd.allColumnNames.addAll(objectTable.columns.stream().map(c -> objectTable.name + "." + c.name).collect(Collectors.toList()));

		// Extend table and column expression for inherited domain classes
		Predicate<Column> isNonStandardColumnPredicate = c -> !objectsEqual(c.name, Const.ID_COL) && !objectsEqual(c.name, Const.DOMAIN_CLASS_COL);
		Class<? extends SqlDomainObject> derivedDomainClass = sqlRegistry.getCastedSuperclass(objectDomainClass);
		while (derivedDomainClass != SqlDomainObject.class) {

			SqlDbTable inheritedTable = sqlRegistry.getTableFor(derivedDomainClass);
			sd.joinedTableExpression += " JOIN " + inheritedTable.name + " ON " + inheritedTable.name + ".ID=" + objectTable.name + ".ID";
			sd.allColumnNames.addAll(inheritedTable.columns.stream().filter(isNonStandardColumnPredicate).map(c -> inheritedTable.name + "." + c.name).collect(Collectors.toList()));

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

		Class<?> fieldClass = complexField.getType();

		if (Collection.class.isAssignableFrom(fieldClass)) {

			// Column for elements of collection
			sde.allColumnNames.add(Const.ELEMENT_COL);
			if (List.class.isAssignableFrom(fieldClass)) {

				// Column for list order and ORDER BY clause
				sde.allColumnNames.add(Const.ORDER_COL);
				sde.orderByClause = Const.ORDER_COL;
			}
		}
		else {
			// Column for keys and values of map
			sde.allColumnNames.add(Const.KEY_COL);
			sde.allColumnNames.add(Const.VALUE_COL);
		}
		return sde;
	}

	// Load object records for one object domain class - means one record per object, containing data of all tables associated with object domain class according inheritance
	// e.g. class Racebike extends Bike -> tables [ DOM_BIKE, DOM_RACEBIKE ])
	static Map<Long, SortedMap<String, Object>> retrieveRecordsFromDatabase(Connection cn, SqlDomainController sdc, int limit, Class<? extends SqlDomainObject> objectDomainClass, String whereClause,
			String syncWhereClause) {

		String whereClauseIncludingSyncCondition = whereClause;
		if (!isEmpty(syncWhereClause)) {
			whereClauseIncludingSyncCondition = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "") + syncWhereClause;
		}

		Map<Long, SortedMap<String, Object>> loadedRecordMap = new HashMap<>();
		try {
			// Load (main) object records and build up loaded records by id map
			SelectDescription sd = buildSelectDescriptionForMainObjectRecords(((SqlRegistry) sdc.registry), objectDomainClass);
			List<SortedMap<String, Object>> loadedRecords = sdc.sqlDb.selectFrom(cn, sd.joinedTableExpression, sd.allColumnNames, whereClauseIncludingSyncCondition, null, limit);
			if (CList.isEmpty(loadedRecords)) {
				return loadedRecordMap;
			}
			for (SortedMap<String, Object> rec : loadedRecords) {
				loadedRecordMap.put(((Number) rec.get(Const.ID_COL)).longValue(), rec);
			}

			// Load entry records for all complex (table related) fields and assign them to main object records using entry table name as key
			String objectTableName = ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).name;
			for (Class<?> domainClass : sdc.registry.getDomainClassesFor(objectDomainClass)) {

				// For all table related fields...
				for (Field complexField : sdc.registry.getComplexFields(sdc.registry.castDomainClass(domainClass))) {

					// Build table expression, column names and order-by clause to SELECT entry records
					SelectDescription sde = buildSelectDescriptionForEntryRecords(((SqlRegistry) sdc.registry), sd.joinedTableExpression, objectTableName, complexField);

					// Load entry records (do not include sync condition in this secondary WHERE clause to ensure correct data loading even if another thread allocated object exclusively)
					List<SortedMap<String, Object>> loadedEntryRecords = new ArrayList<>();
					if (limit > 0) {

						// If # of loaded object records is limited SELECT only entry records for actually loaded object records
						String whereClauseBase = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "");
						for (String idsList : Helpers.buildIdsListsWithMaxIdCount(loadedRecordMap.keySet(), 1000)) { // Oracle limitation max 1000 elements in lists
							String idListWhereClause = whereClauseBase + objectTableName + ".ID IN (" + idsList + ")";
							loadedEntryRecords.addAll(sdc.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, idListWhereClause, sde.orderByClause, 0));
						}
					}
					else {
						// SELECT all entry records
						loadedEntryRecords.addAll(sdc.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, 0));
					}

					// Group entry records by objects where they belong to
					Map<Long, List<SortedMap<String, Object>>> entryRecordsByObjectIdMap = new HashMap<>();
					String refIdColumnName = ((SqlRegistry) sdc.registry).getMainTableRefIdColumnFor(complexField).name;
					Set<String> missingObjectNames = new HashSet<>();
					for (SortedMap<String, Object> entryRecord : loadedEntryRecords) {

						// Ignore entry record if main object record (referenced by object id) is not present
						long objectId = ((Number) entryRecord.get(refIdColumnName)).longValue();
						if (!loadedRecordMap.containsKey(objectId)) {
							missingObjectNames.add(domainClass.getSimpleName() + "@" + objectId);
							continue;
						}

						// Add entry record to entry records for object
						entryRecordsByObjectIdMap.computeIfAbsent(objectId, m -> new ArrayList<>()).add(entryRecord);
					}

					if (log.isDebugEnabled() && !missingObjectNames.isEmpty()) {
						log.debug("SDC: Ignore loaded entry records for objects {} where main record was not loaded before!", missingObjectNames);
					}

					// Build collection or map from entry records and add it to loaded object record with entry table name as key
					// (perform table entries -> collection/map conversion here and not later during assignment of values to objects)
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
			Map<Long, SortedMap<String, Object>> loadedRecordsMap = retrieveRecordsFromDatabase(cn, sdc, 0, objectDomainClass, whereClause, null);
			if (!CMap.isEmpty(loadedRecordsMap)) {
				loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);
			}
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// Select supplier for loading objects of given object domain class which records in database match given WHERE clause (usage needs knowledge about Java -> SQL mapping)
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> select(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> objectDomainClass,
			String whereClause, int maxCount) {

		// Try to SELECT object records FOR UPDATE
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = LoadHelpers.retrieveRecordsFromDatabase(cn, sdc, maxCount, objectDomainClass, whereClause, null);
		if (CMap.isEmpty(loadedRecordsMap)) {
			return Collections.emptyMap();
		}

		// Build map to return from supplier method
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);
		return loadedRecordsMapByDomainClassMap;
	}

	// Create object with given id - only used for exclusive selection methods
	private static <S extends SqlDomainObject> S createWithId(SqlDomainController sdc, Class<S> domainObjectClass, long id) {

		S obj = sdc.instantiate(domainObjectClass);
		if (obj != null) {
			if (!sdc.registerById(obj, id)) { // An object of this domain class already exists and is registered
				return null;
			}
			if (log.isDebugEnabled()) {
				log.debug("SDC: Created {} with given id", obj.name());
			}
		}
		return obj;
	}

	public static long successfulExclusiveAccessCount = 0L;
	public static long inUseBySameInstanceAccessCount = 0L;
	public static long inUseByDifferentInstanceAccessCount = 0L;

	// Select supplier used for synchronization if multiple instances access one database and have to process distinct objects (like orders)
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> selectExclusively(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> objectDomainClass,
			Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount) {

		// Build WHERE clause
		String objectTableName = ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).name;
		String inProgressTableName = ((SqlRegistry) sdc.registry).getTableFor(inProgressClass).name;
		String syncWhereClause = objectTableName + ".ID NOT IN (SELECT ID FROM " + inProgressTableName + ")";

		// SELECT object records
		Map<Long, SortedMap<String, Object>> rawRecordsMap = LoadHelpers.retrieveRecordsFromDatabase(cn, sdc, maxCount, objectDomainClass, whereClause, syncWhereClause);
		if (CMap.isEmpty(rawRecordsMap)) {
			return Collections.emptyMap();
		}

		// Try to create and INSERT in-progress record with same id as records found (works only for one in-progress record per record found because of UNIQUE constraint for ID field) - provide only
		// records where in-progress record could be inserted
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = new HashMap<>();
		for (Entry<Long, SortedMap<String, Object>> entry : rawRecordsMap.entrySet()) {

			long id = entry.getKey();
			try {
				SqlDomainObject inProgressObject = createWithId(sdc, inProgressClass, id);
				if (inProgressObject != null) {
					sdc.save(cn, inProgressObject);
					loadedRecordsMap.put(id, entry.getValue());
					successfulExclusiveAccessCount++;
				}
				else {
					if (log.isDebugEnabled()) {
						log.debug("SDC: {}@{} is already in progress (by another thread of same controller instance)", objectDomainClass.getSimpleName(), id);
					}
					inUseBySameInstanceAccessCount++;
				}
			}
			catch (SQLException sqlex) {
				if (log.isDebugEnabled()) {
					log.info("SDC: {} record with id {} is already in progress (by another instance)", objectDomainClass.getSimpleName(), id);
				}
				inUseByDifferentInstanceAccessCount++;
			}
			catch (SqlDbException sqldbex) {
				log.error("SDC: {} occurred trying to INSERT {} record", sqldbex, inProgressClass.getSimpleName());
			}
		}

		// Build map to return from supplier method
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);

		return loadedRecordsMapByDomainClassMap;
	}

	// SELECT record(s) for specific object from database and build domain object record for this object - returns empty record map if object could not be loaded
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> selectObjectRecord(Connection cn, SqlDomainController sdc, SqlDomainObject obj) {

		String idWhereClause = ((SqlRegistry) sdc.registry).getTableFor(obj.getClass()).name + "." + Const.ID_COL + "=" + obj.getId();
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		Map<Long, SortedMap<String, Object>> loadedRecordsMap = LoadHelpers.retrieveRecordsFromDatabase(cn, sdc, 0, obj.getClass(), idWhereClause, null);
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
		Number parentObjectIdNumber = (Number) sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(foreignKeyColumnName);
		Long parentObjectIdFromLocalObjectRecord = (parentObjectIdNumber != null ? parentObjectIdNumber.longValue() : null);

		if (parentObj == null && parentObjectIdFromLocalObjectRecord != null) {
			log.warn("SDC: Reference field '{}' of object '{}' has unsaved null reference which will be overridden by reference to '{}@{}' from database!", refField.getName(), obj.name(),
					refField.getType().getSimpleName(), parentObjectIdFromDatabase);
			obj.setFieldWarning(refField, "Discarded unsaved changed null reference of field '" + refField.getName() + "' on loading object from database");
		}
		else if (parentObj != null && (parentObjectIdFromLocalObjectRecord == null || parentObj.getId() != parentObjectIdFromLocalObjectRecord)) {
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
		Object fieldValueFromObjectRecord = Conversion.column2FieldValue(dataField.getType(), columnValueFromObjectRecord);

		if (!objectsEqual(fieldValue, fieldValueFromObjectRecord)) {
			log.warn("SDC: Data field '{}' of object '{}' has unsaved changed value {} which will be overridden by value {} from database!", dataField.getName(), obj.name(),
					CLog.forSecretLogging(dataField, fieldValue), CLog.forSecretLogging(dataField, columnValueFromDatabase));
			obj.setFieldWarning(dataField,
					"Discarded unsaved changed value " + CLog.forSecretLogging(dataField, fieldValue) + " of field '" + dataField.getName() + "' on loading object from database");
		}
	}

	// Check if object has unsaved changes in a collection or map
	private static void assignFieldWarningOnUnsavedComplexFieldChange(SqlDomainController sdc, SqlDomainObject obj, Field complexField, String entryTableName, Object collectionOrMapFromObject,
			boolean isCollection) {

		if (isCollection) { // Collection
			Collection<?> colFromObjRecord = (Collection<?>) sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);
			if (!objectsEqual(collectionOrMapFromObject, colFromObjRecord)) {
				log.warn("SDC: Collection field '{}' of object '{}' {} has unsaved changes and will be overridden by collection {} from database!", complexField.getName(), obj.name(),
						CLog.forSecretLogging(complexField, collectionOrMapFromObject), CLog.forSecretLogging(complexField, colFromObjRecord));
				obj.setFieldWarning(complexField,
						"Discarded unsaved changed collection " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading collection from database");
			}
		}
		else { // Map
			Map<?, ?> mapFromObjRecord = (Map<?, ?>) sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);
			if (!objectsEqual(collectionOrMapFromObject, mapFromObjRecord)) {
				log.warn("SDC: Key/value map field '{}' of object '{}' {} has unsaved changes and will be overridden by map {} from database!", complexField.getName(), obj.name(),
						CLog.forSecretLogging(complexField, collectionOrMapFromObject), CLog.forSecretLogging(complexField, mapFromObjRecord));
				obj.setFieldWarning(complexField, "Discarded unsaved changed key/value map " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading map from database");
			}
		}
	}

	// Assign changed data in record from database to corresponding fields of domain object - check for unsaved changes before, which will then be discarded
	@SuppressWarnings("unchecked")
	private static void assignDataToDomainObject(SqlDomainController sdc, SqlDomainObject obj, boolean isNew, SortedMap<String, Object> databaseChangesMap,
			Set<SqlDomainObject> objectsWhereReferencesChanged, List<UnresolvedReference> unresolvedReferences) {

		// Assign loaded data to fields for all inherited domain classes of domain object
		for (Class<? extends SqlDomainObject> domainClass : sdc.registry.getDomainClassesFor(obj.getClass())) {

			// Reference fields: assign new references immediately if referenced object exists - otherwise collect unresolved references
			for (Field refField : sdc.registry.getReferenceFields(domainClass)) {
				String foreignKeyColumnName = ((SqlRegistry) sdc.registry).getColumnFor(refField).name;

				boolean hasValueChanged = databaseChangesMap.containsKey(foreignKeyColumnName);
				Number parentIdNumber = (Number) databaseChangesMap.get(foreignKeyColumnName);
				SqlDomainObject currentParentObject = (SqlDomainObject) obj.getFieldValue(refField);

				Long parentObjectId = (hasValueChanged ? (parentIdNumber != null ? parentIdNumber.longValue() : null) : currentParentObject != null ? currentParentObject.getId() : null);

				if (hasValueChanged && !isNew) {
					assignFieldWarningOnUnsavedReferenceChange(sdc, obj, refField, foreignKeyColumnName, parentObjectId);
				}

				if (parentObjectId == null) { // Null reference
					if (hasValueChanged) {
						obj.setFieldValue(refField, null);
					}
				}
				else { // Object reference

					// Check if referenced object is registered (already loaded on change in database)
					SqlDomainObject parentObject = sdc.find(sdc.registry.getCastedReferencedDomainClass(refField), parentObjectId);
					if (parentObject != null) { // Referenced object is registered (already loaded on change in database)
						if (hasValueChanged) {
							obj.setFieldValue(refField, parentObject);
						}
					}
					else { // Referenced object is not registered
						if (hasValueChanged) { // Reference was changed in database

							// Referenced object is still not loaded (data horizon or circular reference) -> collect unresolved reference
							obj.setFieldValue(refField, null); // Temporarily reset reference - this will be set to referenced object after loading this
							unresolvedReferences.add(new UnresolvedReference(sdc, obj, refField, parentObjectId));
						}
						else { // Reference is unchanged (but referenced object is not registered anymore)

							// Referenced object was unregistered (may be due to data horizon condition) -> re-register parent object to assure referential integrity again
							sdc.reregister(currentParentObject);
						}
					}
				}
				objectsWhereReferencesChanged.add(obj); // Collect objects where references changed for subsequent update of accumulations
			}

			// Data fields: assign - potentially converted - values
			Predicate<Field> hasValueChangedPredicate = (f -> databaseChangesMap.containsKey(((SqlRegistry) sdc.registry).getColumnFor(f).name));
			for (Field dataField : sdc.registry.getDataFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				String columnName = ((SqlRegistry) sdc.registry).getColumnFor(dataField).name;
				Object columnValueFromDatabase = databaseChangesMap.get(columnName);

				if (!isNew) {
					assignFieldWarningOnUnsavedValueChange(sdc, obj, dataField, columnName, columnValueFromDatabase /* only for logging */);
				}
				Object fieldValue = Conversion.column2FieldValue(dataField.getType(), columnValueFromDatabase);

				// Set value for field
				obj.setFieldValue(dataField, fieldValue);

				// Replace loaded column value by field value in database changes map - which will be used to update object record
				databaseChangesMap.put(columnName, fieldValue);
			}

			// Complex (table related) fields: set field values of object to collection or map (conversion from entry table record was already done directly after loading entry records)
			Predicate<Field> hasEntriesChangedPredicate = (f -> databaseChangesMap.containsKey(((SqlRegistry) sdc.registry).getEntryTableFor(f).name));
			for (Field complexField : sdc.registry.getComplexFields(domainClass).stream().filter(hasEntriesChangedPredicate).collect(Collectors.toList())) {

				String entryTableName = ((SqlRegistry) sdc.registry).getEntryTableFor(complexField).name;
				boolean isCollection = Collection.class.isAssignableFrom(complexField.getType());
				Object collectionOrMapFromObject = obj.getFieldValue(complexField);

				if (!isNew) {
					assignFieldWarningOnUnsavedComplexFieldChange(sdc, obj, complexField, entryTableName, collectionOrMapFromObject, isCollection);
				}

				if (isCollection) { // Collection
					Collection<Object> colFromObject = (Collection<Object>) collectionOrMapFromObject;
					colFromObject.clear();
					colFromObject.addAll((Collection<Object>) databaseChangesMap.get(entryTableName));
				}
				else { // Map
					Map<Object, Object> mapFromObject = (Map<Object, Object>) collectionOrMapFromObject;
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
			Set<SqlDomainObject> loadedObjects, Set<SqlDomainObject> objectsWhereReferencesChanged, List<UnresolvedReference> unresolvedReferences) {

		if (!CMap.isEmpty(loadedRecordsMap) && log.isTraceEnabled()) {
			log.trace("SDC: Build objects from loaded records...");
		}

		Set<SqlDomainObject> newObjects = new HashSet<>(); // Only for logging
		Set<SqlDomainObject> changedObjects = new HashSet<>();

		// Filter object domain classes where records were loaded for and handle loaded objects in order of parent/child relationship to avoid unnecessary unresolved references
		List<Class<? extends SqlDomainObject>> relevantObjectDomainClasses = sdc.registry.getRegisteredObjectDomainClasses().stream().filter(loadedRecordsMap::containsKey)
				.collect(Collectors.toList());
		for (Class<? extends SqlDomainObject> objectDomainClass : relevantObjectDomainClasses) {

			// Determine table and column name association - only for secret logging
			List<String> columnNames = new ArrayList<>();
			Map<String, String> columnTableMap = new HashMap<>();
			for (Class<? extends SqlDomainObject> domainClass : sdc.registry.getDomainClassesFor(objectDomainClass)) {
				SqlDbTable table = ((SqlRegistry) sdc.registry).getTableFor(domainClass);
				for (Column column : table.columns) {
					columnNames.add(column.name);
					columnTableMap.put(column.name, table.name);
				}
			}

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
							databaseChangesMap.put(col, newValue);
						}
					}

					// Check if object was changed in database
					if (!databaseChangesMap.isEmpty()) {

						if (log.isDebugEnabled()) {
							log.debug("SDC: Loaded record for '{}@{}' differs from current record. New values: {}", objectDomainClass.getSimpleName(), id,
									JdbcHelpers.forSecretLoggingRecord(databaseChangesMap, columnNames, columnTableMap));
							if (log.isTraceEnabled()) {
								log.trace("SDC: Current object record: {}", JdbcHelpers.forSecretLoggingRecord(objectRecord, columnNames, columnTableMap));
							}
						}

						databaseChangesMap.put(Const.LAST_MODIFIED_COL, loadedRecord.get(Const.LAST_MODIFIED_COL)); // Change last modification date if any logical change was detected
						changedObjects.add(obj);
					}
				}

				// Assign loaded data to corresponding fields of domain object and collect objects where references were changed
				assignDataToDomainObject(sdc, obj, isNew, databaseChangesMap, objectsWhereReferencesChanged, unresolvedReferences);
				if (log.isTraceEnabled()) {
					log.trace("SDC: Loaded {}object '{}': {}", (isNew ? "new " : ""), obj.name(), JdbcHelpers.forSecretLoggingRecord(loadedRecord, columnNames, columnTableMap));
				}
				loadedObjects.add(obj);
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("SDC: Loaded: #'s of new objects: {}, #'s of changed objects: {} (#'s of total loaded objects: {})", Helpers.groupCountsByDomainClassName(newObjects),
					Helpers.groupCountsByDomainClassName(changedObjects), Helpers.groupCountsByDomainClassName(loadedObjects));
		}

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

	// Determine object domain class of (missing) object given by id and derived domain class by loading object record
	private static Class<? extends SqlDomainObject> determineObjectDomainClass(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> domainClass, long id)
			throws SQLException, SqlDbException {

		if (log.isDebugEnabled()) {
			log.debug("SDC: Domain class '{}' is not an object domain class -> determine object domain class for missing object(s).", domainClass.getSimpleName());
		}

		// Determine object domain class of missing referenced object by retrieving domain class name from loaded record for object record
		String tableName = ((SqlRegistry) sdc.registry).getTableFor(domainClass).name;
		List<SortedMap<String, Object>> records = sdc.sqlDb.selectFrom(cn, tableName, Const.DOMAIN_CLASS_COL, "ID=" + id, null, 0);
		if (records.isEmpty()) {
			log.error("No record found for object {}@{} which is referenced and therefore should exist", domainClass.getSimpleName(), id);
			throw new SqlDbException("Could not determine referenced " + domainClass.getSimpleName() + "@" + id + " object's object domain class");
		}
		else {
			String objectDomainClassName = (String) records.get(0).get(Const.DOMAIN_CLASS_COL); // Assume JDBC type of column is String for String field
			if (log.isDebugEnabled()) {
				log.debug("SDC: Object domain class for {}@{} is: '{}'", domainClass.getSimpleName(), id, objectDomainClassName);
			}
			return sdc.getDomainClassByName(objectDomainClassName);
		}
	}

	// Load records of objects which were not yet loaded but which are referenced by loaded objects
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadMissingObjects(Connection cn, SqlDomainController sdc, List<UnresolvedReference> unresolvedReferences)
			throws SQLException, SqlDbException {

		// Collect missing objects - check if missing object was already loaded before (this happens on circular references)
		Map<Class<? extends SqlDomainObject>, Set<Long>> missingObjectIdsMap = new HashMap<>();
		for (UnresolvedReference ur : unresolvedReferences) {

			SqlDomainObject obj = sdc.find(ur.parentDomainClass, ur.parentObjectId);
			if (obj != null) {
				if (log.isDebugEnabled()) {
					log.debug("SDC: {} was already loaded after detecting unresolved reference (circular reference)", obj.name());
				}
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
			if (log.isDebugEnabled()) {
				log.debug("SDC: Load {} missing '{}' object(s){}", missingObjectIds.size(), objectDomainClass.getSimpleName(), (missingObjectIds.size() <= 32 ? " " + missingObjectIds : "..."));
			}

			// Build WHERE clause(s) with IDs and load missing objects
			Map<Long, SortedMap<String, Object>> collectedRecordMap = new HashMap<>();
			String tableName = ((SqlRegistry) sdc.registry).getTableFor(objectDomainClass).name;
			for (String idsList : Helpers.buildIdsListsWithMaxIdCount(missingObjectIds, 1000)) { // Oracle limitation max 1000 elements in lists
				String idListWhereClause = tableName + ".ID IN (" + idsList + ")";
				collectedRecordMap.putAll(retrieveRecordsFromDatabase(cn, sdc, 0, objectDomainClass, idListWhereClause, null));
			}
			loadedMissingRecordsMap.put(objectDomainClass, collectedRecordMap);
		}

		return loadedMissingRecordsMap;
	}

	// Resolve collected unresolved references by now loaded parent object
	static void resolveUnresolvedReferences(SqlDomainController sdc, List<UnresolvedReference> unresolvedReferences) {

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
		if (!CCollection.isEmpty(unresolvedReferences) && (log.isDebugEnabled())) {
			log.debug("SDC: Resolved {} references which remained unresolved during last load cycle.", unresolvedReferences.size());
		}
	}

	// -------------------------------------------------------------------------
	// Load objects assuring referential integrity
	// -------------------------------------------------------------------------

	// Load objects from database using SELECT supplier, finalize these objects and load and load missing referenced objects in a loop to ensure referential integrity.
	static boolean loadAssuringReferentialIntegrity(SqlDomainController sdc, Function<Connection, Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>>> select,
			Set<SqlDomainObject> loadedObjects) throws SQLException, SqlDbException {

		// Get database connection from pool
		try (SqlConnection sqlcn = SqlConnection.open(sdc.sqlDb.pool, true)) {

			// Initially load object records using given select-supplier
			Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap = select.apply(sqlcn.cn);

			// Instantiate newly loaded objects, assign changed data and references to objects, collect initially unresolved references
			Set<SqlDomainObject> objectsWhereReferencesChanged = new HashSet<>();
			List<UnresolvedReference> unresolvedReferences = new ArrayList<>();
			boolean hasChanges = buildObjectsFromLoadedRecords(sdc, loadedRecordsMap, loadedObjects, objectsWhereReferencesChanged, unresolvedReferences);

			// Cyclicly load and instantiate missing referenced objects and detect unresolved references on these objects
			int c = 1;
			Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> missingRecordsMap;
			List<UnresolvedReference> furtherUnresolvedReferences = new ArrayList<>();
			while (!unresolvedReferences.isEmpty()) {

				if (log.isDebugEnabled()) {
					log.debug("SDC: There were in total {} unresolved reference(s) of {} objects referenced by {} objects detected in {}. load cycle", unresolvedReferences.size(),
							unresolvedReferences.stream().map(ur -> ur.refField.getType().getSimpleName()).distinct().collect(Collectors.toList()),
							unresolvedReferences.stream().map(ur -> ur.obj.getClass().getSimpleName()).distinct().collect(Collectors.toList()), c++);
				}

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
			objectsWhereReferencesChanged.forEach(sdc::updateAccumulationsOfParentObjects);

			return hasChanges;
		}
	}

}
