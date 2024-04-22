package com.icx.domain.sql;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.AESCrypt;
import com.icx.common.CCollection;
import com.icx.common.CDateTime;
import com.icx.common.CFile;
import com.icx.common.CList;
import com.icx.common.CLog;
import com.icx.common.CMap;
import com.icx.common.Common;
import com.icx.domain.DomainObject;
import com.icx.domain.sql.Annotations.Crypt;
import com.icx.jdbc.SqlDbException;
import com.icx.jdbc.SqlDbHelpers;
import com.icx.jdbc.SqlDbTable;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;

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
		Predicate<SqlDbColumn> isNonStandardColumnPredicate = c -> !objectsEqual(c.name, Const.ID_COL) && !objectsEqual(c.name, Const.DOMAIN_CLASS_COL);
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
	private static SelectDescription buildSelectDescriptionForEntryRecords(SqlRegistry sqlRegistry, String baseTableExpression, String objectTableName, Field complexField) throws SqlDbException {

		// Build table and column clause for entry records - join entry table and main object table
		SelectDescription sde = new SelectDescription();
		String entryTableName = sqlRegistry.getEntryTableFor(complexField).name;
		String refIdColumnName = sqlRegistry.getMainTableRefIdColumnFor(complexField).name;
		sde.joinedTableExpression = entryTableName + " JOIN " + baseTableExpression + " ON " + entryTableName + "." + refIdColumnName + "=" + objectTableName + ".ID";
		sde.allColumnNames.add(entryTableName + "." + refIdColumnName); // Column referencing main table for domain class

		Class<?> fieldClass = complexField.getType();

		if (Collection.class.isAssignableFrom(fieldClass) || fieldClass.isArray()) {

			// Column for elements of collection
			sde.allColumnNames.add(entryTableName + "." + Const.ELEMENT_COL);
			if (List.class.isAssignableFrom(fieldClass) || fieldClass.isArray()) {

				// Column for list order and ORDER BY clause
				sde.allColumnNames.add(entryTableName + "." + Const.ORDER_COL);
				sde.orderByClause = Const.ORDER_COL;
			}
		}
		else if (Map.class.isAssignableFrom(fieldClass)) {

			// Column for keys and values of map
			sde.allColumnNames.add(entryTableName + "." + Const.KEY_COL);
			sde.allColumnNames.add(entryTableName + "." + Const.VALUE_COL);
		}
		else {
			throw new SqlDbException("Complex field '" + complexField + "' is neither array nor collection nor map!");
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
			SelectDescription sd = buildSelectDescriptionForMainObjectRecords(sdc.getSqlRegistry(), objectDomainClass);
			List<SortedMap<String, Object>> loadedRecords = sdc.sqlDb.selectFrom(cn, sd.joinedTableExpression, sd.allColumnNames, whereClauseIncludingSyncCondition, null, limit, null);
			if (CList.isEmpty(loadedRecords)) {
				return loadedRecordMap;
			}
			for (SortedMap<String, Object> rec : loadedRecords) {
				loadedRecordMap.put(((Number) rec.get(Const.ID_COL)).longValue(), rec);
			}

			// Load entry records for all complex (table related) fields and assign them to main object records using entry table name as key
			String objectTableName = sdc.getSqlRegistry().getTableFor(objectDomainClass).name;
			for (Class<?> domainClass : sdc.getRegistry().getDomainClassesFor(objectDomainClass)) {

				// For all table related fields...
				for (Field complexField : sdc.getRegistry().getComplexFields(sdc.getRegistry().castDomainClass(domainClass))) {

					// Build table expression, column names and order-by clause to SELECT entry records
					SelectDescription sde = buildSelectDescriptionForEntryRecords(sdc.getSqlRegistry(), sd.joinedTableExpression, objectTableName, complexField);

					// Load entry records (do not include sync condition in this secondary WHERE clause to ensure correct data loading even if another thread allocated object exclusively)
					List<SortedMap<String, Object>> loadedEntryRecords = new ArrayList<>();
					if (limit > 0) {

						// If # of loaded object records is limited SELECT only entry records for actually loaded object records
						String whereClauseBase = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "");
						for (String idsList : Helpers.buildStringLists(loadedRecordMap.keySet(), 1000)) { // Oracle limitation max 1000 elements in lists
							String idListWhereClause = whereClauseBase + objectTableName + ".ID IN (" + idsList + ")";
							loadedEntryRecords.addAll(sdc.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, idListWhereClause, sde.orderByClause, 0, null));
						}
					}
					else {
						// SELECT all entry records
						loadedEntryRecords.addAll(sdc.sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, 0, null));
					}

					// Group entry records by objects where they belong to
					Map<Long, List<SortedMap<String, Object>>> entryRecordsByObjectIdMap = new HashMap<>();
					String refIdColumnName = sdc.getSqlRegistry().getMainTableRefIdColumnFor(complexField).name;
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
					String entryTableName = sdc.getSqlRegistry().getEntryTableFor(complexField).name;
					if (complexField.getType().isArray()) {
						for (long objectId : entryRecordsByObjectIdMap.keySet()) {
							Object array = Array.newInstance(complexField.getType().getComponentType(), entryRecordsByObjectIdMap.get(objectId).size());
							int r = 0;
							for (SortedMap<String, Object> entryRecord : entryRecordsByObjectIdMap.get(objectId)) {
								Array.set(array, r++, entryRecord.get(Const.ELEMENT_COL)); // Element of array cannot be a collection or map itself! (not supported)
							}

							loadedRecordMap.get(objectId).put(entryTableName, array);
						}
					}
					else {
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
		for (Class<? extends SqlDomainObject> objectDomainClass : sdc.getRegistry().getRegisteredObjectDomainClasses()) {
			if (domainClassesToExclude != null && Stream.of(domainClassesToExclude).anyMatch(objectDomainClass::isAssignableFrom)) { // Ignore objects of excluded domain classes
				continue;
			}

			// For data horizon controlled object domain classes build WHERE clause for data horizon control
			String whereClause = (sdc.getRegistry().isDataHorizonControlled(objectDomainClass) ? Const.LAST_MODIFIED_COL + ">=" + dataHorizon : null);

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

	/**
	 * Informative: counter for successfully exclusively accessed objects since startup.
	 */
	public static long successfulExclusiveAccessCount = 0L;

	/**
	 * Informative: counter for exclusive access collisions caused by concurrent access tries of same domain controller instance since startup.
	 */
	public static long inUseBySameInstanceAccessCount = 0L;

	/**
	 * Informative: counter for exclusive access collisions caused by concurrent access tries of different domain controller instances since startup.
	 */
	public static long inUseByDifferentInstanceAccessCount = 0L;

	// Select supplier used for synchronization if multiple instances access one database and have to process distinct objects (like orders)
	static Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> selectExclusively(Connection cn, SqlDomainController sdc, Class<? extends SqlDomainObject> objectDomainClass,
			Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount) {

		// Build WHERE clause
		String objectTableName = sdc.getSqlRegistry().getTableFor(objectDomainClass).name;
		String inProgressTableName = sdc.getSqlRegistry().getTableFor(inProgressClass).name;
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
				log.info("SDC: {} record with id {} is already in progress (by another instance)", objectDomainClass.getSimpleName(), id);
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

		String idWhereClause = sdc.getSqlRegistry().getTableFor(obj.getClass()).name + "." + Const.ID_COL + "=" + obj.getId();
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

	// Check if object has unsaved reference changes and assign field warning in this case
	private static void checkForUnsavedReferenceChange(SqlDomainController sdc, SqlDomainObject obj, Field refField, String foreignKeyColumnName, Long parentObjectIdFromDatabase) {

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

	// Check if object has unsaved value changes and assign field warning in this case
	private static void checkForUnsavedValueChange(SqlDomainController sdc, SqlDomainObject obj, Field dataField, String columnName, Object fieldValueFromDatabase) {

		Object fieldValue = obj.getFieldValue(dataField);
		Object fieldValueFromObjectRecord = sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(columnName);

		if (!objectsEqual(fieldValue, fieldValueFromObjectRecord)) {
			log.warn("SDC: Data field '{}' of object '{}' has unsaved changed value {} which will be overridden by value {} from database!", dataField.getName(), obj.name(),
					CLog.forSecretLogging(dataField, fieldValue), CLog.forSecretLogging(dataField, fieldValueFromDatabase));
			obj.setFieldWarning(dataField,
					"Discarded unsaved changed value " + CLog.forSecretLogging(dataField, fieldValue) + " of field '" + dataField.getName() + "' on loading object from database");
		}
	}

	// Check if object has unsaved changes in a collection or map and assign field warning in this case
	private static void checkForUnsavedComplexFieldChange(SqlDomainController sdc, SqlDomainObject obj, Field complexField, String entryTableName, Object complexObjectFromField) {

		Object complexObjectFromColumn = sdc.recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName);
		String objectType = (complexObjectFromField.getClass().isArray() ? "Array" : complexObjectFromField instanceof Collection ? "Collection" : "Map");

		if (!objectsEqual(complexObjectFromField, complexObjectFromColumn)) {
			log.warn("SDC: {} field '{}' of object '{}' {} has unsaved changes and will be overridden by value {} from database!", objectType, complexField.getName(), obj.name(),
					CLog.forSecretLogging(complexField, complexObjectFromField), CLog.forSecretLogging(complexField, complexObjectFromColumn));
			obj.setFieldWarning(complexField, "Discarded unsaved changed value " + complexObjectFromField + " of field '" + complexField.getName() + "' on loading from database");
		}
	}

	// Assign changed data in record from database to corresponding fields of domain object - check for unsaved changes before, which will then be discarded
	@SuppressWarnings("unchecked")
	private static boolean assignDataToDomainObjectAndCheckReferentialIntegrity(SqlDomainController sdc, SqlDomainObject obj, boolean isNew, SortedMap<String, Object> databaseChangesMap,
			List<UnresolvedReference> unresolvedReferences) throws SqlDbException {

		// Assign loaded data to fields for all inherited domain classes of domain object
		boolean isAnyReferenceChanged = false;
		for (Class<? extends SqlDomainObject> domainClass : sdc.getRegistry().getDomainClassesFor(obj.getClass())) {

			// Reference fields: assign new references immediately if referenced object exists - otherwise collect unresolved references
			for (Field refField : sdc.getRegistry().getReferenceFields(domainClass)) {
				String foreignKeyColumnName = sdc.getSqlRegistry().getColumnFor(refField).name;

				SqlDomainObject oldParentObject = (SqlDomainObject) obj.getFieldValue(refField);
				Long oldParentId = (oldParentObject != null ? oldParentObject.getId() : null);

				if (databaseChangesMap.containsKey(foreignKeyColumnName)) { // Reference changed in database

					Long newParentId = null;
					Number parentIdNumber = (Number) databaseChangesMap.get(foreignKeyColumnName);
					if (parentIdNumber != null) {
						newParentId = parentIdNumber.longValue();
					}

					if (!isNew) {
						checkForUnsavedReferenceChange(sdc, obj, refField, foreignKeyColumnName, oldParentId);
					}

					if (newParentId == null) { // Null reference
						obj.setFieldValue(refField, null);
					}
					else { // Object reference

						// Check if referenced object is registered (already loaded on change in database)
						SqlDomainObject newParentObject = sdc.find(sdc.getRegistry().getCastedReferencedDomainClass(refField), newParentId);
						if (newParentObject != null) { // Referenced object is registered (already loaded on change in database)
							obj.setFieldValue(refField, newParentObject);
						}
						else { // Referenced object is not registered
								// Referenced object is still not loaded (data horizon or circular reference) -> collect unresolved reference
							obj.setFieldValue(refField, null); // Temporarily reset reference - this will be set to referenced object after loading this
							unresolvedReferences.add(new UnresolvedReference(sdc, obj, refField, newParentId));
						}
					}

					isAnyReferenceChanged = true; // To collect objects where references changed for subsequent update of accumulations
				}
				else if (oldParentId != null) { // Existing reference is unchanged

					// Check if referenced object is still registered and re-register it if not to assure referential integrity again
					// Note: this scenario normally cannot happen because objects, which are referenced by any registered object, will not be unregistered automatically on synchronization (due to
					// data horizon condition) - unit test explicitly unregisters referenced objects to force this branch
					SqlDomainObject newParentObject = sdc.find(sdc.getRegistry().getCastedReferencedDomainClass(refField), oldParentId);
					if (newParentObject == null) { // Referenced object is registered (already loaded on change in database)
						sdc.reregister(oldParentObject);
					}
				}
			}

			// Data fields: assign - potentially converted - values
			Predicate<Field> hasValueChangedPredicate = (f -> databaseChangesMap.containsKey(sdc.getSqlRegistry().getColumnFor(f).name));
			for (Field dataField : sdc.getRegistry().getDataFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				String columnName = sdc.getSqlRegistry().getColumnFor(dataField).name;
				Object fieldValueFromDatabase = databaseChangesMap.get(columnName);

				if (!isNew) {
					checkForUnsavedValueChange(sdc, obj, dataField, columnName, fieldValueFromDatabase /* only for logging */);
				}

				// Decrypt encrypted value
				Object fieldValue = null;
				if (dataField.isAnnotationPresent(Crypt.class) && dataField.getType() == String.class && fieldValueFromDatabase != null) {

					if (!isEmpty(sdc.cryptPassword)) {
						try {
							fieldValue = AESCrypt.decrypt((String) fieldValueFromDatabase, sdc.cryptPassword, sdc.cryptSalt);
						}
						catch (Exception ex) {
							log.error("SDC: Decryption of value of column '{}' failed for '{}' by {}", columnName, obj.name(), ex);
							fieldValue = fieldValueFromDatabase;
							obj.setFieldError(dataField, "Value could not be decrypted on reading from database! " + ex);
						}
					}
					else {
						log.warn("SCD: Value of column '{}' cannot be decrypted because 'cryptPassword' is not configured in 'domain.properties'", columnName);
						obj.setFieldError(dataField, "Value could not be decrypted on reading from database! Missing 'cryptPassword' property in 'domain.properties'");
					}
				}
				else if (File.class.isAssignableFrom(dataField.getType())) {

					String tableName = sdc.getSqlRegistry().getTableFor(domainClass).name;
					if (log.isDebugEnabled()) {
						log.debug("SDC: Try to rebuild file from file entry in column '{}.{}'", tableName, columnName);
					}

					byte[] fileEntryBytes = (byte[]) fieldValueFromDatabase;
					try {
						fieldValue = Helpers.rebuildFile(fileEntryBytes);
					}
					catch (IOException ioex) { // Thrown if file got from file entry could not be written

						int pathLength = Helpers.getPathLength(fileEntryBytes);
						File file = Helpers.getFile(fileEntryBytes, pathLength);

						log.warn("SDC: File '{}' retrieved from column '{}.{}' could not be written to original directory! ({}). Try to write file to current directory: '{}'", file, tableName,
								columnName, ioex.getMessage(), CFile.getCurrentDir());

						if (file != null) {
							file = new File(file.getName());
							try {
								CFile.writeBinary(file, Helpers.getFileContent(fileEntryBytes, pathLength));
								log.info("SDC: Successfully wrote '{}'", file);
								fieldValue = file;
							}
							catch (IOException e) {
								log.error("SDC: File '{}' could not be written! ({})", file, ioex.getMessage());
							}
						}
					}
				}
				else {
					fieldValue = fieldValueFromDatabase;
				}

				// Set value for field
				obj.setFieldValue(dataField, fieldValue);

				// Replace loaded column value by field value in database changes map - which will be used to update object record
				databaseChangesMap.put(columnName, fieldValue);
			}

			// Complex (table related) fields: set field values of object to collection or map (conversion from entry table record was already done on loading entry records)
			Predicate<Field> hasEntriesChangedPredicate = (f -> databaseChangesMap.containsKey(sdc.getSqlRegistry().getEntryTableFor(f).name));
			for (Field complexField : sdc.getRegistry().getComplexFields(domainClass).stream().filter(hasEntriesChangedPredicate).collect(Collectors.toList())) {

				String entryTableName = sdc.getSqlRegistry().getEntryTableFor(complexField).name;
				Object complexObjectFromField = obj.getFieldValue(complexField);

				if (!isNew) {
					checkForUnsavedComplexFieldChange(sdc, obj, complexField, entryTableName, complexObjectFromField);
				}

				if (complexField.getType().isArray()) {
					Object arrayFromColumn = databaseChangesMap.get(entryTableName);
					Object arrayForField = Array.newInstance(complexField.getType().getComponentType(), Array.getLength(arrayFromColumn));
					System.arraycopy(arrayFromColumn, 0, arrayForField, 0, Array.getLength(arrayFromColumn));
					obj.setFieldValue(complexField, arrayForField);
				}
				else if (Collection.class.isAssignableFrom(complexField.getType())) { // Collection
					Collection<Object> collectionFromField = (Collection<Object>) complexObjectFromField;
					collectionFromField.clear();
					collectionFromField.addAll((Collection<Object>) databaseChangesMap.get(entryTableName));
				}
				else if (Map.class.isAssignableFrom(complexField.getType())) { // Map
					Map<Object, Object> mapFromField = (Map<Object, Object>) complexObjectFromField;
					mapFromField.clear();
					mapFromField.putAll((Map<Object, Object>) databaseChangesMap.get(entryTableName));
				}
				else {
					throw new SqlDbException("Complex field '" + complexField + "' is neither array nor collection nor map!");
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

		return isAnyReferenceChanged;
	}

	// Load result containing multiple information collected during building objects from records loaded in one load cycle
	static class IntermediateLoadResult {
		boolean hasChanges = false;
		Set<SqlDomainObject> loadedObjects = new HashSet<>();
		Set<SqlDomainObject> objectsWhereReferencesChanged = new HashSet<>();
		List<UnresolvedReference> unresolvedReferences = new ArrayList<>();
	}

	// Update local object records, instantiate new objects and assign data to all new or changed objects. Collect objects having changed and/or still unresolved references
	static IntermediateLoadResult buildObjectsFromLoadedRecords(SqlDomainController sdc, Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap)
			throws SqlDbException {

		if (!CMap.isEmpty(loadedRecordsMap) && log.isTraceEnabled()) {
			log.trace("SDC: Build objects from loaded records...");
		}

		IntermediateLoadResult loadResult = new IntermediateLoadResult();
		Set<SqlDomainObject> newObjects = new HashSet<>(); // Only for logging
		Set<SqlDomainObject> changedObjects = new HashSet<>();

		// Filter object domain classes where records were loaded for and handle loaded objects in order of parent/child relationship to avoid unnecessary unresolved references
		List<Class<? extends SqlDomainObject>> relevantObjectDomainClasses = sdc.getRegistry().getRegisteredObjectDomainClasses().stream().filter(loadedRecordsMap::containsKey)
				.collect(Collectors.toList());
		for (Class<? extends SqlDomainObject> objectDomainClass : relevantObjectDomainClasses) {

			// Determine table and column name association - only for secret logging
			List<String> columnNames = new ArrayList<>();
			Map<String, String> columnTableMap = new HashMap<>();
			for (Class<? extends SqlDomainObject> domainClass : sdc.getRegistry().getDomainClassesFor(objectDomainClass)) {
				SqlDbTable table = sdc.getSqlRegistry().getTableFor(domainClass);
				for (SqlDbColumn column : table.columns) {
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

						// Add column/value entry to changes map if current and loaded values differ - ignore last modified column and consider only logical changes
						if (!Const.LAST_MODIFIED_COL.equals(col) && !logicallyEqual(oldValue, newValue)) {
							databaseChangesMap.put(col, newValue);
						}
					}

					// Check if object was changed in database
					if (!databaseChangesMap.isEmpty()) {

						if (log.isDebugEnabled()) {
							log.debug("SDC: Loaded record for '{}@{}' differs from current record. New values: {}", objectDomainClass.getSimpleName(), id,
									SqlDbHelpers.forSecretLoggingRecord(databaseChangesMap, columnNames, columnTableMap));
							if (log.isTraceEnabled()) {
								log.trace("SDC: Current object record: {}", SqlDbHelpers.forSecretLoggingRecord(objectRecord, columnNames, columnTableMap));
							}
						}

						databaseChangesMap.put(Const.LAST_MODIFIED_COL, loadedRecord.get(Const.LAST_MODIFIED_COL)); // Change last modification date if any logical change was detected
						changedObjects.add(obj);
					}
				}

				// Assign loaded data to corresponding fields of domain object, check if all referenced objects are registered and collect objects where references were changed
				if (assignDataToDomainObjectAndCheckReferentialIntegrity(sdc, obj, isNew, databaseChangesMap, loadResult.unresolvedReferences)) {
					loadResult.objectsWhereReferencesChanged.add(obj);
				}
				if (log.isTraceEnabled()) {
					log.trace("SDC: Loaded {}object '{}': {}", (isNew ? "new " : ""), obj.name(), SqlDbHelpers.forSecretLoggingRecord(loadedRecord, columnNames, columnTableMap));
				}

				loadResult.loadedObjects.add(obj);
			}
		}

		if (log.isDebugEnabled()) {
			log.debug("SDC: Loaded: #'s of new objects: {}, #'s of changed objects: {} (#'s of total loaded objects: {})", Helpers.groupCountsByDomainClassName(newObjects),
					Helpers.groupCountsByDomainClassName(changedObjects), Helpers.groupCountsByDomainClassName(loadResult.loadedObjects));
		}

		loadResult.hasChanges = (!changedObjects.isEmpty() || !newObjects.isEmpty()); // true if any changes in database were detected

		return loadResult;
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

		UnresolvedReference(
				SqlDomainController sdc,
				SqlDomainObject obj,
				Field refField,
				long parentObjectId) {

			this.obj = obj;
			this.refField = refField;
			this.parentDomainClass = sdc.getRegistry().getCastedReferencedDomainClass(refField);
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
		String tableName = sdc.getSqlRegistry().getTableFor(domainClass).name;
		List<SortedMap<String, Object>> records = sdc.sqlDb.selectFrom(cn, tableName, Const.DOMAIN_CLASS_COL, "ID=" + id, null, 0, null);
		if (records.isEmpty()) {
			log.error("SDC: No record found for object {}@{} which is referenced and therefore should exist", domainClass.getSimpleName(), id);
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
				if (!sdc.getRegistry().isObjectDomainClass(ur.parentDomainClass)) {
					objectDomainClass = determineObjectDomainClass(cn, sdc, ur.parentDomainClass, ur.parentObjectId);
				}
				missingObjectIdsMap.computeIfAbsent(objectDomainClass, l -> new HashSet<>()).add(ur.parentObjectId);
			}
		}

		// Load missing object records
		Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedMissingRecordsMap = new HashMap<>();
		for (Entry<Class<? extends SqlDomainObject>, Set<Long>> entry : missingObjectIdsMap.entrySet()) {

			Class<? extends SqlDomainObject> objectDomainClass = entry.getKey();
			Set<Long> missingObjectIds = entry.getValue();
			if (log.isDebugEnabled()) {
				log.debug("SDC: Load {} missing '{}' object(s){}", missingObjectIds.size(), objectDomainClass.getSimpleName(), (missingObjectIds.size() <= 32 ? " " + missingObjectIds : "..."));
			}

			Map<Long, SortedMap<String, Object>> collectedRecordMap = new HashMap<>();
			String tableName = sdc.getSqlRegistry().getTableFor(objectDomainClass).name;

			for (String idsList : Helpers.buildStringLists(missingObjectIds, 1000)) { // Oracle limitation max 1000 elements in lists
				collectedRecordMap.putAll(retrieveRecordsFromDatabase(cn, sdc, 0, objectDomainClass, tableName + ".ID IN (" + idsList + ")", null));
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

}
