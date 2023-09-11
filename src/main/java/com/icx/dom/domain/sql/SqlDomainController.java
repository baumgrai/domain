package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
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
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
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
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainException;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.jdbc.ConfigException;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

/**
 * Singleton to manage persistence using SQL database. Includes methods to load persisted domain objects from database and to create and immediately persist new objects.
 * 
 * @author RainerBaumgärtel
 */
public abstract class SqlDomainController extends DomainController {

	static final Logger log = LoggerFactory.getLogger(SqlDomainController.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final String DOMAIN_PROPERIES_FILE = "domain.properties";
	public static final String DATA_HORIZON_PERIOD_PROP = "dataHorizonPeriod";

	// -------------------------------------------------------------------------
	// Static members
	// -------------------------------------------------------------------------

	// Database connection object
	public static SqlDb sqlDb = null;

	// Config properties from 'domain.properties'
	private static String dataHorizonPeriod = "1M"; // Data horizon controlled objects will be loaded from database only if they are modified after data horizon (now minus this period)

	// Record map: map of object records by object domain class by object id
	protected static Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> recordMap = null;

	// -------------------------------------------------------------------------
	// Initialize
	// -------------------------------------------------------------------------

	/**
	 * Register domain classes by given domain package.
	 * <p>
	 * All top level classes in this package and in any sub package which extends DomainObject class will be registered as domain classes.
	 * 
	 * @param domainPackageName
	 *            name of package where domain classes reside
	 * 
	 * @throws DomainException
	 */
	public static void registerDomainClasses(String domainPackageName) throws DomainException {
		DomainController.registerDomainClasses(SqlDomainObject.class, domainPackageName);
	}

	/**
	 * Register given object domain classes and derived domain classes.
	 * <p>
	 * Classes must extend DomainObject class. Only object domain class (highest class in derivation order) must be given here if derived domain classes exists.
	 * 
	 * @param domainClasses
	 *            list of object domain classes to register
	 * 
	 * @throws DomainException
	 */
	@SafeVarargs
	public static void registerDomainClasses(Class<? extends DomainObject>... domainClasses) throws DomainException {
		DomainController.registerDomainClasses(SqlDomainObject.class, domainClasses);
	}

	/**
	 * Initialize database connection and register domain class - table association
	 * 
	 * @param dbProperties
	 *            database connection properties: 'dbConnectionString', 'dbUser', 'dbPassword'
	 * @param domainProperties
	 *            Domain properties - currently only 'data horizon' - or null (data horizon defaults to 1 month)
	 * 
	 * @throws SQLException
	 *             on database connection errors and on errors retrieving table meta data
	 * @throws ConfigException
	 *             on unsupported database (currently other than Oracle, MS-SQLServer, MySql, MariaDB)
	 * @throws SqlDbException
	 *             on inconsistencies between Java domain classes and SQL tables
	 */
	public static void associateDomainClassesAndDatabaseTables(Properties dbProperties, Properties domainProperties) throws SQLException, ConfigException, SqlDbException {

		// Create database connection object
		sqlDb = new SqlDb(dbProperties);

		// Get configuration parameters
		if (domainProperties != null) {
			dataHorizonPeriod = domainProperties.getProperty(DATA_HORIZON_PERIOD_PROP, "1M");
		}

		// Register database tables for domain classes
		try (SqlConnection sqlConnection = SqlConnection.open(sqlDb.pool, true)) {
			SqlRegistry.registerDomainClassTableAssociation(sqlConnection.cn, sqlDb);
		}

		// Initialize object record map and map for recently changed objects
		recordMap = new ConcurrentHashMap<>();
		Registry.getRegisteredObjectDomainClasses().forEach(c -> recordMap.put(c, new ConcurrentHashMap<>()));
	}

	// -------------------------------------------------------------------------
	// Miscellaneous
	// -------------------------------------------------------------------------

	/**
	 * Get current data horizon date (date before that no data horizon controlled objects will be loaded)
	 * 
	 * @return current data horizon date
	 */
	public static LocalDateTime getCurrentDataHorizon() {
		return CDateTime.subtract(LocalDateTime.now(), dataHorizonPeriod);
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	// Build "(<ids>)" lists with at maximum 1000 ids (Oracle limit for # of elements in WHERE IN (...) clause = 1000)
	private static List<String> buildMax1000IdsLists(Set<Long> ids) {

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

	// Determine object domain class of (missing) object given by id and derived domain class by loading object record
	private static Class<? extends DomainObject> determineObjectDomainClassOfMissingObject(Connection cn, Class<? extends DomainObject> domainClass, long id) {

		log.info("SDC: Domain class '{}' is not an object domain class -> determine object domain class for missing object(s).", domainClass.getSimpleName());

		// Determine object domain class by retrieving domain class name from selected referenced object record
		try {
			String refTableName = SqlRegistry.getTableFor(domainClass).name;
			List<SortedMap<String, Object>> records = sqlDb.selectFrom(cn, refTableName, SqlDomainObject.DOMAIN_CLASS_COL, "ID=" + id, null, CList.newList(String.class));
			if (records.isEmpty()) {
				log.error("No record found for object {}@{} which is referenced by child object and therefore should exist", domainClass.getSimpleName(), id);
			}
			else {
				String objectDomainClassName = (String) records.get(0).get(SqlDomainObject.DOMAIN_CLASS_COL); // Assume JDBC type of column is String for a String field
				log.info("SDC: Object domain class for referenced domain class '{}' is: '{}'", domainClass.getSimpleName(), objectDomainClassName);
				return getDomainClassByName(objectDomainClassName);
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
	private static SelectDescription buildSelectDescriptionFor(Class<? extends DomainObject> objectDomainClass) {

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
	@SuppressWarnings("unchecked")
	static Map<Long, SortedMap<String, Object>> retrieveRecordsForObjectDomainClass(Connection cn, int limit, Class<? extends DomainObject> objectDomainClass, String whereClause) {

		Map<Long, SortedMap<String, Object>> loadedRecordMap = new HashMap<>();

		try {
			// First load main object records

			// Build select description for object records
			SelectDescription sd = buildSelectDescriptionFor(objectDomainClass);

			// SELECT object records and return empty map if no (matching) object found in database
			List<SortedMap<String, Object>> loadedRecords = sqlDb.selectFrom(cn, sd.joinedTableExpression, sd.allColumnNames, whereClause, null, limit, null, sd.allFieldTypes);
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

			// Second load entry records for all table related fields and assign them to main object records using entry table name as key

			SqlDbTable objectTable = SqlRegistry.getTableFor(objectDomainClass);

			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(objectDomainClass)) {
				for (Field complexField : Registry.getComplexFields(domainClass)) {

					// Build table expression, column names and order by clause to SELECT entry records
					SelectDescription sde = buildSelectDescriptionForEntriesOf(sd.joinedTableExpression, objectTable, complexField);

					List<SortedMap<String, Object>> entryRecords = new ArrayList<>();
					if (limit > 0) {

						// If # of loaded object records is limited SELECT only entry records for loaded object records
						List<String> idsLists = buildMax1000IdsLists(loadedRecordMap.keySet());
						for (String idsList : idsLists) {
							String localWhereClause = (!isEmpty(whereClause) ? whereClause + " AND " : "") + objectTable.name + ".ID IN (" + idsList + ")";
							entryRecords.addAll(sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, localWhereClause, sde.orderByClause, sde.allFieldTypes));
						}
					}
					else {
						// SELECT all entry records
						entryRecords.addAll(sqlDb.selectFrom(cn, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, sde.allFieldTypes));
					}

					// Get entry table and column referencing id of main object record for complex (collection or map) field
					SqlDbTable entryTable = SqlRegistry.getEntryTableFor(complexField);
					Column refIdColumn = SqlRegistry.getMainTableRefIdColumnFor(complexField);

					// Add records of entry table to related object records using entry table name as key
					for (SortedMap<String, Object> entryRecord : entryRecords) {

						// Get object id and check if object record is present
						long objectId = (long) entryRecord.get(refIdColumn.name);
						if (!loadedRecordMap.containsKey(objectId)) {
							log.warn("SDC: Object {}@{} was not loaded before (trying) updating collection or map field '{}'", domainClass.getSimpleName(), objectId, complexField.getName());
							continue;
						}

						// Add entry record to record list
						((List<SortedMap<String, Object>>) loadedRecordMap.get(objectId).computeIfAbsent(entryTable.name, t -> new ArrayList<>())).add(entryRecord);
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
	// SELECT supplier methods
	// -------------------------------------------------------------------------

	// Load all objects from database considering data horizon for data horizon controlled domain classes
	@SafeVarargs
	private static Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> selectAll(Connection cn, Class<? extends DomainObject>... domainClassesToExclude) {

		// Map to return
		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		// Build database specific datetime string for data horizon
		String dataHorizon = String.format(sqlDb.getDbType().dateTemplate(), getCurrentDataHorizon().format(DateTimeFormatter.ofPattern(CDateTime.DATETIME_MS_FORMAT)));

		// Load objects records for all registered object domain classes - consider data horizon if specified
		for (Class<? extends DomainObject> objectDomainClass : Registry.getRegisteredObjectDomainClasses()) {

			// Ignore objects of excluded domain classes
			if (domainClassesToExclude != null && Stream.of(domainClassesToExclude).anyMatch(objectDomainClass::isAssignableFrom)) {
				continue;
			}

			// For data horizon controlled object domain classes build WHERE clause for data horizon control
			String whereClause = (Registry.isDataHorizonControlled(objectDomainClass) ? SqlDomainObject.LAST_MODIFIED_COL + ">=" + dataHorizon : null);

			// SELECT objects
			Map<Long, SortedMap<String, Object>> loadedObjectsMap = retrieveRecordsForObjectDomainClass(cn, 0, objectDomainClass, whereClause);

			// Fill loaded records map
			if (!CMap.isEmpty(loadedObjectsMap)) {
				loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedObjectsMap);
			}
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// This is for synchronization if multiple instances access one database and have to process distinct objects (like orders)
	private static synchronized Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> selectForExclusiveUse(Connection cn, Class<? extends DomainObject> objectDomainClass,
			Class<? extends SqlDomainObject> inProgressClass, int maxCount) {

		// Build WHERE clause
		String inProgressTableName = SqlRegistry.getTableFor(inProgressClass).name;
		String whereClause = "ID NOT IN (SELECT ID FROM " + inProgressTableName + ")";

		// Try to SELECT object records FOR UPDATE
		Map<Long, SortedMap<String, Object>> rawRecordsMap = retrieveRecordsForObjectDomainClass(cn, maxCount, objectDomainClass, whereClause);
		if (CMap.isEmpty(rawRecordsMap)) {
			return Collections.emptyMap();
		}

		// Try to INSERT in-progress record with same id as records found (works only for one in-progress record per record found because of UNIQUE constraint for ID field) - provide only records
		// where in-progress record could be inserted
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = new HashMap<>();
		for (Entry<Long, SortedMap<String, Object>> entry : rawRecordsMap.entrySet()) {

			long id = entry.getKey();
			try {
				SqlDomainObject inProgressObject = createWithId(inProgressClass, id);
				inProgressObject.save();
				loadedRecordsMap.put(id, entry.getValue());
			}
			catch (SQLException sqlex) {
				log.info("SDC: {} record with id {} is already in progress (by another instance)", objectDomainClass.getSimpleName(), id);
			}
			catch (SqlDbException sqldbex) {
				log.error("SDC: {} occurred trying to INSERT {} record", sqldbex, inProgressClass.getSimpleName());
			}
		}

		// Build map to return from supplier method
		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);

		return loadedRecordsMapByDomainClassMap;
	}

	// -------------------------------------------------------------------------
	// Finalize and ensure referential integrity
	// -------------------------------------------------------------------------

	// Unresolved reference
	private static class UnresolvedReference {

		DomainObject obj;
		Field refField;
		Class<? extends DomainObject> refDomainClass;
		long refObjectId;

		public UnresolvedReference(
				DomainObject obj,
				Field refField,
				long refObjectId) {

			this.obj = obj;
			this.refField = refField;
			this.refDomainClass = Registry.getReferencedDomainClass(refField);
			this.refObjectId = refObjectId;
		}
	}

	// Load records of objects which were not loaded initially (because they are out of data horizon) but which are referenced by initially loaded objects
	private static Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadMissingObjectRecords(Connection cn, Set<UnresolvedReference> unresolvedReferences) {

		// Build up map containing id's of all missing objects ordered by domain object classes

		Map<Class<? extends DomainObject>, Set<Long>> missingObjectsMap = new HashMap<>();
		for (UnresolvedReference ur : unresolvedReferences) {

			// Use object domain class of referenced object as key - this may be not the referenced domain class itself (e.g.: referenced domain class is Car but object domain class is Sportscar)
			Map<Class<? extends DomainObject>, Class<? extends DomainObject>> cacheMap = new HashMap<>();
			Class<? extends DomainObject> refObjectDomainClass = null;

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
			DomainObject obj = find(refObjectDomainClass, ur.refObjectId);
			if (obj == null) {
				missingObjectsMap.computeIfAbsent(refObjectDomainClass, c -> new HashSet<>()).add(ur.refObjectId);
			}
			else if (log.isDebugEnabled()) {
				log.debug("SDC: Missing object {} was already loaded after detecting unresolved reference and do not have to be loaded again (circular reference)", obj.name());
			}
		}

		// Load missing object records

		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedMissingRecordsMap = new HashMap<>();
		for (Entry<Class<? extends DomainObject>, Set<Long>> entry : missingObjectsMap.entrySet()) {
			Class<? extends DomainObject> objectDomainClass = entry.getKey();

			Set<Long> missingObjectIds = missingObjectsMap.get(objectDomainClass);
			log.info("SDC: Load {} missing '{}' object(s){}", missingObjectIds.size(), objectDomainClass.getSimpleName(), (missingObjectIds.size() <= 32 ? " " + missingObjectIds : "..."));

			// Build WHERE clause(s) with IDs and load missing objects
			Map<Long, SortedMap<String, Object>> collectedRecordMap = new HashMap<>();
			List<String> idsLists = buildMax1000IdsLists(entry.getValue());
			for (String idsList : idsLists) {
				collectedRecordMap.putAll(retrieveRecordsForObjectDomainClass(cn, 0, objectDomainClass, SqlRegistry.getTableFor(objectDomainClass).name + ".ID IN (" + idsList + ")"));
			}

			loadedMissingRecordsMap.put(objectDomainClass, collectedRecordMap);
		}

		return loadedMissingRecordsMap;
	}

	// Check if object has unsaved reference changes
	private static void checkUnsavedReferenceChanges(SqlDomainObject obj, Field refField, String foreignKeyColumnName, Long refObjectIdFromDatabase) {

		DomainObject refObj = (DomainObject) obj.getFieldValue(refField);
		Long refObjectIdFromLocalObjectRecord = (Long) recordMap.get(obj.getClass()).get(obj.getId()).get(foreignKeyColumnName);

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
		Object columnValueFromObjectRecord = recordMap.get(obj.getClass()).get(obj.getId()).get(columnName);
		Object fieldValueFromObjectRecord = Helpers.columnToFieldValue(dataField.getType(), columnValueFromObjectRecord);

		if (!logicallyEqual(fieldValue, fieldValueFromObjectRecord)) {

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

			Collection<?> colFromObjRecord = Helpers.entryRecordsToCollection(genericFieldType, (List<SortedMap<String, Object>>) recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName));
			if (!logicallyEqual(collectionOrMapFromObject, colFromObjRecord)) {

				log.warn("SDC: Collection field '{}' of object '{}' {} has unsaved changes and will be overridden by collection {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, colFromObjRecord);

				obj.setFieldWarning(complexField,
						"Discarded unsaved changed collection " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading collection from database");
			}
		}
		else { // Map
			Map<?, ?> mapFromObjRecord = Helpers.entryRecordsToMap(genericFieldType, (List<SortedMap<String, Object>>) recordMap.get(obj.getClass()).get(obj.getId()).get(entryTableName));
			if (!logicallyEqual(collectionOrMapFromObject, mapFromObjRecord)) {

				log.warn("SDC: Key/value map field '{}' of object '{}' {} has unsaved changes and will be overridden by map {} from database!", complexField.getName(), obj.name(),
						collectionOrMapFromObject, mapFromObjRecord);

				obj.setFieldWarning(complexField, "Discarded unsaved changed key/value map " + collectionOrMapFromObject + " of field '" + complexField.getName() + "' on loading map from database");
			}

		}
	}

	// Assign changed data in record from database to corresponding fields of domain object - check for unsaved changes which will be discarded
	@SuppressWarnings("unchecked")
	private static void assignDataToObject(SqlDomainObject obj, boolean isNew, SortedMap<String, Object> databaseChangesMap, Set<DomainObject> objectsWhereReferencesChanged,
			Set<UnresolvedReference> unresolvedReferences) {

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
					SqlDomainObject newRefObject = find((Class<? extends SqlDomainObject>) refField.getType(), refObjectIdFromDatabase);
					if (newRefObject != null) { // Referenced object is already loaded
						obj.setFieldValue(refField, newRefObject);
					}
					else { // Referenced object is still not loaded (data horizon or circular reference) -> unresolved reference
						obj.setFieldValue(refField, null);
						unresolvedReferences.add(new UnresolvedReference(obj, refField, refObjectIdFromDatabase));
					}
				}

				objectsWhereReferencesChanged.add(obj); // Collect objects where references changed for subsequent update of accumulations
			}

			// Data fields: assign - potentially converted - values
			for (Field dataField : Registry.getDataFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				String columnName = SqlRegistry.getColumnFor(dataField).name;
				Object columnValueFromDatabase = databaseChangesMap.get(columnName);
				Object fieldValueFromDatabase = Helpers.columnToFieldValue(dataField.getType(), columnValueFromDatabase);

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

					Collection<?> colFromDatabase = Helpers.entryRecordsToCollection(genericFieldType, (List<SortedMap<String, Object>>) databaseChangesMap.get(entryTableName));
					Collection<Object> colFromObject = (Collection<Object>) collectionOrMapFromFieldValue;

					colFromObject.clear();
					colFromObject.addAll(colFromDatabase);
				}
				else { // Map
					Map<?, ?> mapFromDatabase = Helpers.entryRecordsToMap(genericFieldType, (List<SortedMap<String, Object>>) databaseChangesMap.get(entryTableName));
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
	private static Set<SqlDomainObject> buildObjectsFromLoadedRecords(Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap,
			Set<DomainObject> objectsWhereReferencesChanged, Set<UnresolvedReference> unresolvedReferences) {

		if (!CMap.isEmpty(loadedRecordsMap)) {
			log.info("SDC: Build objects from loaded records...");
		}

		Set<SqlDomainObject> loadedObjects = new HashSet<>();
		Set<SqlDomainObject> newObjects = new HashSet<>();
		Set<SqlDomainObject> changedObjects = new HashSet<>();

		// Finalize loaded objects in order of parent/child relationship (to avoid unnecessary unresolved references)
		for (Class<? extends DomainObject> objectDomainClass : Registry.getRegisteredObjectDomainClasses().stream().filter(loadedRecordsMap::containsKey).collect(Collectors.toList())) {

			// Get column names of table for subsequent logging
			List<String> columnNames = SqlRegistry.getTableFor(objectDomainClass).columns.stream().map(c -> c.name).collect(Collectors.toList());

			// Handle loaded object records: instantiate non-existing objects and assign loaded data to new and changed objects
			for (Entry<Long, SortedMap<String, Object>> entry : loadedRecordsMap.get(objectDomainClass).entrySet()) {

				// Loaded object record
				long id = entry.getKey();
				SortedMap<String, Object> loadedRecord = entry.getValue();

				// Try to find registered domain object for loaded object record
				SqlDomainObject obj = (SqlDomainObject) find(objectDomainClass, id);
				if (obj == null) { // Object is newly loaded

					// Newly loaded domain object: instantiate, register and initialize object
					obj = (SqlDomainObject) instantiate(objectDomainClass);
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
					SortedMap<String, Object> objectRecord = recordMap.get(objectDomainClass).get(id);

					// Collect changes (we assume that differences between object and database values found here can only be caused by changes in database caused by another domain controller instance)
					SortedMap<String, Object> databaseChangesMap = new TreeMap<>();
					for (Entry<String, Object> loadedRecordEntry : loadedRecord.entrySet()) {

						String col = loadedRecordEntry.getKey();
						Object value = loadedRecordEntry.getValue();

						// Add column/value entry to changes map if current and loaded values differ - ignore last modified column; consider only logical changes
						if (!objectsEqual(col, SqlDomainObject.LAST_MODIFIED_COL) && !Helpers.logicallyEqual(value, objectRecord.get(col))) {
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

		return loadedObjects;
	}

	// Resolve collected unresolved references by now loaded parent object
	private static void resolveUnresolvedReferences(Set<UnresolvedReference> unresolvedReferences) {

		for (UnresolvedReference ur : unresolvedReferences) {

			// Find parent object
			Class<? extends SqlDomainObject> referencedDomainClass = Registry.getReferencedDomainClass(ur.refField);
			SqlDomainObject parentObj = find(referencedDomainClass, ur.refObjectId);

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

	// -------------------------------------------------------------------------
	// Public data load methods
	// -------------------------------------------------------------------------

	/**
	 * Load objects from database using SELECT supplier, finalize these objects and load and finalize missing referenced objects too in a loop to ensure referential integrity.
	 * 
	 * @param cn
	 *            database connection
	 * @param select
	 *            SELECT supplier
	 * 
	 * @return loaded objects
	 */
	static Set<SqlDomainObject> load(Connection cn, Function<Connection, Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>>> select) {

		// Initially load object records using given select-supplier
		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap = select.apply(cn);

		Set<DomainObject> objectsWhereReferencesChanged = new HashSet<>();
		Set<UnresolvedReference> unresolvedReferences = new HashSet<>();

		// Instantiate newly loaded objects, assign changed data and references to objects, collect initially unresolved references
		Set<SqlDomainObject> loadedObjects = buildObjectsFromLoadedRecords(loadedRecordsMap, objectsWhereReferencesChanged, unresolvedReferences);

		// Cyclicly load and instantiate missing (parent) objects and detect unresolved references on these objects
		int c = 1;
		while (!unresolvedReferences.isEmpty()) {

			log.info("SDC: There were in total {} unresolved reference(s) of {} objects referenced by {} objects detected in {}. load cycle", unresolvedReferences.size(),
					unresolvedReferences.stream().map(ur -> ur.refField.getType().getSimpleName()).distinct().collect(Collectors.toList()),
					unresolvedReferences.stream().map(ur -> ur.obj.getClass().getSimpleName()).distinct().collect(Collectors.toList()), c++);

			// Load and instantiate missing objects of unresolved references and add loaded records to total loaded object records
			Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedMissingObjectRecordMapByDomainClassMap = loadMissingObjectRecords(cn, unresolvedReferences);
			for (Entry<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> entry : loadedMissingObjectRecordMapByDomainClassMap.entrySet()) {

				Class<? extends DomainObject> objectDomainClass = entry.getKey();
				Map<Long, SortedMap<String, Object>> objectRecordMap = entry.getValue();

				loadedRecordsMap.computeIfAbsent(objectDomainClass, odc -> new HashMap<>());
				loadedRecordsMap.get(objectDomainClass).putAll(objectRecordMap);
			}

			// Initialize missed objects and determine further unresolved references
			Set<UnresolvedReference> nextUnresolvedReferences = new HashSet<>();
			loadedObjects.addAll(buildObjectsFromLoadedRecords(loadedMissingObjectRecordMapByDomainClassMap, objectsWhereReferencesChanged, nextUnresolvedReferences));

			// Resolve unresolved references of last cycle after missing objects were instantiated
			resolveUnresolvedReferences(unresolvedReferences);
			unresolvedReferences = nextUnresolvedReferences;
		}

		// Update accumulations of all objects which are referenced by any of the objects where references changed
		objectsWhereReferencesChanged.forEach(o -> o.updateAccumulationsOfParentObjects());

		return loadedObjects;
	}

	/**
	 * Save potentially existing unsaved new objects to database, than load all currently relevant objects from database and unregister objects which were deleted in database or fell out of data
	 * horizon but are not referenced by any other object.
	 * <p>
	 * Field values of existing local objects will generally be overridden by values retrieved from database on load. Unsaved field values which also changed in database will be discarded and
	 * overridden by database values! Warn logs will be written and warnings will be assigned to affected objects in this case. It's recommended to ensure that all local changes are saved before
	 * calling this method.
	 * <p>
	 * For data horizon controlled domain classes first load only objects within data horizon {@link @useDataHorizon}. But method ensures referential integrity by loading all referenced objects even
	 * if they are out of data horizon.
	 * 
	 * @param domainClassesToExclude
	 *            optional domain classes which objects shall not be loaded from database
	 * 
	 * @throws SQLException
	 *             if executed SELECT statement throws SQLException
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 */
	@SafeVarargs
	public static void synchronize(Class<? extends DomainObject>... domainClassesToExclude) throws SQLException, SqlDbException {

		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, true)) {

			log.info("SDC: Synchronize with database... {}",
					(domainClassesToExclude != null ? " - domain classes to exclude from loading: " + Stream.of(domainClassesToExclude).map(Class::getSimpleName).collect(Collectors.toList()) : ""));

			// Save all new objects to database (but do not save unsaved changes of already saved objects to avoid overriding database changes)
			for (DomainObject obj : findAll(o -> !((SqlDomainObject) o).isStored())) {
				((SqlDomainObject) obj).save();
			}

			// Load all objects from database - override unsaved local object changes by changes in database on contradiction
			Set<SqlDomainObject> loadedObjects = load(sqlcn.cn, cn -> selectAll(cn, domainClassesToExclude));

			// Unregister existing objects which were not loaded from database again (deleted in database by another instance or fell out of data horizon) and which are not referenced by any object
			for (DomainObject obj : findAll(o -> !loadedObjects.contains(o) && !o.isReferenced())) {
				((SqlDomainObject) obj).unregister();
			}

			log.info("SDC: Synchronization with database done.");
		}
	}

	/**
	 * Mark objects of a specified object domain class for exclusive use by current instance and return these objects.
	 * <p>
	 * Mark objects which status' matches a specified 'available' status and immediately update status to specified 'in-use' status in database and so avoid that another instance is able to mark these
	 * objects too for exclusive use. (SELECT FOR UPDATE WHERE status = availableStatus, UPDATE SET status = inUseStatus and COMMIT).
	 * <p>
	 * Use this method if multiple process instances operate on the same database and it must be ensured that objects are processed by one instance exclusively (like orders)
	 * 
	 * @param objectDomainClass
	 *            domain class of objects to retrieve
	 * @param statusFieldName
	 *            name of status field
	 * @param availableStatus
	 *            status which objects to find must have - Object to allow enums or booleans
	 * @param inUseStatus
	 *            status to set for objects found
	 * @param maxCount
	 *            maximum # of objects to retrieve
	 * 
	 * @return objects found
	 * 
	 * @throws SQLException
	 *             if executed SELECT FOR UPDATE or UPDATE statement throws SQLException
	 */
	@SuppressWarnings("unchecked")
	public static synchronized <T extends SqlDomainObject> Set<T> allocateForExclusiveUse(Class<T> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, int maxCount)
			throws SQLException {

		log.info("SDC: SELECT {} '{}' objects and try to INSERT '{}' record for these objects", (maxCount > 0 ? maxCount : ""), objectDomainClass.getSimpleName(), inProgressClass.getSimpleName());

		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, false)) {

			// Load objects based on given object domain class
			Set<SqlDomainObject> loadedObjects = load(sqlcn.cn, cn -> selectForExclusiveUse(cn, objectDomainClass, inProgressClass, maxCount));
			loadedObjects = loadedObjects.stream().filter(o -> o.getClass().equals(objectDomainClass)).collect(Collectors.toSet());

			log.info("SDC: {} '{}' objects exclusively retrieved", loadedObjects.size(), objectDomainClass.getSimpleName());

			// Filter objects of given object domain class (because loaded objects may contain referenced objects of other domain classes too)
			return (Set<T>) loadedObjects;
		}
	}

	// TODO: Ensure synchronization for multiple instances (formerly used SELECT FOR UPDATE) on allocateAndUpdate() or remove method
	/**
	 * Retrieve objects of a specified object domain class for exclusive use by current instance; update, save and return objects matching a given predicate .
	 * 
	 * @param <T>
	 * @param objectDomainClass
	 *            domain class of objects to retrieve
	 * @param predicate
	 *            predicate to match
	 * @param update
	 *            update action for matching records
	 * @param maxCount
	 *            maximum count of records primarily to retrieve
	 * 
	 * @return matching and updated record(s)
	 * 
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * @throws SQLException
	 *             on error saving object to database
	 */
	public static final synchronized <T extends SqlDomainObject> Set<T> allocateAndUpdate(Class<T> objectDomainClass, Predicate<? super T> predicate, Consumer<? super T> update, int maxCount)
			throws SQLException, SqlDbException {

		Set<T> loadedObjects = new HashSet<>();

		for (T t : all(objectDomainClass)) {

			try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, false)) {

				t.reload(sqlcn.cn);
				if (predicate.test(t)) {

					update.accept(t);
					t.save(sqlcn.cn);

					loadedObjects.add(t);
				}
			}

			if (maxCount > 0 && loadedObjects.size() >= maxCount) {
				break;
			}
		}

		return loadedObjects;
	}

	// -------------------------------------------------------------------------
	// Create and save objects
	// -------------------------------------------------------------------------

	/**
	 * Create, initialize, register and save object of domain class using given database connection.
	 * <p>
	 * Calls initialization function before object registration to ensure that registered object is initialized.
	 * <p>
	 * Uses default constructor which therefore must be defined explicitly if other constructors are defined!
	 * <p>
	 * If object cannot be saved due to SQL exception on INSERT object will marked as invalid and exception and field error(s) will be assigned to object.
	 * 
	 * @param <T>
	 *            specific domain object type
	 * @param sqlcn
	 *            database connection
	 * @param objectDomainClass
	 *            top-level domain class of objects to create (object domain class)
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created domain object
	 * 
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * @throws SQLException
	 *             on error saving object to database
	 */
	public static <T extends SqlDomainObject> T createAndSave(Connection cn, Class<T> objectDomainClass, Consumer<T> init) throws SQLException, SqlDbException {

		T obj = create(objectDomainClass, init);
		obj.save(cn);

		return obj;
	}

	/**
	 * Create, initialize, register and save object of domain class.
	 * <p>
	 * Calls initialization function before object registration to ensure that registered object is initialized.
	 * <p>
	 * Uses default constructor which therefore must be defined explicitly if other constructors are defined!
	 * <p>
	 * If object cannot be saved due to SQL exception on INSERT object will marked as invalid and exception and field error(s) will be assigned to object.
	 * 
	 * @param <T>
	 *            specific domain object class type
	 * @param objectDomainClass
	 *            top-level domain class of objects to create (object domain class)
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created domain object
	 * 
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * @throws SQLException
	 *             on error saving object to database
	 */
	public static <T extends SqlDomainObject> T createAndSave(Class<T> objectDomainClass, Consumer<T> init) throws SQLException, SqlDbException {

		T obj = create(objectDomainClass, init);
		obj.save();

		return obj;
	}

	// -------------------------------------------------------------------------
	// Valid objects
	// -------------------------------------------------------------------------

	/**
	 * Retrieve all registered and valid objects of a specific domain class
	 * 
	 * @param domainClass
	 *            any domain class (must not be top-level/instantiable)
	 * 
	 * @return set of all objects of given domain class
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends DomainObject> Set<T> allValid(Class<T> domainClass) {
		return (Set<T>) objectMap.get(domainClass).values().stream().filter(o -> ((SqlDomainObject) o).isValid()).collect(Collectors.toSet());
	}

}
