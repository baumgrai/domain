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

import com.icx.dom.common.CBase;
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

//	TODO(V2?): Generalize serialization of collections and maps: use separate tables also for collections and maps which are parts of other collection and maps
// TODO(V2?): Implement reverse behavior: generate classes from existing database (scripts)

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
	 * Initialize database connection and register tables for domain classes
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

		if (Registry.isObjectDomainClass(domainClass)) {
			return domainClass;
		}
		else {
			log.info("SDC: Domain class '{}' is not an object domain class -> determine object domain class for missing object(s).", domainClass.getSimpleName());

			// Determine object domain class by retrieving domain class name from selected referenced object record
			try {
				String refTableName = SqlRegistry.getTableFor(domainClass).name;
				List<SortedMap<String, Object>> records = sqlDb.selectFrom(cn, false, refTableName, SqlDomainObject.DOMAIN_CLASS_COL, "ID=" + id, null, 0, null, CList.newList(String.class));
				if (records.isEmpty()) {
					log.error("No record found for object {}@{} which is referenced by child object and therefore must exist", domainClass.getSimpleName(), id);
				}
				else {
					String objectDomainClassName = (String) records.get(0).get(SqlDomainObject.DOMAIN_CLASS_COL); // Assume JDBC type of column is String for a String field

					log.info("SDC: Object domain class for referenced domain class '{}' is: '{}'", domainClass.getSimpleName(), objectDomainClassName);

					return getDomainClassByName(objectDomainClassName);
				}
			}
			catch (SQLException | SqlDbException e) {
				log.error("SDC: Exception determining object domain class for domain class '{}'", domainClass, e);
				return null;
			}
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

		Predicate<Column> isNonStandardColumn = c -> !CBase.objectsEqual(c.name, SqlDomainObject.ID_COL) && !CBase.objectsEqual(c.name, SqlDomainObject.DOMAIN_CLASS_COL);

		Class<? extends DomainObject> derivedDomainClass = Registry.getSuperclass(objectDomainClass);
		while (derivedDomainClass != SqlDomainObject.class) {

			SqlDbTable inheritedTable = SqlRegistry.getTableFor(derivedDomainClass);

			sd.joinedTableExpression += " JOIN " + inheritedTable.name + " ON " + inheritedTable.name + ".ID=" + objectTable.name + ".ID";
			sd.allColumnNames.addAll(inheritedTable.columns.stream().filter(isNonStandardColumn).map(c -> inheritedTable.name + "." + c.name).collect(Collectors.toList()));
			sd.allFieldTypes.addAll(inheritedTable.columns.stream().filter(isNonStandardColumn).map(SqlRegistry::getRequiredJdbcTypeFor).collect(Collectors.toList()));

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
				elementType = String.class; // Collections and maps are stored as strings in database
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
				valueType = String.class; // Collections and maps are stored as strings in database
			}
			else {
				valueType = Helpers.requiredJdbcTypeFor((Class<?>) valueType);
			}
			sde.allFieldTypes.add((Class<?>) valueType);
		}

		return sde;
	}

	// Load object records for one object domain class - means one record per object, containing data of all tables associated to object domain class (inheritance stack - e.g. class Racebike extends
	// Bike -> tables [ DOM_BIKE, DOM_RACEBIKE ])
	@SuppressWarnings("unchecked")
	static Map<Long, SortedMap<String, Object>> retrieveRecordsForObjectDomainClass(Connection cn, boolean forUpdate, int limit, Class<? extends DomainObject> objectDomainClass, String whereClause) {

		Map<Long, SortedMap<String, Object>> loadedRecordMap = new HashMap<>();

		try {
			// First load main object records

			// Build select description for object records
			SelectDescription sd = buildSelectDescriptionFor(objectDomainClass);

			// SELECT object records and return empty map if no (matching) object found in database
			List<SortedMap<String, Object>> loadedRecords = sqlDb.selectFrom(cn, forUpdate, sd.joinedTableExpression, sd.allColumnNames, whereClause, null, limit, null, sd.allFieldTypes);
			if (CList.isEmpty(loadedRecords)) {
				return loadedRecordMap;
			}

			// Build up loaded records by id map
			for (SortedMap<String, Object> rec : loadedRecords) {

				// Check if record is a 'real' (not derived) object record
				String actualObjectDomainClassName = (String) rec.get(SqlDomainObject.DOMAIN_CLASS_COL);
				if (CBase.objectsEqual(actualObjectDomainClassName, objectDomainClass.getSimpleName())) {

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

						// In limit is given for object records SELECT only entry records for loaded object records
						List<String> idsLists = buildMax1000IdsLists(loadedRecordMap.keySet());
						for (String idsList : idsLists) {
							String localWhereClause = (!CBase.isEmpty(whereClause) ? whereClause + " AND " : "") + objectTable.name + ".ID IN (" + idsList + ")";
							entryRecords.addAll(sqlDb.selectFrom(cn, forUpdate, sde.joinedTableExpression, sde.allColumnNames, localWhereClause, sde.orderByClause, 0, null, sde.allFieldTypes));
						}
					}
					else {
						// SELECT all entry records
						entryRecords.addAll(sqlDb.selectFrom(cn, forUpdate, sde.joinedTableExpression, sde.allColumnNames, whereClause, sde.orderByClause, 0, null, sde.allFieldTypes));
					}

					// Get entry table and column referencing id of main object record for collection or map field
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
						if (!loadedRecordMap.get(objectId).containsKey(entryTable.name)) {
							loadedRecordMap.get(objectId).put(entryTable.name, new ArrayList<>());
						}
						((List<SortedMap<String, Object>>) loadedRecordMap.get(objectId).get(entryTable.name)).add(entryRecord);
					}
				}
			}
		}
		catch (SQLException | SqlDbException e) {

			log.error("SDC: {} loading objects of domain class '{}' from database: {}", e.getClass().getSimpleName(), objectDomainClass.getName(), e.getMessage());

			if (forUpdate) {
				try {
					cn.rollback();
				}
				catch (SQLException e1) {
					log.error("SDC: Exception ROLLing BACK after failed SELECT FOR UPDATE: ", e);
				}
			}
		}

		return loadedRecordMap;
	}

	// -------------------------------------------------------------------------
	// SELECT supplier methods
	// -------------------------------------------------------------------------

	// private static synchronized <T extends DomainObject> Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> selectForUpdate(Connection cn, Class<T> objectDomainClass,
	// String whereClause, int maxCount) {
	//
	// // Map to return
	// Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
	//
	// // SELECT object records FOR UPDATE
	// Map<Long, SortedMap<String, Object>> loadedRecordsMap = retrieveRecordsForObjectDomainClass(cn, true, maxCount, objectDomainClass, whereClause);
	//
	// // If objects found add entry for object domain class to return map
	// if (!CMap.isEmpty(loadedRecordsMap)) {
	// loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);
	// }
	//
	// return loadedRecordsMapByDomainClassMap;
	// }

	// Load all objects from database considering data horizon for data horizon controlled domain classes
	@SafeVarargs
	private static Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> selectAll(Connection cn, Class<? extends DomainObject>... domainClassesToExclude) {

		// Map to return
		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		// Build database specific datetime string for data horizon
		String dataHorizon = String.format(sqlDb.getDbType().dateTemplate(), getCurrentDataHorizon().format(DateTimeFormatter.ofPattern(CDateTime.DATETIME_MS_FORMAT)));

		// Load objects records for all registered - but excluded - object domain classes - only objects which were added/modified after current data horizon for data horizon controlled domain objects
		for (Class<? extends DomainObject> objectDomainClass : Registry.getRegisteredObjectDomainClasses()) {

			// Ignore objects of excluded domain classes
			if (domainClassesToExclude != null && Stream.of(domainClassesToExclude).anyMatch(objectDomainClass::isAssignableFrom)) {
				continue;
			}

			// For data horizon controlled object domain classes build WHERE clause for data horizon control
			String whereClause = (Registry.isDataHorizonControlled(objectDomainClass) ? SqlDomainObject.LAST_MODIFIED_COL + ">=" + dataHorizon : null);

			// SELECT objects
			Map<Long, SortedMap<String, Object>> loadedObjectsMap = retrieveRecordsForObjectDomainClass(cn, false, 0, objectDomainClass, whereClause);

			// Fill loaded records map
			if (!CMap.isEmpty(loadedObjectsMap)) {
				loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedObjectsMap);
			}
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// SELECT FOR UPDATE and load objects of given object domain class WHERE a specific status field has a specific value, UPDATE status with another given value and COMMIT operation
	// This is for synchronization if multiple instances access one database and have to process distinct objects (like orders)
	@SuppressWarnings({ "unchecked" })
	private static synchronized Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> selectAndUpdateStatus(Connection cn, Class<? extends DomainObject> objectDomainClass,
			String statusFieldName, String availableStatus, String inUseStatus, int maxCount) {

		// Find specified status field and containing domain class
		Field field = Registry.getFieldByName(objectDomainClass, statusFieldName);
		if (field == null) {
			log.error("SDC: Given status field '{}' is not a field of object domain class '{}' or any of the domain classes where this class is derrived from!", statusFieldName,
					objectDomainClass.getSimpleName());
			return Collections.emptyMap();
		}

		// Get domain class where status field is defined
		Class<? extends DomainObject> domainClass = (Class<? extends DomainObject>) field.getDeclaringClass();

		// Check status field type
		if (field.getType() != String.class && field.getType() != Enum.class) {
			log.error("SDC: Given status field '{}' of domain class '{}' is neither a string nor an enum field! (but must be one of these both to use it as status field for synchronization)",
					statusFieldName, domainClass.getSimpleName());
			return Collections.emptyMap();
		}

		// Build WHERE clause for status field
		SqlDbTable table = SqlRegistry.getTableFor(domainClass);
		Column column = SqlRegistry.getColumnFor(field);
		String whereClause = column.name + "='" + availableStatus + "'";

		// Try to SELECT one object record FOR UPDATE
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = retrieveRecordsForObjectDomainClass(cn, true, maxCount, objectDomainClass, whereClause);

		// Map to return
		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		// If matching object found...
		if (!CMap.isEmpty(loadedRecordsMap)) {

			// Insert loaded records for object domain class
			loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);

			Set<Long> idsOfSelectedRecords = new HashSet<>(loadedRecordsMap.keySet());

			try {
				// UPDATE status field of selected records in database and COMMIT
				SortedMap<String, Object> oneCVMap = CMap.newSortedMap(column.name, inUseStatus);
				List<String> idsLists = buildMax1000IdsLists(idsOfSelectedRecords);
				for (String idsList : idsLists) {
					sqlDb.update(cn, table.name, oneCVMap, "ID IN (" + idsList + ")");
				}
				cn.commit();

				log.info("SDC: SELECT FOR UPDATE transaction committed and status set to '{}'", inUseStatus);

				// Update status field in records retrieved
				for (SortedMap<String, Object> loadedRecord : loadedRecordsMap.values()) {
					loadedRecord.put(column.name, inUseStatus);
				}
			}
			catch (SQLException | SqlDbException e) {
				log.error("SDC: {} updating status of selected objects of domain class '{}' from database: {}", e.getClass().getSimpleName(), domainClass.getName(), e.getMessage());
				try {
					cn.rollback();
				}
				catch (SQLException e1) {
					log.error("SDC: Exception ROLLing BACK transaction after UPDATE failed in SELECT FOR UPDATE context: ", e1);
				}
			}
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// -------------------------------------------------------------------------
	// Finalize and ensure referential integrity
	// -------------------------------------------------------------------------

	// Unresolved reference
	private static class UnresolvedReference {

		DomainObject obj;
		Field refField;
		long refObjectId;

		public UnresolvedReference(
				DomainObject obj,
				Field refField,
				long refObjectId) {

			this.obj = obj;
			this.refField = refField;
			this.refObjectId = refObjectId;
		}
	}

	// Load records of objects which were not loaded initially (because they are out of data horizon) but which are referenced by initially loaded objects
	private static Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadMissingObjectRecords(Connection cn, Set<UnresolvedReference> unresolvedReferences) {

		// Build up map containing all missing objects by domain object classes
		Map<Class<? extends DomainObject>, Set<Long>> missingObjectsMap = new HashMap<>();

		Map<Class<? extends DomainObject>, Class<? extends DomainObject>> refObjectDomainClassMap = new HashMap<>();
		for (UnresolvedReference ur : unresolvedReferences) {

			// Get type of referenced object from reference field type
			Class<? extends DomainObject> refDomainClass = Registry.getReferencedDomainClass(ur.refField);

			// Determine object domain class of referenced object - this may be not the referenced domain class itself but any of the inherited domain classes (e.g.: referenced domain class is Car but
			// object domain class is Sportscar)
			Class<? extends DomainObject> refObjectDomainClass = refObjectDomainClassMap.get(refDomainClass);
			if (refObjectDomainClass == null) {
				refObjectDomainClass = determineObjectDomainClassOfMissingObject(cn, refDomainClass, ur.refObjectId);

				refObjectDomainClassMap.put(refDomainClass, refObjectDomainClass);
				missingObjectsMap.put(refObjectDomainClass, new HashSet<>());
			}

			// Check if missing object was already loaded (this happens on circular references: at the end all objects were loaded but not all references could be resolved during loading)
			DomainObject obj = find(refObjectDomainClass, ur.refObjectId);
			if (obj == null) {

				// Collect missing objects by object domain classes
				missingObjectsMap.get(refObjectDomainClass).add(ur.refObjectId);
			}
			else if (log.isDebugEnabled()) {
				log.debug("SDC: Missing object {} was already loaded after detecting unresolved reference and do not have to be loaded again (circular reference)", obj.name());
			}
		}

		Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedMissingRecordsMap = new HashMap<>();

		// For object domain classes where missing objects were detected...
		for (Entry<Class<? extends DomainObject>, Set<Long>> entry : missingObjectsMap.entrySet()) {
			Class<? extends DomainObject> objectDomainClass = entry.getKey();

			Set<Long> missingObjectIds = missingObjectsMap.get(objectDomainClass);
			log.info("SDC: Load {} missing '{}' object(s){}", missingObjectIds.size(), objectDomainClass.getSimpleName(), (missingObjectIds.size() <= 32 ? " " + missingObjectIds : "..."));

			// Build WHERE clause(s) with IDs and load missing objects
			Map<Long, SortedMap<String, Object>> collectedRecordMap = new HashMap<>();
			List<String> idsLists = buildMax1000IdsLists(entry.getValue());
			for (String idsList : idsLists) {
				collectedRecordMap.putAll(retrieveRecordsForObjectDomainClass(cn, false, 0, objectDomainClass, SqlRegistry.getTableFor(objectDomainClass).name + ".ID IN (" + idsList + ")"));
			}

			loadedMissingRecordsMap.put(objectDomainClass, collectedRecordMap);
		}

		return loadedMissingRecordsMap;
	}

	// Assign changed data in record from database to corresponding fields of domain object - check for unsaved changes which will be discarded
	@SuppressWarnings("unchecked")
	private static void assignDataToObject(SqlDomainObject obj, boolean isNew, SortedMap<String, Object> databaseChangesMap, Set<DomainObject> objectsWhereReferencesChanged,
			Set<UnresolvedReference> unresolvedReferences) throws SqlDbException {

		// Changed field predicates
		Predicate<Field> hasValueChangedPredicate = (f -> SqlRegistry.getColumnFor(f) != null && databaseChangesMap.containsKey(SqlRegistry.getColumnFor(f).name));
		Predicate<Field> hasEntriesChangedPredicate = (f -> SqlRegistry.getEntryTableFor(f) != null && databaseChangesMap.containsKey(SqlRegistry.getEntryTableFor(f).name));

		for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(obj.getClass())) {

			// Reference fields: assign new references immediately if referenced object exists - otherwise collect unresolved references
			for (Field refField : Registry.getReferenceFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				// Get column for referenced object id in object records and referenced domain class
				Column fkColumn = SqlRegistry.getColumnFor(refField);
				Class<? extends SqlDomainObject> refObjectClass = Registry.getReferencedDomainClass(refField);

				// Collect objects where reference(s) to parent object(s) have been changed for subsequent update of accumulations containing these (child) objects
				objectsWhereReferencesChanged.add(obj);

				// Get referenced object's id (or null for null reference) from loaded object record (database)
				Long refObjectIdFromDatabase = (Long) databaseChangesMap.get(fkColumn.name);

				// Get currently referenced object from field and check for unsaved reference change
				DomainObject refObj = (DomainObject) obj.getFieldValue(refField);

				if (!isNew) {

					// Check for unsaved reference change
					Long refObjectIdFromObjectRecord = (Long) recordMap.get(obj.getClass()).get(obj.getId()).get(fkColumn.name);
					if (refObj == null && refObjectIdFromObjectRecord != null) {

						log.warn("SDC: Reference field '{}' of object '{}' has unsaved null reference which will be overridden by reference to '{}@{}' from database!", refField.getName(), obj.name(),
								refField.getType().getSimpleName(), refObjectIdFromDatabase);

						obj.setFieldWarning(refField, "Discarded unsaved changed null reference of field '" + refField.getName() + "' on loading object from database");
					}
					else if (refObj != null && !CBase.logicallyEqual(refObj.getId(), refObjectIdFromObjectRecord)) {

						if (refObjectIdFromDatabase != null) {
							log.warn("SDC: Referenced field '{}' of object '{}' has unsaved changed reference to '{}@{}' which will be overridden by reference to '{}@{}' from database!",
									refField.getName(), obj.name(), refObjectClass.getSimpleName(), refObj.getId(), refObjectClass.getSimpleName(), refObjectIdFromDatabase);
						}
						else {
							log.warn("SDC: Referenced field '{}' of object '{}' has unsaved changed reference to '{}@{}' which will be overridden by null reference from database!", refField.getName(),
									obj.name(), refObjectClass.getSimpleName(), refObj.getId());
						}

						obj.setFieldWarning(refField, "Discarded unsaved changed reference to " + DomainObject.name(refObj) + " of field '" + refField.getName() + "' on loading object from database");
					}
				}

				// Assign new referenced object to object
				if (refObjectIdFromDatabase != null) {

					// Check if referenced object is registered
					SqlDomainObject newRefObject = find(refObjectClass, refObjectIdFromDatabase);
					if (newRefObject != null) {

						// Assign newly referenced object to reference field
						obj.setKnownFieldValue(refField, newRefObject);
					}
					else {
						// Temporarily set reference field to null and collect unresolved reference for subsequently loading referenced object and resolving reference
						obj.setKnownFieldValue(refField, null);
						unresolvedReferences.add(new UnresolvedReference(obj, refField, refObjectIdFromDatabase));
					}
				}
				else {
					// Assign null reference to reference field
					obj.setKnownFieldValue(refField, null);
				}
			}

			// Data fields: assign - possibly converted - values
			for (Field dataField : Registry.getDataFields(domainClass).stream().filter(hasValueChangedPredicate).collect(Collectors.toList())) {

				// Determine column for value in object records
				Column column = SqlRegistry.getColumnFor(dataField);

				// Get new value from database
				Object columnValueFromDatabase = databaseChangesMap.get(column.name);
				Object fieldValueFromDatabase = Helpers.columnToFieldValue(dataField.getType(), columnValueFromDatabase);

				// Get current field value
				Object fieldValue = obj.getFieldValue(dataField);

				if (!isNew) {

					// Check for unsaved value change
					Object columnValueFromObjectRecord = recordMap.get(obj.getClass()).get(obj.getId()).get(column.name);
					Object fieldValueFromObjectRecord = Helpers.columnToFieldValue(dataField.getType(), columnValueFromObjectRecord);

					if (!CBase.logicallyEqual(fieldValue, fieldValueFromObjectRecord)) {

						log.warn("SDC: Data field '{}' of object '{}' has unsaved changed value {} which will be overridden by value {} from database!", dataField.getName(), obj.name(),
								CLog.forSecretLogging(dataField.getName(), fieldValue), CLog.forSecretLogging(dataField.getName(), columnValueFromDatabase));

						obj.setFieldWarning(dataField, "Discarded unsaved changed value " + CLog.forSecretLogging(dataField.getName(), fieldValue) + " of field '" + dataField.getName()
								+ "' on loading object from database");
					}
				}

				// Assign new value from database to object
				obj.setFieldValue(dataField, fieldValueFromDatabase);
			}

			// Table related fields: convert entry records to collections and maps
			for (Field complexField : Registry.getComplexFields(domainClass).stream().filter(hasEntriesChangedPredicate).collect(Collectors.toList())) {

				SqlDbTable entryTable = SqlRegistry.getEntryTableFor(complexField);
				ParameterizedType genericFieldType = (ParameterizedType) complexField.getGenericType();

				if (Collection.class.isAssignableFrom(complexField.getType())) {

					Collection<?> collectionFromDatabase = Helpers.entryRecordsToCollection(genericFieldType, (List<SortedMap<String, Object>>) databaseChangesMap.get(entryTable.name));
					Collection<Object> collectionFromObject = (Collection<Object>) obj.getFieldValue(complexField);

					if (!isNew) {

						// Check for unsaved changes
						Collection<?> collectionFromObjectRecord = Helpers.entryRecordsToCollection(genericFieldType,
								(List<SortedMap<String, Object>>) recordMap.get(obj.getClass()).get(obj.getId()).get(entryTable.name));

						if (!CBase.logicallyEqual(collectionFromObject, collectionFromObjectRecord)) {

							log.warn("SDC: Collection field '{}' of object '{}' {} has unsaved changes and will be overridden by collection {} from database!", complexField.getName(), obj.name(),
									collectionFromObject, collectionFromObjectRecord);

							obj.setFieldWarning(complexField,
									"Discarded unsaved changed collection " + collectionFromObject + " of field '" + complexField.getName() + "' on loading collection from database");
						}
					}

					// Assign collection from database record to object
					collectionFromObject.clear();
					if (!CCollection.isEmpty(collectionFromDatabase)) {
						collectionFromObject.addAll(collectionFromDatabase);
					}
				}
				else { /* if (Map.class.isAssignableFrom(entryField.getType())) */

					Map<?, ?> mapFromDatabase = Helpers.entryRecordsToMap(genericFieldType, (List<SortedMap<String, Object>>) databaseChangesMap.get(entryTable.name));
					Map<Object, Object> mapFromObject = (Map<Object, Object>) obj.getFieldValue(complexField);

					if (!isNew) {

						// Check for unsaved changes
						Map<?, ?> mapFromObjectRecord = Helpers.entryRecordsToMap(genericFieldType,
								(List<SortedMap<String, Object>>) recordMap.get(obj.getClass()).get(obj.getId()).get(entryTable.name));
						if (!CBase.logicallyEqual(mapFromObject, mapFromObjectRecord)) {

							log.warn("SDC: Key/value map field '{}' of object '{}' {} has unsaved changes and will be overridden by map {} from database!", complexField.getName(), obj.name(),
									mapFromObject, mapFromObjectRecord);

							obj.setFieldWarning(complexField, "Discarded unsaved changed key/value map " + mapFromObject + " of field '" + complexField.getName() + "' on loading map from database");
						}
					}

					// Assign map from database record to object
					mapFromObject.clear();
					if (!CMap.isEmpty(mapFromDatabase)) {
						mapFromObject.putAll(mapFromDatabase);
					}
				}
			}
		}

		// Reflect changes in database in local object record - do this not until all checks for unsaved changes were done
		if (isNew) {
			SqlDomainController.recordMap.get(obj.getClass()).put(obj.getId(), databaseChangesMap);
		}
		else {
			SqlDomainController.recordMap.get(obj.getClass()).get(obj.getId()).putAll(databaseChangesMap);
		}
	}

	// Update local object records, instantiate new objects and assign data to all new or changed objects. Determine objects with changed references and still unresolved references
	private static Set<SqlDomainObject> buildObjectsFromLoadedRecords(Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap,
			Set<DomainObject> objectsWhereReferencesChanged, Set<UnresolvedReference> unresolvedReferences) throws Exception {

		log.info("SDC: Build objects from loaded records...");

		Set<SqlDomainObject> loadedObjects = new HashSet<>();
		Set<SqlDomainObject> newObjects = new HashSet<>();
		Set<SqlDomainObject> changedObjects = new HashSet<>();

		// Finalize loaded objects in order of parent/child relationship (to avoid unnecessary unresolved references)
		for (Class<? extends DomainObject> objectDomainClass : Registry.getRegisteredObjectDomainClasses()) {

			// Ignore object domain classes where no objects were loaded for
			if (!loadedRecordsMap.containsKey(objectDomainClass)) {
				continue;
			}

			// Get column names of table for subsequent logging
			SqlDbTable table = SqlRegistry.getTableFor(objectDomainClass);
			List<String> columnNames = table.columns.stream().map(c -> c.name).collect(Collectors.toList());

			// Handle loaded object records: instantiate non-existing objects and assign loaded data to new and changed objects
			for (Entry<Long, SortedMap<String, Object>> entry : loadedRecordsMap.get(objectDomainClass).entrySet()) {

				// Loaded object record
				long id = entry.getKey();
				SortedMap<String, Object> loadedRecord = entry.getValue();

				// Try to find registered domain object for loaded object record
				SqlDomainObject obj = (SqlDomainObject) find(objectDomainClass, id);

				if (obj == null) { // Object is newly loaded

					// Newly loaded domain object: Instantiate object
					obj = (SqlDomainObject) instantiate(objectDomainClass);

					// Register object by given id in object store
					obj.registerById(id);

					// Initialize object
					obj.setIsSaved();
					obj.lastModifiedInDb = ((LocalDateTime) loadedRecord.get(SqlDomainObject.LAST_MODIFIED_COL));

					// Assign loaded data to corresponding fields of domain object, collect unresolved references and objects where references were changed
					assignDataToObject(obj, true, loadedRecord, objectsWhereReferencesChanged, unresolvedReferences);

					if (log.isTraceEnabled()) {
						log.trace("SDC: Loaded new object '{}': {}", obj.name(), JdbcHelpers.forLoggingSqlResult(loadedRecord, columnNames));
					}

					loadedObjects.add(obj);
					newObjects.add(obj);
				}
				else { // Object is already registered

					// Get current object record
					SortedMap<String, Object> objectRecord = recordMap.get(objectDomainClass).get(id);

					// Collect changes (we assume that differences between object and database values found here can only be caused by another domain controller instance)
					SortedMap<String, Object> databaseChangesMap = new TreeMap<>();
					for (Entry<String, Object> loadedRecordEntry : loadedRecord.entrySet()) {
						String col = loadedRecordEntry.getKey();

						// Add column/value entry to changes map if current and loaded values differ - ignore last modified column; consider only logical changes
						if (!CBase.objectsEqual(col, SqlDomainObject.LAST_MODIFIED_COL) && !Helpers.logicallyEqual(loadedRecordEntry.getValue(), objectRecord.get(col))) {
							databaseChangesMap.put(col, loadedRecordEntry.getValue());
						}
					}

					loadedObjects.add(obj);

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
			}
		}

		log.info("SDC: Loaded: #'s of new objects: {}, #'s of changed objects: {}", Helpers.groupCountsByDomainClassName(newObjects), Helpers.groupCountsByDomainClassName(changedObjects));

		return loadedObjects;
	}

	// Resolve collected unresolved references by now existing and registered object
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
				ur.obj.setKnownFieldValue(ur.refField, parentObj);
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
	 * Load objects from database using SELECT supplier, finalize these objects and load and finalize missing referenced objects too in a loop to ensure referential integrity
	 * 
	 * @param cn
	 *            database connection
	 * @param select
	 *            SELECT supplier
	 * 
	 * @return loaded objects
	 * 
	 * @throws SqlDbException
	 */
	static Set<SqlDomainObject> load(Connection cn, Function<Connection, Map<Class<? extends DomainObject>, Map<Long, SortedMap<String, Object>>>> select) throws Exception {

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
	 * Field values of existing local objects will generally be overridden by values retrieved from database on load. Unsaved field values which also changed in database will be overridden discarded
	 * and by database values! Warn logs will be written and warnings will be assigned to affected objects in this case. It's recommended to ensure that all local changes are saved before calling this
	 * method.
	 * <p>
	 * For data horizon controlled domain classes first load only objects within data horizon {@link @useDataHorizon}. But method ensures referential integrity by loading all referenced objects even
	 * if they are out of data horizon.
	 * 
	 * @param domainClassesToExclude
	 *            optional domain classes which objects shall not be loaded from database
	 * 
	 * @throws Exception
	 *             if executed SELECT statement throws SQLException, on type conversion and internal errors
	 */
	@SafeVarargs
	public static void synchronize(Class<? extends DomainObject>... domainClassesToExclude) throws Exception {

		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, true)) {

			log.info("SDC: Synchronize with database... {}",
					(domainClassesToExclude != null ? " - domain classes to exclude from loading objects: " + Stream.of(domainClassesToExclude).map(Class::getSimpleName).collect(Collectors.toList())
							: ""));

			// Save all unsaved new objects to database (but do not save unsaved changes of already saved objects to avoid overriding database changes)
			for (DomainObject obj : findAll(o -> !((SqlDomainObject) o).isSaved())) {
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
	 * Mark objects of a specified object domain class for exclusive usage by current instance and return these objects.
	 * <p>
	 * Mark objects which status' matches a specified 'available' status and immediately update status to specified 'in-use' status in database and so avoid that another instance is able to mark these
	 * objects too for exclusive usage. (SELECT FOR UPDATE WHERE status = availableStatus, UPDATE SET status = inUseStatus and COMMIT).
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
	 *             if executed SELECT FOR UPDATE or UPDATE statement throws SQLException, on type conversion and internal errors
	 */
	@SuppressWarnings("unchecked")
	public static synchronized <T extends SqlDomainObject> Set<T> allocateForExclusivUsage(Class<T> objectDomainClass, String statusFieldName, Object availableStatus, Object inUseStatus, int maxCount)
			throws Exception {

		if (availableStatus == null || inUseStatus == null) {
			log.error("SDC: 'available' status or 'in-use' status is null - cannot select objects exclusively");
			return Collections.emptySet();
		}

		log.info("SDC: SELECT {}'{}' objects with status '{}' of status field '{}'  FOR UPDATE and UPDATE status to '{}' for these objects", (maxCount > 0 ? maxCount : ""),
				objectDomainClass.getSimpleName(), availableStatus, statusFieldName, inUseStatus);

		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, false)) {

			// Load objects based on given object domain class
			Set<SqlDomainObject> loadedObjects = load(sqlcn.cn, cn -> selectAndUpdateStatus(cn, objectDomainClass, statusFieldName, availableStatus.toString(), inUseStatus.toString(), maxCount));

			log.info("SDC: {} '{}' objects exclusively retrieved", loadedObjects.size(), objectDomainClass.getSimpleName());

			// Filter objects of given object domain class (because loaded objects may contain referenced objects of other domain classes too)
			return (Set<T>) loadedObjects.stream().filter(o -> o.getClass().equals(objectDomainClass)).collect(Collectors.toSet());
		}
	}

	/**
	 * Retrieve objects of a specified object domain class for exclusive usage by current instance; update, save and return objects matching a given predicate .
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
	 * @throws Exception
	 */
	public static final synchronized <T extends SqlDomainObject> Set<T> allocateAndUpdate(Class<T> objectDomainClass, Predicate<? super T> predicate, Consumer<? super T> update, int maxCount)
			throws Exception {

		Set<T> loadedObjects = new HashSet<>();

		for (T t : all(objectDomainClass)) {

			try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, false)) {

				t.load(sqlcn.cn, true);
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

		// // Load objects based on given object domain class
		// loadedObjects = load(sqlcn.cn, cn -> selectForUpdate(sqlcn.cn, objectDomainClass, whereClauseToOptimizeSelection, maxCount));
		//
		// // Filter objects of given object domain class (because loaded objects may contain referenced objects of other domain classes too)
		// Set<T> loadedTObjects = (Set<T>) loadedObjects.stream().filter(o -> o.getClass().equals(objectDomainClass)).collect(Collectors.toSet());
		//
		// // Select (filter) T objects by given predicate
		// Set<T> selectedTObjects = loadedTObjects.stream().filter(predicate).collect(Collectors.toSet());
		//
		// // Update and save selected T objects
		// for (T t : selectedTObjects) {
		// update.accept(t);
		// t.save(sqlcn.cn);
		// }
		//
		// // Return selected and updated object and commit FOR UPDATE transaction on closing connection
		// return selectedTObjects;
	}

	// -------------------------------------------------------------------------
	// Create and save objects
	// -------------------------------------------------------------------------

	/**
	 * Create, initialize, register and save object of domain class using given database connection.
	 * <p>
	 * Calls initialization function before object registration.
	 * <p>
	 * If object cannot be saved due to SQL exception on INSERT object will marked as invalid and exception and field error(s) will be assigned to object.
	 * <p>
	 * For data horizon controlled objects every hundredth call of this method to create a new object tries to unregister old objects which last modification was before current data horizon.
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
	 * @throws Exception
	 *             on instantiation or save error
	 */
	public static <T extends SqlDomainObject> T createAndSave(Connection cn, Class<T> objectDomainClass, Consumer<T> init) throws Exception {

		T obj = create(objectDomainClass, init);
		obj.save(cn);

		return obj;
	}

	/**
	 * Create, initialize, register and save object of domain class.
	 * <p>
	 * Calls initialization function before object registration.
	 * <p>
	 * If object cannot be saved due to SQL exception on INSERT object will marked as invalid and exception and field error(s) will be assigned to object.
	 * <p>
	 * For data horizon controlled objects every hundredth call of this method to create a new object tries to unregister old objects which last modification was before current data horizon (cleanup).
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
	 * @throws Exception
	 *             on instantiation or save error
	 */
	public static <T extends SqlDomainObject> T createAndSave(Class<T> objectDomainClass, Consumer<T> init) throws Exception {

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

	// -------------------------------------------------------------------------
	// Test...
	// -------------------------------------------------------------------------

	/*
	 * public abstract static class A extends SqlDomainObject {
	 * 
	 * public String name; public AA aa;
	 * 
	 * @Override public String toString() { return name; } }
	 * 
	 * public static class AA extends A {
	 * 
	 * public BB bb; }
	 * 
	 * public static abstract class B extends SqlDomainObject {
	 * 
	 * public String name;
	 * 
	 * @SqlColumn(isNull = false) public A a; public AA aa;
	 * 
	 * @Override public String toString() { return name; } }
	 * 
	 * public static class BB extends B {
	 * 
	 * public B b; }
	 * 
	 * public static void main(String[] args) throws Exception {
	 * 
	 * registerDomainClasses(AA.class, BB.class);
	 * 
	 * Properties dbProps = Prop.readEnvironmentSpecificProperties(Prop.findPropertiesFile("db.properties"), "local/mysql/survey_test", null); Properties domainProps =
	 * Prop.readProperties(Prop.findPropertiesFile("domain.properties"));
	 * 
	 * initializeSql(dbProps, domainProps);
	 * 
	 * boolean loadExistingObjects = (args == null || args.length < 1 ? false : Boolean.valueOf(args[0])); boolean deleteObjects = (args == null || args.length < 2 ? false : Boolean.valueOf(args[1]));
	 * 
	 * if (loadExistingObjects) { loadAll(); } else { AA aa1 = create(AA.class, aa -> aa.name = "a1"); AA aa2 = create(AA.class, aa -> { aa.name = "a2"; aa.aa = aa1; }); AA aa3 = create(AA.class, aa
	 * -> { aa.name = "a3"; aa.aa = aa2; });
	 * 
	 * BB bb1 = create(BB.class, bb -> { bb.name = "b1"; bb.a = aa2; bb.aa = aa1; }); bb1.b = bb1;
	 * 
	 * aa1.aa = aa3; aa1.bb = bb1; aa2.bb = bb1; aa3.bb = bb1;
	 * 
	 * bb1.save(); }
	 * 
	 * if (deleteObjects) {
	 * 
	 * AA aa1 = any(AA.class, aa -> CBase.objectsEqual(aa.name, "a1")); if (aa1 != null) { aa1.delete(); } } }
	 */
}
