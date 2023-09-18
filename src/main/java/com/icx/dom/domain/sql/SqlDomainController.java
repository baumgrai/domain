package com.icx.dom.domain.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CDateTime;
import com.icx.dom.common.CMap;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainException;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.domain.sql.LoadAndSaveHelpers.UnresolvedReference;
import com.icx.dom.jdbc.ConfigException;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;

//	TODO: Check why 'picture' byte array will be treated as changed on reload

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
	public static <S extends SqlDomainObject> void registerDomainClasses(Class<S>... domainClasses) throws DomainException {
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
	// SELECT suppliers
	// -------------------------------------------------------------------------

	// Load all objects from database considering data horizon for data horizon controlled domain classes
	@SafeVarargs
	private static <S extends SqlDomainObject> Map<Class<S>, Map<Long, SortedMap<String, Object>>> selectAll(Connection cn, Class<? extends SqlDomainObject>... domainClassesToExclude) {

		// Map to return
		Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();

		// Build database specific datetime string for data horizon
		String dataHorizon = String.format(SqlDomainController.sqlDb.getDbType().dateTemplate(),
				SqlDomainController.getCurrentDataHorizon().format(DateTimeFormatter.ofPattern(CDateTime.DATETIME_MS_FORMAT)));

		// Load objects records for all registered object domain classes - consider data horizon if specified
		for (Class<? extends DomainObject> odc : Registry.getRegisteredObjectDomainClasses()) {
			@SuppressWarnings("unchecked")
			Class<S> objectDomainClass = (Class<S>) odc;

			// Ignore objects of excluded domain classes
			if (domainClassesToExclude != null && Stream.of(domainClassesToExclude).anyMatch(objectDomainClass::isAssignableFrom)) {
				continue;
			}

			// For data horizon controlled object domain classes build WHERE clause for data horizon control
			String whereClause = (Registry.isDataHorizonControlled(objectDomainClass) ? Const.LAST_MODIFIED_COL + ">=" + dataHorizon : null);

			// SELECT objects
			Map<Long, SortedMap<String, Object>> loadedObjectsMap = LoadAndSaveHelpers.retrieveRecordsFor(cn, 0, objectDomainClass, whereClause);

			// Fill loaded records map
			if (!CMap.isEmpty(loadedObjectsMap)) {
				loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedObjectsMap);
			}
		}

		return loadedRecordsMapByDomainClassMap;
	}

	// Load objects of given object domain class which records in database map given WHERE clause (needs knowledge about Java -> SQL mapping)
	private static <S extends SqlDomainObject> Map<Class<S>, Map<Long, SortedMap<String, Object>>> select(Connection cn, Class<S> objectDomainClass, String whereClause, int maxCount) {

		// Try to SELECT object records FOR UPDATE
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = LoadAndSaveHelpers.retrieveRecordsFor(cn, maxCount, objectDomainClass, whereClause);
		if (CMap.isEmpty(loadedRecordsMap)) {
			return Collections.emptyMap();
		}

		// Build map to return from supplier method
		Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);

		return loadedRecordsMapByDomainClassMap;
	}

	// This is for synchronization if multiple instances access one database and have to process distinct objects (like orders)
	private static <S extends SqlDomainObject> Map<Class<S>, Map<Long, SortedMap<String, Object>>> selectExclusively(Connection cn, Class<S> objectDomainClass,
			Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount) {

		// Build WHERE clause
		String objectTableName = SqlRegistry.getTableFor(objectDomainClass).name;
		String inProgressTableName = SqlRegistry.getTableFor(inProgressClass).name;
		whereClause = (!isEmpty(whereClause) ? "(" + whereClause + ") AND " : "") + objectTableName + ".ID NOT IN (SELECT ID FROM " + inProgressTableName + ")";

		// SELECT object records
		Map<Long, SortedMap<String, Object>> rawRecordsMap = LoadAndSaveHelpers.retrieveRecordsFor(cn, maxCount, objectDomainClass, whereClause);
		if (CMap.isEmpty(rawRecordsMap)) {
			return Collections.emptyMap();
		}

		// Try to INSERT in-progress record with same id as records found (works only for one in-progress record per record found because of UNIQUE constraint for ID field) - provide only records
		// where in-progress record could be inserted
		Map<Long, SortedMap<String, Object>> loadedRecordsMap = new HashMap<>();
		for (Entry<Long, SortedMap<String, Object>> entry : rawRecordsMap.entrySet()) {

			long id = entry.getKey();
			try {
				SqlDomainObject inProgressObject = Helpers.createWithId(inProgressClass, id);
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
		Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		loadedRecordsMapByDomainClassMap.put(objectDomainClass, loadedRecordsMap);

		return loadedRecordsMapByDomainClassMap;
	}

	// -------------------------------------------------------------------------
	// Load using SELECT supplier
	// -------------------------------------------------------------------------

	// Load objects from database using SELECT supplier, finalize these objects and load and finalize missing referenced objects in a loop to ensure referential integrity.
	static <S extends SqlDomainObject> boolean loadAssuringReferentialIntegrity(Function<Connection, Map<Class<S>, Map<Long, SortedMap<String, Object>>>> select, Set<S> loadedObjects)
			throws SQLException {

		// Initially load object records using given select-supplier
		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, true)) {

			Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap = select.apply(sqlcn.cn);

			Set<S> objectsWhereReferencesChanged = new HashSet<>();
			Set<UnresolvedReference<S>> unresolvedReferences = new HashSet<>();

			// Instantiate newly loaded objects, assign changed data and references to objects, collect initially unresolved references
			boolean hasChanges = LoadAndSaveHelpers.buildObjectsFromLoadedRecords(loadedRecordsMap, loadedObjects, objectsWhereReferencesChanged, unresolvedReferences);

			// Cyclicly load and instantiate missing (parent) objects and detect unresolved references on these objects
			int c = 1;
			while (!unresolvedReferences.isEmpty()) {

				log.info("SDC: There were in total {} unresolved reference(s) of {} objects referenced by {} objects detected in {}. load cycle", unresolvedReferences.size(),
						unresolvedReferences.stream().map(ur -> ur.refField.getType().getSimpleName()).distinct().collect(Collectors.toList()),
						unresolvedReferences.stream().map(ur -> ur.obj.getClass().getSimpleName()).distinct().collect(Collectors.toList()), c++);

				// Load and instantiate missing objects of unresolved references and add loaded records to total loaded object records
				Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedMissingObjectRecordMapByDomainClassMap = LoadAndSaveHelpers.loadMissingObjectRecords(sqlcn.cn, unresolvedReferences);
				for (Entry<Class<S>, Map<Long, SortedMap<String, Object>>> entry : loadedMissingObjectRecordMapByDomainClassMap.entrySet()) {

					Class<S> objectDomainClass = entry.getKey();
					Map<Long, SortedMap<String, Object>> objectRecordMap = entry.getValue();

					loadedRecordsMap.computeIfAbsent(objectDomainClass, odc -> new HashMap<>());
					loadedRecordsMap.get(objectDomainClass).putAll(objectRecordMap);
				}

				// Initialize missed objects and determine further unresolved references
				Set<UnresolvedReference<S>> nextUnresolvedReferences = new HashSet<>();
				hasChanges |= LoadAndSaveHelpers.buildObjectsFromLoadedRecords(loadedMissingObjectRecordMapByDomainClassMap, loadedObjects, objectsWhereReferencesChanged, nextUnresolvedReferences);

				// Resolve unresolved references of last cycle after missing objects were instantiated
				LoadAndSaveHelpers.resolveUnresolvedReferences(unresolvedReferences);
				unresolvedReferences = nextUnresolvedReferences;
			}

			// Update accumulations of all objects which are referenced by any of the objects where references changed
			objectsWhereReferencesChanged.forEach(o -> o.updateAccumulationsOfParentObjects());

			return hasChanges;
		}
	}

	// -------------------------------------------------------------------------
	// Synchronization
	// -------------------------------------------------------------------------

	/**
	 * Synchronize this domain controller instance with the database.
	 * 
	 * First saves potentially existing unsaved new objects to database, than load all currently relevant objects from database and after that unregister objects which were deleted in database or fell
	 * out of data horizon (and which are not referenced by any other object).
	 * <p>
	 * Field values of existing local objects will generally be overridden by values retrieved from database on load. Unsaved changed field values, which were changed in database too, will be
	 * discarded and overridden by database values! Warn logs will be written and warnings will be assigned to affected objects in this case. It's recommended to ensure that all local changes are
	 * saved before calling {@link #synchronize(Class...)}.
	 * <p>
	 * For data horizon controlled domain classes this method first loads only objects within data horizon {@link @useDataHorizon}. But method ensures referential integrity by generally loading all
	 * referenced objects even if they are out of data horizon (ensures referential integrity)..
	 * 
	 * @param domainClassesToExclude
	 *            optional domain classes which objects shall not be loaded from database
	 * 
	 * @throws SQLException
	 *             if executed SELECT statement throws SQLException
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * 
	 * @return true if any changes detected in database, false otherwise
	 */
	@SafeVarargs
	public static boolean synchronize(Class<? extends SqlDomainObject>... domainClassesToExclude) throws SQLException, SqlDbException {

		log.info("SDC: Synchronize with database... {}",
				(domainClassesToExclude != null ? " - domain classes to exclude from loading: " + Stream.of(domainClassesToExclude).map(Class::getSimpleName).collect(Collectors.toList()) : ""));

		// Save all new objects to database (but do not save unsaved changes of already saved objects to avoid overriding database changes)
		for (DomainObject obj : findAll(o -> !((SqlDomainObject) o).isStored())) {
			((SqlDomainObject) obj).save();
		}

		// Load all objects from database - override unsaved local object changes by changes in database on contradiction
		Set<? extends SqlDomainObject> loadedObjects = new HashSet<>();
		boolean hasChanges = loadAssuringReferentialIntegrity(cn -> selectAll(cn, domainClassesToExclude), loadedObjects);

		// Unregister existing objects which were not loaded from database again (deleted in database by another instance or fell out of data horizon) and which are not referenced by any object
		for (DomainObject obj : findAll(o -> !loadedObjects.contains(o) && !o.isReferenced())) {
			((SqlDomainObject) obj).unregister();
		}

		log.info("SDC: Synchronization with database done.");

		return hasChanges;
	}

	// -------------------------------------------------------------------------
	// Load specific objects from database
	// -------------------------------------------------------------------------

	/**
	 * Load objects of only one (primary) object domain class (selected by WHERE clause if specified) and also objects directly or indirectly referenced by these primarily loaded objects (to assure
	 * referential integrity).
	 * 
	 * @param objectDomainClass
	 *            object domain class of primary objects to load
	 * @param whereClause
	 *            to further shrink amount of loaded objects
	 * @param maxCount
	 *            maximum number of primary objects to load
	 * 
	 * @return objects loaded from database potentially including referenced objects of other than the given object domain class
	 * 
	 * @throws SQLException
	 *             on opening database connection or performing SELECT statements
	 */
	public static <S extends SqlDomainObject> Set<S> loadOnly(Class<S> objectDomainClass, String whereClause, int maxCount) throws SQLException {

		log.info("SDC: Load {}'{}' objects{}", (maxCount > 0 ? "max " + maxCount + " " : ""), objectDomainClass.getSimpleName(), (!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));

		Set<S> loadedObjects = new HashSet<>();
		loadAssuringReferentialIntegrity(cn -> select(cn, objectDomainClass, whereClause, maxCount), loadedObjects);

		return loadedObjects;
	}

	// -------------------------------------------------------------------------
	// Loading specific objects and allocating objects exclusively for domain controller instance
	// -------------------------------------------------------------------------

	/**
	 * Allocate objects of one domain class for exclusive use by this domain controller instance.
	 * <p>
	 * This method is intended to synchronize access if multiple domain controller instances concurrently operate on the same database.
	 * <p>
	 * Allocated objects must later be released from exclusive use to allow exclusive use by other instances.
	 * <p>
	 * Exclusive allocation of objects is realized by inserting a record in a 'shadow' table for every allocated object with the object id as record id. The UNIQUE constraint for ID ensures, that this
	 * can be done only one time for an object.
	 * <p>
	 * Objects allocated here are typically already loaded by synchronization.
	 * 
	 * @param objectDomainClass
	 *            object domain class of objects to allocate exclusively
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation
	 * @param whereClause
	 *            WHERE clause to build SELECT statement for objects to allocate (e.g. STATUS='new')
	 * @param maxCount
	 *            maximum # of objects to allocate
	 * @param update
	 *            function to compute immediately on allocated objects or null - objects will be saved to database immediately after computing (e.g. o -> o.status = 'processing')
	 * 
	 * @return allocated objects
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL SELECT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public static <S extends SqlDomainObject> Set<S> allocateExclusively(Class<S> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount,
			Consumer<? super S> update) throws SQLException, SqlDbException {

		log.info("SDC: Allocate {}'{}' objects{} exclusively for this domain controller instance", (maxCount > 0 ? "max " + maxCount + " " : ""), objectDomainClass.getSimpleName(),
				(!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));

		// Load objects related to given object domain class
		Set<S> allocatedObjects = new HashSet<>();
		loadAssuringReferentialIntegrity(cn -> selectExclusively(cn, objectDomainClass, inProgressClass, whereClause, maxCount), allocatedObjects);

		// Filter objects of object domain class itself (because loaded objects may contain referenced objects of other domain classes too)
		allocatedObjects = allocatedObjects.stream().filter(o -> o.getClass().equals(objectDomainClass)).collect(Collectors.toSet());
		if (!allocatedObjects.isEmpty()) {

			log.info("SDC: {} '{}' objects exclusively allocated", allocatedObjects.size(), objectDomainClass.getSimpleName());

			// If update is specified: change object as defined by update parameter and UPDATE record in database by saving object, release object from exclusive use if specified
			if (update != null) {

				log.info("SDC: Update exclusively allocated objects...");

				for (S loadedObject : allocatedObjects) {
					update.accept(loadedObject);
					loadedObject.save();
				}
			}
		}
		else {
			log.info("SDC: No '{}' objects could exclusively be allocated", objectDomainClass.getSimpleName());
		}

		return allocatedObjects;
	}

	/**
	 * Exclusively compute a function on objects of one domain class and save updated objects immediately.
	 * <p>
	 * Works like {@link #allocateExclusively(Class, Class, String, int, Consumer)} but releases objects immediately after computing update function. Releasing updated objects
	 * ({@link SqlDomainObject#release(Class, Class, Consumer)} is not necessary.
	 * 
	 * @param objectDomainClass
	 *            object domain class of objects to allocate exclusively
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation
	 * @param whereClause
	 *            WHERE clause to build SELECT statement for objects to allocate (e.g. status='new')
	 * @param update
	 *            update function to compute immediately on selected objects or null - objects will be saved immediately after computing (e.g. o -> o.status = 'processed')
	 * 
	 * @return allocated objects
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL SELECT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public static <S extends SqlDomainObject> Set<S> computeExclusively(Class<S> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, String whereClause, Consumer<? super S> update)
			throws SQLException, SqlDbException {

		// Allocate objects exclusively and - if specified - performing 'update' for any allocated object
		Set<S> loadedObjects = allocateExclusively(objectDomainClass, inProgressClass, whereClause, -1, update);

		// Immediately release allocated objects from exclusive use
		for (S object : loadedObjects) {
			object.release(objectDomainClass, inProgressClass, null);
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
	 * @param <S>
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
	public static <S extends SqlDomainObject> S createAndSave(Connection cn, Class<S> objectDomainClass, Consumer<S> init) throws SQLException, SqlDbException {

		S obj = create(objectDomainClass, init);
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
	 * @param <S>
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
	public static <S extends SqlDomainObject> S createAndSave(Class<S> objectDomainClass, Consumer<S> init) throws SQLException, SqlDbException {

		S obj = create(objectDomainClass, init);
		obj.save();

		return obj;
	}

}
