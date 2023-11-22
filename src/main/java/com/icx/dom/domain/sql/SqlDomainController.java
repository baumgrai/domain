package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CDateTime;
import com.icx.dom.common.Common;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainException;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.jdbc.ConfigException;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;
import com.icx.dom.jdbc.SqlDbTable.UniqueConstraint;

/**
 * Singleton to manage persistence using SQL database. Includes methods to load persisted domain objects from database and to create and immediately persist new objects.
 * 
 * @author RainerBaumgärtel
 */
public class SqlDomainController extends DomainController<SqlDomainObject> {

	static final Logger log = LoggerFactory.getLogger(SqlDomainController.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final String DOMAIN_PROPERIES_FILE = "domain.properties";
	public static final String DATA_HORIZON_PERIOD_PROP = "dataHorizonPeriod";

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Database connection object
	public SqlDb sqlDb = null;

	// Config properties from 'domain.properties'
	private String dataHorizonPeriod = "1M"; // Data horizon controlled objects will be loaded from database only if they are modified after data horizon (now minus this period)

	// Record map: map of object records by object domain class by object id
	protected Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> recordMap = null;

	// -------------------------------------------------------------------------
	// Constructor
	// -------------------------------------------------------------------------

	public SqlDomainController() {
		registry = new SqlRegistry();
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	// Initialize database connection by given connection properties and associate domain classes and database tables
	void initializeDatabase(Properties dbProperties, Properties domainProperties) throws SQLException, ConfigException, SqlDbException {

		sqlDb = new SqlDb(dbProperties);
		if (domainProperties != null) {
			dataHorizonPeriod = domainProperties.getProperty(DATA_HORIZON_PERIOD_PROP, "1M");
		}
		try (SqlConnection sqlConnection = SqlConnection.open(sqlDb.pool, true)) {
			((SqlRegistry) registry).registerDomainClassTableAssociation(sqlConnection.cn, sqlDb);
		}
		recordMap = new ConcurrentHashMap<>();
		registry.getRegisteredObjectDomainClasses().forEach(c -> recordMap.put(c, new ConcurrentHashMap<>()));
	}

	/**
	 * Register domain classes by given domain package.
	 * <p>
	 * All top level classes in this package and in any sub package which extends SqlDomainObject class will be registered as domain classes.
	 * 
	 * @param dbProperties
	 *            database connection properties ('dbConnectionString', 'dbUser', 'dbPassword')
	 * @param domainProperties
	 *            currently only 'dataHorizonPeriod' (e.g. 1h, 30d)
	 * @param domainPackageName
	 *            name of package where domain classes reside
	 * 
	 * @throws DomainException
	 *             on error registering domain classes
	 * @throws ConfigException
	 *             on invalid or missing database connection string of unsupported database type
	 * @throws SQLException
	 *             if database connection cannot be established or on SQL errors associating domain classes and database tables
	 * @throws SqlDbException
	 *             on errors associating domain classes and database tables
	 */
	public void initialize(Properties dbProperties, Properties domainProperties, String domainPackageName) throws DomainException, SQLException, ConfigException, SqlDbException {

		registerDomainClasses(SqlDomainObject.class, domainPackageName);
		initializeDatabase(dbProperties, domainProperties);
	}

	/**
	 * Register given object domain classes and derived domain classes.
	 * <p>
	 * Classes must extend SqlDomainObject class. Only object domain class (highest class in derivation order) must be given here if derived domain classes exists.
	 * 
	 * @param dbProperties
	 *            database connection properties ('dbConnectionString', 'dbUser', 'dbPassword')
	 * @param domainProperties
	 *            currently only 'dataHorizonPeriod' (e.g. 1h, 30d)
	 * @param domainClasses
	 *            list of object domain classes to register
	 * 
	 * @throws DomainException
	 *             on error registering domain classes
	 * @throws ConfigException
	 *             on invalid or missing database connection string of unsupported database type
	 * @throws SQLException
	 *             if database connection cannot be established or on SQL errors associating domain classes and database tables
	 * @throws SqlDbException
	 *             on errors associating domain classes and database tables
	 */
	@SafeVarargs
	public final void initialize(Properties dbProperties, Properties domainProperties, Class<? extends SqlDomainObject>... domainClasses)
			throws DomainException, SQLException, ConfigException, SqlDbException {

		registerDomainClasses(SqlDomainObject.class, domainClasses);
		initializeDatabase(dbProperties, domainProperties);
	}

	/**
	 * Get current data horizon date (date before that no data horizon controlled objects will be loaded)
	 * 
	 * @return current data horizon date
	 */
	public LocalDateTime getCurrentDataHorizon() {
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
	public final <S extends SqlDomainObject> Set<S> allValid(Class<S> domainClass) {
		return (Set<S>) objectMap.get(domainClass).values().stream().filter(o -> (o).isValid()).collect(Collectors.toSet());
	}

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
	 * referenced objects even if they are out of data horizon..
	 * 
	 * @param domainClassesToExclude
	 *            (optional) domain classes which objects shall not be loaded from database
	 * 
	 * @return true if any changes detected in database, false otherwise
	 * 
	 * @throws SQLException
	 *             if executed SELECT statement throws SQLException
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@SafeVarargs
	public final boolean synchronize(Class<? extends SqlDomainObject>... domainClassesToExclude)
			throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		log.info("SDC: Synchronize with database... {}",
				(domainClassesToExclude != null ? " - domain classes to exclude from loading: " + Stream.of(domainClassesToExclude).map(Class::getSimpleName).collect(Collectors.toList()) : ""));

		// Save all new objects to database (but do not save unsaved changes of already saved objects to avoid overriding database changes)
		for (SqlDomainObject obj : findAll(o -> !o.isStored())) {
			save(obj);
		}

		// Load all objects from database - override unsaved local object changes by changes in database on contradiction
		Set<SqlDomainObject> loadedObjects = new HashSet<>();
		boolean hasChanges = LoadHelpers.loadAssuringReferentialIntegrity(this, cn -> LoadHelpers.selectAll(cn, this, domainClassesToExclude), loadedObjects);

		// Unregister existing objects which were not loaded from database again (deleted in database by another instance or fell out of data horizon) and which are not referenced by any object
		for (SqlDomainObject obj : findAll(o -> !loadedObjects.contains(o) && !isReferenced(o))) {
			unregister(obj);
		}

		log.info("SDC: Synchronization with database done.");

		return hasChanges;
	}

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
	 * @throws SqlDbException
	 *             if object domain class of any referenced object could not be determined by SELECTing object record
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public Set<SqlDomainObject> loadOnly(Class<? extends SqlDomainObject> objectDomainClass, String whereClause, int maxCount)
			throws SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, SqlDbException {

		log.info("SDC: Load {}'{}' objects{}", (maxCount > 0 ? "max " + maxCount + " " : ""), objectDomainClass.getSimpleName(), (!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));
		Set<SqlDomainObject> loadedObjects = new HashSet<>();
		LoadHelpers.loadAssuringReferentialIntegrity(this, cn -> LoadHelpers.select(cn, this, objectDomainClass, whereClause, maxCount), loadedObjects);
		return loadedObjects;
	}

	/**
	 * (Re)load object from database.
	 * <p>
	 * If object is not initially saved or is not registered this method does nothing.
	 * <p>
	 * If direct or indirect parent objects exist which are not yet loaded due to data horizon control these object will be loaded (and instantiated) too.
	 * <p>
	 * Attention: Overrides unsaved changes of this object.
	 * 
	 * @throws SQLException
	 *             on error establishing database connection
	 * @throws SqlDbException
	 *             if object domain class of any referenced object could not be determined by SELECTing object record
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public <S extends SqlDomainObject> boolean reload(S obj) throws SQLException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, SqlDbException {

		if (!obj.isStored) {
			log.warn("SDO: {} is not yet stored in database and therefore cannot be loaded!", obj.name());
			return false;
		}
		else if (!isRegistered(obj)) {
			log.warn("SDO: {} is not registered in object store and cannot be loaded from database!", obj.name());
			return false;
		}

		if (log.isDebugEnabled()) {
			log.debug("SDO: Load {} from database", obj.name());
		}
		return LoadHelpers.loadAssuringReferentialIntegrity(this, cn -> LoadHelpers.selectObjectRecord(cn, this, obj), new HashSet<>());
	}

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
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@SuppressWarnings("unchecked")
	public <S extends SqlDomainObject> Set<S> allocateObjectsExclusively(Class<S> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount,
			Consumer<? super S> update) throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		log.info("SDC: Allocate {}'{}' objects{} exclusively for this domain controller instance", (maxCount > 0 ? "max " + maxCount + " " : ""), objectDomainClass.getSimpleName(),
				(!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));

		// Load objects related to given object domain class
		Set<S> allocatedObjects = new HashSet<>();
		LoadHelpers.loadAssuringReferentialIntegrity(this, cn -> LoadHelpers.selectExclusively(cn, this, objectDomainClass, inProgressClass, whereClause, maxCount),
				(Set<SqlDomainObject>) allocatedObjects);

		// Filter objects of object domain class itself (because loaded objects may contain referenced objects of other domain classes too)
		allocatedObjects = allocatedObjects.stream().filter(o -> o.getClass().equals(objectDomainClass)).collect(Collectors.toSet());
		if (!allocatedObjects.isEmpty()) {
			log.info("SDC: {} '{}' objects exclusively allocated", allocatedObjects.size(), objectDomainClass.getSimpleName());

			// If update is specified: change object as defined by update parameter and UPDATE record in database by saving object, release object from exclusive use if specified
			if (update != null) {
				log.info("SDC: Update exclusively allocated objects...");
				for (S loadedObject : allocatedObjects) {
					update.accept(loadedObject);
					save(loadedObject);
				}
			}
		}
		else {
			log.info("SDC: No '{}' objects could exclusively be allocated", objectDomainClass.getSimpleName());
		}

		return allocatedObjects;
	}

	/**
	 * Allocate this object exclusively, compute an update function on this object and save changed object immediately.
	 * 
	 * @param domainObjectClass
	 *            formal parameter - only to allow specifying update function without class cast
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation
	 * @param update
	 *            update function to perform on this object on allocating or null (e.g. o -> o.status = 'processing')
	 * 
	 * @return true if this object could be allocated exclusively (and 'update' could be computed if specified), false otherwise
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL UPDATE statement on saving object
	 * @throws SqlDbException
	 *             on internal errors
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	@SuppressWarnings("unchecked")
	public <S extends SqlDomainObject> boolean allocateObjectExclusively(S obj, Class<? extends SqlDomainObject> inProgressClass, Consumer<? super S> update)
			throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		String whereClause = ((SqlRegistry) registry).getTableFor(obj.getClass()).name + ".ID=" + obj.getId();
		Set<S> allocatedObjects = allocateObjectsExclusively((Class<S>) obj.getClass(), inProgressClass, whereClause, -1, update);
		return (!allocatedObjects.isEmpty());
	}

	/**
	 * Release this object from exclusive use.
	 * 
	 * @param domainObjectClass
	 *            formal parameter - only to allow specifying update function without class cast
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation
	 * @param update
	 *            update function to perform on this object on releasing or null (e.g. o -> o.status = 'done')
	 * 
	 * @return true if object was exclusively allocated before, false otherwise
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL UPDATE statement on saving object
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public <S extends SqlDomainObject> boolean releaseObject(S obj, Class<? extends SqlDomainObject> inProgressClass, Consumer<? super S> update) throws SQLException, SqlDbException {

		// Check if object is allocated for exclusive use
		SqlDomainObject inProgressObject = find(inProgressClass, obj.getId());
		if (inProgressObject == null) {
			log.warn("SDO: {} is currently not allocated for exclusive usage", obj);
			return false;
		}

		// Change object as defined by update parameter and UPDATE record in database on saving object
		log.info("SDO: Release {} from exclusive use ({})", obj, update);
		if (update != null) {
			update.accept(obj);
			save(obj);
		}

		// Delete in-progress record
		delete(inProgressObject);

		return true;
	}

	/**
	 * Release multiple objects
	 * 
	 * @param <S>
	 * @param objects
	 * @param inProgressClass
	 * @throws SQLException
	 * @throws SqlDbException
	 */
	public <S extends SqlDomainObject> void releaseObjects(Collection<S> objects, Class<? extends SqlDomainObject> inProgressClass) throws SQLException, SqlDbException {
		for (S obj : objects) {
			releaseObject(obj, inProgressClass, null);
		}
	}

	/**
	 * Exclusively compute a function on objects of one domain class and save updated objects immediately.
	 * <p>
	 * Works like {@link #allocateObjectsExclusively(Class, Class, String, int, Consumer)} but releases objects immediately after computing update function. Releasing updated objects
	 * ({@link SqlDomainObject#releaseObject(Class, Class, Consumer)} is not necessary.
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
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public <S extends SqlDomainObject> Set<S> computeExclusivelyOnObjects(Class<S> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, String whereClause, Consumer<? super S> update)
			throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		Set<S> loadedObjects = allocateObjectsExclusively(objectDomainClass, inProgressClass, whereClause, -1, update);
		releaseObjects(loadedObjects, inProgressClass);
		return loadedObjects;
	}

	/**
	 * Allocate this object exclusively, compute an update function on this object, save changed object and releases object immediately from exclusive use.
	 * 
	 * @param domainObjectClass
	 *            formal parameter - only to allow specifying update function without class cast
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation
	 * @param update
	 *            update function to perform on this object or null (e.g. o -> o.count++)
	 * 
	 * @return true if this object could be allocated exclusively (and 'update' could be computed if specified), false otherwise
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL UPDATE statement on saving object
	 * @throws SqlDbException
	 *             on internal errors
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public boolean computeExclusivelyOnObject(SqlDomainObject obj, Class<? extends SqlDomainObject> inProgressClass, Consumer<? super SqlDomainObject> update)
			throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		if (allocateObjectExclusively(obj, inProgressClass, update)) {
			releaseObject(obj, inProgressClass, update);
			return true;
		}
		else {
			return false;
		}
	}

	/**
	 * Save object on existing database connection.
	 * 
	 * @param cn
	 *            database connection
	 * 
	 * @throws SQLException
	 *             exception thrown during execution of INSERT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public void save(Connection cn, SqlDomainObject obj) throws SQLException, SqlDbException {

		if (!isRegistered(obj)) {
			log.warn("SDO: Object {} cannot be saved because it was not registered before!", obj.name());
			return;
		}

		try {
			SaveHelpers.save(cn, this, obj, new ArrayList<>());
		}
		catch (SqlDbException sqldbex) {
			log.error("SDO: Object {} cannot be saved", obj.name());
			log.info("SDO: {}: {}", sqldbex.getClass().getSimpleName(), sqldbex.getMessage());

			obj.currentException = sqldbex;
			cn.rollback();

			log.warn("SDO: Whole save transaction rolled back!");

			throw sqldbex;
		}
	}

	/**
	 * Save changed object to database.
	 * <p>
	 * INSERTs object records for new object or UPDATE columns associated to changed fields for existing objects. UPDATEs element and key/value tables associated to entry fields of object too.
	 * <p>
	 * On failed saving the SQL exception thrown and the field error(s) recognized will be assigned to object. In this case object is marked as invalid and will not be found using
	 * {@link SqlDomainController#allValid()}. Invalid field content remains (to allow using it as base for correction). If invalid field content shall be overridden by existing valid content one can
	 * use {@link #reload()}. If object was already saved UPDATE will be tried for every column separately to keep impact of failure small.
	 * <p>
	 * If initial saving (INSERTing object records) fails whole transaction will be ROLLed BACK
	 * 
	 * @throws SQLException
	 *             exception thrown during establishing database connection or execution of INSERT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public void save(SqlDomainObject obj) throws SQLException, SqlDbException {

		// Use one transaction for all INSERTs and UPDATEs to allow ROLL BACK of whole transaction on error - on success transaction will automatically be committed on closing connection
		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, false)) {
			save(sqlcn.cn, obj);
		}
	}

	/**
	 * Create, initialize, register and save object of domain class using given database connection.
	 * <p>
	 * Calls initialization function before object registration to ensure that registered object is initialized.
	 * <p>
	 * Uses default constructor which therefore must be defined explicitly if other constructors are defined!
	 * <p>
	 * If object cannot be saved due to SQL exception on INSERT object will marked as invalid and exception and field error(s) will be assigned to object.
	 * 
	 * @param cn
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
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public <S extends SqlDomainObject> S createAndSave(Connection cn, Class<S> objectDomainClass, Consumer<S> init)
			throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		S obj = create(objectDomainClass, init);
		save(cn, obj);
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
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public <S extends SqlDomainObject> S createAndSave(Class<S> objectDomainClass, Consumer<S> init)
			throws SQLException, SqlDbException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		S obj = create(objectDomainClass, init);
		save(obj);
		return obj;
	}

	// Create object with given id - only used for exclusive selection methods
	final <S extends SqlDomainObject> S createWithId(final Class<S> domainObjectClass, long id)
			throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		S obj = instantiate(domainObjectClass);
		if (obj != null) {
			if (!registerById(obj, id)) { // An object of this domain class already exists and is registered
				return null;
			}
			if (log.isDebugEnabled()) {
				log.debug("DC: Created {}.", obj.name());
			}
		}
		return obj;
	}

	// Remove object record from record map and unregister object from object store
	@Override
	protected void unregister(SqlDomainObject obj) {
		if (recordMap.containsKey(obj.getClass())) {
			recordMap.get(obj.getClass()).remove(obj.getId());
		}
		super.unregister(obj);
	}

	// Only for unit tests
	public void unregisterOnlyForTest(SqlDomainObject obj) {
		unregister(obj);
	}

	// Re-register object (on failed deletion)
	void reregister(SqlDomainObject obj) {

		// Re-register object in object store
		registerById(obj, obj.getId());

		// Collect field changes (because object record was removed here all field/value pairs will be found) and re-generate object record from field/value pairs of all inherited domain classes
		SortedMap<String, Object> objectRecord = new TreeMap<>();
		for (Class<? extends SqlDomainObject> domainClass : registry.getDomainClassesFor(obj.getClass())) {
			Map<Field, Object> fieldChangesMap = SaveHelpers.getFieldChangesForDomainClass(((SqlRegistry) registry), obj, objectRecord, domainClass);
			objectRecord.putAll(SaveHelpers.fieldChangesMap2ColumnValueMap(((SqlRegistry) registry), fieldChangesMap, obj));
		}

		// Re-insert object record
		recordMap.get(obj.getClass()).put(obj.getId(), objectRecord);

		log.info("DC: Re-registered {} (by original id)", obj.name());
	}

	// Only for unit tests
	public void reregisterOnlyForTest(SqlDomainObject obj) {
		reregister(obj);
	}

	// Check for UNIQUE constraint violation
	public boolean hasUniqueConstraintViolations(SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass) {

		boolean isConstraintViolated = false;

		// Check if UNIQUE constraint is violated (UNIQUE constraints of single columns are also realized as table UNIQUE constraints)
		SqlDbTable table = ((SqlRegistry) registry).getTableFor(domainClass);
		for (UniqueConstraint uc : table.uniqueConstraints) {

			// Build predicate to check combined uniqueness and build lists of involved fields and values
			Predicate<SqlDomainObject> multipleUniqueColumnsPredicate = null;
			List<Field> combinedUniqueFields = new ArrayList<>();
			List<Object> fieldValues = new ArrayList<>();

			for (Column col : uc.columns) {
				for (Field fld : registry.getDataAndReferenceFields(domainClass)) {
					if (Common.objectsEqual(col, ((SqlRegistry) registry).getColumnFor(fld))) {
						combinedUniqueFields.add(fld);
						Object fldValue = obj.getFieldValue(fld);
						fieldValues.add(fldValue);
						if (multipleUniqueColumnsPredicate == null) {
							multipleUniqueColumnsPredicate = o -> Common.objectsEqual(o.getFieldValue(fld), fldValue);
						}
						else {
							multipleUniqueColumnsPredicate = multipleUniqueColumnsPredicate.and(o -> Common.objectsEqual(o.getFieldValue(fld), fldValue));
						}
					}
				}
			}

			if (multipleUniqueColumnsPredicate != null && count(registry.getCastedDomainClass(obj), multipleUniqueColumnsPredicate) > 1) {

				List<String> fieldNames = combinedUniqueFields.stream().map(Member::getName).collect(Collectors.toList());
				if (uc.columns.size() == 1) {
					log.error("SDO: \tColumn '{}' is UNIQUE by constraint '{}' but '{}' object '{}' already exists with same value '{}' of field '{}'!",
							uc.columns.stream().map(c -> c.name).collect(Collectors.toList()).get(0), uc.name, obj.getClass().getSimpleName(), DomainObject.name(obj), fieldValues.get(0),
							fieldNames.get(0));
				}
				else {
					log.error("SDO: \tColumns {} are UNIQUE together by constraint '{}' but '{}' object '{}'  already exists with same values {} of fields {}!",
							uc.columns.stream().map(c -> c.name).collect(Collectors.toList()), uc.name, obj.getClass().getSimpleName(), DomainObject.name(obj), fieldValues, fieldNames);
				}

				for (Field fld : combinedUniqueFields) {
					obj.setFieldError(fld, "COMBINED_UNIQUE_CONSTRAINT_VIOLATION_OF " + fieldNames + ": " + fieldValues);
					isConstraintViolated = true;
				}
			}
		}

		return isConstraintViolated;
	}

	// Check if value is too long for storing in associated column (yields only for enum fields - values of String fields which are to long will be truncated and field warning is generated on saving)
	public boolean hasColumnSizeViolations(SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass) {

		boolean isConstraintViolated = false;
		for (Field field : registry.getDataAndReferenceFields(domainClass)) {
			Column column = ((SqlRegistry) registry).getColumnFor(field);
			Object fieldValue = obj.getFieldValue(field);

			if ((fieldValue instanceof String || fieldValue instanceof Enum) && column.maxlen < fieldValue.toString().length()) {
				log.error("SDO: \tField  '{}': value '{}' is too long for associated column '{}' with maxlen {} for object {}!", field.getName(), fieldValue, column.name, column.maxlen,
						DomainObject.name(obj));
				obj.setFieldError(field, "COLUMN_SIZE_VIOLATION");
				isConstraintViolated = true;
			}
		}
		return isConstraintViolated;
	}

	// Check if null value is provided for any column which has NOT NULL constraint
	public boolean hasNotNullConstraintViolations(SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass) {

		boolean isConstraintViolated = false;
		for (Field field : registry.getDataAndReferenceFields(domainClass)) {
			Column column = ((SqlRegistry) registry).getColumnFor(field);

			if (!column.isNullable && obj.getFieldValue(field) == null) {
				log.error("SDO: \tField  '{}': associated with column '{}' has NOT NULL constraint but field value provided is null for object {}!", field.getName(), column.name,
						DomainObject.name(obj));
				obj.setFieldError(field, "NOT_NULL_CONSTRAINT_VIOLATION");
				isConstraintViolated = true;
			}
		}
		return isConstraintViolated;
	}

	/**
	 * Check constraint violations without involving database.
	 * <p>
	 * Assign field errors on fields for which constraints are violated.
	 * <p>
	 * Note: column size violation are detected here only for String fields which hold enums - values of text fields which are to long will be truncated and field warning is generated on saving.
	 * 
	 * @return true if any field constraint is violated, false otherwise
	 */
	public boolean hasConstraintViolations(SqlDomainObject obj) {

		boolean isAnyConstraintViolated = false;
		for (Class<? extends SqlDomainObject> domainClass : registry.getDomainClassesFor(obj.getClass())) {
			isAnyConstraintViolated |= hasNotNullConstraintViolations(obj, domainClass);
			isAnyConstraintViolated |= hasUniqueConstraintViolations(obj, domainClass);
			isAnyConstraintViolated |= hasColumnSizeViolations(obj, domainClass);
		}
		return isAnyConstraintViolated;
	}

	/**
	 * Delete object and child objects using existing database connection without can-be-deleted check.
	 * 
	 * @param cn
	 *            database connection
	 * 
	 * @throws SQLException
	 *             exceptions thrown on executing SQL DELETE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public void delete(Connection cn, SqlDomainObject obj) throws SQLException, SqlDbException {

		// Delete object and children
		List<SqlDomainObject> unregisteredObjects = new ArrayList<>();
		try {
			DeleteHelpers.deleteRecursiveFromDatabase(cn, this, obj, unregisteredObjects, null, 0);
		}
		catch (SQLException | SqlDbException sqlex) {
			log.error("SDO: Delete: Object {} cannot be deleted", obj.name());
			log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

			// Assign exception to object and ROLL BACK complete DELETE transaction
			obj.currentException = sqlex;
			cn.rollback();

			// Re-register already unregistered objects and re-generate object records
			for (SqlDomainObject o : unregisteredObjects) {
				reregister(o);
			}

			log.warn("SDO: Whole delete transaction rolled back!");
			throw sqlex;
		}
	}

	/**
	 * Check if object can be deleted and if so unregisters object and all direct and indirect child objects, delete associated records from database and removes object from existing accumulations of
	 * parent objects.
	 * <p>
	 * No object will be unregistered, no database record will be deleted and no accumulation will be changed if deletion of any child object is not possible (complete SQL transaction will be ROLLed
	 * BACK and already unregistered objects will be re-registered in this case).
	 * <p>
	 * Note: Database records of old 'data horizon' controlled child objects, which were not loaded, will be deleted by ON DELETE CASCADE (which is automatically set for FOREIGN KEYs in tables of
	 * 'data horizon' controlled domain classes in SQL scripts for database generation).
	 * 
	 * @return true if deletion was successful, false if object or at least one of its direct or indirect children cannot be deleted by can-be-deleted check.
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL DELETE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public boolean delete(SqlDomainObject obj) throws SQLException, SqlDbException {

		LocalDateTime now = LocalDateTime.now();

		// Recursively check if this object and all direct and indirect children can be deleted
		if (!canBeDeletedRecursive(obj, new ArrayList<>())) {
			log.info("SDO: {} cannot be deleted because overriden #canBeDeleted() of {} returned false!", obj.name(), obj.getClass().getSimpleName());
			return false;
		}

		// Use one transaction for all DELETE operations to allow ROLLBACK of whole transaction on error - on success transaction will automatically be committed later on closing connection
		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.pool, false)) {

			delete(sqlcn.cn, obj);
			if (log.isDebugEnabled()) {
				log.debug("SDO: Deletion of {} and all children took: {}", obj.name(), ChronoUnit.MILLIS.between(now, LocalDateTime.now()) + "ms");
			}
			return true;
		}
		catch (SQLException sqlex) {
			log.error("SDO: Delete: Object {} cannot be deleted", obj.name());
			log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

			obj.currentException = sqlex;

			throw sqlex;
		}
	}

}
