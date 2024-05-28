package com.icx.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

import com.icx.common.CDateTime;
import com.icx.common.CLog;
import com.icx.common.Common;
import com.icx.domain.DomainController;
import com.icx.domain.DomainException;
import com.icx.domain.DomainObject;
import com.icx.domain.sql.Annotations.StoreAsString;
import com.icx.domain.sql.Annotations.UseDataHorizon;
import com.icx.domain.sql.Loader.IntermediateLoadResult;
import com.icx.domain.sql.Loader.UnresolvedReference;
import com.icx.domain.sql.tools.Java2Sql;
import com.icx.jdbc.ConfigException;
import com.icx.jdbc.ConnectionPool;
import com.icx.jdbc.SqlConnection;
import com.icx.jdbc.SqlDb;
import com.icx.jdbc.SqlDbException;
import com.icx.jdbc.SqlDbHelpers;
import com.icx.jdbc.SqlDbTable;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;
import com.icx.jdbc.SqlDbTable.SqlDbUniqueConstraint;

/**
 * Domain controller for SQL based persistence mechanism. Provides methods to load and store domain objects from and to database. Provides also method to delete persisted objects from object store and
 * database.
 * 
 * @author baumgrai
 */
public class SqlDomainController extends DomainController<SqlDomainObject> {

	static final Logger log = LoggerFactory.getLogger(SqlDomainController.class);

	// TODO: Support file encryption

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	/**
	 * Name of standard properties file for domain properties
	 */
	public static final String DOMAIN_PROPERIES_FILE = "domain.properties";
	static final String DATA_HORIZON_PERIOD_PROP = "dataHorizonPeriod";
	static final String CRYPT_PASSWORD_PROP = "cryptPassword";
	static final String CRYPT_SALT_PROP = "cryptSalt";

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Database connection object
	SqlDb sqlDb = null;

	// Config properties from 'domain.properties'
	String dataHorizonPeriod = "1M"; // Data horizon controlled objects will be loaded from database only if they are modified after data horizon ('now' minus data horizon period)
	String cryptPassword = null;
	String cryptSalt = null;

	// Record map: map of object records by object domain class by object id
	// Note: Objects of domain classes which are derived from other domain classes however have only one object record with column content of all tables for derived classes
	Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> recordMap = null;

	// List field order cache
	Map<String, Map<Long, List<Long>>> listOrderCacheMap = new HashMap<>();

	List<Long> getOrderedListOrderNumbers(String entryTableName, long objectId) {
		return listOrderCacheMap.computeIfAbsent(entryTableName, m -> new HashMap<>()).computeIfAbsent(objectId, m -> new ArrayList<>());
	}

	void setOrderedListOrderNumbers(String entryTableName, long objectId, List<Long> orderedOrderNumbers) {
		listOrderCacheMap.computeIfAbsent(entryTableName, m -> new HashMap<>()).put(objectId, orderedOrderNumbers);
	}

	/**
	 * Informative: counter for successfully exclusively accessed objects since startup.
	 */
	public long successfulExclusiveAccessCount = 0L;

	/**
	 * Informative: counter for exclusive access collisions caused by concurrent access tries of same domain controller instance since startup.
	 */
	public long inUseBySameInstanceAccessCount = 0L;

	/**
	 * Informative: counter for exclusive access collisions caused by concurrent access tries of different domain controller instances since startup.
	 */
	public long inUseByDifferentInstanceAccessCount = 0L;

	// -------------------------------------------------------------------------
	// Constructor & basics
	// -------------------------------------------------------------------------

	/**
	 * Constructor.
	 */
	public SqlDomainController() {
		setRegistry(new SqlRegistry());
	}

	/**
	 * Only for internal use!
	 * 
	 * @return SQL registry
	 */
	// Get SQL registry
	public SqlRegistry getSqlRegistry() {
		return (SqlRegistry) getRegistry();
	}

	/**
	 * Get high level SQL database connection object.
	 * 
	 * @return high level SQL database connection object
	 */
	public SqlDb getSqlDb() {
		return sqlDb;
	}

	/**
	 * Get connection pool associated to associated high level SQL database connection object.
	 * 
	 * @return connection pool
	 */
	public ConnectionPool getPool() {
		return sqlDb.getPool();
	}

	/**
	 * Close potentially open SQL connections in connection pool associated to high level SQL database connection object.
	 * 
	 * @throws SQLException
	 *             on exception closing SQL database connection
	 */
	public void close() throws SQLException {
		sqlDb.close();
	}

	/**
	 * Only for unit tests.
	 * 
	 * @return period
	 */
	public String getDataHorizonPeriodForTest() {
		return dataHorizonPeriod;
	}

	/**
	 * Only for unit tests.
	 * 
	 * @param dataHorizonPeriod
	 *            period to set
	 */
	public void setDataHorizonPeriodForTest(String dataHorizonPeriod) {
		this.dataHorizonPeriod = dataHorizonPeriod;
	}

	// -------------------------------------------------------------------------
	// Register to-string and from-string converters
	// -------------------------------------------------------------------------

	/**
	 * Register to-string and from-string converter for individual field types.
	 * <p>
	 * Fields of types, for which to-string and from-string converters are defined, must be annotated by {@link StoreAsString} annotation. Columns associated with these fields are of database specific
	 * string type - (N)VARCHAR(charsize). Values will be converted to string using to-string converter before being stored in database on {@code save()} and loaded values will be re-converted using
	 * from-string converter on loading objects from database ({@code synchronize()}, {@code load...()}).
	 * <p>
	 * Registering specific to-string and from-string converters is effective only for value types which are not natively supported by Domain persistence mechanism (see {@link Java2Sql}).
	 * <p>
	 * Note: Another possibility to define to-string and from-string conversion is to declare public methods {@code toString()} and {@code valueOf(String)} for specific field types, which than
	 * automatically will be called by Reflection for conversion on storing and retrieving field values. Registration of these methods is not necessary. Fields holding values of such types must also
	 * be annotated by {@link StoreAsString} annotation.
	 * 
	 * @param cls
	 *            class of values to convert
	 * @param toStringConverter
	 *            to-string converter or null for toString()
	 * @param fromStringConverter
	 *            from-string converter
	 */
	public static void registerStringConvertersForType(Class<?> cls, Function<Object, String> toStringConverter, Function<String, Object> fromStringConverter) {

		SqlDbHelpers.addToStringConverter(cls, toStringConverter);
		SqlDbHelpers.addFromStringConverter(cls, fromStringConverter);
	}

	// -------------------------------------------------------------------------
	// Initialization methods
	// -------------------------------------------------------------------------

	// Initialize database connection by given connection properties and associate domain classes and database tables
	void initializeDatabase(Properties dbProperties, Properties domainProperties) throws SQLException, ConfigException, SqlDbException {

		sqlDb = new SqlDb(dbProperties);
		if (domainProperties != null) {
			dataHorizonPeriod = domainProperties.getProperty(DATA_HORIZON_PERIOD_PROP, "1M");
			cryptPassword = domainProperties.getProperty(CRYPT_PASSWORD_PROP, null);
			cryptSalt = domainProperties.getProperty(CRYPT_SALT_PROP, "SALTSALT");
			if (isEmpty(cryptPassword)) {
				log.warn(
						"SDC: Use of @Crypt annotation for fields or domain classes needs non-empty 'cryptPassword' property in 'domain.properties' file! If this property is not configured field values will be stored in database without encryption!");
			}
		}

		try (SqlConnection sqlConnection = SqlConnection.open(sqlDb.getPool(), true)) {
			getSqlRegistry().registerDomainClassTableAssociation(sqlConnection.cn, sqlDb);
		}

		recordMap = new ConcurrentHashMap<>();

		getRegistry().getRegisteredObjectDomainClasses().forEach(c -> recordMap.put(c, new ConcurrentHashMap<>()));
	}

	/**
	 * Register domain classes by given domain package where these classes reside.
	 * <p>
	 * All classes in this package and in any sub package which extend {@link SqlDomainObject} class will be registered as domain classes.
	 * 
	 * @param dbProperties
	 *            database connection properties ('dbConnectionString', 'dbUser', 'dbPassword')
	 * @param domainProperties
	 *            only 'dataHorizonPeriod' is relevant here (e.g. '1h', '30d')
	 * @param domainPackageName
	 *            name of package where domain classes reside
	 * 
	 * @throws DomainException
	 *             on error registering domain classes
	 * @throws ConfigException
	 *             on invalid or missing database connection string or unsupported database type
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
	 * Register given object domain classes and base domain classes.
	 * <p>
	 * Provided classes must extend {@link SqlDomainObject} class. Only object domain class (highest class in inheritance order) must be provided here if inherited domain classes exists.
	 * <p>
	 * Inner domain classes and parent domain classes (which are referenced by provided domain classes) will also be registered.
	 * 
	 * @param dbProperties
	 *            database connection properties ('dbConnectionString', 'dbUser', 'dbPassword')
	 * @param domainProperties
	 *            currently only 'dataHorizonPeriod' (e.g. '1h', '30d')
	 * @param objectDomainClasses
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
	public final void initialize(Properties dbProperties, Properties domainProperties, Class<? extends SqlDomainObject>... objectDomainClasses)
			throws DomainException, SQLException, ConfigException, SqlDbException {

		registerDomainClasses(SqlDomainObject.class, objectDomainClasses);
		initializeDatabase(dbProperties, domainProperties);
	}

	// -------------------------------------------------------------------------
	// Miscellaneous
	// -------------------------------------------------------------------------

	/**
	 * Get current data horizon date ('now' minus configured data horizon).
	 * 
	 * @return current data horizon date
	 */
	public LocalDateTime getCurrentDataHorizon() {
		return CDateTime.subtract(LocalDateTime.now(), dataHorizonPeriod);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.icx.domain.DomainController#unregister(com.icx.domain.DomainObject)
	 */
	// Remove object record from record map and unregister object from object store
	@Override
	protected void unregister(SqlDomainObject obj) {
		if (recordMap.containsKey(obj.getClass())) {
			recordMap.get(obj.getClass()).remove(obj.getId());
		}
		super.unregister(obj);
	}

	/**
	 * Only for unit tests.
	 * 
	 * @param obj
	 *            object to unregister
	 */
	public void unregisterOnlyForTest(SqlDomainObject obj) {
		unregister(obj);
	}

	// Re-register object (on failed deletion)
	void reregister(SqlDomainObject obj) {

		// Re-register object in object store
		registerById(obj, obj.getId());

		// Collect field changes (because object record was removed here all field/value pairs will be found) and re-generate object record from field/value pairs of all inherited domain classes
		SortedMap<String, Object> objectRecord = new TreeMap<>();
		for (Class<? extends SqlDomainObject> domainClass : getRegistry().getDomainClassesFor(obj.getClass())) {
			Map<Field, Object> fieldChangesMap = Saver.getFieldChangesForDomainClass(this, obj, objectRecord, domainClass);
			objectRecord.putAll(Saver.fieldChangesMap2ColumnValueMap(this, fieldChangesMap, obj));
		}

		// Re-insert object record
		recordMap.get(obj.getClass()).put(obj.getId(), objectRecord);

		if (log.isDebugEnabled()) {
			log.debug("DC: Re-registered {} (by original id)", obj.name());
		}
	}

	/**
	 * Only for unit tests.
	 * 
	 * @param obj
	 *            object to re-register
	 */
	public void reregisterOnlyForTest(SqlDomainObject obj) {
		reregister(obj);
	}

	/**
	 * Retrieve all registered and valid objects of a specific domain class.
	 * <p>
	 * 'valid' object means that object could be saved to database before without error ({@link #save(Connection, SqlDomainObject)}).
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param domainClass
	 *            any domain class (must not be top-level/instantiable)
	 * 
	 * @return set of all objects of given domain class
	 */
	@SuppressWarnings("unchecked")
	public final <S extends SqlDomainObject> Set<S> allValid(Class<S> domainClass) {
		return (Set<S>) objectMap.get(domainClass).values().stream().filter(o -> o.isValid()).collect(Collectors.toSet());
	}

	// -------------------------------------------------------------------------
	// Synchronization
	// -------------------------------------------------------------------------

	// Load result containing objects loaded in all load cycles and information, if changes were detected in any load cycle
	static class LoadResult {
		boolean hasChanges = false;
		Set<SqlDomainObject> loadedObjects = null;
	}

	// Load objects from database using SELECT supplier, finalize these objects and load and load missing referenced objects in a loop to ensure referential integrity.
	private LoadResult loadAssuringReferentialIntegrity(Function<Connection, Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>>> select) throws SQLException, SqlDbException {

		// Get database connection from pool
		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.getPool(), true)) {

			// Initially load object records using given select-supplier
			Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> loadedRecordsMap = select.apply(sqlcn.cn);

			// Create loader object containing domain controller and SQL connection
			Loader loader = new Loader(this, sqlcn.cn);

			// Instantiate newly loaded objects, assign changed data and references to objects, collect initially unresolved references
			IntermediateLoadResult intermediateLoadResult = loader.buildObjectsFromLoadedRecords(loadedRecordsMap);

			// Determine if database changes were detected (ignoring unsaved local object changes) and collect loaded objects and objects where references were changed in initial load cycle
			LoadResult loadResult = new LoadResult();
			loadResult.hasChanges = intermediateLoadResult.hasChanges;
			loadResult.loadedObjects = new HashSet<>(intermediateLoadResult.loadedObjects);
			Set<SqlDomainObject> objectsWhereReferencesChanged = new HashSet<>(intermediateLoadResult.objectsWhereReferencesChanged);

			// Cyclicly load and instantiate missing referenced objects and detect unresolved references on these objects
			int c = 1;
			Map<Class<? extends SqlDomainObject>, Map<Long, SortedMap<String, Object>>> missingRecordsMap;
			List<UnresolvedReference> currentUnresolvedReferences = null;
			while (!intermediateLoadResult.unresolvedReferences.isEmpty()) {

				if (log.isDebugEnabled()) {
					log.debug("SDC: There were in total {} unresolved reference(s) of {} objects referenced by {} objects detected in {}. load cycle",
							intermediateLoadResult.unresolvedReferences.size(),
							intermediateLoadResult.unresolvedReferences.stream().map(ur -> ur.refField.getType().getSimpleName()).distinct().collect(Collectors.toList()),
							intermediateLoadResult.unresolvedReferences.stream().map(ur -> ur.obj.getClass().getSimpleName()).distinct().collect(Collectors.toList()), c++);
				}

				// Load and instantiate missing objects of unresolved references
				missingRecordsMap = loader.loadMissingObjects(intermediateLoadResult.unresolvedReferences);

				// Store current unresolved references to later resolve them after all missing objects were initiated and initialized
				currentUnresolvedReferences = new ArrayList<>(intermediateLoadResult.unresolvedReferences);

				// Instantiate and initialize missed objects, store current unresolved references and determine further unresolved references
				intermediateLoadResult = loader.buildObjectsFromLoadedRecords(missingRecordsMap);

				// Determine if database changes were detected in subsequent load cycle and further collect loaded objects and objects where references were changed
				loadResult.hasChanges |= intermediateLoadResult.hasChanges;
				loadResult.loadedObjects.addAll(intermediateLoadResult.loadedObjects);
				objectsWhereReferencesChanged.addAll(intermediateLoadResult.objectsWhereReferencesChanged);

				// Resolve current unresolved references after missing objects were instantiated and initialized
				loader.resolveUnresolvedReferences(currentUnresolvedReferences);
			}

			// Update accumulations of all objects which are referenced by any of the objects where references changed
			objectsWhereReferencesChanged.forEach(this::updateAccumulationsOfParentObjects);

			return loadResult;
		}
	}

	/**
	 * Synchronize domain objects controlled by this domain controller instance with the associated persistence database.
	 * <p>
	 * Method first saves potentially existing new objects (objects which are not already stored in database) to database, than loads all currently relevant objects from database and after that
	 * unregisters objects which were deleted in database (by another instance) or which fell out of data horizon ({@link UseDataHorizon}) and are not referenced by any loaded child object.
	 * <p>
	 * Field values of existing local objects will be overridden by values retrieved from database on load. So, unsaved changes of field values, which were changed in database too, will be discarded
	 * and overridden by database values! Warn logs will be written and warnings will be assigned to affected objects in this case. It's recommended to ensure that all local changes are saved before
	 * calling {@code synchronize()}.
	 * <p>
	 * For data horizon controlled domain classes this method first loads only objects within data horizon (see {@link UseDataHorizon}). But method ensures referential integrity by generally loading
	 * all referenced objects even if they are out of data horizon.
	 * <p>
	 * This method is used for initial loading of domain objects from persistence database on startup. If there is a single domain controller instance connected to persistence database this method
	 * only removes objects which fell out of data horizon from object store on subsequent calls. This also means that for single domain controller instance configurations where data horizon control
	 * is not active it is sufficient to call this method once at startup.
	 * 
	 * @param objectDomainClassesToExclude
	 *            (optional) object domain classes which objects shall not be loaded from database - if objects of this classes are referenced by loaded objects they will be loaded anyway (referential
	 *            integrity)
	 * 
	 * @return true if at least one new or changed object was loaded from database, false otherwise
	 * 
	 * @throws SQLException
	 *             if executed SELECT statement throws SQLException
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 */
	@SafeVarargs
	public final boolean synchronize(Class<? extends SqlDomainObject>... objectDomainClassesToExclude) throws SQLException, SqlDbException {

		log.info("SDC: Synchronize with database... {}",
				(objectDomainClassesToExclude != null && objectDomainClassesToExclude.length > 0
						? " - domain classes to exclude from loading: " + Stream.of(objectDomainClassesToExclude).map(Class::getSimpleName).collect(Collectors.toList())
						: ""));

		// Save all new, un-stored objects to database (but do not save unsaved changes of already stored objects to avoid overriding with database changes without notification)
		for (SqlDomainObject obj : findAll(o -> !o.isStored())) {
			save(obj);
		}

		// Load all objects from database - override unsaved local object changes by changes in database on contradiction, assign field warning(s) to such objects in this case
		LoadResult loadResult = loadAssuringReferentialIntegrity(cn -> new Loader(this, cn).selectAll(objectDomainClassesToExclude));

		// Unregister existing objects which were not loaded from database again (deleted in database by another instance or fell out of data horizon) and which are not referenced by any object
		for (SqlDomainObject obj : findAll(o -> !loadResult.loadedObjects.contains(o) && !isReferenced(o))) {
			unregister(obj);
		}

		log.info("SDC: Synchronization with database done.");

		return loadResult.hasChanges;
	}

	// -------------------------------------------------------------------------
	// (Re)loading specific objects
	// -------------------------------------------------------------------------

	/**
	 * Load objects of only one (primary) object domain class (selected by WHERE clause if specified) and also objects directly or indirectly referenced by these primarily loaded objects (to ensure
	 * referential integrity).
	 * <p>
	 * Note: To provide a WHERE clause one needs knowledge of the relation of Java class/field names and associated SQL table/column names and also knowledge about column types associated to field
	 * types:
	 * <p>
	 * The basic rule for conversion of Java class/field names to SQL table/column names is: CaseFormat.UPPER_CAMEL -> CaseFormat.UPPER_UNDERSCORE. Tables are additionally prefixed by 'DOM_'. E.g.:
	 * class name -> table name: {@code Xyz} -> 'DOM_Xyz', {@code XYZ} -> 'DOM_X_Y_Z' and field name -> column name: {@code xyz} -> 'XYZ'. {@code xYz} -> 'X_YZ'.
	 * <p>
	 * If field is a reference field referencing another domain class, the corresponding column has appendix '_ID' (object references are realized as foreign key columns referencing unique object id).
	 * E.g. <code> class Bike { Manufacturer manufacturer; }</code> -> column 'MANUFACTURER_ID' in table 'DOM_BIKE'.
	 * <p>
	 * If field name is or may be a reserved name in SQL, column name is prefixed by 'DOM_' (e.g.: {@code LocalDate date} -> 'DOM_DATE', {@code Type type} -> 'DOM_TYPE', {@code Double number} ->
	 * 'DOM_NUMBER').
	 * <p>
	 * Fixed values of {@code String} and {@code Enum} fields in WHERE clause have to be specified as string literals -> NAME='Order1' AND STATUS='open'. Number values ({@code Integer}, {@code Long},
	 * {@code Double}, {@code BigInteger}, {@code BigDecimal}) have to be specified as appropriate numbers -> PRICE>12.0. LocalDate, LocalTime, LocalDateTime values have to be valid database specific
	 * date/time strings.
	 * <p>
	 * Note: Columns for {@code byte[]}, {@code char[]} and other array fields, as well as collection and map fields may not be used for WHERE clauses.
	 * 
	 * @param objectDomainClass
	 *            object domain class of primary objects to load
	 * @param whereClause
	 *            to further shrink amount of loaded objects
	 * @param maxCount
	 *            maximum number of primary objects to load
	 * 
	 * @return objects loaded from database potentially including referenced objects of other classes than the given object domain class (referential integrity)
	 * 
	 * @throws SQLException
	 *             on opening database connection or performing SELECT statements
	 * @throws SqlDbException
	 *             if object domain class of any referenced object could not be determined
	 */
	public Set<SqlDomainObject> loadOnly(Class<? extends SqlDomainObject> objectDomainClass, String whereClause, int maxCount) throws SQLException, SqlDbException {

		if (log.isDebugEnabled()) {
			log.debug("SDC: Load {}'{}' objects{}", (maxCount > 0 ? "max " + maxCount + " " : ""), objectDomainClass.getSimpleName(),
					(!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));
		}

		LoadResult loadResult = loadAssuringReferentialIntegrity(cn -> new Loader(this, cn).select(objectDomainClass, whereClause, maxCount));

		return loadResult.loadedObjects;
	}

	/**
	 * (Re)load object from database.
	 * <p>
	 * If object is not initially saved or is not registered, this method does nothing.
	 * <p>
	 * If direct or indirect parent objects exist, which are not yet loaded due to data horizon condition, these object will be loaded (and instantiated) too.
	 * <p>
	 * Attention: Overrides unsaved changes of this object.
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param obj
	 *            object to reload from database
	 * 
	 * @return true, if object differs in and therefore was loaded from database, false otherwise
	 * 
	 * @throws SQLException
	 *             on error establishing database connection
	 * @throws SqlDbException
	 *             if object domain class of any referenced object could not be determined
	 */
	public <S extends SqlDomainObject> boolean reload(S obj) throws SQLException, SqlDbException {

		if (!obj.isStored) {
			log.warn("SDC: {} is not yet stored in database and therefore cannot be loaded!", obj.name());
			return false;
		}
		else if (!isRegistered(obj)) {
			log.warn("SDC: {} is not registered in object store and cannot be loaded from database!", obj.name());
			return false;
		}

		if (log.isDebugEnabled()) {
			log.debug("SDC: Load {} from database", obj.name());
		}

		LoadResult loadResult = loadAssuringReferentialIntegrity(cn -> new Loader(this, cn).selectObjectRecord(obj));

		return loadResult.hasChanges;
	}

	// -------------------------------------------------------------------------
	// Allocating objects for exclusive use by this domain controller
	// -------------------------------------------------------------------------

	/**
	 * Select and allocate objects of given domain class for exclusive use.
	 * <p>
	 * This method is intended to synchronize access if multiple threads and/or multiple domain controller instances concurrently operate on the same database.
	 * <p>
	 * Allocated objects must later be released from exclusive use using {@link #releaseObjects(Collection, Class)}.
	 * <p>
	 * Note: exclusive allocation of objects is realized by inserting a record in a 'shadow' table for every allocated object with the object id as record id. The UNIQUE constraint for ID ensures,
	 * that this can be done only once for one object (<b>No</b> FOR UPDATE clause is used on SELECT statement for access synchronization).
	 * <p>
	 * If 'update' function is specified this function will be computed initially for all exclusively allocated objects, and allocated objects will be saved to database immediately (e.g.
	 * {@code o -> o.status = 'processing'} -> STATUS = 'processing').
	 * <p>
	 * Objects allocated here are typically already loaded by preceding synchronization.
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param objectDomainClass
	 *            object domain class of objects to allocate exclusively (non-object domain classes are not allowed here! E.g. if {@code RaceBike extends Bike} only {@code RaceBike} may be provided)
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation - e.g. an inner class of given object domain class (or one of it's base classes) named {@code InProgress} extending
	 *            {@code SqlDomainObject}
	 * @param whereClause
	 *            WHERE clause to build SELECT statement for objects to allocate (e.g. STATUS='new') - @see {@link #loadOnly(Class, String, int)} for details regarding WHERE clause
	 * @param maxCount
	 *            maximum # of objects to allocate
	 * @param update
	 *            optional function to compute immediately on allocated objects (or null)
	 * 
	 * @return allocated objects
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL SELECT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	@SuppressWarnings("unchecked")
	public <S extends SqlDomainObject> Set<S> allocateObjectsExclusively(Class<S> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, String whereClause, int maxCount,
			Consumer<? super S> update) throws SQLException, SqlDbException {

		if (log.isTraceEnabled()) {
			log.trace("SDC: Allocate {}'{}' objects{} exclusively for this domain controller instance", (maxCount > 0 ? "max " + maxCount + " " : ""), objectDomainClass.getSimpleName(),
					(!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));
		}

		// Load objects related to given object domain class
		LoadResult loadResult = loadAssuringReferentialIntegrity(cn -> new Loader(this, cn).selectExclusively(objectDomainClass, inProgressClass, whereClause, maxCount));

		// Filter objects of object domain class itself (because loaded objects may contain referenced objects of other domain classes too)
		Set<S> allocatedObjects = new HashSet<>(loadResult.loadedObjects.stream().filter(o -> o.getClass().equals(objectDomainClass)).map(o -> (S) o).collect(Collectors.toSet()));
		if (!allocatedObjects.isEmpty()) {
			if (log.isDebugEnabled()) {
				log.debug("SDC: {} '{}' objects exclusively allocated {} ", allocatedObjects.size(), objectDomainClass.getSimpleName(),
						(!isEmpty(whereClause) ? " WHERE " + whereClause.toUpperCase() : ""));
			}

			// If update is specified: change object as defined by update parameter and UPDATE record in database by saving object, release object from exclusive use if specified
			if (update != null) {
				if (log.isTraceEnabled()) {
					log.trace("SDC: Update exclusively allocated objects...");
				}
				for (S loadedObject : allocatedObjects) {
					update.accept(loadedObject);
					save(loadedObject);
				}
			}
		}
		else {
			log.debug("SDC: No '{}' objects could exclusively be allocated", objectDomainClass.getSimpleName());
		}

		return allocatedObjects;
	}

	/**
	 * Select and allocate one object for exclusive use.
	 * <p>
	 * For exclusive object allocation see @{@link #allocateObjectsExclusively(Class, Class, String, int, Consumer)}.
	 * <p>
	 * Allocated object must later be released from exclusive use using {@link #releaseObject(SqlDomainObject, Class, Consumer)}.
	 * <p>
	 * If 'update' function is specified this function will be computed initially for allocated object, and object will be saved to database immediately (e.g. {@code o -> o.status = 'processing'} ->
	 * STATUS = 'processing').
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param obj
	 *            object to allocate exclusively
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation - e.g. an inner class of given object domain class (or one of it's base classes) named {@code InProgress} extending
	 *            {@code SqlDomainObject}
	 * @param update
	 *            optional function to compute immediately on allocated objects (or null)
	 * 
	 * @return true if this object could be allocated exclusively, false otherwise
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL UPDATE statement on saving object
	 * @throws SqlDbException
	 *             on internal errors
	 */
	@SuppressWarnings("unchecked")
	public <S extends SqlDomainObject> boolean allocateObjectExclusively(S obj, Class<? extends SqlDomainObject> inProgressClass, Consumer<? super S> update) throws SQLException, SqlDbException {

		String whereClause = getSqlRegistry().getTableFor(obj.getClass()).name + ".ID=" + obj.getId();
		Set<S> allocatedObjects = allocateObjectsExclusively((Class<S>) obj.getClass(), inProgressClass, whereClause, -1, update);
		return (!allocatedObjects.isEmpty());
	}

	/**
	 * Release this object from exclusive use.
	 * <p>
	 * if 'update' function is given this function will be computed for this exclusively allocated objects and this objects will be saved to database immediately (e.g. {@code o -> o.status = 'done'}
	 * -> STATUS = 'done').
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param obj
	 *            object to release from exclusive use
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation - @see {@link #allocateObjectExclusively(SqlDomainObject, Class, Consumer)}
	 * @param update
	 *            update function to perform on this object on releasing or null (e.g. {@code o -> o.status = 'done'})
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
			log.warn("SDC: {} is currently not allocated for exclusive usage", obj);
			return false;
		}

		// Change object as defined by update parameter and UPDATE record in database on saving object
		if (log.isDebugEnabled()) {
			log.debug("SDC: Release {} from exclusive use ({})", obj, update);
		}
		if (update != null) {
			update.accept(obj);
			save(obj);
		}

		// Delete in-progress record
		delete(inProgressObject);

		return true;
	}

	/**
	 * Release multiple exclusively allocated objects from exclusive use.
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param objects
	 *            objects to release from exclusive use
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation - @see {@link #allocateObjectExclusively(SqlDomainObject, Class, Consumer)}
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL UPDATE statement on saving object
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public <S extends SqlDomainObject> void releaseObjects(Collection<S> objects, Class<? extends SqlDomainObject> inProgressClass) throws SQLException, SqlDbException {
		for (S obj : objects) {
			releaseObject(obj, inProgressClass, null);
		}
	}

	/**
	 * Exclusively compute a function on objects of one domain class and save updated objects immediately.
	 * <p>
	 * Works like {@link #allocateObjectsExclusively(Class, Class, String, int, Consumer)} but releases objects immediately after computing update function. Explicitly releasing updated objects is not
	 * necessary.
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param objectDomainClass
	 *            object domain class of objects to allocate exclusively
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation - @see {@link #allocateObjectExclusively(SqlDomainObject, Class, Consumer)}
	 * @param whereClause
	 *            WHERE clause to build SELECT statement for objects to allocate (e.g. STATUS='new') - @see {@link #loadOnly(Class, String, int)} for details regarding WHERE clause
	 * @param update
	 *            update function to compute immediately on selected objects (or null) - objects will be saved immediately after computing (e.g. {@code o -> o.status = 'done'} -> STATUS = 'done')
	 * 
	 * @return objects for which update function were computed
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL SELECT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public <S extends SqlDomainObject> Set<S> computeExclusivelyOnObjects(Class<S> objectDomainClass, Class<? extends SqlDomainObject> inProgressClass, String whereClause, Consumer<? super S> update)
			throws SQLException, SqlDbException {

		Set<S> loadedObjects = allocateObjectsExclusively(objectDomainClass, inProgressClass, whereClause, -1, update);
		releaseObjects(loadedObjects, inProgressClass);
		return loadedObjects;
	}

	// -------------------------------------------------------------------------
	// Saving objects to database
	// -------------------------------------------------------------------------

	/**
	 * Save object to database on existing database connection.
	 * <p>
	 * If object is 'new', which means it was not stored in database before, appropriate INSERT statement(s) will be performed, otherwise UPDATE statements for changed fields. If there are no
	 * differences between object and object representation in database, nothing will be done and {@code false} will be returned.
	 * <p>
	 * If (any) domain class of this object has array, collection or map fields, INSERT and UPDATE statements will also be performed on 'entry' tables associated with these fields.
	 * <p>
	 * On saving an object one assumes that this object was not meanwhile changed in database (by another controller instance). If in doubt allocate object before saving using
	 * {@link #allocateObjectExclusively(SqlDomainObject, Class, Consumer)} which reloads object from database and allows secure saving. Values of simple data and reference fields override values in
	 * database without any check on saving. For lists, set and array and map fields new value will be checked against value stored in local object record before removing, inserting or updating
	 * database entries which represent these complex fields. This may lead to unpredictable results on saving complex fields if they were changed and saved by another instance before!
	 * <p>
	 * On SQL exception this exception and the field error(s) recognized will be assigned to the object. In this case object is marked as invalid and will not be found using
	 * {@link SqlDomainController#allValid(Class)}. If object was already saved, UPDATE's will afterwards be tried for every field/column separately to keep impact of failure small. For non-updatable
	 * fields original content is restored to ensure data consistency between object and database representation.
	 * <p>
	 * If a temporary invalid object can successfully be saved again (with changed and valid field values), assigned exception and field error(s) will be removed from object, so object than is
	 * considered again as valid. again.
	 * <p>
	 * If initial saving - inserting object record(s) - fails, whole transaction will be rolled back (if connection is not in auto-commit mode).
	 * 
	 * @param cn
	 *            database connection
	 * @param obj
	 *            object to save
	 * 
	 * @return true if object's changes were saved to database, false if object was up-to-date
	 * 
	 * @throws SQLException
	 *             exception thrown during execution of INSERT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public boolean save(Connection cn, SqlDomainObject obj) throws SQLException, SqlDbException {

		try {
			// Save object
			boolean wasChanged = new Saver(this, cn).save(obj, new ArrayList<>());

			// COMMIT whole save transaction if any INSERT or UPDATE statement was performed
			if (wasChanged) {
				SqlConnection.commit(cn);

				if (log.isDebugEnabled()) {
					log.debug("SDC: Saved {}", obj.name());
				}
			}
			else if (!log.isTraceEnabled()) {
				log.trace("SDC: {} is up-to-date", obj.name());
			}

			return wasChanged;
		}
		catch (SqlDbException | SQLException sqlex) { // Error logs are already written (or an 'in-progress' object for exclusive access could not be created - which is not an error case)

			// ROLL BACK complete save transaction
			SqlConnection.rollback(cn);

			throw sqlex;
		}
	}

	/**
	 * Save changed object to database.
	 * <p>
	 * Grabs a non-auto-commit database connection form {@link ConnectionPool} assigned to this domain controller and gives this connection back after saving object.
	 * <p>
	 * For detailed description see {@link #save(Connection, SqlDomainObject)}.
	 * 
	 * @param obj
	 *            object to save
	 * 
	 * @return true if object's changes were saved to database, false if object was up-to-date
	 * 
	 * @throws SQLException
	 *             exception thrown during establishing database connection or execution of INSERT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public boolean save(SqlDomainObject obj) throws SQLException, SqlDbException {

		// Use one transaction for all INSERTs or UPDATEs to allow ROLL BACK of whole transaction on error - on success transaction will be committed
		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.getPool(), false)) {
			return save(sqlcn.cn, obj);
		}
	}

	/**
	 * Create, initialize, register and {@link #save(Connection, SqlDomainObject)} object of domain class using given database connection.
	 * <p>
	 * If provided, calls initialization function before object registration to ensure that registered object is logically initialized.
	 * <p>
	 * Note: method uses default constructor, which therefore must explicitly be defined if other constructors are defined!
	 * <p>
	 * If object cannot be saved due to SQL exception on INSERT, object will marked as invalid and exception and field error(s) will be assigned to object.
	 * 
	 * @param cn
	 *            database connection
	 * @param objectDomainClass
	 *            top-level domain class of object to create (object domain class)
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created domain object
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * @throws SQLException
	 *             on error saving object to database
	 */
	public <S extends SqlDomainObject> S createAndSave(Connection cn, Class<S> objectDomainClass, Consumer<S> init) throws SQLException, SqlDbException {

		S obj = create(objectDomainClass, init);
		save(cn, obj);
		return obj;
	}

	/**
	 * Create, initialize, register and save object of domain class.
	 * <p>
	 * See {@link #createAndSave(Connection, Class, Consumer)} and {@link #save(SqlDomainObject)}.
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param objectDomainClass
	 *            top-level domain class of objects to create (object domain class)
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created and saved domain object
	 * 
	 * @throws SqlDbException
	 *             on Java/SQL inconsistencies
	 * @throws SQLException
	 *             on error saving object to database
	 */
	public <S extends SqlDomainObject> S createAndSave(Class<S> objectDomainClass, Consumer<S> init) throws SQLException, SqlDbException {

		S obj = create(objectDomainClass, init);
		save(obj);
		return obj;
	}

	/**
	 * Create, initialize, register and save object of domain class without throwing exception on error.
	 * <p>
	 * See {@link #createAndSave(Connection, Class, Consumer)} and {@link #save(SqlDomainObject)}.
	 * <p>
	 * On unsuccessful saving object will be marked as invalid and field error(s) will be assigned to object. Thrown exception will be logged as well as exception context.
	 * 
	 * @param <S>
	 *            specific domain object class type
	 * @param objectDomainClass
	 *            top-level domain class of objects to create (object domain class)
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created and saved domain object or null if an exception occurred saving object
	 */
	public <S extends SqlDomainObject> S createAndSaveNoException(Class<S> objectDomainClass, Consumer<S> init) {

		S obj = create(objectDomainClass, init);
		try {
			save(obj);
		}
		catch (SQLException | SqlDbException e) {
			log.error("SDC: {} could not be saved!", obj);
		}

		return obj;
	}

	// -------------------------------------------------------------------------
	// Deleting objects
	// -------------------------------------------------------------------------

	/**
	 * If possible delete this object and all referencing objects using existing database connection.
	 * <p>
	 * Deletion of an object means: unregistering object and all direct and indirect child objects from object store, deleting associated records from database tables and removing object from
	 * potentially existing accumulations of parent objects.
	 * <p>
	 * Initially checks if this object and all of the direct and indirect children can be deleted (see {@link DomainObject#canBeDeleted()}). No object will be deleted at all if this check fails (and
	 * connection is non-auto-commit).
	 * <p>
	 * Note: Database records of old 'data horizon' controlled child objects (which are not registered), will be deleted directly by ON DELETE CASCADE clause (which is automatically defined for
	 * FOREIGN KEYs in tables of 'data horizon' controlled domain classes in SQL scripts for database generation).
	 * 
	 * @param obj
	 *            object to delete
	 * @param cn
	 *            database connection
	 * 
	 * @return true if deletion was successful, false if object or at least one of its direct or indirect children cannot be deleted by can-be-deleted check.
	 * 
	 * @throws SQLException
	 *             exceptions thrown on executing SQL DELETE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public boolean delete(Connection cn, SqlDomainObject obj) throws SQLException, SqlDbException {

		LocalDateTime now = LocalDateTime.now();

		// Recursively check if this object and all direct and indirect children can be deleted
		if (!canBeDeletedRecursive(obj, new ArrayList<>())) {
			log.info("SDC: {} cannot be deleted because #canBeDeletedRecursive() returned false!", obj.name());
			return false;
		}

		List<SqlDomainObject> unregisteredObjects = new ArrayList<>(); // Collect objects unregistered during deletion process to allow re-registering on exception
		try {
			if (log.isDebugEnabled()) {
				log.debug("SDC: Delete {}", obj.name());
			}

			// Delete object and children
			new Deleter(this, cn).deleteRecursiveFromDatabase(obj, unregisteredObjects, null, 0);

			if (log.isDebugEnabled()) {
				log.debug("SDC: Deleted {}", obj.name());
				if (log.isTraceEnabled()) {
					log.trace("SDC: Deletion of {} and all children took: {}", obj.name(), ChronoUnit.MILLIS.between(now, LocalDateTime.now()) + "ms");
				}
			}

			// COMMIT whole delete transaction
			SqlConnection.commit(cn);

			return true;
		}
		catch (SQLException | SqlDbException sqlex) {
			log.error("SDC: Delete: Object {} cannot be deleted", obj.name());
			obj.currentException = sqlex;

			// ROLL BACK complete delete transaction
			SqlConnection.rollback(cn);

			// Re-register already unregistered objects and re-generate object records
			for (SqlDomainObject o : unregisteredObjects) {
				reregister(o);
			}

			throw sqlex;
		}
	}

	/**
	 * If possible delete object and all referencing objects .
	 * <p>
	 * Grabs a non-auto-commit database connection form connection pool ({@link ConnectionPool} assigned to database connection object {@link SqlDb} and gives this connection back after deletion.
	 * <p>
	 * See {@link #delete(Connection, SqlDomainObject)}.
	 * 
	 * @param obj
	 *            object to delete
	 * 
	 * @return true if deletion was successful, false if object or at least one of its direct or indirect children cannot be deleted by can-be-deleted check.
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL DELETE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public boolean delete(SqlDomainObject obj) throws SQLException, SqlDbException {

		// Use one transaction for all DELETE operations to allow ROLLBACK of whole transaction on error - on success transaction will automatically be committed later on closing connection
		try (SqlConnection sqlcn = SqlConnection.open(sqlDb.getPool(), false)) {
			delete(sqlcn.cn, obj);
			return true;
		}
	}

	// -------------------------------------------------------------------------
	// Checking constraint violations
	// -------------------------------------------------------------------------

	// Check for UNIQUE constraint violation
	boolean hasUniqueConstraintViolations(SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass) {

		boolean isConstraintViolated = false;

		// Check if UNIQUE constraint is violated (UNIQUE constraints of single columns are also realized as table UNIQUE constraints)
		SqlDbTable table = getSqlRegistry().getTableFor(domainClass);
		for (SqlDbUniqueConstraint uc : table.uniqueConstraints) {

			// Build predicate to check combined uniqueness and build lists of involved fields and values
			Predicate<SqlDomainObject> multipleUniqueColumnsPredicate = null;
			List<Field> combinedUniqueFields = new ArrayList<>();
			List<Object> fieldValues = new ArrayList<>();

			for (SqlDbColumn col : uc.columns) {
				for (Field fld : getRegistry().getDataAndReferenceFields(domainClass)) {
					if (Common.objectsEqual(col, getSqlRegistry().getColumnFor(fld))) {
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

			if (multipleUniqueColumnsPredicate != null && count(getRegistry().getCastedDomainClass(obj), multipleUniqueColumnsPredicate) > 1) {

				List<String> fieldNames = combinedUniqueFields.stream().map(Member::getName).collect(Collectors.toList());
				if (uc.columns.size() == 1) {
					log.error("SDC: \tColumn '{}' is UNIQUE by constraint '{}' but '{}' object '{}' already exists with same value '{}' of field '{}'!",
							uc.columns.stream().map(c -> c.name).collect(Collectors.toList()).get(0), uc.name, obj.getClass().getSimpleName(), DomainObject.name(obj), fieldValues.get(0),
							fieldNames.get(0));
				}
				else {
					log.error("SDC: \tColumns {} are UNIQUE together by constraint '{}' but '{}' object '{}'  already exists with same values {} of fields {}!",
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
	boolean hasColumnSizeViolations(SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass) {

		boolean isConstraintViolated = false;
		for (Field field : getRegistry().getDataAndReferenceFields(domainClass)) {
			SqlDbColumn column = getSqlRegistry().getColumnFor(field);
			Object fieldValue = obj.getFieldValue(field);

			if ((fieldValue instanceof String || fieldValue instanceof Enum) && column.maxlen < fieldValue.toString().length()) {
				log.error("SDC: \tField  '{}': value '{}' is too long for associated column '{}' with maxlen {} for object {}!", field.getName(), CLog.forSecretLogging(field, fieldValue), column.name,
						column.maxlen, DomainObject.name(obj));
				obj.setFieldError(field, "COLUMN_SIZE_VIOLATION");
				isConstraintViolated = true;
			}
		}
		return isConstraintViolated;
	}

	// Check if null value is provided for any column which has NOT NULL constraint
	boolean hasNotNullConstraintViolations(SqlDomainObject obj, Class<? extends SqlDomainObject> domainClass) {

		boolean isConstraintViolated = false;
		for (Field field : getRegistry().getDataAndReferenceFields(domainClass)) {
			SqlDbColumn column = getSqlRegistry().getColumnFor(field);

			if (!column.isNullable && obj.getFieldValue(field) == null) {
				log.error("SDC: \tField  '{}': associated with column '{}' has NOT NULL constraint but field value provided is null for object {}!", field.getName(), column.name,
						DomainObject.name(obj));
				obj.setFieldError(field, "NOT_NULL_CONSTRAINT_VIOLATION");
				isConstraintViolated = true;
			}
		}
		return isConstraintViolated;
	}

	/**
	 * Check for constraint violations of a domain object without involving database.
	 * <p>
	 * Logs violations found and assigns field errors on fields for which constraints are violated.
	 * <p>
	 * Note: column size violation are detected here only for enum fields - values of String fields which are to long for corresponding column will be truncated and a field warning will be generated
	 * on saving.
	 * 
	 * @param obj
	 *            object to check
	 * 
	 * @return true if any field constraint is violated, false otherwise
	 */
	public boolean hasConstraintViolations(SqlDomainObject obj) {

		boolean isAnyConstraintViolated = false;
		for (Class<? extends SqlDomainObject> domainClass : getRegistry().getDomainClassesFor(obj.getClass())) {
			isAnyConstraintViolated |= hasNotNullConstraintViolations(obj, domainClass);
			isAnyConstraintViolated |= hasUniqueConstraintViolations(obj, domainClass);
			isAnyConstraintViolated |= hasColumnSizeViolations(obj, domainClass);
		}
		return isAnyConstraintViolated;
	}

}
