package com.icx.dom.domain;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CRandom;

/**
 * Singleton that manages domain object store.
 * <p>
 * Specific domain controllers extend this class.
 * 
 * @author RainerBaumgärtel
 */
public abstract class DomainController {

	static final Logger log = LoggerFactory.getLogger(DomainController.class);

	// -------------------------------------------------------------------------
	// Static members
	// -------------------------------------------------------------------------

	// Object map by domain class of maps by object id
	protected static Map<Class<? extends DomainObject>, SortedMap<Long, DomainObject>> objectMap = new ConcurrentHashMap<>();

	// -------------------------------------------------------------------------
	// Register domain classes
	// -------------------------------------------------------------------------

	/**
	 * Register domain classes by given domain package.
	 * <p>
	 * All top level classes in this package and in any sub package which extends DomainObject class will be registered as domain classes.
	 * 
	 * @param baseClass
	 *            class where all domain classes to register are derived from ({@code SqlDomainObject.class})
	 * @param domainPackageName
	 *            name of package where domain classes reside
	 * 
	 * @throws DomainException
	 */
	protected static void registerDomainClasses(Class<? extends DomainObject> baseClass, String domainPackageName) throws DomainException {

		Registry.registerDomainClasses(baseClass, domainPackageName);
		Registry.getRegisteredDomainClasses().forEach(c -> objectMap.put(c, new ConcurrentSkipListMap<>()));
	}

	/**
	 * Register given object domain classes and derived domain classes.
	 * <p>
	 * Classes must extend DomainObject class. Only object domain class (highest class in derivation order) must be given here if derived domain classes exists.
	 * 
	 * @param baseClass
	 *            class where all domain classes to register are derived from ({@code SqlDomainObject.class})
	 * @param domainClasses
	 *            list of object domain classes to register
	 * 
	 * @throws DomainException
	 */
	@SafeVarargs
	protected static void registerDomainClasses(Class<? extends DomainObject> baseClass, Class<? extends DomainObject>... domainClasses) throws DomainException {

		Registry.registerDomainClasses(baseClass, domainClasses);
		Registry.getRegisteredDomainClasses().forEach(c -> objectMap.put(c, new ConcurrentSkipListMap<>()));
	}

	public static final Class<? extends DomainObject> getDomainClassByName(String className) {

		Optional<Class<? extends DomainObject>> o = Registry.getRegisteredDomainClasses().stream().filter(c -> c.getSimpleName().equals(className)).findFirst();
		if (o.isPresent()) {
			return o.get();
		}
		else {
			log.error("Class '{}' of missing object is not registered as domain class", className);
		}

		return null;
	}

	// -------------------------------------------------------------------------
	// Create domain objects
	// -------------------------------------------------------------------------

	// Use milliseconds, thread id and random value for unique identifier
	protected static synchronized long generateUniqueId(final Class<? extends DomainObject> domainObjectClass) {

		long id = new Date().getTime() * 100000 + (Thread.currentThread().getId() % 100) * 1000 + CRandom.randomInt(1000);

		for (Class<? extends Object> domainClass : Registry.getInheritanceStack(domainObjectClass)) {
			while (objectMap.get(domainClass).containsKey(id)) {
				id++;
			}
		}

		return id;
	}

	// Instantiate domain object - called on loading new object from database and if objects are created using create() methods of domain controller
	// Uses default constructor which therefore must be defined explicitly if other constructors are defined!
	protected static final <T extends DomainObject> T instantiate(Class<T> objectDomainClass) throws Exception {

		T obj = Registry.getConstructor(objectDomainClass).newInstance();

		obj.initializeFields();

		return obj;
	}

	/**
	 * Create, initialize and register object of domain class.
	 * <p>
	 * Calls initialization function before object registration to ensure that registered object is initialized.
	 * 
	 * @param <T>
	 *            specific domain object class type
	 * @param domainObjectClass
	 *            instantiable domain (object) class
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created domain object
	 * 
	 * @throws Exception
	 */
	public static final synchronized <T extends DomainObject> T create(final Class<T> domainObjectClass, Consumer<T> init) throws Exception {

		T obj = instantiate(domainObjectClass);

		if (init != null) {
			init.accept(obj);
		}

		obj.registerById(generateUniqueId(obj.getClass()));

		if (log.isDebugEnabled()) {
			log.debug("DC: Created {}.", obj.name());
		}

		return obj;
	}

	// -------------------------------------------------------------------------
	// Access domain objects
	// -------------------------------------------------------------------------

	/**
	 * Get object by domain class and id.
	 *
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 * @param objectId
	 *            object id
	 *
	 * @return identified object
	 *
	 * @throws ObjectNotFoundException
	 *             if denoted object does not exist
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends DomainObject> T get(Class<T> domainClass, long objectId) throws ObjectNotFoundException {

		T object = (T) objectMap.get(domainClass).get(objectId);
		if (object != null)
			return object;

		throw new ObjectNotFoundException("No " + domainClass.getSimpleName().toLowerCase() + " found for id: " + objectId);
	}

	/**
	 * Find object by id using {@link #get(Class, long)}, return null if not found.
	 * 
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 * @param objectId
	 *            object id
	 * 
	 * @return object found or null
	 */
	public static final <T extends DomainObject> T find(Class<T> domainClass, long objectId) {
		try {
			return get(domainClass, objectId);
		}
		catch (ObjectNotFoundException e) {
			return null;
		}
	}

	/**
	 * Retrieve all registered objects fulfilling given predicate.
	 * 
	 * @param predicate
	 *            predicate to fulfill
	 * 
	 * @return set of all objects currently loaded into object store
	 */
	public static final Set<DomainObject> findAll(Predicate<? super DomainObject> predicate) {

		Set<DomainObject> all = new HashSet<>();
		for (Class<? extends DomainObject> objectDomainClass : Registry.getRegisteredObjectDomainClasses()) {
			all.addAll(findAll(objectDomainClass, predicate));
		}

		return all;
	}

	/**
	 * Retrieve all registered objects of a specific domain class.
	 *
	 * @param <T>
	 *            specific domain object class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 *
	 * @return set of all objects of given domain class
	 */
	@SuppressWarnings("unchecked")
	public static final <T extends DomainObject> Set<T> all(Class<T> domainClass) {
		return (Set<T>) new HashSet<>(objectMap.get(domainClass).values());
	}

	/**
	 * Check if any object is registered for given domain class.
	 *
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 *
	 * @return true if such object exists, false otherwise
	 */
	public static final <T extends DomainObject> boolean hasAny(Class<T> domainClass) {
		return !objectMap.get(domainClass).isEmpty();
	}

	/**
	 * Check if any domain object of a specific domain class is registered fulfilling given predicate.
	 *
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 * @param predicate
	 *            predicate to fulfill
	 *
	 * @return true if such object exists, false otherwise
	 */
	public static final <T extends DomainObject> boolean hasAny(Class<T> domainClass, Predicate<? super T> predicate) {
		return all(domainClass).stream().anyMatch(predicate);
	}

	/**
	 * Find any domain object of a specific domain class fulfilling predicate.
	 *
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 * @param predicate
	 *            predicate to fulfill by object to find
	 *
	 * @return any domain object found or null if no object fulfills given predicate
	 */
	public static final <T extends DomainObject> T findAny(Class<T> domainClass, Predicate<? super T> predicate) {

		Optional<T> opt = all(domainClass).stream().filter(predicate).findAny();

		return (opt.isPresent() ? opt.get() : null);
	}

	/**
	 * Retrieve all domain objects of a specific object domain class fulfilling given predicate.
	 *
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 * @param predicate
	 *            predicate to fulfill by object to select
	 *
	 * @return Domain object fulfilling given predicate
	 */
	public static final <T extends DomainObject> Set<T> findAll(Class<T> domainClass, Predicate<? super T> predicate) {
		return all(domainClass).stream().filter(predicate).collect(Collectors.toSet());
	}

	/**
	 * Get # of domain objects of a specific object domain class fulfilling predicate.
	 *
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 * @param predicate
	 *            predicate to fulfill by object
	 *
	 * @return Domain object fulfilling given predicate
	 */
	public static final <T extends DomainObject> long count(Class<T> domainClass, Predicate<? super T> predicate) {
		return all(domainClass).stream().filter(predicate).count();
	}

	/**
	 * Build sorted list from collection of domain objects.
	 * <p>
	 * Sort by id or according to overridden {@code compareTo()} method.
	 * 
	 * @param objectCollection
	 *            collection of domain objects
	 * 
	 * @return sorted list of domain objects
	 */
	public static final <T extends DomainObject> List<T> sort(Collection<T> objectCollection) {

		List<T> objects = new ArrayList<>(objectCollection);
		Collections.sort(objects);

		return objects;
	}

	// -------------------------------------------------------------------------
	// Accumulations
	// -------------------------------------------------------------------------

	/**
	 * Group accumulation by given classifier
	 * <p>
	 * Example: {@code Map<Manufacturer, List<Car>> carByManufacturerMap = dc.groupBy(cars, c -> c.manufacturer)}
	 * 
	 * @param <T1>
	 *            type to group by
	 * @param <T2>
	 *            type of objects to group
	 * @param accumulation
	 *            accumulation
	 * @param classifier
	 *            classifier for elements of accumulation - e.g.: a reference to another domain object, a property, a property of a referenced domain object
	 * 
	 * @return map with accumulated objects grouped by classifier
	 */
	public static <T1, T2 extends DomainObject> Map<T1, Set<T2>> groupBy(Set<T2> accumulation, Function<? super T2, ? extends T1> classifier) {

		// Group only T2 elements where classifier applies a non-null result to avoid assertNonNull exception thrown by groupingBy()
		Map<T1, List<T2>> t2ListsGroupedByT1Map = accumulation.stream().filter(e -> classifier.apply(e) != null).collect(Collectors.groupingBy(classifier));
		Map<T1, Set<T2>> t2SetsGroupedByT1Map = new HashMap<>();
		for (Entry<T1, List<T2>> entry : t2ListsGroupedByT1Map.entrySet()) {
			t2SetsGroupedByT1Map.put(entry.getKey(), new HashSet<>(entry.getValue()));
		}

		// Add T2 elements where classifier applies a non-null result to T1 (if exist)
		Set<T2> t2sWithT1NullReference = accumulation.stream().filter(e -> classifier.apply(e) == null).collect(Collectors.toSet());
		if (!t2sWithT1NullReference.isEmpty()) {
			t2SetsGroupedByT1Map.put(null, t2sWithT1NullReference);
		}

		return t2SetsGroupedByT1Map;
	}

	/**
	 * Count accumulation elements by general classifier.
	 * <p>
	 * 
	 * @see {@link #groupBy(Set, Function)}.
	 * 
	 * @param <T1>
	 *            type to group by
	 * @param <T2>
	 *            type of objects to group
	 * @param accumulation
	 *            accumulation
	 * @param classifier
	 *            classifier for elements of accumulation - e.g.: a reference to another domain object, a property, a property of a referenced domain object
	 * 
	 * @return map with count of accumulated objects grouped by classifier
	 */
	public static <T1, T2 extends DomainObject> Map<T1, Integer> countBy(Set<T2> accumulation, Function<? super T2, ? extends T1> classifier) {

		Map<T1, Set<T2>> t2GroupedByT1Map = groupBy(accumulation, classifier);

		Map<T1, Integer> countGroupedByT1Map = new HashMap<>();
		for (Entry<T1, Set<T2>> entry : t2GroupedByT1Map.entrySet()) {
			countGroupedByT1Map.put(entry.getKey(), entry.getValue().size());
		}

		return countGroupedByT1Map;
	}

}
