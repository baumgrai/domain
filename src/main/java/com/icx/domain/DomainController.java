package com.icx.domain;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CRandom;
import com.icx.common.CReflection;
import com.icx.common.Common;
import com.icx.domain.sql.SqlDomainController;
import com.icx.domain.sql.Annotations.Accumulation;

/**
 * Manages domain object store.
 * <p>
 * Register domain classes. Create and register domain objects. Manages accumulations (of child objects).
 * <p>
 * Specific domain controllers extend this class.
 * 
 * @param <T>
 *            Base type of domain objects
 * 
 * @author baumgrai
 */
public abstract class DomainController<T extends DomainObject> extends Common {

	static final Logger log = LoggerFactory.getLogger(DomainController.class);

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Registry
	private Registry<T> registry = new Registry<>();

	/**
	 * Only for internal use!
	 * 
	 * @return registry
	 */
	// Get registry
	public Registry<T> getRegistry() {
		return registry;
	}

	/**
	 * Only for internal use!
	 * 
	 * @param registry
	 *            registry
	 */
	// Set registry
	public void setRegistry(Registry<T> registry) {
		this.registry = registry;
	}

	// Object store - map of maps of objects by object id by domain class
	// Note: Objects of domain classes which are derived from other domain classes have multiple entries - one entry per (derived) domain class - here
	protected Map<Class<? extends T>, SortedMap<Long, T>> objectMap = new ConcurrentHashMap<>();

	// -------------------------------------------------------------------------
	// Register domain classes
	// -------------------------------------------------------------------------

	/**
	 * Register domain classes by given domain package.
	 * <p>
	 * All classes in this package and in any sub package which extends DomainObject class will be registered as domain classes.
	 * <p>
	 * Top level domain classes are so called 'object domain classes'. E.g.: RaceBike extends Bike (extends SqlDomainObject) -> RaceBike is 'object' domain class, Bike is domain class. Non-'object'
	 * domain classes must be abstract.
	 * 
	 * @param baseClass
	 *            class where all domain classes to register are derived from ({@code SqlDomainObject.class})
	 * @param domainPackageName
	 *            name of package where domain classes reside
	 * 
	 * @throws DomainException
	 *             on registration error
	 */
	public void registerDomainClasses(Class<T> baseClass, String domainPackageName) throws DomainException {

		registry.registerDomainClasses(baseClass, domainPackageName);
		registry.getRegisteredDomainClasses().forEach(c -> objectMap.put(c, new ConcurrentSkipListMap<>()));
	}

	/**
	 * Register given object domain classes and derived domain classes.
	 * <p>
	 * Classes must extend DomainObject class. Automatically registers base domain classes for given object domain class on inherited domain classes (RaceBike extends Bike -> only RaceBike has to be
	 * provided). Automatically registers inner domain classes and parent domain classes (Bike references Manufacturer -> only Bike has to be provided) too.
	 * 
	 * @param baseClass
	 *            class where all domain classes to register are derived from ({@code SqlDomainObject.class})
	 * @param domainClasses
	 *            list of object domain classes to register
	 * 
	 * @throws DomainException
	 *             on registration error
	 */
	@SafeVarargs
	public final void registerDomainClasses(Class<T> baseClass, Class<? extends T>... domainClasses) throws DomainException {

		registry.registerDomainClasses(baseClass, domainClasses);
		registry.getRegisteredDomainClasses().forEach(c -> objectMap.put(c, new ConcurrentSkipListMap<>()));
	}

	/**
	 * Only for internal use!
	 * 
	 * @param className
	 *            name of domain class
	 * 
	 * @return domain class object
	 */
	// Get registered domain class by it's name
	public final Class<? extends T> getDomainClassByName(String className) {

		Optional<Class<? extends T>> o = registry.getRegisteredDomainClasses().stream().filter(c -> c.getSimpleName().equals(className)).findFirst();
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

	static AtomicInteger counterWithinMilliseconds = new AtomicInteger(0);
	static long last = new Date().getTime();

	// Use milliseconds, atomic int and random value for unique identifier
	private static synchronized long generateUniqueIdStatic() {

		long now = new Date().getTime();
		if (now > last) {
			last = now;
			counterWithinMilliseconds = new AtomicInteger(CRandom.randomInt(100) * 100);
		}

		return now * 1000000 + (counterWithinMilliseconds.getAndIncrement() % 1000) * 1000 + CRandom.randomInt(1000);
	}

	/**
	 * Only for internal use but may be overridden: Generate unique object id.
	 * <p>
	 * Id scheme: {@code Date().getTime()} * 1.000.000 + (atomic counter % 1000 starting randomly with 100, 200, ..., 900) * 1000 + random integer % 1000
	 * 
	 * @return unique id
	 */
	@SuppressWarnings("static-method")
	protected synchronized long generateUniqueId() {
		return generateUniqueIdStatic();
	}

	/**
	 * Only for internal use!
	 * 
	 * @param <S>
	 *            specific object domain class type
	 * @param objectDomainClass
	 *            object domain class
	 * 
	 * @return new domain object
	 */
	// Instantiate domain object
	@SuppressWarnings("unchecked")
	public final <S extends T> S instantiate(Class<S> objectDomainClass) {

		try {
			S obj = (S) registry.getConstructor(objectDomainClass).newInstance();
			initializeFields(obj);
			return obj;
		}
		catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
			// Note #instantiate() method is only used in context of domain persistence mechanism where these exceptions may not be thrown because it is ensured on domain controller startup that a
			// public parameterless constructor exists for - non abstract - object domain classes, that this constructor is accessible per Reflection and that it does not throw exceptions itself
			log.error("DC: Exception {} occurred trying to instantiate object of domain class '{}'", ex, objectDomainClass.getName());
			return null;
		}
	}

	/**
	 * Create, initialize and register object of domain class.
	 * <p>
	 * Calls initialization function before object registration to ensure that registered object is initialized.
	 * <p>
	 * Uses default constructor which therefore must be defined explicitly if other constructors are defined!
	 * 
	 * @param <S>
	 *            specific object domain class type
	 * @param domainObjectClass
	 *            instantiable domain (object) class
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created domain object
	 */
	public final <S extends T> S create(final Class<S> domainObjectClass, Consumer<S> init) {

		S obj = instantiate(domainObjectClass);
		if (obj == null) {
			return obj;
		}

		if (init != null) {
			init.accept(obj);
		}
		obj.setDc(this);
		register(obj);
		if (log.isDebugEnabled()) {
			log.debug("DC: Created {}.", obj.name());
		}
		return obj;
	}

	// -------------------------------------------------------------------------
	// Accumulations
	// -------------------------------------------------------------------------

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            child object
	 */
	// Update accumulations (if exist) of parent objects reflecting any reference change of this object
	public void updateAccumulationsOfParentObjects(T obj) {

		if (obj.refForAccuShadowMap == null) {
			return;
		}

		for (Entry<Field, DomainObject> entry : obj.refForAccuShadowMap.entrySet()) {
			Field refField = entry.getKey();
			DomainObject newReferencedObj = (DomainObject) obj.getFieldValue(refField);
			DomainObject oldReferencedObj = entry.getValue();

			if (!objectsEqual(newReferencedObj, oldReferencedObj)) {
				obj.refForAccuShadowMap.put(refField, newReferencedObj);
				Field accuField = registry.getAccumulationFieldForReferenceField(refField);

				if (oldReferencedObj != null && !oldReferencedObj.getAccumulationSet(accuField).remove(obj)) {
					log.warn("DOB: Could not remove {} from accumulation {} of {} (was not contained in accumulation)", obj.name(), CReflection.qualifiedName(accuField),
							DomainObject.name(oldReferencedObj));
				}

				if (newReferencedObj != null && !newReferencedObj.getAccumulationSet(accuField).add(obj)) {
					log.warn("DOB: Could not add {} to accumulation {} of {} (was already contained in accumulation)", obj.name(), CReflection.qualifiedName(accuField),
							DomainObject.name(newReferencedObj));
				}
			}
		}
	}

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            child object
	 */
	// Remove object from accumulations (if exist) of parent object
	protected void removeFromAccumulationsOfParentObjects(T obj) {

		if (obj.refForAccuShadowMap == null) {
			return;
		}

		for (Entry<Field, DomainObject> entry : obj.refForAccuShadowMap.entrySet()) {
			Field refField = entry.getKey();
			DomainObject referencedObj = entry.getValue();
			obj.refForAccuShadowMap.put(refField, null);

			if (referencedObj != null) {
				Field accuField = registry.getAccumulationFieldForReferenceField(refField);
				if (!referencedObj.getAccumulationSet(accuField).remove(obj)) {
					log.warn("DOB: Could not remove {} from accumulation {} of {} (was not contained in accumulation)", obj.name(), CReflection.qualifiedName(accuField),
							DomainObject.name(referencedObj));
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Initialize and register domain objects
	// -------------------------------------------------------------------------

	// Initialize reference shadow map with null values for any reference field and initialize accumulation and complex fields with empty collections or maps if they are not already initialized
	private void initializeFields(T obj) {

		// Initialize domain object for all domain classes
		for (Class<? extends T> domainClass : registry.getDomainClassesFor(registry.getCastedDomainClass(obj))) {

			// Initialize reference shadow map for reference fields where accumulations of parent objects are associated to
			// - to allow subsequent checking if references were changed and updating accumulations
			registry.getReferenceFields(domainClass).stream().filter(f -> registry.getAccumulationFieldForReferenceField(f) != null).forEach(f -> obj.refForAccuShadowMap.put(f, null));

			// Initialize registered collection/map fields if not already done
			registry.getComplexFields(domainClass).stream().filter(f -> obj.getFieldValue(f) == null).forEach(f -> obj.setFieldValue(f, CReflection.newComplexObject(f.getType())));

			// Initialize own accumulation fields
			registry.getAccumulationFields(domainClass).stream().filter(f -> obj.getFieldValue(f) == null).forEach(f -> obj.setFieldValue(f, ConcurrentHashMap.newKeySet()));
		}
	}

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            object
	 * @param id
	 *            id
	 * 
	 * @return true if object could be registered, false otherwise
	 */
	// Register domain object by given id for object domain class and all inherited domain classes
	public final synchronized boolean registerById(T obj, long id) {

		Class<? extends T> objectDomainClass = registry.getCastedDomainClass(obj);
		if (objectMap.get(objectDomainClass).containsKey(id)) {
			if (log.isDebugEnabled()) {
				log.debug("DC: {} is an already registered object", obj);
			}
			return false;
		}

		obj.setDc(this);
		obj.setId(id);
		registry.getDomainClassesFor(objectDomainClass).forEach(c -> objectMap.get(c).put(id, obj));
		updateAccumulationsOfParentObjects(obj);
		if (log.isTraceEnabled()) {
			log.trace("DC: Registered: {}", obj.name());
		}

		return true;
	}

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            object to unregister
	 */
	// Unregister domain object and remove it from all accumulations
	protected void unregister(T obj) {

		removeFromAccumulationsOfParentObjects(obj);
		registry.getDomainClassesFor(registry.getCastedDomainClass(obj)).forEach(c -> objectMap.get(c).remove(obj.getId()));
		if (log.isDebugEnabled()) {
			log.debug("DC: Unregistered: {}", obj.name());
		}
	}

	/**
	 * Register object in object store.
	 * <p>
	 * To call if constructor is used to create domain object instead of using {@link DomainController#create(Class, java.util.function.Consumer)} or
	 * {@link SqlDomainController#createAndSave(Class, java.util.function.Consumer)}.
	 * 
	 * @param <S>
	 *            object domain class
	 * @param obj
	 *            object to register in object store
	 * 
	 * @return this object
	 */
	public <S extends T> S register(S obj) {
		initializeFields(obj);
		registerById(obj, generateUniqueId());
		return obj;
	}

	/**
	 * Check if object is registered.
	 * 
	 * @param obj
	 *            object to check
	 * 
	 * @return true if object is registered, false if it was deleted or if it was not loaded because it was out of data horizon on load time
	 */
	public boolean isRegistered(DomainObject obj) {
		return objectMap.get(obj.getClass()).containsKey(obj.getId());
	}

	// -------------------------------------------------------------------------
	// Children
	// -------------------------------------------------------------------------

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            parent object
	 * 
	 * @return children
	 */
	// Get objects which references this object ordered by reference field
	protected Map<Field, Set<T>> getDirectChildrenByRefField(T obj) {

		Map<Field, Set<T>> childrenByRefFieldMap = new HashMap<>();
		for (Field refField : registry.getAllReferencingFields(registry.getCastedDomainClass(obj))) { // For fields of all domain classes referencing any of object's domain classes (inheritance)
			Class<T> referencingClass = registry.getCastedDeclaringDomainClass(refField);

			for (T child : findAll(referencingClass, ch -> objectsEqual(ch.getFieldValue(refField), obj))) {
				childrenByRefFieldMap.computeIfAbsent(refField, f -> new HashSet<>()).add(child);
			}
		}
		return childrenByRefFieldMap;
	}

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            parent object
	 * 
	 * @return children
	 */
	// Get all objects which references this object
	public Set<T> getDirectChildren(T obj) {

		Set<T> children = new HashSet<>();
		for (Entry<Field, Set<T>> entry : getDirectChildrenByRefField(obj).entrySet()) {
			children.addAll(entry.getValue());
		}
		return children;
	}

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            object to check
	 * 
	 * @return true if any children exist, false otherwise
	 */
	// Check if object is referenced by any registered object
	protected boolean isReferenced(T obj) {
		return (!getDirectChildren(obj).isEmpty());
	}

	// -------------------------------------------------------------------------
	// Deletion
	// -------------------------------------------------------------------------

	/**
	 * Only for internal use!
	 * 
	 * @param obj
	 *            object to check
	 * @param objectsToCheck
	 *            list used to avoid endless recursion
	 * 
	 * @return true if object can be deleted, false otherwise
	 */
	// Recursively check if all direct and indirect children can be deleted
	protected boolean canBeDeletedRecursive(T obj, List<DomainObject> objectsToCheck) {

		if (!obj.canBeDeleted()) {
			log.info("SDC: {} cannot be deleted because overriden #canBeDeleted() of {} class or any base class returned false!", obj.name(), obj.getClass().getSimpleName());
			return false;
		}

		if (!objectsToCheck.contains(obj)) { // Avoid endless recursion on circular references
			objectsToCheck.add(obj);

			for (T child : getDirectChildren(obj)) {
				if (!canBeDeletedRecursive(child, objectsToCheck)) {
					return false;
				}
			}
		}

		return true;
	}

	// -------------------------------------------------------------------------
	// Access domain objects
	// -------------------------------------------------------------------------

	/**
	 * Find object by id, return null if not found.
	 * 
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 * @param objectId
	 *            object id
	 * 
	 * @return object found or null
	 */
	@SuppressWarnings("unchecked")
	public final <S extends T> S find(Class<S> domainClass, long objectId) {
		return (S) objectMap.get(domainClass).get(objectId);
	}

	/**
	 * Get object by domain class and id, throws exception if not found.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 * @param objectId
	 *            object id
	 *
	 * @return identified object
	 *
	 * @throws ObjectNotFoundException
	 *             if denoted object does not exist
	 */
	public final <S extends T> S get(Class<S> domainClass, long objectId) throws ObjectNotFoundException {

		S object = find(domainClass, objectId);
		if (object == null) {
			throw new ObjectNotFoundException("No " + domainClass.getSimpleName().toLowerCase() + " found for id: " + objectId);
		}

		return object;
	}

	/**
	 * Find object by universal object id, return null if not found.
	 * 
	 * @param universalId
	 *            universal object id (scheme: 'domainclassname@objectid')
	 * 
	 * @return object found or null (also on invalid universal id)
	 */
	public final T find(String universalId) {
		return find(getDomainClassByName(untilFirst(universalId, "@")), Long.parseLong(behindFirst(universalId, "@")));
	}

	/**
	 * Retrieve all registered objects of a specific domain class.
	 * <p>
	 * Returns new set containing objects instead of existing collection which is part of object store.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 *
	 * @return newly created set of all registered objects of given domain class
	 */
	@SuppressWarnings("unchecked")
	public final <S extends T> Set<S> all(Class<S> domainClass) {

		Set<S> objects = new HashSet<>();
		objectMap.get(domainClass).values().forEach(v -> objects.add((S) v));
		return objects;
	}

	/**
	 * Check if any object is registered for given domain class.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 *
	 * @return true if a registered object of given domain class exists, false otherwise
	 */
	public final <S extends T> boolean hasAny(Class<S> domainClass) {
		return (!objectMap.get(domainClass).isEmpty());
	}

	/**
	 * Check if any domain object of a specific domain class is registered fulfilling given predicate.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 * @param predicate
	 *            predicate to fulfill
	 *
	 * @return true if such object exists, false otherwise
	 */
	public final <S extends T> boolean hasAny(Class<S> domainClass, Predicate<S> predicate) {
		return all(domainClass).stream().anyMatch(predicate);
	}

	/**
	 * Find any registered domain object of a specific domain class fulfilling given predicate.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 * @param predicate
	 *            predicate to fulfill
	 *
	 * @return any domain object found or null if no object exists fulfilling given predicate
	 */
	public final <S extends T> S findAny(Class<S> domainClass, Predicate<S> predicate) {

		if (predicate != null) {
			return all(domainClass).stream().filter(predicate).findAny().orElse(null);
		}
		else {
			return all(domainClass).stream().findAny().orElse(null);
		}
	}

	/**
	 * Retrieve all registered domain objects of a specific domain class fulfilling given predicate.
	 * <p>
	 * Returns new set containing objects instead of existing collection which is part of object store.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 * @param predicate
	 *            predicate to fulfill
	 *
	 * @return Registered domain objects fulfilling given predicate
	 */
	public final <S extends T> Set<S> findAll(Class<S> domainClass, Predicate<S> predicate) {

		if (predicate != null) {
			return all(domainClass).stream().filter(predicate).collect(Collectors.toSet());
		}
		else {
			return all(domainClass).stream().collect(Collectors.toSet());
		}
	}

	/**
	 * Get # of registered domain objects of a specific domain class fulfilling given predicate.
	 *
	 * @param <S>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may be an object domain class or a - non instantiable - base class of an object domain class (RaceBike or Bike)
	 * @param predicate
	 *            predicate to fulfill
	 *
	 * @return Number of domain objects of given domain class fulfilling given predicate
	 */
	public final <S extends T> long count(Class<S> domainClass, Predicate<S> predicate) {
		if (predicate != null) {
			return all(domainClass).stream().filter(predicate).count();
		}
		else {
			return all(domainClass).stream().count();
		}
	}

	/**
	 * Retrieve all registered domain objects fulfilling given predicate.
	 * 
	 * @param predicate
	 *            predicate to fulfill
	 * 
	 * @return set of all registered objects - independently of their domain classes - fulfilling given predicate
	 */
	@SuppressWarnings("unchecked")
	public final Set<T> findAll(Predicate<T> predicate) {

		Set<T> all = new HashSet<>();
		for (Class<? extends T> objectDomainClass : registry.getRegisteredObjectDomainClasses()) {
			all.addAll(findAll((Class<T>) objectDomainClass, predicate));
		}
		return all;
	}

	/**
	 * Build sorted list from collection of domain objects of a specific domain class.
	 * <p>
	 * Sort by id or according to overridden {@code compareTo()} method.
	 * 
	 * @param <S>
	 *            specific domain class type
	 * @param objectCollection
	 *            collection of domain objects
	 * 
	 * @return sorted list of domain objects
	 */
	public final <S extends T> List<S> sort(Collection<S> objectCollection) {
		List<S> objects = new ArrayList<>(objectCollection);
		Collections.sort(objects);
		return objects;
	}

	// -------------------------------------------------------------------------
	// Accumulations
	// -------------------------------------------------------------------------

	/**
	 * Group (accumulated) elements by given classifier.
	 * <p>
	 * Example: {@code Map<Manufacturer, List<Bike>> bikeByManufacturerMap = DomainController.groupBy(bikes, c -> c.manufacturer)}
	 * 
	 * @param <S1>
	 *            type of classifier objects
	 * @param <S2>
	 *            domain class type of (child) objects to group
	 * @param accumulation
	 *            set of (child) objects to group; typically an 'accumulation' - a managed set of children defined using {@link Accumulation} annotation
	 * @param classifier
	 *            classifier for elements of accumulation - e.g.: a reference to another domain object, a property, a property of a referenced domain object
	 * 
	 * @return map with (accumulated) objects grouped by classifier
	 */
	public <S1, S2 extends T> SortedMap<S1, Set<S2>> groupBy(Set<S2> accumulation, Function<S2, S1> classifier) {

		// Group only elements where classifier applies a non-null result to avoid assertNonNull exception thrown by groupingBy()
		Map<S1, List<S2>> t2ListsGroupedByT1Map = accumulation.stream().filter(e -> classifier.apply(e) != null).collect(Collectors.groupingBy(classifier));
		Map<S1, Set<S2>> t2SetsGroupedByT1Map = new HashMap<>();
		for (Entry<S1, List<S2>> entry : t2ListsGroupedByT1Map.entrySet()) {
			t2SetsGroupedByT1Map.put(entry.getKey(), new HashSet<>(entry.getValue()));
		}

		// Add elements where classifier applies a non-null result to T1 (if exist)
		Set<S2> t2sWithT1NullReference = accumulation.stream().filter(e -> classifier.apply(e) == null).collect(Collectors.toSet());
		if (!t2sWithT1NullReference.isEmpty()) {
			t2SetsGroupedByT1Map.put(null, t2sWithT1NullReference);
		}

		return new TreeMap<>(t2SetsGroupedByT1Map);
	}

	/**
	 * Count (accumulated) elements by given classifier.
	 * <p>
	 * Like {@link #groupBy(Set, Function)}.
	 * 
	 * @param <S1>
	 *            type of classifier objects
	 * @param <S2>
	 *            domain class type of (child) objects to group
	 * @param accumulation
	 *            set of (child) objects to group; typically an 'accumulation' - a managed set of children defined using {@link Accumulation} annotation
	 * @param classifier
	 *            classifier for elements of accumulation - e.g.: a reference to another domain object, a property, a property of a referenced domain object
	 * 
	 * @return map with count of (accumulated) objects grouped by classifier
	 */
	public <S1, S2 extends T> SortedMap<S1, Integer> countBy(Set<S2> accumulation, Function<S2, S1> classifier) {

		SortedMap<S1, Set<S2>> t2GroupedByT1Map = groupBy(accumulation, classifier);
		SortedMap<S1, Integer> countGroupedByT1Map = new TreeMap<>();
		for (Entry<S1, Set<S2>> entry : t2GroupedByT1Map.entrySet()) {
			countGroupedByT1Map.put(entry.getKey(), entry.getValue().size());
		}

		return countGroupedByT1Map;
	}
}
