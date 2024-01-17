package com.icx.dom.domain;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Reflection;
import com.icx.common.base.CRandom;
import com.icx.common.base.Common;
import com.icx.dom.domain.sql.SqlDomainController;

/**
 * Manages domain object store.
 * <p>
 * Register domain classes. Create and register domain objects. Manages accumulations (of child objects).
 * <p>
 * Specific domain controllers extend this class.
 * 
 * @author RainerBaumgärtel
 */
public abstract class DomainController<T extends DomainObject> extends Common {

	static final Logger log = LoggerFactory.getLogger(DomainController.class);

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Registry
	public Registry<T> registry = new Registry<>();

	// Object map by domain class of maps by object id
	protected Map<Class<? extends T>, SortedMap<Long, T>> objectMap = new ConcurrentHashMap<>();

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
	 *             on registration error
	 */
	public void registerDomainClasses(Class<T> baseClass, String domainPackageName) throws DomainException {
		registry.registerDomainClasses(baseClass, domainPackageName);
		registry.getRegisteredDomainClasses().forEach(c -> objectMap.put(c, new ConcurrentSkipListMap<>()));
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
	 *             on registration error
	 */
	@SafeVarargs
	public final void registerDomainClasses(Class<T> baseClass, Class<? extends T>... domainClasses) throws DomainException {
		registry.registerDomainClasses(baseClass, domainClasses);
		registry.getRegisteredDomainClasses().forEach(c -> objectMap.put(c, new ConcurrentSkipListMap<>()));
	}

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

	// Use milliseconds, thread id and random value for unique identifier
	protected synchronized <S extends T> long generateUniqueId(Class<S> domainObjectClass) {

		@SuppressWarnings("deprecation")
		long id = new Date().getTime() * 100000 + (Thread.currentThread().getId() % 100) * 1000 + CRandom.randomInt(1000);
		for (Class<? extends T> domainClass : registry.getDomainClassesFor(domainObjectClass)) {
			while (objectMap.get(domainClass).containsKey(id)) {
				id++;
			}
		}
		return id;
	}

	// Instantiate domain object - called on loading new object from database and if objects are created using create() methods of domain controller
	@SuppressWarnings("unchecked")
	public final <S extends T> S instantiate(Class<S> objectDomainClass) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		S obj = (S) registry.getConstructor(objectDomainClass).newInstance();
		initializeFields(obj);
		return obj;
	}

	/**
	 * Create, initialize and register object of domain class.
	 * <p>
	 * Calls initialization function before object registration to ensure that registered object is initialized.
	 * <p>
	 * Uses default constructor which therefore must be defined explicitly if other constructors are defined!
	 * 
	 * @param <T>
	 *            specific domain object class type
	 * @param domainObjectClass
	 *            instantiable domain (object) class
	 * @param init
	 *            object initialization function
	 * 
	 * @return newly created domain object
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	public final <S extends T> S create(final Class<S> domainObjectClass, Consumer<S> init) throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

		S obj = instantiate(domainObjectClass);
		if (init != null) {
			init.accept(obj);
		}
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
	 * Update accumulations (if exist) of parent objects reflecting any reference change of this object.
	 */
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
					log.warn("DOB: Could not remove {} from accumulation {} of {} (was not contained in accumulation)", obj.name(), Reflection.qualifiedName(accuField),
							DomainObject.name(oldReferencedObj));
				}

				if (newReferencedObj != null && !newReferencedObj.getAccumulationSet(accuField).add(obj)) {
					log.warn("DOB: Could not add {} to accumulation {} of {} (was already contained in accumulation)", obj.name(), Reflection.qualifiedName(accuField),
							DomainObject.name(newReferencedObj));
				}
			}
		}
	}

	// Remove object from accumulations (if exist) of parent objects
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
					log.warn("DOB: Could not remove {} from accumulation {} of {} (was not contained in accumulation)", obj.name(), Reflection.qualifiedName(accuField),
							DomainObject.name(referencedObj));
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Initialize and register domain objects
	// -------------------------------------------------------------------------

	// Initialize reference shadow map with null values for any reference field and initialize accumulation and complex fields with empty collections or maps if they are not already initialized
	void initializeFields(T obj) {

		// Initialize domain object for all domain classes
		for (Class<? extends T> domainClass : registry.getDomainClassesFor(registry.getCastedDomainClass(obj))) {

			// Initialize reference shadow map for reference fields where accumulations of parent objects are associated to
			// - to allow subsequent checking if references were changed and updating accumulations
			registry.getReferenceFields(domainClass).stream().filter(f -> registry.getAccumulationFieldForReferenceField(f) != null).forEach(f -> obj.refForAccuShadowMap.put(f, null));

			// Initialize registered collection/map fields if not already done
			registry.getComplexFields(domainClass).stream().filter(f -> obj.getFieldValue(f) == null).forEach(f -> obj.setFieldValue(f, Reflection.newComplexObject(f.getType())));

			// Initialize own accumulation fields
			registry.getAccumulationFields(domainClass).stream().filter(f -> obj.getFieldValue(f) == null).forEach(f -> obj.setFieldValue(f, ConcurrentHashMap.newKeySet()));
		}
	}

	// Register domain object by given id for object domain class and all inherited domain classes
	public final boolean registerById(T obj, long id) {

		Class<? extends T> objectDomainClass = registry.getCastedDomainClass(obj);
		if (objectMap.get(objectDomainClass).containsKey(id)) {
			log.info("{} is an already registered object", obj);
			return false;
		}

		obj.id = id;
		registry.getDomainClassesFor(objectDomainClass).forEach(c -> objectMap.get(c).put(id, obj));
		updateAccumulationsOfParentObjects(obj);
		if (log.isTraceEnabled()) {
			log.trace("DC: Registered: {}", obj.name());
		}

		return true;
	}

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
	 * To call if application specific constructor is used to create domain object instead of using {@link DomainController#create(Class, java.util.function.Consumer)} or
	 * {@link SqlDomainController#createAndSave(Class, java.util.function.Consumer)}.
	 * 
	 * @param <T>
	 *            domain object class
	 * 
	 * @return this object
	 */
	public <S extends T> S register(S obj) {
		initializeFields(obj);
		registerById(obj, generateUniqueId(registry.castDomainClass(obj.getClass())));
		return obj;
	}

	/**
	 * Check if object is registered.
	 * 
	 * @return true if object is registered, false if it was deleted or if it was not loaded because it was out of data horizon on load time
	 */
	public boolean isRegistered(DomainObject obj) {
		return objectMap.get(obj.getClass()).containsKey(obj.getId());
	}

	// -------------------------------------------------------------------------
	// Children
	// -------------------------------------------------------------------------

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

	// Get objects which references this object
	public Set<T> getDirectChildren(T obj) {

		Set<T> children = new HashSet<>();
		for (Entry<Field, Set<T>> entry : getDirectChildrenByRefField(obj).entrySet()) {
			children.addAll(entry.getValue());
		}
		return children;
	}

	// Check if object is referenced by any registered object
	public boolean isReferenced(T obj) {
		return (!getDirectChildren(obj).isEmpty());
	}

	// -------------------------------------------------------------------------
	// Deletion
	// -------------------------------------------------------------------------

	// Recursively check if all direct and indirect children can be deleted
	public boolean canBeDeletedRecursive(T obj, List<DomainObject> objectsToCheck) {

		if (!obj.canBeDeleted()) {
			return false;
		}

		if (!objectsToCheck.contains(obj)) { // Avoid endless recursion
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
	public final <S extends T> S get(Class<S> domainClass, long objectId) throws ObjectNotFoundException {

		S object = (S) objectMap.get(domainClass).get(objectId);
		if (object != null) {
			return object;
		}

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
	public final <S extends T> S find(Class<S> domainClass, long objectId) {
		try {
			return get(domainClass, objectId);
		}
		catch (ObjectNotFoundException e) {
			return null;
		}
	}

	/**
	 * Retrieve all registered objects of a specific domain class.
	 * <p>
	 * Returns new set containing objects, not internally used collection.
	 *
	 * @param <T>
	 *            specific domain object class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 *
	 * @return set of all objects of given domain class
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
	 * @param <T>
	 *            specific domain class type
	 * @param domainClass
	 *            domain class - may also be any - non instantiable - base class of a domain object class
	 *
	 * @return true if such object exists, false otherwise
	 */
	public final <S extends T> boolean hasAny(Class<S> domainClass) {
		return (!objectMap.get(domainClass).isEmpty());
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
	public final <S extends T> boolean hasAny(Class<S> domainClass, Predicate<S> predicate) {
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
	public final <S extends T> S findAny(Class<S> domainClass, Predicate<S> predicate) {
		Optional<S> opt = all(domainClass).stream().filter(predicate).findAny();
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
	public final <S extends T> Set<S> findAll(Class<S> domainClass, Predicate<S> predicate) {
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
	public final <S extends T> long count(Class<S> domainClass, Predicate<S> predicate) {
		return all(domainClass).stream().filter(predicate).count();
	}

	/**
	 * Retrieve all registered objects fulfilling given predicate.
	 * 
	 * @param predicate
	 *            predicate to fulfill
	 * 
	 * @return set of all objects currently loaded into object store
	 */
	@SuppressWarnings("unchecked")
	public final <S extends T> Set<S> findAll(Predicate<S> predicate) {

		Set<S> all = new HashSet<>();
		for (Class<? extends T> objectDomainClass : registry.getRegisteredObjectDomainClasses()) {
			all.addAll(findAll((Class<S>) objectDomainClass, predicate));
		}
		return all;
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
	public final <S extends T> List<S> sort(Collection<S> objectCollection) {
		List<S> objects = new ArrayList<>(objectCollection);
		Collections.sort(objects);
		return objects;
	}

	// -------------------------------------------------------------------------
	// Accumulations
	// -------------------------------------------------------------------------

	/**
	 * Group accumulation by given classifier.
	 * <p>
	 * Example: {@code Map<Manufacturer, List<Car>> carByManufacturerMap = DomainController.groupBy(cars, c -> c.manufacturer)}
	 * 
	 * @param accumulation
	 *            accumulation
	 * @param classifier
	 *            classifier for elements of accumulation - e.g.: a reference to another domain object, a property, a property of a referenced domain object
	 * 
	 * @return map with accumulated objects grouped by classifier
	 */
	public <S1, S2 extends T> Map<S1, Set<S2>> groupBy(Set<S2> accumulation, Function<S2, S1> classifier) {

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

		return t2SetsGroupedByT1Map;
	}

	/**
	 * Count accumulation elements by general classifier.
	 * <p>
	 * 
	 * @see {@link #groupBy(Set, Function)}.
	 * 
	 * @param accumulation
	 *            accumulation
	 * @param classifier
	 *            classifier for elements of accumulation - e.g.: a reference to another domain object, a property, a property of a referenced domain object
	 * 
	 * @return map with count of accumulated objects grouped by classifier
	 */
	public <S1, S2 extends T> Map<S1, Integer> countBy(Set<S2> accumulation, Function<S2, S1> classifier) {

		Map<S1, Set<S2>> t2GroupedByT1Map = groupBy(accumulation, classifier);
		Map<S1, Integer> countGroupedByT1Map = new HashMap<>();
		for (Entry<S1, Set<S2>> entry : t2GroupedByT1Map.entrySet()) {
			countGroupedByT1Map.put(entry.getKey(), entry.getValue().size());
		}
		return countGroupedByT1Map;
	}
}
