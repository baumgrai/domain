package com.icx.dom.domain;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CBase;
import com.icx.dom.common.CLog;
import com.icx.dom.common.Reflection;
import com.icx.dom.domain.sql.SqlDomainController;
import com.icx.dom.jdbc.SqlDbException;

/**
 * Base class for objects managed by any domain controller.
 * <p>
 * Any domain object must extend this class. Objects managed by specific domain controllers may have a specific base class which extends this class.
 * 
 * @author RainerBaumgärtel
 */
public abstract class DomainObject implements Comparable<DomainObject> {

	static final Logger log = LoggerFactory.getLogger(DomainObject.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final String ID_FIELD = "id";

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// ID
	protected long id = 0;

	public long getId() {
		return id;
	}

	// Shadows for reference fields where accumulations are assigned to -
	// contains referenced objects before updating accumulations and so allow both changing accumulation of old and new referenced objects
	transient Map<Field, DomainObject> refForAccuShadowMap = new HashMap<>();

	// -------------------------------------------------------------------------
	// Overrides
	// -------------------------------------------------------------------------

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return (getClass().getSimpleName() + "@" + id);
	}

	// Default comparison - may be overridden
	@Override
	public int compareTo(DomainObject o) {
		return CBase.compare(id, o.id);
	}

	@Override
	public boolean equals(Object o) {
		return super.equals(o);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	// -------------------------------------------------------------------------
	// Name
	// -------------------------------------------------------------------------

	// Identifier - domain object class name and object id plus - if different - toString() for object - used for logging
	public String name() {

		String internalName = getClass().getSimpleName() + "@" + id;

		String logicalName;
		try {
			logicalName = toString();
		}
		catch (Exception ex) {
			logicalName = null;
		}

		String name = internalName;
		if (logicalName != null && !CBase.objectsEqual(logicalName, internalName)) {
			name += " ('" + logicalName + "')";
		}

		return name;
	}

	public static final String name(DomainObject obj) {
		return (obj == null ? "null" : obj.name());
	}

	// -------------------------------------------------------------------------
	// Initialization and registration
	// -------------------------------------------------------------------------

	// Initialize reference shadow map with null values for any reference field and initialize accumulation and complex fields with empty collections or maps if they are not already initialized
	void initializeFields() {

		// Initialize domain object for all domain classes
		for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(getClass())) {

			// Initialize reference shadow map for reference fields where accumulations of parent objects are associated to
			// - to allow subsequent checking if references were changed and updating accumulations
			Registry.getReferenceFields(domainClass).stream().filter(f -> Registry.getAccumulationFieldForReferenceField(f) != null).forEach(f -> refForAccuShadowMap.put(f, null));

			// Initialize registered collection/map fields if not already done
			Registry.getComplexFields(domainClass).stream().filter(f -> getFieldValue(f) == null).forEach(f -> setKnownFieldValue(f, Reflection.newComplex(f.getType())));

			// Initialize own accumulation fields
			Registry.getAccumulationFields(domainClass).stream().filter(f -> getFieldValue(f) == null).forEach(f -> setKnownFieldValue(f, new HashSet<>()));
		}
	}

	// Register domain object by given id for all domain classes
	public final void registerById(long id) {

		this.id = id;

		Registry.getInheritanceStack(getClass()).forEach(c -> DomainController.objectMap.get(c).put(id, this));

		updateAccumulationsOfParentObjects();

		if (log.isTraceEnabled()) {
			log.trace("DC: Registered: {}", name());
		}
	}

	// Unregister domain object and remove it from all accumulations
	protected void unregister() {

		removeFromAccumulationsOfParentObjects();

		Registry.getInheritanceStack(getClass()).forEach(c -> DomainController.objectMap.get(c).remove(id));

		if (log.isDebugEnabled()) {
			log.debug("DC: Unregistered: {}", name());
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
	@SuppressWarnings("unchecked")
	public <T extends DomainObject> T register() {

		initializeFields();

		registerById(DomainController.generateUniqueId(getClass()));

		return (T) this;
	}

	/**
	 * Check if object is registered.
	 * 
	 * @return true if object is registered, false if it was deleted or if it was not loaded because it was out of data horizon on load time
	 */
	public boolean isRegistered() {
		return DomainController.objectMap.get(getClass()).containsKey(id);
	}

	// -------------------------------------------------------------------------
	// Fields
	// -------------------------------------------------------------------------

	// Get field value for object
	public Object getFieldValue(Field field) {
		try {
			return field.get(this);
		}
		catch (IllegalArgumentException | IllegalAccessException e) {

			// Do not throw exception on getting field value because an exception is thrown here only on bug in code
			log.error("DOB: {} '{}' occurred trying to get value of field '{}' for object {}", e.getClass().getSimpleName(), e.getMessage(), Reflection.qualifiedName(field), name());
			return null;
		}
	}

	// Set field value for object
	public void setFieldValue(Field field, Object value) throws SqlDbException {
		try {
			field.set(this, value);
		}
		catch (IllegalArgumentException | IllegalAccessException e) {
			log.error("DC: {} '{}' oocurred", e.getClass().getSimpleName(), e.getMessage());
			throw new SqlDbException(e.getClass().getSimpleName() + " '" + e.getMessage() + "' occurred trying to set field '" + Reflection.qualifiedName(field) + "' of object " + name() + " to "
					+ CLog.forAnalyticLogging(value));
		}
	}

	// Set field value for object without throwing exception (use for values fixed in code or well known)
	public void setKnownFieldValue(Field field, Object value) {
		try {
			field.set(this, value);
		}
		catch (IllegalArgumentException | IllegalAccessException e) {

			// Do not throw exception on setting field to a value fixed in code or well known because an exception is thrown here only on bug in code
			log.error("DOB: {} '{}' occurred trying to set field '{}' for object {} to {}", e.getClass().getSimpleName(), e.getMessage(), Reflection.qualifiedName(field), name(),
					CLog.forAnalyticLogging(value));
		}
	}

	// -------------------------------------------------------------------------
	// Children
	// -------------------------------------------------------------------------

	// Get objects which references this object ordered by reference field
	protected Map<Field, Set<DomainObject>> getDirectChildrenByRefField() {

		Map<Field, Set<DomainObject>> childrenByRefFieldMap = new HashMap<>();

		for (Field refField : Registry.getAllReferencingFields(getClass())) { // For fields of all domain classes referencing any of object's domain classes (inheritance)
			Class<? extends DomainObject> referencingClass = Registry.getDeclaringDomainClass(refField);

			for (DomainObject child : DomainController.findAll(referencingClass, ch -> CBase.objectsEqual(ch.getFieldValue(refField), this))) {
				childrenByRefFieldMap.computeIfAbsent(refField, f -> new HashSet<>()).add(child);
			}
		}

		return childrenByRefFieldMap;
	}

	// Get objects which references this object
	public Set<DomainObject> getDirectChildren() {

		Set<DomainObject> children = new HashSet<>();
		for (Entry<Field, Set<DomainObject>> entry : getDirectChildrenByRefField().entrySet()) {
			children.addAll(entry.getValue());
		}

		return children;
	}

	public boolean isReferenced() {
		return (!getDirectChildren().isEmpty());
	}

	// -------------------------------------------------------------------------
	// Deletion
	// -------------------------------------------------------------------------

	/**
	 * To override by specific domain class to check if object can be deleted.
	 * 
	 * @return true if object can be deleted, false otherwise
	 */
	@SuppressWarnings("static-method")
	public boolean canBeDeleted() {
		return true;
	}

	// Recursively check if all direct and indirect children can be deleted
	protected boolean canBeDeletedRecursive(List<DomainObject> objectsToCheck) {

		if (!canBeDeleted()) {
			return false;
		}

		if (!objectsToCheck.contains(this)) { // Avoid endless recursion
			objectsToCheck.add(this);

			for (DomainObject child : getDirectChildren()) {
				if (!child.canBeDeletedRecursive(objectsToCheck)) {
					return false;
				}
			}
		}

		return true;
	}

	// -------------------------------------------------------------------------
	// Accumulations of children (referencing objects)
	// -------------------------------------------------------------------------

	// Get accumulation set from accumulation field for object - ensure set is not null
	@SuppressWarnings("unchecked")
	public Set<DomainObject> getAccumulationSet(Field accuField) {

		Set<DomainObject> accumulationSet = (Set<DomainObject>) getFieldValue(accuField);
		if (accumulationSet == null) {
			accumulationSet = ConcurrentHashMap.newKeySet();
			setKnownFieldValue(accuField, accumulationSet);
		}

		return accumulationSet;
	}

	/**
	 * Update accumulations (if exist) of parent objects reflecting any reference change of this object.
	 */
	public synchronized void updateAccumulationsOfParentObjects() {

		if (refForAccuShadowMap == null) {
			return;
		}

		for (Entry<Field, DomainObject> entry : refForAccuShadowMap.entrySet()) {

			Field refField = entry.getKey();
			DomainObject newReferencedObj = (DomainObject) getFieldValue(refField);
			DomainObject oldReferencedObj = entry.getValue();

			if (!CBase.objectsEqual(newReferencedObj, oldReferencedObj)) {

				refForAccuShadowMap.put(refField, newReferencedObj);

				Field accuField = Registry.getAccumulationFieldForReferenceField(refField);

				if (oldReferencedObj != null && !oldReferencedObj.getAccumulationSet(accuField).remove(this)) {
					log.warn("DOB: Could not remove {} from accumulation {} of {} (was not contained in accumulation)", name(), Reflection.qualifiedName(accuField),
							DomainObject.name(oldReferencedObj));
				}

				if (newReferencedObj != null && !newReferencedObj.getAccumulationSet(accuField).add(this)) {
					log.warn("DOB: Could not add {} to accumulation {} of {} (was already contained in accumulation)", name(), Reflection.qualifiedName(accuField),
							DomainObject.name(newReferencedObj));
				}
			}
		}
	}

	// Remove object from accumulations (if exist) of parent objects
	protected synchronized void removeFromAccumulationsOfParentObjects() {

		if (refForAccuShadowMap == null) {
			return;
		}

		for (Entry<Field, DomainObject> entry : refForAccuShadowMap.entrySet()) {

			Field refField = entry.getKey();
			DomainObject referencedObj = entry.getValue();

			refForAccuShadowMap.put(refField, null);

			if (referencedObj != null) {

				Field accuField = Registry.getAccumulationFieldForReferenceField(refField);

				if (!referencedObj.getAccumulationSet(accuField).remove(this)) {
					log.warn("DOB: Could not remove {} from accumulation {} of {} (was not contained in accumulation)", name(), Reflection.qualifiedName(accuField), DomainObject.name(referencedObj));
				}
			}
		}
	}
}