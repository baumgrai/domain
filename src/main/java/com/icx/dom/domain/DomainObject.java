package com.icx.dom.domain;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Reflection;
import com.icx.common.base.CLog;
import com.icx.common.base.Common;

/**
 * Base class for objects managed by any domain controller.
 * <p>
 * Any specific domain object class must extend this class. Objects managed by specific domain controllers may have a specific base class which extends this class.
 * 
 * @author RainerBaumgärtel
 */
public abstract class DomainObject extends Common implements Comparable<DomainObject> {

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

	public void setId(long id) { // May not be called from applications - only used in SqlDomainController#selectForExclusiveUse()
		this.id = id;
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
		return defaultName();
	}

	// Default comparison - may be overridden
	@Override
	public int compareTo(DomainObject o) {
		return compare(id, o.id);
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

	// Default object name consisting of class name (preceded by outer class name on member classes) and obkect id
	private String defaultName() {

		String name = (getClass().getSimpleName() + "@" + id);
		if (getClass().isMemberClass()) {
			name = getClass().getDeclaringClass().getSimpleName() + "." + name;
		}
		return name;
	}

	// Identifier - domain object class name and object id plus - if working and different - toString() for object - used for logging
	public String name() {

		String name = defaultName();
		String logicalName;
		try {
			logicalName = toString();
		}
		catch (Exception ex) {
			logicalName = null;
		}

		if (logicalName != null && !objectsEqual(logicalName, name)) {
			name += " ('" + logicalName + "')";
		}
		return name;
	}

	public static final String name(DomainObject obj) {
		return (obj == null ? "null" : obj.name());
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
			log.error("DOB: {} '{}' occurred trying to get value of field '{}' for object {}", e.getClass().getSimpleName(), e.getMessage(), Reflection.qualifiedName(field), name());
			return null;
		}
	}

	// Set field value for object
	public void setFieldValue(Field field, Object value) {
		try {
			field.set(this, value);
		}
		catch (IllegalArgumentException | IllegalAccessException e) {
			log.error("DC: {} '{}' oocurred trying to set field '{}' of object {} to {}", e.getClass().getSimpleName(), e.getMessage(), Reflection.qualifiedName(field), name(),
					CLog.forAnalyticLogging(value));
		}
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

	// -------------------------------------------------------------------------
	// Accumulations of children (referencing objects)
	// -------------------------------------------------------------------------

	// Get accumulation set from accumulation field for object - ensure set is not null
	@SuppressWarnings("unchecked")
	public Set<DomainObject> getAccumulationSet(Field accuField) {

		Set<DomainObject> accumulationSet = (Set<DomainObject>) getFieldValue(accuField);
		if (accumulationSet == null) {
			accumulationSet = ConcurrentHashMap.newKeySet();
			setFieldValue(accuField, accumulationSet);
		}
		return accumulationSet;
	}
}
