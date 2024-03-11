package com.icx.domain;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CLog;
import com.icx.common.CReflection;
import com.icx.common.Common;
import com.icx.domain.sql.SqlDomainController;
import com.icx.domain.sql.SqlDomainObject;

/**
 * Base class for objects managed by a domain controller.
 * <p>
 * Any specific domain object class must extend this class. Objects managed by specific domain controllers may have a specific base class which extends this class (e.g.: {@link SqlDomainObject} for
 * {@link SqlDomainController}).
 * 
 * @author baumgrai
 */
public abstract class DomainObject extends Common implements Comparable<DomainObject> {

	static final Logger log = LoggerFactory.getLogger(DomainObject.class);

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// ID
	private long id = 0;

	/**
	 * Get object id.
	 * 
	 * @return object id
	 */
	public long getId() {
		return id;
	}

	/**
	 * Only for internal use!
	 * 
	 * @param id
	 *            object id
	 */
	public void setId(long id) {
		this.id = id;
	}

	// Associated domain controller
	private DomainController<? extends DomainObject> dc = null;

	/**
	 * Get domain controller which instantiated this object.
	 * 
	 * @return associated domain controller object
	 */
	public DomainController<? extends DomainObject> getDc() {
		return dc;
	}

	/**
	 * Only for internal use!
	 * 
	 * @param dc
	 *            domain controller object to associate
	 */
	public void setDc(DomainController<? extends DomainObject> dc) {
		this.dc = dc;
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
	 * Returns so called 'universal' object id - domainclassname@objectid - if not overridden by specific domain class
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return universalId();
	}

	/*
	 * Compares objects by 'universal' object id - &lt;domainclassname&gt;@&lt;objectid&gt; - if not overridden by specific domain class
	 */
	@Override
	public int compareTo(DomainObject o) {
		return compare(universalId(), o.universalId());
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

	/**
	 * Generates 'universal' object id: scheme: &lt;domainclassname&gt;@&lt;objectid&gt;.
	 * 
	 * @return universal object id
	 */
	public String universalId() {

		String name = (getClass().getSimpleName() + "@" + id);
		if (getClass().isMemberClass()) {
			name = getClass().getDeclaringClass().getSimpleName() + "." + name;
		}
		return name;
	}

	/**
	 * Identifier - 'universal object id' ({@link #universalId()} plus - if working and different - overridden {@code toString()} as 'logical' name for specific object.
	 * <p>
	 * Internally used for logging purposes.
	 * 
	 * @return "universalid[ (logicalname)]"
	 */
	public String name() {

		String name = universalId();
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

	/**
	 * Object's name {@link #name()} or "null" if object itself is null.
	 * 
	 * @param obj
	 *            domain object
	 * 
	 * @return object name or "null"
	 */
	public static final String name(DomainObject obj) {
		return (obj == null ? "null" : obj.name());
	}

	// -------------------------------------------------------------------------
	// Fields
	// -------------------------------------------------------------------------

	/**
	 * Only for internal use!
	 * 
	 * @param field
	 *            field to get value from
	 * 
	 * @return value
	 */
	// Get field value for object
	public Object getFieldValue(Field field) {
		try {
			return field.get(this);
		}
		catch (IllegalArgumentException | IllegalAccessException e) {
			log.error("DOB: {} '{}' occurred trying to get value of field '{}' for object {}", e.getClass().getSimpleName(), e.getMessage(), CReflection.qualifiedName(field), name());
			return null;
		}
	}

	/**
	 * Only for internal use!
	 * 
	 * @param field
	 *            field
	 * @param value
	 *            value
	 */
	// Set value of field
	public void setFieldValue(Field field, Object value) {
		try {
			field.set(this, value);
		}
		catch (IllegalArgumentException | IllegalAccessException e) {
			log.error("DC: {} '{}' oocurred trying to set field '{}' of object {} to {}", e.getClass().getSimpleName(), e.getMessage(), CReflection.qualifiedName(field), name(),
					CLog.forSecretLogging(field, value));
		}
	}

	// -------------------------------------------------------------------------
	// Deletion
	// -------------------------------------------------------------------------

	/**
	 * To override by specific domain class to check if object can be deleted.
	 * <p>
	 * If this method returns false for any object in a deletion process (object itself or any direct or indirect child) no object will be deleted at all.
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
	Set<DomainObject> getAccumulationSet(Field accuField) {

		Set<DomainObject> accumulationSet = (Set<DomainObject>) getFieldValue(accuField);
		if (accumulationSet == null) {
			accumulationSet = ConcurrentHashMap.newKeySet();
			setFieldValue(accuField, accumulationSet);
		}
		return accumulationSet;
	}

	// -------------------------------------------------------------------------
	// Convenience methods
	// -------------------------------------------------------------------------

	/**
	 * Convenience method to check if object is currently registered in object store.
	 * 
	 * @return true if object is registered, false if not
	 */
	public boolean isRegistered() {
		return dc.isRegistered(this);
	}
}
