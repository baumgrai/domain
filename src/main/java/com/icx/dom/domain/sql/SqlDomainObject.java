package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.domain.DomainObject;
import com.icx.dom.jdbc.SqlDbException;

/**
 * Base class for objects managed by {@link SqlDomainController}. Includes methods for saving and deleting domain objects. Supports SQL error handling.
 * <p>
 * Any SQL persisted domain object must extend this class.
 * 
 * @author baumgrai
 */
public abstract class SqlDomainObject extends DomainObject {

	protected static final Logger log = LoggerFactory.getLogger(SqlDomainObject.class);

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Date of last modification
	public LocalDateTime lastModifiedInDb = null;

	// Is already saved to database?
	transient boolean isStored = false;

	public boolean isStored() {
		return isStored;
	}

	public void setIsStored() {
		isStored = true;
	}

	// Get associated SQL domain controller
	public SqlDomainController sdc() {
		return (SqlDomainController) dc;
	}

	// -------------------------------------------------------------------------
	// Field errors
	// -------------------------------------------------------------------------

	// SQL exception occurred during last try to save object
	public transient Exception currentException = null;

	// Field errors which prohibit object saving or warnings which force truncating field content on saving
	private transient Map<Field, FieldError> fieldErrorMap = new ConcurrentHashMap<>();

	void setFieldError(Field field, String message) {
		fieldErrorMap.put(field, new FieldError(true, this, field, message));
	}

	void setFieldWarning(Field field, String message) {
		fieldErrorMap.put(field, new FieldError(false, this, field, message));
	}

	void clearErrors() {
		currentException = null;
		fieldErrorMap.clear();
	}

	/**
	 * Check if domain object could be saved without critical errors (or is still not saved to database).
	 * 
	 * @return true if domain object was saved without critical errors or is still not saved to database, false otherwise
	 */
	public boolean isValid() {
		return (currentException == null && fieldErrorMap.values().stream().noneMatch(fe -> fe.isCritical));
	}

	/**
	 * Check if warnings or errors were generated during last saving of domain object.
	 * <p>
	 * A warning will be generated if size of string contained in a String field exceeds size of associated text column in database table and string therefore was truncated on saving.
	 * 
	 * @return true if any (String) field had to be truncated on last object saving, false otherwise
	 */
	public boolean hasErrorsOrWarnings() {
		return (!fieldErrorMap.isEmpty());
	}

	/**
	 * Get fields with errors or warnings.
	 * 
	 * @return field error map
	 */
	public Set<Field> getInvalidFields() {
		return fieldErrorMap.keySet();
	}

	/**
	 * Get field error (or warning) for given field.
	 * 
	 * @param field
	 *            field
	 * 
	 * @return field error object or null
	 */
	public FieldError getErrorOrWarning(Field field) {
		return fieldErrorMap.get(field);
	}

	/**
	 * Get field errors for this object.
	 * 
	 * @return field errors
	 */
	public List<FieldError> getErrorsAndWarnings() {
		return new ArrayList<>(fieldErrorMap.values());
	}

	// -------------------------------------------------------------------------
	// Convenience methods
	// -------------------------------------------------------------------------

	public boolean wasChangedLocally() {

		Map<Field, Object> fieldChangesMap = new HashMap<>();
		for (Class<? extends SqlDomainObject> domainClass : sdc().registry.getDomainClassesFor(this.getClass())) {
			fieldChangesMap.putAll(SaveHelpers.getFieldChangesForDomainClass(((SqlRegistry) sdc().registry), this, sdc().recordMap.get(this.getClass()).get(this.getId()), domainClass));
		}
		return !fieldChangesMap.isEmpty();
	}

	/**
	 * Convenience method to delete object without throwing exception - see {@link SqlDomainController#delete(SqlDomainObject)}.
	 * <p>
	 * Also to have the possibility to override delete method for specific domain classes.
	 * 
	 * @return true if deletion succeeded, false on exception or if any of the objects to delete is not deletable by {@link DomainObject#canBeDeleted()} check
	 */
	public boolean delete() {
		try {
			return sdc().delete(this);
		}
		catch (SQLException | SqlDbException e) {
			return false;
		}
	}

	/**
	 * Convenience method to save object to database without throwing exception - see {@link SqlDomainController#save(SqlDomainObject)}.
	 * 
	 * @return true if object's changes were saved to database, false if object was up-to-date
	 */
	public boolean save() {
		try {
			return sdc().save(this);
		}
		catch (SQLException | SqlDbException e) {
			return false;
		}
	}

	/**
	 * Convenience method to save object and all direct and indirect children without throwing exception see {@link SqlDomainController#saveIncludingChildren(SqlDomainObject)}.
	 */
	public void saveIncludingChildren() {

		for (SqlDomainObject child : ((SqlDomainController) dc).getDirectChildren(this)) {
			child.saveIncludingChildren();
		}

		save();
	}

}
