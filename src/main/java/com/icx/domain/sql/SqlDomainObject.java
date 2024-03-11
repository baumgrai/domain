package com.icx.domain.sql;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.domain.DomainObject;
import com.icx.jdbc.SqlDbException;

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
	LocalDateTime lastModifiedInDb = null;

	// Is already saved to database?
	transient boolean isStored = false;

	/**
	 * Check if domain object is already stored to database (persisted).
	 * 
	 * @return true, if domain object is already stored, false otherwise
	 */
	public boolean isStored() {
		return isStored;
	}

	void setIsStored() {
		isStored = true;
	}

	/**
	 * Get associated SQL domain controller.
	 * 
	 * @return associated SQL domain controller
	 */
	public SqlDomainController sdc() {
		return (SqlDomainController) getDc();
	}

	// -------------------------------------------------------------------------
	// Field errors
	// -------------------------------------------------------------------------

	// SQL exception occurred during last try to save object
	transient Exception currentException = null;

	/**
	 * Get exception occurred on trying to save object or null.
	 * 
	 * @return exception, if exception occurred on trying to save object, null otherwise
	 */
	public Exception getCurrentException() {
		return currentException;
	}

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
	 * Check if domain object could be recently saved without errors (or if it is still not saved to database).
	 * 
	 * @return true if domain object was saved without critical errors or is still not saved to database, false otherwise ({@link SqlDomainController#save(java.sql.Connection, SqlDomainObject)})
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
	 * Get field errors and warnings for this object.
	 * 
	 * @return field errors
	 */
	public List<FieldError> getErrorsAndWarnings() {
		return new ArrayList<>(fieldErrorMap.values());
	}

	// -------------------------------------------------------------------------
	// Convenience methods
	// -------------------------------------------------------------------------

	/**
	 * Convenience method to delete object without throwing exception - see {@link SqlDomainController#delete(SqlDomainObject)}.
	 * <p>
	 * Potentially occurring SQL exception won't be thrown but will be logged with ERROR level.
	 * <p>
	 * May be overridden for specific domain classes.
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
	 * <p>
	 * Potentially occurring SQL exception won't be thrown but will be logged with ERROR level.
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

}
