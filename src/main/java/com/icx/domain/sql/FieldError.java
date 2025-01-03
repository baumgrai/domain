package com.icx.domain.sql;

import java.lang.reflect.Field;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Error description which will be associated to a field of a specific domain object on problems saving or loading field data to/from database
 * 
 * @author baumgrai
 */
public class FieldError {

	static final Logger log = LoggerFactory.getLogger(FieldError.class);

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Domain object
	SqlDomainObject obj;

	// Field
	Field field;

	// Error or warning
	boolean isCritical = true;

	// Invalid field content
	Object invalidContent = null;

	// Error message on try to save object (from JDBC driver)
	String message = null;

	// Date of generation
	LocalDateTime generated = null;

	// -------------------------------------------------------------------------
	// Constructor
	// -------------------------------------------------------------------------

	FieldError(
			boolean isCritical,
			SqlDomainObject obj,
			Field field,
			String message) {

		this.obj = obj;
		this.field = field;
		this.isCritical = isCritical;
		this.invalidContent = obj.getFieldValue(field);
		this.message = message;
		this.generated = LocalDateTime.now();
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	@Override
	public String toString() {
		return (isCritical ? "ERROR" : "WARNING") + " for '" + obj + "' on field '" + field.getName() + "': " + message + " (invalid content: '" + invalidContent + "')";
	}

}
