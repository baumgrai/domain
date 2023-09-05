package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CBase;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;
import com.icx.dom.jdbc.SqlDbTable.UniqueConstraint;

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

	public FieldError(
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

	// Check if null value is provided for any column which has NOT NULL constraint
	public static boolean hasNotNullConstraintViolations(SqlDomainObject obj, Class<? extends DomainObject> domainClass) {

		boolean isConstraintViolated = false;
		for (Field field : Registry.getDataAndReferenceFields(domainClass)) {
			Column column = SqlRegistry.getColumnFor(field);

			if (!column.isNullable && obj.getFieldValue(field) == null) {

				log.error("SDO: \tField  '{}': associated with column '{}' has NOT NULL constraint but field value provided is null for object {}!", field.getName(), column.name,
						DomainObject.name(obj));

				obj.setFieldError(field, "NOT_NULL_CONSTRAINT_VIOLATION");
				isConstraintViolated = true;
			}
		}

		return isConstraintViolated;
	}

	// Check for UNIQUE constraint violation
	public static boolean hasUniqueConstraintViolations(SqlDomainObject obj, Class<? extends DomainObject> domainClass) {

		boolean isConstraintViolated = false;

		// Check if UNIQUE constraint is violated (UNIQUE constraints of single columns are also realized as table UNIQUE constraints)
		SqlDbTable table = SqlRegistry.getTableFor(domainClass);
		for (UniqueConstraint uc : table.uniqueConstraints) {

			// Build predicate to check combined uniqueness and build lists of involved fields and values
			Predicate<DomainObject> multipleUniqueColumnsPredicate = null;
			List<Field> combinedUniqueFields = new ArrayList<>();
			List<Object> fieldValues = new ArrayList<>();

			for (Column col : uc.columns) {
				for (Field fld : Registry.getDataAndReferenceFields(domainClass)) {
					if (CBase.objectsEqual(col, SqlRegistry.getColumnFor(fld))) {
						combinedUniqueFields.add(fld);
						Object fldValue = obj.getFieldValue(fld);
						fieldValues.add(fldValue);
						if (multipleUniqueColumnsPredicate == null) {
							multipleUniqueColumnsPredicate = o -> CBase.objectsEqual(o.getFieldValue(fld), fldValue);
						}
						else {
							multipleUniqueColumnsPredicate = multipleUniqueColumnsPredicate.and(o -> CBase.objectsEqual(o.getFieldValue(fld), fldValue));
						}
					}
				}
			}

			if (multipleUniqueColumnsPredicate != null && DomainController.count(obj.getClass(), multipleUniqueColumnsPredicate) > 1) {

				List<String> fieldNames = combinedUniqueFields.stream().map(Member::getName).collect(Collectors.toList());

				if (uc.columns.size() == 1) {
					log.error("SDO: \tColumn '{}' is UNIQUE by constraint '{}' but '{}' object '{}' already exists with same value '{}' of field '{}'!",
							uc.columns.stream().map(c -> c.name).collect(Collectors.toList()).get(0), uc.name, obj.getClass().getSimpleName(), DomainObject.name(obj), fieldValues.get(0),
							fieldNames.get(0));
				}
				else {
					log.error("SDO: \tColumns {} are UNIQUE together by constraint '{}' but '{}' object '{}'  already exists with same values {} of fields {}!",
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
	public static boolean hasColumnSizeViolations(SqlDomainObject obj, Class<? extends DomainObject> domainClass) {

		boolean isConstraintViolated = false;
		for (Field field : Registry.getDataAndReferenceFields(domainClass)) {
			Column column = SqlRegistry.getColumnFor(field);
			Object fieldValue = obj.getFieldValue(field);

			if ((fieldValue instanceof String || fieldValue instanceof Enum) && column.maxlen < fieldValue.toString().length()) {

				log.error("SDO: \tField  '{}': value '{}' is too long for associated column '{}' with maxlen {} for object {}!", field.getName(), fieldValue, column.name, column.maxlen,
						DomainObject.name(obj));

				obj.setFieldError(field, "COLUMN_SIZE_VIOLATION");
				isConstraintViolated = true;
			}
		}

		return isConstraintViolated;
	}

	/**
	 * Check constraint violations without involving database.
	 * <p>
	 * Assign field errors on fields for which constraints are violated.
	 * <p>
	 * Note: column size violation are detected here only for String fields which hold enums - values of text fields which are to long will be truncated and field warning is generated on saving.
	 * 
	 * @return true if any field constraint is violated, false otherwise
	 */
	public static boolean hasConstraintViolations(SqlDomainObject obj) {

		boolean isAnyConstraintViolated = false;
		for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(obj.getClass())) {

			isAnyConstraintViolated |= hasNotNullConstraintViolations(obj, domainClass);
			isAnyConstraintViolated |= hasUniqueConstraintViolations(obj, domainClass);
			isAnyConstraintViolated |= hasColumnSizeViolations(obj, domainClass);
		}

		return isAnyConstraintViolated;
	}

}
