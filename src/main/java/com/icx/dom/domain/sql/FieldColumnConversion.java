package com.icx.dom.domain.sql;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Reflection;
import com.icx.common.base.CLog;
import com.icx.common.base.Common;

/**
 * Field/column type and value conversion
 * 
 * @author baumgrai
 */
public abstract class FieldColumnConversion extends Common {

	static final Logger log = LoggerFactory.getLogger(FieldColumnConversion.class);

	// Determine required JDBC type for field type - will be used on SELECT's to
	static Class<?> requiredJdbcTypeFor(Class<?> fieldClass) {

		if (fieldClass == BigInteger.class) {
			return Long.class;
		}
		else if (fieldClass == BigDecimal.class) {
			return Double.class;
		}
		else if (Enum.class.isAssignableFrom(fieldClass)) {
			return String.class;
		}
		else if (File.class.isAssignableFrom(fieldClass)) {
			return String.class;
		}
		else {
			return Reflection.getBoxingWrapperType(fieldClass);
		}
	}

	// Convert field value to value to store in database
	static Object field2ColumnValue(Object fieldValue) {

		if (fieldValue instanceof Enum) {
			return fieldValue.toString();
		}
		else if (fieldValue instanceof BigInteger) {
			return ((BigInteger) fieldValue).longValue();
		}
		else if (fieldValue instanceof BigDecimal) {
			return ((BigDecimal) fieldValue).doubleValue();
		}
		else if (fieldValue instanceof File) {
			return ((File) fieldValue).getPath();
		}
		else {
			return fieldValue;
		}
	}

	// Convert value retrieved from database to value for field
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T> T column2FieldValue(Class<? extends T> fieldType, Object columnValue) {

		if (fieldType == null) {
			return (T) columnValue;
		}

		try {
			if (columnValue == null) {
				return null;
			}
			else if (Enum.class.isAssignableFrom(fieldType)) {
				return (T) Enum.valueOf((Class<? extends Enum>) fieldType, (String) columnValue);
			}
			else if (fieldType == BigInteger.class) {
				return (T) BigInteger.valueOf((long) columnValue);
			}
			else if (fieldType == BigDecimal.class) {

				Double d = (double) columnValue;
				if (d % 1.0 == 0 && d < Long.MAX_VALUE) { // Avoid artifacts BigDecimal@4 -> BigDecimal@4.0
					return (T) BigDecimal.valueOf(d.longValue());
				}
				else {
					return (T) BigDecimal.valueOf(d);
				}
			}
			else if (File.class.isAssignableFrom(fieldType)) {
				return (T) new File((String) columnValue);
			}
			else {
				return (T) columnValue;
			}
		}
		catch (IllegalArgumentException iaex) {
			log.error("SQL: Column value {} cannot be converted to enum type '{}'! ({})", CLog.forAnalyticLogging(columnValue), ((Class<? extends Enum>) fieldType).getName(), iaex.getMessage());
		}
		catch (ClassCastException ex) {
			log.error("SQL: Column value {} cannot be converted to  '{}'! ({})", CLog.forAnalyticLogging(columnValue), fieldType.getName(), ex);
		}

		return null;
	}
}
