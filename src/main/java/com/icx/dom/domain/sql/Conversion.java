package com.icx.dom.domain.sql;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.CLog;
import com.icx.common.base.Common;

/**
 * Conversion of column values retrieved from database to values of fields of domain objects
 * 
 * @author baumgrai
 */
public abstract class Conversion extends Common {

	static final Logger log = LoggerFactory.getLogger(Conversion.class);

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
			else if (fieldType == Integer.class || fieldType == int.class) {
				return (T) Integer.valueOf(((Number) columnValue).intValue());
			}
			else if (fieldType == Long.class || fieldType == long.class) {
				return (T) Long.valueOf(((Number) columnValue).longValue());
			}
			else if (fieldType == Double.class || fieldType == double.class) {
				return (T) Double.valueOf(((Number) columnValue).doubleValue());
			}
			else if (fieldType == BigInteger.class) {
				return (T) BigInteger.valueOf(((Number) columnValue).longValue());
			}
			else if (fieldType == BigDecimal.class) {

				Number number = (Number) columnValue;
				if (number.doubleValue() % 1.0 == 0 && number.longValue() < Long.MAX_VALUE) { // Avoid artifacts BigDecimal@4 -> BigDecimal@4.0
					return (T) BigDecimal.valueOf(number.longValue());
				}
				else {
					return (T) BigDecimal.valueOf(number.doubleValue());
				}
			}
			else if (Boolean.class.isAssignableFrom(fieldType) || boolean.class.isAssignableFrom(fieldType)) {
				return (T) (columnValue instanceof Boolean ? columnValue : Boolean.valueOf((String) columnValue)); // JDBC getObject() may auto-convert 'true' and 'false' string to boolean
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
