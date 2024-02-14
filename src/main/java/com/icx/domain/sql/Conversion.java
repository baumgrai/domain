package com.icx.domain.sql;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.CLog;
import com.icx.common.base.Common;

/**
 * Conversion of column values retrieved from database to values of fields of domain objects. This type of conversion cannot be made within {@code SqlDb} because it needs the field type which is not
 * known there.
 * 
 * @author baumgrai
 */
public abstract class Conversion extends Common {

	static final Logger log = LoggerFactory.getLogger(Conversion.class);

	// Map containing registered from-string converters for classes
	protected static Map<Class<?>, Function<String, Object>> fromStringConverterMap = new HashMap<>();

	// Map containing valueOf(String) method or null for all classes which were once checked for having valueOf(String) declared
	private static Map<Class<?>, Method> valueOfStringMethodCacheMap = new HashMap<>();

	// Check if valueOf(String) method is defined and invoke this method to build object from string value
	@SuppressWarnings("unchecked")
	private static <T> T tryToBuildFieldValueFromStringValue(Class<? extends T> fieldType, String stringValue) {

		// Check for registered from-string converter
		if (fromStringConverterMap.containsKey(fieldType)) {

			// Convert string value by registered from-string converter
			if (log.isTraceEnabled()) {
				log.trace("SQL: Use registered from-string converter for class '{}'", fieldType.getSimpleName());
			}

			return (T) fromStringConverterMap.get(fieldType).apply(stringValue);
		}
		else {
			// Check for declared valueOf(String) method
			Method valueOfString = null;
			if (!valueOfStringMethodCacheMap.containsKey(fieldType)) {

				// Initially check if valueOf(String) method is declared for field type
				try {
					valueOfString = fieldType.getDeclaredMethod("valueOf", String.class);
					if (log.isDebugEnabled()) {
						log.debug("SQL: Found declared method valueOf(String) for class '{}'", fieldType.getSimpleName());
					}
					valueOfStringMethodCacheMap.put(fieldType, valueOfString);
				}
				catch (NoSuchMethodException nsmex) { // Field type does not have valueOf(String) method (initial check)
					valueOfStringMethodCacheMap.put(fieldType, null);
					return (T) stringValue;
				}
			}

			valueOfString = valueOfStringMethodCacheMap.get(fieldType);
			if (valueOfString != null) {

				// Convert String value to object using declared valueOf(String)
				try {
					if (log.isTraceEnabled()) {
						log.trace("SQL: Invoke declared method valueOf(String) for class '{}'", fieldType.getSimpleName());
					}
					return (T) valueOfString.invoke(null, stringValue);
				}
				catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException iex) {
					log.error("SQL: valueOf(String) method threw {}! Column's String value cannot be converted to '{}'!", iex, fieldType.getName());
				}
			}
		}

		log.error(
				"SQL: Column's string value cannot be converted to field type '{}' because no specific from-string conversion is defined! (neither a from-string converter function was registered nor valueOf(String) method is declared)",
				fieldType.getName());

		return null;
	}

	// Convert value retrieved from database to value for field
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <T> T column2FieldValue(Class<? extends T> fieldType, Object columnValue) {

		// Null checks
		if (columnValue == null) {
			return null;
		}
		else if (fieldType == null) {
			return (T) columnValue;
		}

		try {
			// Convert value based on field type
			if (fieldType == String.class) {
				return (T) (String) columnValue;
			}
			else if (Enum.class.isAssignableFrom(fieldType)) {
				return (T) Enum.valueOf((Class<? extends Enum>) fieldType, (String) columnValue);
			}
			else if (fieldType == Character.class || fieldType == char.class) {
				return (T) (Character) ((String) columnValue).charAt(0);
			}
			else if (fieldType == Short.class || fieldType == short.class) {
				return (T) Short.valueOf(((Number) columnValue).shortValue());
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
			else if (fieldType == Boolean.class || fieldType == boolean.class) {
				return (T) (columnValue instanceof Boolean ? columnValue : Boolean.valueOf((String) columnValue)); // JDBC getObject() may auto-convert 'true' and 'false' string to boolean
			}
			else if (fieldType == LocalDate.class) {
				if (columnValue instanceof LocalDateTime) {
					return (T) ((LocalDateTime) columnValue).toLocalDate();
				}
				else {
					return (T) columnValue;
				}
			}
			else if (fieldType == LocalTime.class) {
				if (columnValue instanceof LocalDateTime) {
					return (T) ((LocalDateTime) columnValue).toLocalTime();
				}
				else {
					return (T) columnValue;
				}
			}
			else if (File.class.isAssignableFrom(fieldType)) {
				return (T) (columnValue instanceof File ? columnValue : new File((String) columnValue)); // JDBC getObject() may auto-convert file path to File object
			}
			else {
				if (columnValue instanceof String) { // Try to convert value using either registered from-string converter or declared valueOf(String) method
					return tryToBuildFieldValueFromStringValue(fieldType, (String) columnValue);
				}
				else {
					return (T) columnValue;
				}
			}
		}
		catch (IllegalArgumentException iaex) {
			log.error("SQL: Column value {} cannot be converted to enum type '{}'! ({})", CLog.forAnalyticLogging(columnValue), ((Class<? extends Enum>) fieldType).getName(), iaex.getMessage());
		}
		catch (ClassCastException ccex) {
			log.error("SQL: Column value of type {} cannot be converted to '{}'! ({})", columnValue.getClass().getSimpleName(), fieldType.getName(), ccex);
		}

		return null;
	}
}
