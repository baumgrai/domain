package com.icx.dom.domain.sql;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Reflection;
import com.icx.common.base.CLog;
import com.icx.common.base.Common;

/**
 * Field/column conversion and general helpers
 * 
 * @author baumgrai
 */
public abstract class Helpers extends Common {

	static final Logger log = LoggerFactory.getLogger(Helpers.class);

	// -------------------------------------------------------------------------
	// Conversion from/to field to/from column value
	// -------------------------------------------------------------------------

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

	// -------------------------------------------------------------------------
	// General helpers
	// -------------------------------------------------------------------------

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

	// Count new and changed objects grouped by object domain classes (for logging only)
	static <T extends SqlDomainObject> Set<Entry<String, Integer>> groupCountsByDomainClassName(Set<T> objects) {

		return objects.stream().collect(Collectors.groupingBy(Object::getClass)).entrySet().stream().map(e -> new SimpleEntry<>(e.getKey().getSimpleName(), e.getValue().size()))
				.collect(Collectors.toSet());
	}

	// Build "(<ids>)" lists with at maximum 1000 ids (Oracle limit for # of elements in WHERE IN (...) clause = 1000)
	static List<String> buildMax1000IdsLists(Set<Long> ids) {

		List<String> idStringLists = new ArrayList<>();
		if (ids == null || ids.isEmpty()) {
			return idStringLists;
		}

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (long id : ids) {

			if (i % 1000 != 0) {
				sb.append(",");
			}

			sb.append(id);

			if (i % 1000 == 999) {
				idStringLists.add(sb.toString());
				sb.setLength(0);
			}

			i++;
		}

		if (sb.length() > 0) {
			idStringLists.add(sb.toString());
		}

		return idStringLists;
	}

	// Build string list of elements for WHERE clause (of DELETE statement)
	static String buildElementList(Set<Object> elements) {

		StringBuilder sb = new StringBuilder();
		sb.append("(");

		for (Object element : elements) {

			element = Helpers.field2ColumnValue(element);
			if (element instanceof String) {
				sb.append("'" + element + "'");
			}
			else {
				sb.append(element);
			}

			sb.append(",");
		}

		sb.replace(sb.length() - 1, sb.length(), ")");

		return sb.toString();
	}

}
