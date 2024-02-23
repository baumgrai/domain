package com.icx.jdbc;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Reflection;
import com.icx.common.base.CLog;
import com.icx.common.base.Common;
import com.icx.jdbc.SqlDb.DbType;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;

/**
 * Internal static JDBC/SQL database helpers, Java/JDBC type conversion
 * 
 * @author baumgrai
 */
public abstract class SqlDbHelpers extends Common {

	private static final Logger log = LoggerFactory.getLogger(SqlDbHelpers.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final int MAX_ORA_IDENT_SIZE = 30;
	public static final int MAX_MSSQL_IDENT_SIZE = 128;

	// -------------------------------------------------------------------------
	// String helpers
	// -------------------------------------------------------------------------

	public static boolean isBasicType(Class<?> cls) {
		return (cls == String.class || cls == char.class || cls == Character.class || cls == Boolean.class || cls == boolean.class
				|| Number.class.isAssignableFrom(Reflection.getBoxingWrapperType(cls)) || Enum.class.isAssignableFrom(cls) || cls == LocalDateTime.class || cls == LocalDate.class
				|| cls == LocalTime.class || cls == Date.class || cls == File.class);
	}

	// Get Java type string (unqualified on 'java.lang' and domain object classes, qualified otherwise)
	public static String getTypeString(Class<?> type) {
		return (type.getName().startsWith("java.lang.") || com.icx.domain.DomainObject.class.isAssignableFrom(type) ? type.getSimpleName() : type.getName());
	}

	// Get string value from object if object is of type string
	protected static String valueToTrimmedString(Object value) {

		if (value == null || !value.getClass().equals(String.class)) {
			return "";
		}

		return value.toString().trim();
	}

	// Check if a value is a SQL function call
	protected static boolean isSqlFunctionCall(Object value) {

		if (value == null) {
			return false;
		}

		return (valueToTrimmedString(value).startsWith("SQL:"));
	}

	// Get SQL function call from value
	protected static String getSqlColumnExprOrFunctionCall(Object value) {
		return valueToTrimmedString(value).substring("SQL:".length());
	}

	// Check if a value is an Oracle sequence nextval expression
	protected static boolean isOraSeqNextvalExpr(Object value) {

		if (value == null) {
			return false;
		}

		return (valueToTrimmedString(value).toUpperCase().endsWith(SqlDb.ORACLE_SEQ_NEXTVAL));
	}

	// Get sequence from Oracle sequence nextval expression
	protected static String getOraSeqName(Object value) {

		String s = valueToTrimmedString(value).toUpperCase();
		return s.substring(0, s.length() - SqlDb.ORACLE_SEQ_NEXTVAL.length());
	}

	// -------------------------------------------------------------------------
	// Helpers for specific database types
	// -------------------------------------------------------------------------

	// Cut identifier to maximum length for database type
	public static String identifier(String name, DbType dbType) {
		return name.substring(0, min(name.length(), (dbType == DbType.ORACLE ? MAX_ORA_IDENT_SIZE : MAX_MSSQL_IDENT_SIZE)));
	}

	// -------------------------------------------------------------------------
	// String conversion
	// -------------------------------------------------------------------------

	// Map containing to-string converters for storing values
	static Map<Class<?>, Function<Object, String>> toStringConverterMap = new HashMap<>();

	public static void addToStringConverter(Class<?> cls, Function<Object, String> toStringConverter) {
		toStringConverterMap.put(cls, toStringConverter);
	}

	// Map containing to-string converter or null for all classes which were once checked for having to-string converter
	private static Map<Class<?>, Method> toStringMethodCacheMap = new HashMap<>();

	// If column value is not of basic type, check for registered to-string converter and, if not found, for declared toString() method - cache things found
	protected static String tryToBuildStringValueFromColumnValue(Object columnValue) {

		Class<?> objectClass = columnValue.getClass();

		Function<Object, String> toStringConverter = toStringConverterMap.get(objectClass);
		if (toStringConverter != null) {

			// Store value as string computed by registered to-string converter
			if (log.isTraceEnabled()) {
				log.trace("SQL: Use registered to-string converter for class '{}'", objectClass.getSimpleName());
			}

			return toStringConverter.apply(columnValue);
		}
		else {
			// Check for declared toString() method
			if (!toStringMethodCacheMap.containsKey(objectClass)) {

				// Initially check if toString() method is declared and cache method in this case
				try {
					Method toString = objectClass.getDeclaredMethod("toString");
					if (log.isDebugEnabled()) {
						log.debug("SQL: Found declared toString() method for class '{}'", objectClass.getSimpleName());
					}
					toStringMethodCacheMap.put(objectClass, toString);
				}
				catch (NoSuchMethodException nsmex) { // Field type does not have toString() method (initial check)
					toStringMethodCacheMap.put(objectClass, null);
				}
			}

			Method toString = toStringMethodCacheMap.get(objectClass);
			if (toString != null) {

				// Store value as string computed by declared toString() method
				if (log.isTraceEnabled()) {
					log.trace("SQL: Invoke declared method toString() for class '{}'", objectClass.getSimpleName());
				}
				try {
					return (String) toString.invoke(columnValue);
				}
				catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException iex) {
					log.error("SQL: toString() method threw {}! Field value of type '{}' cannot be converted to String!", iex, objectClass.getName());
				}
			}
		}

		return null;
	}

	// Map containing registered from-string converters for classes
	static Map<Class<?>, Function<String, Object>> fromStringConverterMap = new HashMap<>();

	public static void addFromStringConverter(Class<?> cls, Function<String, Object> fromStringConverter) {
		fromStringConverterMap.put(cls, fromStringConverter);
	}

	// Map containing valueOf(String) method or null for all classes which were once checked for having valueOf(String) declared
	private static Map<Class<?>, Method> valueOfStringMethodCacheMap = new HashMap<>();

	// Check if valueOf(String) method is defined and invoke this method to build object from string value
	@SuppressWarnings("unchecked")
	protected static <T> T tryToBuildFieldValueFromStringValue(Class<? extends T> fieldType, String stringValue, String columnName) {

		// Check for registered from-string converter
		if (fromStringConverterMap.containsKey(fieldType)) {

			// Convert string value by registered from-string converter
			if (log.isTraceEnabled()) {
				log.trace("SQL: Convert string to object using registered from-string converter for class '{}'", fieldType.getSimpleName());
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
						log.trace("SQL: Convert string to object: Invoke declared method valueOf(String) for class '{}'", fieldType.getSimpleName());
					}
					return (T) valueOfString.invoke(null, stringValue);
				}
				catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException iex) {
					log.error("SQL: valueOf(String) method threw {}! Column's String value cannot be converted to '{}'!", iex, fieldType.getName());
				}
			}
		}

		log.error(
				"SQL: String value of column '{}' cannot be converted to field type '{}' because no specific from-string conversion is defined! (neither a from-string converter function was registered nor valueOf(String) method is declared)",
				columnName, fieldType.getName());

		return null;
	}

	// -------------------------------------------------------------------------
	// Logging helpers
	// -------------------------------------------------------------------------

	private static final String QUESTION_MARKS = "?????";
	private static final String QUESTION_MARKS_EXPRESSION = "\\?\\?\\?\\?\\?";

	// Extract table name from SQL statement and remove unnecessary spaces
	protected static String normalizeAndExtractTableName(String sql, StringBuilder sqlForLoggingBuilder) {

		String tableName = null;

		String[] words = sql.split("\\s+");
		boolean nextWordIsTableName = false;
		for (int i = 0; i < words.length; i++) {

			if (sqlForLoggingBuilder != null) {
				sqlForLoggingBuilder.append(words[i] + " ");
			}

			if (nextWordIsTableName) {
				tableName = words[i].toUpperCase();
				nextWordIsTableName = false;
			}
			else if (objectsEqual(words[i].toUpperCase(), "FROM") || objectsEqual(words[i].toUpperCase(), "INTO") || objectsEqual(words[i].toUpperCase(), "UPDATE")) {
				nextWordIsTableName = true;
			}
		}

		return tableName;
	}

	// Build formatted SQL statement string from column names and column value map containing real values string representation instead of '?' place holders for logging - do not log secret information
	protected static String forSecretLoggingInsertUpdate(String sql, Map<String, Object> columnValueMap, SortedSet<SqlDbColumn> columns) {

		// Extract table name from INSERT or UPDATE statement and remove unnecessary spaces
		StringBuilder sqlForLoggingBuilder = new StringBuilder();
		String tableName = normalizeAndExtractTableName(sql, sqlForLoggingBuilder);
		String sqlForLogging = sqlForLoggingBuilder.toString().trim();

		// Mask '?' characters which are place holders to avoid confusion with '?' characters in texts
		sqlForLogging = sqlForLogging.replace("?", QUESTION_MARKS);

		// Select statement
		if (columnValueMap == null || columns == null) {
			return sqlForLogging;
		}

		// Insert/update statement - replace '?' place holders by values string representation
		for (SqlDbColumn column : columns) {
			Object value = columnValueMap.get(column.name);

			if (isSqlFunctionCall(value) || isOraSeqNextvalExpr(value)) {
				continue;
			}

			sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, Matcher.quoteReplacement(CLog.forSecretLogging(tableName, column.name, value)));
		}

		return sqlForLogging;
	}

	// Return column name only from expression potentially containing bracket or comma
	private static String extractQualifiedColumnName(String rawColumnName) {
		return rawColumnName.replace(",", "").toUpperCase();
	}

	// Extract column names from SELECT statement
	protected static List<String> extractColumnNamesForSelectStatement(String sql) {

		List<String> columnNames = new ArrayList<>();

		String[] words = sql.split("\\s+");
		boolean wordsAreColumnNames = false;
		for (int i = 0; i < words.length; i++) {

			if (objectsEqual(words[i], "FROM")) {
				break;
			}
			else if (objectsEqual(words[i], "SELECT")) {
				wordsAreColumnNames = true;
			}
			else if (wordsAreColumnNames && !objectsEqual(words[i], ",") && !objectsEqual(words[i], "TOP") && !Common.isInteger(words[i])) {
				columnNames.add(extractQualifiedColumnName(words[i]));
			}
		}

		return columnNames;
	}

	// Build formatted SQL statement string from values containing real values string representation instead of '?' place holders for logging
	protected static String forSecretLoggingSelect(String sql, List<Object> values, List<String> outColumnNames) {

		// Extract table name from SELECT statement and remove unnecessary spaces
		StringBuilder sqlForLoggingBuilder = new StringBuilder();
		String tableName = normalizeAndExtractTableName(sql, sqlForLoggingBuilder);
		List<String> columnNames = extractColumnNamesForSelectStatement(sql);
		if (outColumnNames != null) {
			outColumnNames.addAll(columnNames);
		}

		String sqlForLogging = sqlForLoggingBuilder.toString().trim();

		// Mask '?' characters which are place holders to avoid confusion with '?' characters in values
		sqlForLogging = sqlForLogging.replace("?", QUESTION_MARKS);

		// Select statement
		if (values == null) {
			return sqlForLogging;
		}

		// Replace '?' place holders by values string representation
		int c = 0;
		for (Object value : values) {

			if (!isSqlFunctionCall(value) && !isOraSeqNextvalExpr(value)) {
				if (value instanceof Type) {
					sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, "?");
				}
				else {
					sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, Matcher.quoteReplacement(CLog.forSecretLogging(tableName, behindFirst(columnNames.get(c), "."), value)));
				}
			}

			c++;
		}

		return sqlForLogging;
	}

	// -------------------------------------------------------------------------
	// Retrieving and logging result records
	// -------------------------------------------------------------------------

	// Log result map (one record) in order of column declaration.
	public static String forSecretLoggingRecord(SortedMap<String, Object> map, List<String> columnNames, Map<String, String> columnTableMap) {

		StringBuilder resultMapStringBuilder = new StringBuilder();

		// Result record entries for column names provided
		if (columnNames != null) {
			for (int c = 0; c < columnNames.size(); c++) {
				String columnName = columnNames.get(c);
				if (map.containsKey(columnName)) {
					resultMapStringBuilder.append(resultMapStringBuilder.length() == 0 ? "{ " : ", ");
					resultMapStringBuilder.append(columnName + "=" + CLog.forSecretLogging(columnTableMap.get(columnName), columnName, map.get(columnName)));
				}
			}
		}

		// Result record entries where key does not match any of the given column names
		for (Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			if (columnNames == null || !columnNames.contains(key)) {
				resultMapStringBuilder.append(resultMapStringBuilder.length() == 0 ? "{ " : ", ");
				resultMapStringBuilder.append(key + "=" + CLog.forSecretLogging(null, key, entry.getValue()));
			}
		}

		if (resultMapStringBuilder.length() != 0) {
			resultMapStringBuilder.append(" }");
		}

		return resultMapStringBuilder.toString();
	}

	// Controls if logs of SELECT methods contain (some of) selected records
	public static boolean listRecordsInLog = true;

	// Log result
	protected static void logResultRecordsOnDebugLevel(ResultSetMetaData rsmd, String stmt, List<SortedMap<String, Object>> resultRecords) throws SQLException {

		if (!log.isDebugEnabled()) {
			return;
		}

		if (resultRecords.isEmpty()) {
			log.debug("SQL: No records found");
			return;
		}

		// Build ordered lists of column and table names to check secret condition in logging results
		List<String> columnNamesFromStatment = extractColumnNamesForSelectStatement(stmt);
		List<String> orderedColumnNames = new ArrayList<>();
		Map<String, String> columnTableMap = new HashMap<>();
		for (int c = 1; c <= rsmd.getColumnCount(); c++) {

			String columnName = rsmd.getColumnName(c).toUpperCase();
			String tableName = rsmd.getTableName(c).toUpperCase(); // Seems not to work for Oracle and MS/SQL!
			if (isEmpty(tableName)) {
				tableName = untilFirst(columnNamesFromStatment.stream().filter(cn -> cn.contains(columnName)).findAny().orElse(""), ".");
			}

			columnTableMap.put(columnName, tableName);
			orderedColumnNames.add(columnName);
		}

		if (resultRecords.size() == 1 && rsmd.getColumnCount() == 1 && isEmpty(rsmd.getColumnName(1))) { // count(*)
			log.debug("SQL: Retrieved: {}", resultRecords.iterator().next().values().iterator().next());
			return;
		}

		Set<String> tableNames = new HashSet<>(columnTableMap.values());
		log.debug("SQL: {} record(s) retrieved{}", resultRecords.size(), (!tableNames.isEmpty() ? " from " + (tableNames.size() == 1 ? tableNames.iterator().next() : tableNames) : ""));

		if (listRecordsInLog) {

			// Log result records
			int recordCount = 0;
			for (SortedMap<String, Object> resultRecord : resultRecords) {
				if (recordCount < 32 || log.isTraceEnabled()) {
					log.debug("SQL: \t{}", forSecretLoggingRecord(resultRecord, orderedColumnNames, columnTableMap));
				}
				else if (recordCount == 32) {
					log.debug("SQL: \t<further records>...");
				}
				else {
					break;
				}
				recordCount++;
			}
		}
	}

}
