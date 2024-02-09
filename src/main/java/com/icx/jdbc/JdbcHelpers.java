package com.icx.jdbc;

import java.lang.reflect.Type;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.CLog;
import com.icx.common.base.Common;
import com.icx.jdbc.SqlDb.DbType;
import com.icx.jdbc.SqlDbTable.Column;

/**
 * Internal static JDBC/SQL database helpers, Java/JDBC type conversion
 * 
 * @author baumgrai
 */
public abstract class JdbcHelpers extends Common {

	private static final Logger log = LoggerFactory.getLogger(JdbcHelpers.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final int MAX_ORA_IDENT_SIZE = 30;
	public static final int MAX_MSSQL_IDENT_SIZE = 128;

	// -------------------------------------------------------------------------
	// String helpers
	// -------------------------------------------------------------------------

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
	// Logging helpers
	// -------------------------------------------------------------------------

	private static final String QUESTION_MARKS = "?????";
	private static final String QUESTION_MARKS_EXPRESSION = "\\?\\?\\?\\?\\?";

	// Extract table name from SQL statement and remove unnecessary spaces
	private static String normalizeAndExtractTableNameForInsertOrUpdateStatement(String stmt, StringBuilder sqlForLoggingBuilder) {

		String tableName = null;

		String[] words = stmt.split("\\s+");
		boolean nextWordIsTableName = false;
		for (int i = 0; i < words.length; i++) {

			sqlForLoggingBuilder.append(words[i] + " ");
			if (nextWordIsTableName) {
				tableName = words[i].toUpperCase();
				nextWordIsTableName = false;
			}
			else if (objectsEqual(words[i].toUpperCase(), "INTO") || objectsEqual(words[i].toUpperCase(), "UPDATE")) {
				nextWordIsTableName = true;
			}
		}

		return tableName;
	}

	// Build formatted SQL statement string from column names and column value map containing real values string representation instead of '?' place holders for logging - do not log secret information
	protected static String forSecretLoggingInsertUpdate(String stmt, Map<String, Object> columnValueMap, SortedSet<Column> columns) {

		// Extract table name from INSERT or UPDATE statement and remove unnecessary spaces
		StringBuilder sqlForLoggingBuilder = new StringBuilder();
		String tableName = normalizeAndExtractTableNameForInsertOrUpdateStatement(stmt, sqlForLoggingBuilder);
		String sqlForLogging = sqlForLoggingBuilder.toString().trim();

		// Mask '?' characters which are place holders to avoid confusion with '?' characters in texts
		sqlForLogging = sqlForLogging.replace("?", QUESTION_MARKS);

		// Select statement
		if (columnValueMap == null || columns == null) {
			return sqlForLogging;
		}

		// Insert/update statement - replace '?' place holders by values string representation
		for (Column column : columns) {
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
	private static List<String> extractColumnNamesForSelectStatement(String stmt) {

		List<String> columnNames = new ArrayList<>();

		String[] words = stmt.split("\\s+");
		boolean wordsAreColumnNames = false;
		for (int i = 0; i < words.length; i++) {

			if (objectsEqual(words[i], "FROM")) {
				break;
			}
			else if (objectsEqual(words[i], "SELECT")) {
				wordsAreColumnNames = true;
			}
			else if (wordsAreColumnNames && !objectsEqual(words[i], ",")) {
				columnNames.add(extractQualifiedColumnName(words[i]));
			}
		}

		return columnNames;
	}

	// Build formatted SQL statement string from values containing real values string representation instead of '?' place holders for logging
	protected static String forSecretLoggingSelect(String stmt, List<Object> values) {

		// Extract table name from SELECT statement and remove unnecessary spaces
		StringBuilder sqlForLoggingBuilder = new StringBuilder();
		String tableName = normalizeAndExtractTableNameForInsertOrUpdateStatement(stmt, sqlForLoggingBuilder);
		List<String> columnNames = extractColumnNamesForSelectStatement(stmt);
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
					sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, Matcher.quoteReplacement(CLog.forSecretLogging(tableName, columnNames.get(c), value)));
				}
			}

			c++;
		}

		return sqlForLogging;
	}

	// -------------------------------------------------------------------------
	// Retrieving and logging result records
	// -------------------------------------------------------------------------

	// Check if required type differs from default JDBC type of column
	private static boolean requiredTypeDiffers(ResultSetMetaData rsmd, List<Class<?>> requiredResultTypes, int c) throws SQLException {
		return (requiredResultTypes.size() > c && requiredResultTypes.get(c) != null && !objectsEqual(rsmd.getColumnClassName(c + 1), requiredResultTypes.get(c).getName()));
	}

	// Retrieve column values for one row from result set (of select statement or call of stored procedure)
	private static SortedMap<String, Object> retrieveRecord(ResultSet rs, ResultSetMetaData rsmd, List<Class<?>> requiredResultTypes) throws SQLException {

		SortedMap<String, Object> resultRecord = new TreeMap<>();

		if (rsmd == null) { // If no meta data provided (MySQL on stored procedure calls)
			boolean maxReached = false;
			for (int i = 0; !maxReached; i++) {
				try {
					resultRecord.put(String.format("%d", i + 1), rs.getObject(i + 1));
				}
				catch (SQLException sqlex) {
					maxReached = true;
				}
			}
			return resultRecord;
		}

		// Retrieve all values for one result record - JDBC indexes start by 1
		for (int c = 0; c < rsmd.getColumnCount(); c++) {

			// Get value retrieved - required types for CLOB is String and for BLOB is byte[] (see com.icx.dom.domain.sql.tools.Column), max 2GB characters or bytes is supported
			Object value = null;
			int columnType = rsmd.getColumnType(c + 1);
			if (columnType == Types.BLOB) {
				Blob blob = rs.getBlob(c + 1);
				if (blob != null) {
					value = blob.getBytes(1, ((Number) blob.length()).intValue());
				}
			}
			else if (columnType == Types.CLOB) {
				Clob clob = rs.getClob(c + 1);
				if (clob != null) {
					value = clob.getSubString(1, ((Number) clob.length()).intValue());
				}
			}
			else if (columnType == Types.TIMESTAMP || columnType == Types.TIMESTAMP_WITH_TIMEZONE) {
				value = rs.getObject(c + 1, LocalDateTime.class);
			}
			else if (columnType == Types.TIME || columnType == Types.TIME_WITH_TIMEZONE) {
				value = rs.getObject(c + 1, LocalTime.class);
			}
			else if (columnType == Types.DATE) {
				value = rs.getObject(c + 1, LocalDate.class);
			}
			else if (requiredResultTypes != null && requiredTypeDiffers(rsmd, requiredResultTypes, c)) {

				// Try to retrieve result as value of required type if required type is given and differs from columm's JDBC type
				// Attention: for numerical types (Integer, int, Long, long, ...) 0 will be returned on null value in database using getObject() with type specification! (at least using MySQL)
				String columnName = rsmd.getColumnName(c + 1);
				String columnClassName = rsmd.getColumnClassName(c + 1);
				try {
					value = rs.getObject(c + 1, requiredResultTypes.get(c));
					if (log.isTraceEnabled()) {
						log.trace("SQL: Column '{}': autoconverted value from column's JDBC type '{}' to required type {}", columnName, columnClassName, requiredResultTypes.get(c));
					}
				}
				catch (SQLException sqlex) {
					value = rs.getObject(c + 1);
					log.error("SQL: Exception '{}' on retrieving '{}' result in column '{}' as type '{}'!", sqlex.getMessage(), columnClassName, columnName, requiredResultTypes.get(c));
				}
			}
			else {
				value = rs.getObject(c + 1);
			}

			// Put column value retrieved into result map
			resultRecord.put(rsmd.getColumnLabel(c + 1).toUpperCase(), value);
		}

		return resultRecord;
	}

	// Controls if logs of SELECT methods contain (some of) selected records
	public static boolean listRecordsInLog = true;

	// Build and log result
	protected static List<SortedMap<String, Object>> retrieveResultRecords(ResultSet rs, List<Class<?>> requiredResultTypes) throws SQLException {

		// Retrieve result set metadata
		ResultSetMetaData rsmd = rs.getMetaData();
		if (rsmd == null) { // may happen on calling stored procedures using MySQL
			log.warn("SQL: No result set meta information available!");
		}

		// Retrieve results from result set
		List<SortedMap<String, Object>> resultMaps = new ArrayList<>();
		while (rs.next()) {
			resultMaps.add(retrieveRecord(rs, rsmd, requiredResultTypes));
		}

		if (resultMaps.isEmpty() && log.isDebugEnabled()) {
			log.debug("SQL: No records found");
		}

		return resultMaps;
	}

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

	// Build and log result
	protected static void logResultRecords(ResultSetMetaData rsmd, String stmt, List<SortedMap<String, Object>> resultRecords) throws SQLException {

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
