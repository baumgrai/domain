package com.icx.dom.jdbc;

import java.lang.reflect.Type;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
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

import com.icx.dom.common.CBase;
import com.icx.dom.common.CDateTime;
import com.icx.dom.common.CLog;
import com.icx.dom.jdbc.SqlDb.DbType;
import com.icx.dom.jdbc.SqlDbTable.Column;

/**
 * Internal static JDBC/SQL database helpers, Java/JDBC type conversion
 * 
 * @author baumgrai
 */
public abstract class JdbcHelpers {

	private static final Logger log = LoggerFactory.getLogger(JdbcHelpers.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final int MAX_ORA_IDENT_SIZE = 30;
	public static final int MAX_MSSQL_IDENT_SIZE = 128;

	// -------------------------------------------------------------------------
	// String helpers
	// -------------------------------------------------------------------------

	// Get Java type string (unqualified on 'java.lang', qualified otherwise)
	public static String getTypeString(Class<?> type) {
		return (type.getName().startsWith("java.lang.") || com.icx.dom.domain.DomainObject.class.isAssignableFrom(type) ? type.getSimpleName() : type.getName());
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
		return name.substring(0, CBase.min(name.length(), (dbType == DbType.ORACLE ? MAX_ORA_IDENT_SIZE : MAX_MSSQL_IDENT_SIZE)));
	}

	// -------------------------------------------------------------------------
	// Logging helpers
	// -------------------------------------------------------------------------

	// Log SQL value
	public static String forLoggingSql(String key, Object value) {

		if (value instanceof String && CLog.isSecret(key)) {
			return CLog.greyOut((String) value);
		}

		String logString = null;

		if (value == null) {
			logString = "NULL";
		}
		else if (value instanceof String) {
			logString = "'" + value.toString() + "'";
		}
		else if (value instanceof java.sql.Timestamp) {
			logString = "'" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format(((java.sql.Timestamp) value)) + "'";
		}
		else if (value instanceof oracle.sql.TIMESTAMP) {
			try {
				logString = "'" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format(((oracle.sql.TIMESTAMP) value).timestampValue()) + "'";
			}
			catch (SQLException e) {
				logString = "'" + value.toString() + "'";
			}
		}
		else if (value instanceof Calendar) {
			logString = "'" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format(((Calendar) value).getTime()) + "'";
		}
		else if (value instanceof LocalDate) {
			logString = "'" + ((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE) + "'";
		}
		else if (value instanceof LocalTime) {
			logString = "'" + ((LocalTime) value).format(DateTimeFormatter.ISO_LOCAL_TIME) + "'";
		}
		else if (value instanceof LocalDateTime) {
			logString = "'" + ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + "'";
		}
		else if (value instanceof Boolean) {
			logString = value.toString();
		}
		else {
			logString = value.toString();
		}

		return logString.substring(0, CBase.min(1024, logString.length()));
	}

	private static final String QUESTION_MARKS = "?????";
	private static final String QUESTION_MARKS_EXPRESSION = "\\?\\?\\?\\?\\?";

	// Build formatted SQL statement string from column names and column value map containing real values string representation instead of '?' place holders for
	// logging - do not log passwords
	protected static String forLoggingSql(String stmt, Map<String, Object> columnValueMap, SortedSet<Column> columns) {

		if (stmt == null) {
			return "(null)";
		}

		// Remove unnecessary spaces
		StringBuilder sqlForLoggingBuilder = new StringBuilder();
		String[] words = stmt.split("\\s+");
		for (int i = 0; i < words.length; i++) {
			sqlForLoggingBuilder.append(words[i] + " ");
		}
		String sqlForLogging = sqlForLoggingBuilder.toString().trim();

		// Mask '?' characters which are place holders to avoid confusion with '?' characters in texts
		sqlForLogging = sqlForLogging.replaceAll("\\?", QUESTION_MARKS);

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

			sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, Matcher.quoteReplacement(forLoggingSql(column.name, value)));
		}

		return sqlForLogging;
	}

	// Build formatted SQL statement string from values containing real values string representation instead of '?' place holders for logging
	protected static String forLoggingSql(String stmt, List<Object> values) {

		if (stmt == null) {
			return "(null)";
		}

		// Remove unnecessary spaces
		StringBuilder sqlForLoggingBuilder = new StringBuilder();
		String[] words = stmt.split("\\s+");
		for (int i = 0; i < words.length; i++) {
			sqlForLoggingBuilder.append(words[i] + " ");
		}
		String sqlForLogging = sqlForLoggingBuilder.toString().trim();

		// Mask '?' characters which are place holders to avoid confusion with '?' characters in texts
		sqlForLogging = sqlForLogging.replaceAll("\\?", QUESTION_MARKS);

		// Select statement
		if (values == null) {
			return sqlForLogging;
		}

		// Insert/update statement - replace '?' place holders by values string representation
		for (Object value : values) {

			if (isSqlFunctionCall(value) || isOraSeqNextvalExpr(value)) {
				continue;
			}

			if (value instanceof Type) {
				sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, "?");
			}
			else {
				sqlForLogging = sqlForLogging.replaceFirst(QUESTION_MARKS_EXPRESSION, Matcher.quoteReplacement(forLoggingSql(null, value)));
			}
		}

		return sqlForLogging;
	}

	private static void buildResultMapEntry(StringBuilder resultMapStringBuilder, String key, Object value) {

		if (resultMapStringBuilder.length() == 0) {
			resultMapStringBuilder.append("{ ");
		}
		else {
			resultMapStringBuilder.append(", ");
		}

		resultMapStringBuilder.append(key);
		resultMapStringBuilder.append("=");
		resultMapStringBuilder.append(CLog.forSecretLogging(key, value));
	}

	// Log result map in order of column declaration - do not log passwords
	public static String forLoggingSqlResult(SortedMap<String, Object> map, List<String> columnNames) {

		if (map == null) {
			return "(null)";
		}

		StringBuilder resultMapStringBuilder = new StringBuilder();

		// Ordered columns
		if (columnNames != null) {
			for (String columnName : columnNames) {
				if (map.containsKey(columnName)) {
					buildResultMapEntry(resultMapStringBuilder, columnName, map.get(columnName));
				}
			}
		}

		// Additional entries
		for (Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			if (columnNames == null || !columnNames.contains(key)) {
				buildResultMapEntry(resultMapStringBuilder, key, entry.getValue());
			}
		}

		if (resultMapStringBuilder.length() != 0) {
			resultMapStringBuilder.append(" }");
		}

		return resultMapStringBuilder.toString();
	}

	// -------------------------------------------------------------------------
	// Result set helpers
	// -------------------------------------------------------------------------

	// Get ordered columns from select result set
	private static List<String> getOrderedResultColumnNames(ResultSetMetaData rsmd) throws SQLException {

		List<String> orderedColumns = new ArrayList<>();
		for (int i = 1; i <= rsmd.getColumnCount(); i++) {

			String columnName = rsmd.getColumnName(i);
			if (!CBase.isEmpty(columnName)) {
				orderedColumns.add(columnName.toUpperCase());
			}
			else {
				orderedColumns.add(String.valueOf(i) + ".");
			}
		}

		return orderedColumns;
	}

	// Check if required type differs from default JDBC type of column
	private static boolean requiredTypeDiffers(ResultSetMetaData rsmd, List<Class<?>> requiredResultTypes, int c) throws SQLException {

		return (requiredResultTypes != null && requiredResultTypes.size() > c && requiredResultTypes.get(c) != null
				&& !CBase.objectsEqual(rsmd.getColumnClassName(c + 1), requiredResultTypes.get(c).getName()));
	}

	// Retrieve column values for one row from result set (of select statement or call of stored procedure)
	private static SortedMap<String, Object> retrieveValues(ResultSet rs, ResultSetMetaData rsmd, List<Class<?>> requiredResultTypes) throws SQLException {

		SortedMap<String, Object> row = new TreeMap<>();

		// If no meta data provided (MySQL on stored procedure calls)
		if (rsmd == null) {
			boolean maxReached = false;
			for (int i = 0; !maxReached; i++) {
				try {
					row.put(String.format("%d", i + 1), rs.getObject(i + 1));
				}
				catch (SQLException sqlex) {
					maxReached = true;
				}
			}
		}
		else {
			// Retrieve all values for one result record - JDBC indexes start by 1
			for (int c = 0; c < rsmd.getColumnCount(); c++) {

				// Get value retrieved - required types for CLOB is String and for BLOB is byte[] (see com.icx.dom.domain.sql.tools.Column), max 2GB characters or bytes is supported
				Object value = null;
				if (rsmd.getColumnType(c + 1) == Types.BLOB) {
					Blob blob = rs.getBlob(c + 1);
					if (blob != null) {
						value = blob.getBytes(1, ((Number) blob.length()).intValue());
					}
				}
				else if (rsmd.getColumnType(c + 1) == Types.CLOB) {
					Clob clob = rs.getClob(c + 1);
					if (clob != null) {
						value = clob.getSubString(1, ((Number) clob.length()).intValue());
					}
				}
				else if (requiredTypeDiffers(rsmd, requiredResultTypes, c)) {

					// Try to retrieve result as value of required type if required type is given and differs from columm's JDBC type
					// Attention: for numerical types (Integer, int, Long, long, ...) 0 will be returned on null value in database using getObject() with type specification! (at least using MySQL)
					try {
						value = rs.getObject(c + 1, requiredResultTypes.get(c));
					}
					catch (SQLException sqlex) {
						value = rs.getObject(c + 1);
						log.error("SQL: Exception '{}' on retrieving result in column '{}' as type '{}'! Value: {}", sqlex.getMessage(), rsmd.getColumnName(c + 1), requiredResultTypes.get(c),
								CLog.forAnalyticLogging(value));
					}
				}
				else {
					value = rs.getObject(c + 1);
				}

				// Put column value retrieved into result map
				row.put(rsmd.getColumnLabel(c + 1).toUpperCase(), value);
			}
		}

		return row;
	}

	// Controls if logs of SELECT methods contain (some of) selected records
	public static boolean listRecordsInLog = true;

	// Build result
	protected static List<SortedMap<String, Object>> buildAndLogResult(ResultSet rs, List<Class<?>> requiredResultTypes) throws SQLException {

		List<String> orderedColumnNames = new ArrayList<>();
		List<SortedMap<String, Object>> resultMaps = new ArrayList<>();

		// Retrieve result set metadata
		ResultSetMetaData rsmd = rs.getMetaData();
		if (rsmd != null) {

			orderedColumnNames.addAll(getOrderedResultColumnNames(rsmd));

			if (log.isTraceEnabled()) {
				log.trace("SQL: \tSQL and Java data types for results: ( jdbc type [ -> required type ] )");
				for (int c = 0; c < rsmd.getColumnCount(); c++) {
					if (requiredTypeDiffers(rsmd, requiredResultTypes, c)) {
						log.trace("SQL:\t\t{} {}: '{}' -> '{}'", rsmd.getColumnName(c + 1), rsmd.getColumnTypeName(c + 1), rsmd.getColumnClassName(c + 1), requiredResultTypes.get(c).getName());
					}
					else {
						log.trace("SQL:\t\t{} {}: {}", rsmd.getColumnName(c + 1), rsmd.getColumnTypeName(c + 1), rsmd.getColumnClassName(c + 1));
					}
				}
			}
		}
		else { // may happen on calling stored procedures using MySQL
			log.warn("SQL: No result set meta information available!");
		}

		// Retrieve results from result set (may throw SQLServerException: 'deadlock' on heavy load)
		while (rs.next()) {
			resultMaps.add(retrieveValues(rs, rsmd, requiredResultTypes));
		}

		if (resultMaps.isEmpty()) {
			if (log.isDebugEnabled()) {
				log.debug("SQL: No records found");
			}
		}
		else {
			Set<String> tableNames = new HashSet<>();
			if (rsmd == null) {

				// If no metadata are available keys are indexes of columns retrieved
				for (int i = 0; i < resultMaps.get(0).size(); i++) {
					orderedColumnNames.add(String.valueOf(i + 1) + ".");
				}
			}
			else {
				for (int i = 1; i <= rsmd.getColumnCount(); i++) {
					String tableName = rsmd.getTableName(i);
					if (!CBase.isEmpty(tableName)) {
						tableNames.add(tableName.toUpperCase());
					}
				}
			}

			// Log...
			if (resultMaps.size() == 1 && rsmd != null && rsmd.getColumnCount() == 1 && CBase.isEmpty(rsmd.getColumnName(1))) { // count(*)

				if (log.isDebugEnabled()) {
					log.debug("SQL: Retrieved: {}", resultMaps.iterator().next().values().iterator().next());
				}
			}
			else {
				if (log.isDebugEnabled()) {
					log.debug("SQL: {} record(s) retrieved {}", resultMaps.size(), (!tableNames.isEmpty() ? "from " + (tableNames.size() == 1 ? tableNames.iterator().next() : tableNames) : ""));
				}

				if (listRecordsInLog && log.isDebugEnabled()) {

					int recordCount = 0;
					for (SortedMap<String, Object> resultMap : resultMaps) {
						if (recordCount < 32 || log.isTraceEnabled()) {
							log.debug("SQL: \t{}", forLoggingSqlResult(resultMap, orderedColumnNames));
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

		return resultMaps;
	}

}
