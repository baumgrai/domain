package com.icx.jdbc;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CLog;
import com.icx.common.CProp;
import com.icx.common.CReflection;
import com.icx.common.Common;
import com.icx.domain.sql.Annotations.Secret;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;
import com.icx.jdbc.SqlDbTable.SqlDbForeignKeyColumn;
import com.icx.jdbc.SqlDbTable.SqlDbUniqueConstraint;

/**
 * SQL database connection object based on {@link ConnectionPool} provides high level methods to perform SQL SELECT, INSERT, UPDATE, DELETE statements.
 * <p>
 * {@code SqlDb} object can be created using {@link #SqlDb(Properties)} from {@link Properties} object containing <b>{@code dbConnectionString}</b>, <b>{@code dbUser}</b>, <b>{@code dbPassword}</b>
 * properties.
 * <p>
 * Using these high level methods programmer avoids to have to deal with details of JDBC programming like handling {@code ResultSet} and {@code ResultSetMetaData}. Outputs (of SELECT methods) and
 * inputs (of INSERT and UPDATE methods) are sorted maps or list of sorted maps, where such a map is the representation of one table or view record. A record map contains [ column name (label) : value
 * ] pairs and is sorted by column name (or label).
 * <p>
 * With DEBUG log level all SQL statements performed will be logged as they are - for INSERT and UPDATE statements including real column values and data types. In DEBUG logs also records retrieved by
 * SELECT will be logged including data types. Note: Values of fields or columns which are marked as 'secret' (*) will never be logged. (*) Marked as secret generally means: name contains ignore case
 * 'passwor' or 'pwd' or 'sec_'. Fields annotated with {@link Secret} annotation are also secret.
 * <p>
 * Database tables can be 'registered' using {@link #registerTable(Connection, String)}. Resulting {@link SqlDbTable} objects provide meta data of these tables and their columns. {@code SqlDbTable}
 * provides also methods to analyze table reference structure of database. {@link SqlDbTable} objects for registered tables can be retrieved by table name using {@link #findRegisteredTable(String)}.
 * Database tables involved in INSERT and UPDATE methods will be registered internally on first call for performance reasons.
 * <p>
 * Currently database types MS-SQL, Oracle and MySQL (and MariaDB) are supported. Use {@link #getDbType()} to check database type programmatically.
 * 
 * @author baumgrai
 */
public class SqlDb extends Common {

	private static final Logger log = LoggerFactory.getLogger(SqlDb.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	/**
	 * Property name for database query timeout property (seconds)
	 */
	public static final String DB_QUERYTIMEOUT_PROP = "dbQueryTimeout";

	private static final String ORACLE_DATE_TMPL = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF3')";
	private static final String MS_SQL_DATE_TMPL = "CONVERT(datetime, '%s', 121)";
	private static final String MYSQL_DATE_TMPL = "STR_TO_DATE('%s', '%%Y-%%m-%%d %%T.%%f')";

	/**
	 * Supported database types: <code>ORACLE, MS_SQL, MYSQL, MARIA</code>
	 */
	public enum DbType {
		ORACLE, MS_SQL, MYSQL, MARIA;

		/**
		 * Check if database type is MySQL based.
		 * 
		 * @return database type is MySQL based (MySQL or MariaDB)
		 */
		public boolean isMySql() {
			return (this == MYSQL || this == MARIA);
		}

		/**
		 * Get database specific date template.
		 * 
		 * @return database specific date template
		 */
		public String dateTemplate() {
			return (this == ORACLE ? ORACLE_DATE_TMPL : this == MS_SQL ? MS_SQL_DATE_TMPL : this.isMySql() ? MYSQL_DATE_TMPL : null);
		}
	}

	static final Map<DbType, String> DB_DATE_FUNCT = new EnumMap<DbType, String>(DbType.class) {
		static final long serialVersionUID = 1L;
		{
			put(DbType.ORACLE, "SYSDATE");
			put(DbType.MS_SQL, "GETDATE()");
			put(DbType.MYSQL, "SYSDATE()");
			put(DbType.MARIA, "SYSDATE()");
		}
	};

	// Query to retrieve result set metadata
	private static final String DUMMY_QUERY_MASK = "SELECT * FROM %s WHERE 1=0";

	// Oracle sequences
	static final String ORACLE_SEQ_NEXTVAL = ".NEXTVAL";
	static final String ORACLE_SEQ_CURRVAL = ".CURRVAL";

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// DB type
	DbType type = null;

	// Database connection pool
	ConnectionPool pool = null;

	/**
	 * Get connection pool associated to this high level SQL database connection.
	 * 
	 * @return database connection pool associated with SqlDb object
	 */
	public ConnectionPool getPool() {
		return pool;
	}

	// Query timeout for SELECT statements
	int queryTimeout = 60;

	// Registry: Registered tables
	List<SqlDbTable> halfOrderedTables = new ArrayList<>();
	private Set<SqlDbTable> registeredTables = new HashSet<>();

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	// Initialize SqlDb object, request and log database meta data
	private void init() throws SQLException, ConfigException {

		// Get database meta data
		String dbProdName = null;
		try (SqlConnection connection = SqlConnection.open(pool, true)) {
			DatabaseMetaData dbmd = connection.cn.getMetaData();

			dbProdName = dbmd.getDatabaseProductName();
			log.info("SQL: {}, version: {}.{}", dbProdName, dbmd.getDatabaseMajorVersion(), dbmd.getDatabaseMinorVersion());
			log.info("SQL: {}, version: {}, JDBC version: {}.{}", dbmd.getDriverName(), dbmd.getDriverVersion(), dbmd.getJDBCMajorVersion(), dbmd.getJDBCMinorVersion());
			log.info("SQL: ----------------");
		}

		// Determine database type
		if (dbProdName.contains("Microsoft")) {
			type = DbType.MS_SQL;
		}
		else if (dbProdName.contains("Oracle")) {
			type = DbType.ORACLE;
		}
		else if (dbProdName.contains("MySQL")) {
			type = DbType.MYSQL;
		}
		else if (dbProdName.contains("MariaDB")) {
			type = DbType.MARIA;
		}
		else {
			throw new ConfigException("Unsupported database type!");
		}
	}

	/**
	 * Create SQL database object from parameters.
	 * <p>
	 * JDBC driver class must be accessible in Java class path.
	 * 
	 * @param dbConnectionString
	 *            database connection string (e.g.: "jdbc:sqlserver://localhost;instanceName=MyInstance;databaseName=MyDb", "jdbc:oracle:thin:@//localhost:1521/xe")
	 * @param dbUser
	 *            database user or null
	 * @param dbPassword
	 *            password or null
	 * @param connectionPoolSize
	 *            maximum # of open connections kept in connection pool
	 * @param queryTimeout
	 *            query timeout in seconds
	 * 
	 * @throws ConfigException
	 *             if database connection string is null or empty and on unsupported database type (currently supported MS-SQL, Oracle, MySql)
	 * @throws SQLException
	 *             on error opening database connection
	 */
	public SqlDb(
			String dbConnectionString,
			String dbUser,
			String dbPassword,
			int connectionPoolSize,
			int queryTimeout) throws SQLException, ConfigException {

		pool = new ConnectionPool(dbConnectionString, dbUser, dbPassword, connectionPoolSize);
		this.queryTimeout = queryTimeout;
		init();
	}

	/**
	 * Create database object from {@code Properties} object.
	 * <p>
	 * Behaves like {@link #SqlDb(String, String, String, int, int)}.
	 * 
	 * @param databaseProperties
	 *            {@code Properties} object which must contain the following properties: {@code dbConnectionString}, {@code dbUser}, {@code dbPassword} and can optionally contain {@code poolSize}
	 *            (defaults to UNLIMITED) and {@code queryTimeout} (defaults to 60(s)).
	 * 
	 * @throws ConfigException
	 *             if database connection string is null or empty and on unsupported database type (currently supported MS-SQL, Oracle, MySql)
	 * @throws SQLException
	 *             on error opening database connection
	 */
	public SqlDb(
			Properties databaseProperties) throws SQLException, ConfigException {

		pool = new ConnectionPool(databaseProperties);
		queryTimeout = CProp.getIntProperty(databaseProperties, DB_QUERYTIMEOUT_PROP, 60);
		init();
	}

	/**
	 * Close database connection pool and all open database connections.
	 * 
	 * @throws SQLException
	 *             exception on {@link Connection#close()}
	 */
	public void close() throws SQLException {

		if (pool != null) {
			pool.close();
		}
	}

	// -------------------------------------------------------------------------
	// Miscellaneous
	// -------------------------------------------------------------------------

	/**
	 * Get database type of this connection (Oracle, MS/SQL).
	 * 
	 * @return database type
	 */
	public DbType getDbType() {
		return type;
	}

	/**
	 * Get database type specific SQL date function.
	 * 
	 * @return database specific SQL date function
	 */
	public String getSqlDateFunct() {
		return DB_DATE_FUNCT.get(type);
	}

	// -------------------------------------------------------------------------
	// Store records
	// -------------------------------------------------------------------------

	// Assign value to store to place holder - convert value to string on special cases and if converter is registered for specific value type, otherwise rely on internal driver conversion
	private static void assignValue(PreparedStatement pst, int c, SqlDbColumn column, Object columnValue) throws SQLException {

		try {
			if (columnValue == null) {
				pst.setNull(c, SqlDbHelpers.typeIntegerFromJdbcType(column.jdbcType)); // Store null value - provide JDBC type of column
			}
			else {
				Class<?> objectClass = columnValue.getClass();
				if (SqlDbHelpers.isBasicType(objectClass) || objectClass.isArray()) {

					if (objectClass == Character.class || objectClass == Boolean.class || Enum.class.isAssignableFrom(objectClass)) {
						pst.setString(c, columnValue.toString()); // Store values of specific classes as string
					}
					else if (objectClass == byte[].class) {
						pst.setBlob(c, new ByteArrayInputStream((byte[]) columnValue)); // Store other value using JDBC conversion
					}
					else if (objectClass == char[].class) {
						pst.setClob(c, new CharArrayReader((char[]) columnValue)); // Store other value using JDBC conversion
					}
					else {
						pst.setObject(c, columnValue); // Store other value using JDBC conversion
					}
				}
				else {
					pst.setObject(c, SqlDbHelpers.tryToBuildStringValueFromColumnValue(columnValue)); // Store value as string using either registered to-string converter or declared toString()
				}
			}
		}
		catch (IllegalArgumentException ex) {
			log.error("SQL: Column '{}' could not be set to value {} ({})", column.name, CLog.forSecretLogging(column.table.name, column.name, columnValue), ex);
		}
	}

	// -------------------------------------------------------------------------
	// Retrieve records
	// -------------------------------------------------------------------------

	// Get potentially cached table from qualified column name
	private static Map<String, SqlDbTable> tableCacheMap = new HashMap<>();

	private SqlDbTable getTableFromQualifiedColumnName(String qualifiedColumnName) {

		SqlDbTable table = null;
		if (tableCacheMap.containsKey(qualifiedColumnName)) {
			table = tableCacheMap.get(qualifiedColumnName);
		}
		else {
			String tableName = untilFirst(qualifiedColumnName, ".");
			table = findRegisteredTable(tableName);
			if (table != null) {
				tableCacheMap.put(qualifiedColumnName, table);
			}
			else {
				log.warn("SQL: Table for column '{}' could not be determined!", qualifiedColumnName);
			}
		}

		return table;
	}

	// Get potentially cached field type for column from qualified column name
	private static Map<String, Class<?>> fieldtypeCacheMap = new HashMap<>();

	private static Class<?> getFieldTypeFromQualifiedColumnName(SqlDbTable table, String qualifiedColumnName) {

		Class<?> fieldType = null;
		if (fieldtypeCacheMap.containsKey(qualifiedColumnName)) {
			fieldType = fieldtypeCacheMap.get(qualifiedColumnName);
		}
		else {
			SqlDbTable.SqlDbColumn column = table.findColumnByName(behindFirst(qualifiedColumnName, "."));
			if (column != null) {
				fieldType = column.fieldType;
				fieldtypeCacheMap.put(qualifiedColumnName, fieldType);
			}
			else {
				log.warn("SQL: Field type for column '{}' could not be determined!", qualifiedColumnName);
			}
		}

		return fieldType;
	}

	// Column info class
	private static class ColumnInfo {

		String columnName = null;
		String uppercaseColumnLabel = null;
		int columnType = -1;
		String tableName = "<unknown>";
		Class<?> fieldType = null;
	}

	// Try to determine table name and type of associated field for all columns
	private ColumnInfo[] getColumnInfos(ResultSetMetaData rsmd, List<String> orderedColumnNames) throws SQLException {

		ColumnInfo[] columnInfos = new ColumnInfo[rsmd.getColumnCount()];
		for (int c = 1; c <= rsmd.getColumnCount(); c++) {

			ColumnInfo columnInfo = new ColumnInfo();
			columnInfos[c - 1] = columnInfo;

			columnInfo.tableName = "<unknown>";
			columnInfo.columnName = rsmd.getColumnName(c);
			columnInfo.uppercaseColumnLabel = rsmd.getColumnLabel(c).toUpperCase();
			columnInfo.columnType = rsmd.getColumnType(c);
			columnInfo.fieldType = null;

			// Try to determine type of associated field from registered table column by qualified column name
			SqlDbTable table = null;
			if (orderedColumnNames != null && orderedColumnNames.size() >= c) {

				if (orderedColumnNames.get(c - 1).contains(".")) {
					table = getTableFromQualifiedColumnName(orderedColumnNames.get(c - 1));
					if (table != null) {
						columnInfo.tableName = table.name;
						columnInfo.fieldType = getFieldTypeFromQualifiedColumnName(table, orderedColumnNames.get(c - 1));
					}
				}
				else if (log.isDebugEnabled()) {
					log.debug("SQL: Table name and field type of column '{}' could not be determined because column names are not prefixed by table name ({}).", rsmd.getColumnName(c),
							orderedColumnNames);
				}
			}
			else {
				log.warn("SQL: Field type of column '{}' could not be determined because column name list {} does not contain appropriate entry!", rsmd.getColumnName(c), orderedColumnNames);
			}
		}

		return columnInfos;
	}

	// Retrieve column values for one row from result set of SELECT statement
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static SortedMap<String, Object> retrieveRecord(ResultSet rs, ColumnInfo[] columnInfos) throws SQLException {

		SortedMap<String, Object> resultRecord = new TreeMap<>();

		// Retrieve all values for one result record - JDBC indexes start by 1
		for (int c = 1; c <= columnInfos.length; c++) {

			String tableName = columnInfos[c - 1].tableName;
			String columnName = columnInfos[c - 1].columnName;
			int columnType = columnInfos[c - 1].columnType;
			Class<?> fieldType = columnInfos[c - 1].fieldType;
			Object columnValue = null;

			try {
				if (fieldType == null) { // No field type specified

					// Retrieve date/time column values as Java 8 date/time objects and other column values based on column type
					if (columnType == Types.TIMESTAMP || columnType == Types.TIMESTAMP_WITH_TIMEZONE) {
						columnValue = rs.getObject(c, LocalDateTime.class);
					}
					else if (columnType == Types.TIME || columnType == Types.TIME_WITH_TIMEZONE) {
						columnValue = rs.getObject(c, LocalTime.class);
					}
					else if (columnType == Types.DATE) {
						columnValue = rs.getObject(c, LocalDate.class);
					}
					else {
						columnValue = rs.getObject(c);
					}
				}
				else { // Field type is specified

					// Retrieve value based on field type and convert values if necessary
					if (fieldType == String.class) {
						columnValue = rs.getObject(c, String.class);
					}
					else if (fieldType == Character.class || fieldType == char.class) {

						String string = rs.getObject(c, String.class);
						columnValue = (!isEmpty(string) ? string.charAt(0) : null);
					}
					else if (fieldType == boolean.class) {
						columnValue = Boolean.valueOf(rs.getObject(c, String.class));
					}
					else if (fieldType == Boolean.class) {

						String booleanString = rs.getObject(c, String.class);
						if (booleanString != null) {
							columnValue = Boolean.valueOf(booleanString);
						}
					}
					else if (Enum.class.isAssignableFrom(fieldType)) {

						String string = rs.getObject(c, String.class);
						if (string != null) {
							columnValue = Enum.valueOf((Class<? extends Enum>) fieldType, string);
						}
					}
					else if (Number.class.isAssignableFrom(CReflection.getBoxingWrapperType(fieldType))) {

						// Do not use rs.getObject(c, <fieldtype>) on numerical types because 0 may be retrieved for null values!
						columnValue = rs.getObject(c);
						if (columnValue instanceof Number) {
							if (fieldType == Short.class || fieldType == short.class) {
								columnValue = ((Number) columnValue).shortValue();
							}
							else if (fieldType == Integer.class || fieldType == int.class) {
								columnValue = ((Number) columnValue).intValue();
							}
							else if (fieldType == Long.class || fieldType == long.class) {
								columnValue = ((Number) columnValue).longValue();
							}
							else if (fieldType == Double.class || fieldType == double.class) {
								columnValue = ((Number) columnValue).doubleValue();
							}
							else if (fieldType == BigInteger.class) {
								columnValue = BigInteger.valueOf(((Number) columnValue).longValue());
							}
							else if (fieldType == BigDecimal.class) {
								Number number = (Number) columnValue;
								if (number.doubleValue() % 1.0 == 0 && number.longValue() < Long.MAX_VALUE) { // Avoid artifacts BigDecimal@4 -> BigDecimal@4.0
									columnValue = BigDecimal.valueOf(number.longValue());
								}
								else {
									columnValue = BigDecimal.valueOf(number.doubleValue());
								}
							}
						}
						else if (columnValue != null) { // If driver returning other than Number object for numerical fields
							columnValue = rs.getObject(c, fieldType);
						}
					}
					else if (fieldType == LocalDateTime.class || fieldType == LocalDate.class || fieldType == LocalTime.class || Date.class.isAssignableFrom(fieldType) || fieldType == byte[].class) {
						columnValue = rs.getObject(c, fieldType);
					}
					else if (fieldType == byte[].class || File.class.isAssignableFrom(fieldType)) {
						columnValue = rs.getObject(c, byte[].class);
					}
					else if (fieldType == char[].class) {
						columnValue = rs.getObject(c); // char[].class is not accepted as column type - at least for Oracle
						if (columnValue instanceof String) {
							columnValue = ((String) columnValue).toCharArray();
						}
						else if (columnValue instanceof Clob) {
							Clob clob = (Clob) columnValue;
							columnValue = clob.getSubString(1, ((Number) clob.length()).intValue()).toCharArray();
						}
					}
					else {
						columnValue = rs.getObject(c);
						if (columnValue instanceof String) { // Try to convert value using either registered from-string converter or declared valueOf(String) method
							columnValue = SqlDbHelpers.tryToBuildFieldValueFromStringValue(fieldType, (String) columnValue, columnName);
						}
					}
				}

				// Put column value retrieved into result map
				resultRecord.put(columnInfos[c - 1].uppercaseColumnLabel, columnValue);
			}
			catch (IllegalArgumentException iaex) { // Thrown if enum field does not accept column content

				if (fieldType != null && Enum.class.isAssignableFrom(fieldType)) {
					log.error("SQL: Column value {} cannot be converted to enum type '{}' for column '{}.{}'! ({})", CLog.forAnalyticLogging(columnValue),
							((Class<? extends Enum>) fieldType).getName(), tableName, columnName, iaex);
				}
				else {
					log.error("SQL: Value of column '{}.{}' could not be retrieved - {}", tableName, columnName, iaex);
				}
			}
		}

		return resultRecord;

	}

	// -------------------------------------------------------------------------
	// SELECT
	// -------------------------------------------------------------------------

	/**
	 * Perform SQL SELECT operation on given connection.
	 * 
	 * @param cn
	 *            database connection
	 * @param sql
	 *            SQL SELECT statement, may contain '?' for place holders
	 * @param valuesOfPlaceholders
	 *            Values which replace place holders
	 * 
	 * @return result records as list of sorted column/value maps
	 * 
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public List<SortedMap<String, Object>> select(Connection cn, String sql, List<Object> valuesOfPlaceholders) throws SQLException {

		// Retrieve qualified column names from SELECT statement
		List<String> orderedColumnNames = null;
		if (log.isDebugEnabled()) {
			orderedColumnNames = new ArrayList<>();
			log.debug("SQL: {}", SqlDbHelpers.forSecretLoggingSelect(sql, valuesOfPlaceholders, orderedColumnNames));
		}
		else {
			orderedColumnNames = SqlDbHelpers.extractColumnNamesForSelectStatement(sql);
		}

		try (PreparedStatement st = cn.prepareStatement(sql)) {

			if (valuesOfPlaceholders != null) {

				// Set values from input list
				int counter = 1;
				for (Object obj : valuesOfPlaceholders) {
					if (obj instanceof Integer) {
						st.setInt(counter++, (Integer) obj);
					}
					else if (obj instanceof String) {
						st.setString(counter++, (String) obj);
					}
					else if (obj instanceof Double) {
						st.setDouble(counter++, (Double) obj);
					}
				}
			}

			// Create timer to cancel statement explicitly if setQueryTimeout() has no effect
			Timer timer = new Timer();
			timer.schedule(new TimerTask() {
				@Override
				public void run() {
					try {
						if (!st.isClosed()) {
							st.cancel();
						}
					}
					catch (SQLException ex) {
						log.error("Exception:", ex);
					}
				}
			}, (queryTimeout + 1) * 1000L); // A bit more than timeout for setQueryTimeout()
			st.setQueryTimeout(queryTimeout); // Does not work (for Oracle?)

			// Execute SELECT statement and retrieve results
			try (ResultSet rs = st.executeQuery()) {
				timer.cancel();

				// Retrieve result set metadata
				List<SortedMap<String, Object>> resultRecords = new ArrayList<>();
				ResultSetMetaData rsmd = rs.getMetaData();
				if (rsmd != null) {

					// Get basic column information from metadata and try to determine table name and type of associated field for all columns
					ColumnInfo[] columnInfos = getColumnInfos(rsmd, orderedColumnNames);

					// Retrieve results from result set
					while (rs.next()) {
						resultRecords.add(retrieveRecord(rs, columnInfos));
					}

					SqlDbHelpers.logResultRecordsOnDebugLevel(rsmd, sql, resultRecords);
				}
				else {
					log.error("SQL: No result set meta information available! Results cannot be retrieved.");
				}

				return resultRecords;
			}
		}
		catch (Exception ex) {
			log.error("SQL: {} '{}' on '{}'", ex.getClass().getSimpleName(), ex.getMessage().trim(), SqlDbHelpers.forSecretLoggingSelect(sql, valuesOfPlaceholders, null));
			throw ex;
		}
	}

	/**
	 * Perform SQL SELECT statement on given connection.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableExpr
	 *            table name or SQL joined table expression like "LEAVES L JOIN COLOURS C ON L.COLOUR_ID = C.ID"
	 * @param colExpr
	 *            either (1) a list of column names or (2) a SQL column expression string like "NAME, GENDER, BIRTHDATE" or "COUNT(*)" or (3) null, empty list or "*" for "SELECT * FROM..."
	 * @param whereClause
	 *            SQL where clause string (without "WHERE") or null
	 * @param orderByClause
	 *            SQL order by clause string (without "ORDER BY") or null
	 * @param limit
	 *            maximum # of records to retrieve
	 * @param values
	 *            values to assign to placeholders for prepared statement
	 * 
	 * @return result records as list of sorted column/value maps
	 * 
	 * @throws SqlDbException
	 *             if table expression is empty or null or column expression type is not of type {@code String} or {@code List}
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	@SuppressWarnings("unchecked")
	public List<SortedMap<String, Object>> selectFrom(Connection cn, String tableExpr, Object colExpr, String whereClause, String orderByClause, int limit, List<Object> values)
			throws SQLException, SqlDbException {

		// Check preconditions
		if (isEmpty(tableExpr)) {
			throw new SqlDbException("SELECT: No table(s) specified!");
		}

		// Uppercase table expression
		tableExpr = tableExpr.toUpperCase();

		// Build SQL statement
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");

		if (limit > 0 && getDbType() == DbType.MS_SQL) {
			sql.append("TOP " + limit + " ");
		}

		// Allow null, String or List<String> for column expression
		if (colExpr == null || List.class.isAssignableFrom(colExpr.getClass()) && ((List<String>) colExpr).isEmpty() || colExpr instanceof String && ((String) colExpr).isEmpty()) {

			// null or empty
			sql.append("*");
		}
		else if (List.class.isAssignableFrom(colExpr.getClass())) {

			// Column list
			List<String> columns = (List<String>) colExpr;
			boolean first = true;
			for (String col : columns) {
				if (!first) {
					sql.append(", ");
				}
				first = false;
				sql.append(col.toUpperCase());
			}
		}
		else if (colExpr instanceof String) {

			// Column string
			sql.append((String) colExpr);
		}
		else {
			throw new SqlDbException("SELECT: Wrong type '" + colExpr.getClass().getSimpleName() + "' of column expression parameter!");
		}

		// Table expression
		sql.append(" FROM ");
		sql.append(tableExpr);

		// Where clause?
		if (!isEmpty(whereClause)) {
			sql.append(" WHERE " + whereClause);
		}

		if (limit > 0 && getDbType() == DbType.ORACLE) {
			if (isEmpty(whereClause)) {
				sql.append("WHERE ROWNUM <= " + limit);
			}
			else {
				sql.append(" AND ROWNUM <= " + limit);
			}
		}

		// Order by clause?
		if (!isEmpty(orderByClause)) {
			sql.append(" ORDER BY " + orderByClause);
		}

		if (limit > 0 && getDbType().isMySql()) {
			sql.append(" LIMIT " + limit);
		}

		return select(cn, sql.toString(), values);
	}

	/**
	 * SELECT record count from given table for given WHERE clause.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableName
	 *            table name
	 * @param whereClause
	 *            WHERE clause (without "WHERE")
	 * 
	 * @return record count
	 * 
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 * @throws SqlDbException
	 *             (cannot be not be thrown here)
	 */
	public long selectCountFrom(Connection cn, String tableName, String whereClause) throws SQLException, SqlDbException {
		return ((Number) selectFrom(cn, tableName, "count(*)", whereClause, null, 0, null).iterator().next().values().iterator().next()).longValue();
	}

	// -------------------------------------------------------------------------
	// Insert
	// -------------------------------------------------------------------------

	/**
	 * INSERT one record into a database table on given database connection.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableName
	 *            database table name
	 * @param columnValueMap
	 *            Map of: IN: values to set for columns, OUT: real JDBC values set for column after Java -&gt; SQL conversion
	 * 
	 * @return ID of inserted record
	 * 
	 * @throws SqlDbException
	 *             if table name is null or empty or if column/value map is null or on problems registering database table
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public int insertInto(Connection cn, String tableName, SortedMap<String, Object> columnValueMap) throws SQLException, SqlDbException {

		List<SortedMap<String, Object>> columnValueMaps = new ArrayList<>();
		columnValueMaps.add(columnValueMap);

		int[] results = insertInto(cn, tableName, columnValueMaps);

		return (results == null || results.length == 0 ? 0 : results[0]);
	}

	/**
	 * INSERT multiple records per batch insert into a database table on given database connection.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableName
	 *            database table name
	 * @param columnNameValueMaps
	 *            list maps containing column name/value entries to insert
	 * 
	 * @return batch execution results
	 * 
	 * @throws SqlDbException
	 *             if table name is null or empty or if column/value map is null or on problems registering database table
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public int[] insertInto(Connection cn, String tableName, List<SortedMap<String, Object>> columnNameValueMaps) throws SQLException, SqlDbException {

		// Check preconditions
		if (isEmpty(tableName)) {
			throw new SqlDbException("INSERT: Table name is empty or null!");
		}
		else if (columnNameValueMaps == null) {
			throw new SqlDbException("INSERT: Column/value map is null!");
		}

		if (columnNameValueMaps.isEmpty()) {
			if (log.isDebugEnabled()) {
				log.debug("SQL: No records to insert");
			}
			return new int[0];
		}

		// Retrieve table columns metadata and put them into table registry (if not already done)
		tableName = tableName.toUpperCase();
		SqlDbTable table = registerTable(cn, tableName);

		// Build ordered map of columns and value expressions for prepared INSERT statement and collect columns with place holders
		SortedMap<SqlDbColumn, String> columnValueExpressionMap = new TreeMap<>();
		SortedMap<SqlDbColumn, String> columnKeyMap = new TreeMap<>();

		List<String> uppercaseColumnNamesOfTable = table.getColumnNames();
		for (Entry<String, Object> columnNameValueEntry : columnNameValueMaps.get(0).entrySet()) {

			String columnName = columnNameValueEntry.getKey();
			Object value = columnNameValueEntry.getValue();

			if (uppercaseColumnNamesOfTable.contains(columnName.toUpperCase())) {

				SqlDbColumn column = table.findColumnByName(columnName);
				String valueExpression = SqlDbHelpers.getValueExpressionForSqlStatement(value);

				if ("?".equals(valueExpression)) {
					columnKeyMap.put(column, columnName);
				}

				columnValueExpressionMap.put(column, valueExpression);
			}
			else {
				throw new SQLException("SQL: INSERT: Try to set value for column '" + columnName + "' which does not exist in table '" + table.name + "'");
			}
		}

		long numberOfRecords = columnNameValueMaps.size();
		if (numberOfRecords > 1 && log.isDebugEnabled()) {
			log.debug("SQL: Batch insert {} records...", numberOfRecords);
		}

		// Build SQL INSERT statement
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("INSERT INTO " + tableName);
		if (columnValueExpressionMap.isEmpty()) {

			// Insert default values if no value is specified
			sqlBuilder.append(" DEFAULT VALUES");
		}
		else {
			// Build columns and values clause
			StringBuilder columnNameBuilder = new StringBuilder();
			StringBuilder valueExpressionBuilder = new StringBuilder();

			boolean isFirst = true;
			for (Entry<SqlDbColumn, String> columnEntry : columnValueExpressionMap.entrySet()) {
				if (!isFirst) {
					columnNameBuilder.append(", ");
					valueExpressionBuilder.append(", ");
				}
				isFirst = false;
				columnNameBuilder.append(columnEntry.getKey().name);
				valueExpressionBuilder.append(columnEntry.getValue());
			}

			sqlBuilder.append(" (");
			sqlBuilder.append(columnNameBuilder);
			sqlBuilder.append(") VALUES (");
			sqlBuilder.append(valueExpressionBuilder);
			sqlBuilder.append(")");
		}

		String preparedStatementString = sqlBuilder.toString();

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			// Assign values to prepared statement
			for (Map<String, Object> columnNameValueMap : columnNameValueMaps) {

				int c = 1;
				for (Entry<SqlDbColumn, String> columnKeyEntry : columnKeyMap.entrySet()) {
					String keyInColumnNameValueMap = columnKeyEntry.getValue();
					assignValue(pst, c++, columnKeyEntry.getKey(), columnNameValueMap.get(keyInColumnNameValueMap));
				}

				if (log.isDebugEnabled()) {
					log.debug("SQL: {}", SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnNameValueMap, (SortedSet<SqlDbColumn>) columnValueExpressionMap.keySet()));
				}

				if (numberOfRecords > 1) {
					pst.addBatch();
				}
			}

			// Execute INSERT statement and retrieve batch results
			int[] results = null;

			// Execute insert statement
			if (numberOfRecords > 1) {

				// For multiple records insert as batch
				results = pst.executeBatch();

				for (int i = 0; i < results.length; i++) {
					if (results[i] == Statement.EXECUTE_FAILED) {
						log.warn("SQL: Batch execution failed on record {}", i);
					}
				}
			}
			else {
				// Insert one record
				results = new int[] { pst.executeUpdate() };
			}

			if (numberOfRecords == 0) {
				log.error("SQL: No record(s) could be inserted!");
			}
			else if (log.isTraceEnabled()) {
				log.trace("SQL: {} record(s) inserted", numberOfRecords);
			}

			return results;
		}
		catch (SQLException sqlex) {

			if (preparedStatementString.contains("IN_PROGRESS")) {
				if (log.isDebugEnabled()) {
					log.debug(
							"In-progress record used for access synchronization in Domain persistence system could not be inserted - associated object is currently used exclusivly by another thread!");
				}
			}
			else {
				log.error("SQL: {} '{}' on... ", sqlex.getClass().getSimpleName(), sqlex.getMessage().trim()); // Log SQL statement(s) on exception
				for (Map<String, Object> columnValueMap : columnNameValueMaps) {
					log.error("SQL: '{}'", SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnValueMap, (SortedSet<SqlDbColumn>) columnValueExpressionMap.keySet()));
				}
			}

			throw sqlex;
		}
	}

	// -------------------------------------------------------------------------
	// Update
	// -------------------------------------------------------------------------

	/**
	 * UPDATE records of a database table on given database connection.
	 * <p>
	 * Registers table before UPDATE if not already done.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableName
	 *            database table name
	 * @param columnNameValueMap
	 *            Map of: IN: values to set for columns, OUT: real JDBC values set for column after Java -&gt; SQL conversion
	 * @param whereClause
	 *            WHERE clause for UPDATE statement (without "WHERE")
	 * 
	 * @return # of records updated
	 * 
	 * @throws SqlDbException
	 *             if table name is null or empty or if or column/value map is null or on problems registering database table
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public long update(Connection cn, String tableName, Map<String, Object> columnNameValueMap, String whereClause) throws SQLException, SqlDbException {

		// Check preconditions
		if (isEmpty(tableName)) {
			throw new SqlDbException("UPDATE: Table name is empty or null!");
		}
		else if (columnNameValueMap == null) {
			throw new SqlDbException("UPDATE: Column/value map is null!");
		}

		if (columnNameValueMap.isEmpty()) {
			if (log.isDebugEnabled()) {
				log.debug("SQL: Column/value map empty - nothing to update");
			}
			return 0;
		}

		// Retrieve table columns metadata and put them into registry (if not already done)
		tableName = tableName.toUpperCase();
		SqlDbTable table = registerTable(cn, tableName);

		// Build ordered map of columns and value expressions for prepared INSERT statement and collect columns with place holders
		SortedMap<SqlDbColumn, String> columnValueExpressionMap = new TreeMap<>();
		SortedMap<SqlDbColumn, Object> placeholderColumnValueMap = new TreeMap<>();

		List<String> uppercaseColumnNamesOfTable = table.getColumnNames();
		for (Entry<String, Object> columnNameValueEntry : columnNameValueMap.entrySet()) {

			String columnName = columnNameValueEntry.getKey();
			Object value = columnNameValueEntry.getValue();

			if (uppercaseColumnNamesOfTable.contains(columnName.toUpperCase())) {

				SqlDbColumn column = table.findColumnByName(columnName);
				String valueExpression = SqlDbHelpers.getValueExpressionForSqlStatement(value);

				if ("?".equals(valueExpression)) {
					placeholderColumnValueMap.put(column, value);
				}

				columnValueExpressionMap.put(column, valueExpression);
			}
			else {
				throw new SQLException("SQL: UPDATE: Try to set value for column '" + columnName + "' which does not exist in table '" + table.name + "'");
			}
		}

		// Build SQL update statement
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("UPDATE " + tableName + " SET ");

		// Append placeholders for values
		boolean isFirst = true;
		for (Entry<SqlDbColumn, String> columnEntry : columnValueExpressionMap.entrySet()) {
			if (!isFirst) {
				sqlBuilder.append(", ");
			}
			isFirst = false;
			sqlBuilder.append(columnEntry.getKey().name + " = " + columnEntry.getValue());
		}

		// Where clause ?
		if (whereClause != null && !whereClause.isEmpty()) {
			sqlBuilder.append(" WHERE " + whereClause);
		}

		String preparedStatementString = sqlBuilder.toString();
		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			// Assign values to prepared statement
			int c = 1;
			for (Entry<SqlDbColumn, Object> columnValueEntry : placeholderColumnValueMap.entrySet()) {
				assignValue(pst, c++, columnValueEntry.getKey(), columnValueEntry.getValue());
			}

			if (log.isDebugEnabled()) {
				log.debug("SQL: {}", SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnNameValueMap, (SortedSet<SqlDbColumn>) placeholderColumnValueMap.keySet()));
			}

			// Execute UPDATE statement and get update count
			pst.execute();
			long count = pst.getUpdateCount();

			if (log.isDebugEnabled()) {
				log.debug("SQL: {} record(s) updated", count);
			}

			return count;
		}
		catch (SQLException sqlex) {
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage().trim(),
					SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnNameValueMap, (SortedSet<SqlDbColumn>) placeholderColumnValueMap.keySet()));
			throw sqlex;
		}
	}

	// -------------------------------------------------------------------------
	// Delete
	// -------------------------------------------------------------------------

	/**
	 * Delete records from a database table on given database connection.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableName
	 *            database table name
	 * @param whereClause
	 *            WHERE clause for DELETE statement (without "WHERE")
	 * 
	 * @return # of deleted records
	 * 
	 * @throws SqlDbException
	 *             if table name is null or empty
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public static long deleteFrom(Connection cn, String tableName, String whereClause) throws SQLException, SqlDbException {

		// Check preconditions
		if (isEmpty(tableName)) {
			throw new SqlDbException("DELETE: Table name is null or empty!");
		}

		// Build SQL DELETE statement
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("DELETE FROM ");
		sqlBuilder.append(tableName.toUpperCase());

		// Where clause?
		if (whereClause != null && !whereClause.isEmpty()) {
			sqlBuilder.append(" WHERE " + whereClause);
		}

		String preparedStatementString = sqlBuilder.toString();

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			if (log.isDebugEnabled()) {
				log.debug("SQL: {}", preparedStatementString);
			}

			// Execute DELETE statement
			pst.execute();

			int count = pst.getUpdateCount();
			if (log.isDebugEnabled()) {
				log.debug("SQL: {} record(s) deleted", count);
			}

			return count;
		}
		catch (SQLException sqlex) {
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage().trim(), preparedStatementString); // Log SQL statement on exception
			throw sqlex;
		}
	}

	// -------------------------------------------------------------------------
	// Register table
	// -------------------------------------------------------------------------

	/**
	 * Retrieve and register column meta data for given database table and all referenced tables.
	 * 
	 * @param cn
	 *            database connection
	 * @param tableName
	 *            name of database table
	 * 
	 * @return registered table object
	 * 
	 * @throws SqlDbException
	 *             if table name is null or empty, if table does not exist and on JDBC meta data inconsistencies
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public SqlDbTable registerTable(Connection cn, String tableName) throws SQLException, SqlDbException {

		// Uppercase table name before registering
		tableName = tableName.toUpperCase();

		// Avoid multiple registration and opening database connection
		SqlDbTable table = findRegisteredTable(tableName);
		if (table != null) {
			return table;
		}

		return registerTable(cn, tableName, null);
	}

	/**
	 * Try to find already registered table by name.
	 * 
	 * @param tableName
	 *            name of database table
	 * 
	 * @return registered table object or null if table with given name was not registered
	 */
	public SqlDbTable findRegisteredTable(String tableName) {

		for (SqlDbTable registeredTable : registeredTables) {
			if (registeredTable.name.equals(tableName)) {
				return registeredTable;
			}
		}

		return null;
	}

	// Recursive table registration
	private SqlDbTable registerTable(Connection cn, String tableName, SqlDbForeignKeyColumn fkColumnReferencingTable) throws SQLException, SqlDbException {

		if (isEmpty(tableName)) {
			throw new SqlDbException("Table name is empty or null!");
		}

		// Uppercase table name before registering
		tableName = tableName.toUpperCase();

		// Try to find already registered table and create table object if not already registered
		boolean alreadyRegistered = false;
		SqlDbTable table = findRegisteredTable(tableName);
		if (table == null) {
			table = new SqlDbTable(tableName, this);
		}
		else {
			alreadyRegistered = true;
		}

		// Add foreign key column to list of foreign key columns referencing this table
		if (fkColumnReferencingTable != null) {
			table.fkColumnsReferencingThisTable.add(fkColumnReferencingTable);
		}

		// Suppress multiple registrations of same table
		if (alreadyRegistered) {
			return table;
		}

		// Extract simple table name from schema table name if necessary
		String schemaName = null;
		String[] schemaTableArray = tableName.split("\\.");
		if (schemaTableArray.length > 1) {
			schemaName = schemaTableArray[0];
			tableName = schemaTableArray[1];
		}

		// Foreign key columns for subsequent registration of referenced tables
		SortedMap<SqlDbForeignKeyColumn, String> fkColumnMap = new TreeMap<>();

		String sql = null;
		try {
			// Retrieve database meta data
			DatabaseMetaData dbmd = cn.getMetaData();

			// Check if table exists
			try (ResultSet rs = dbmd.getTables(null, schemaName, tableName, null)) {

				boolean tableExist = rs.next();
				if (!tableExist) {
					throw new SqlDbException("Table '" + tableName + "' does not exist in database!");
				}
			}

			// Retrieve primary key meta data
			String primaryKeyColumnName = null;
			try (ResultSet rs = dbmd.getPrimaryKeys(null, schemaName, tableName)) {

				if (rs.next()) {
					primaryKeyColumnName = rs.getString("COLUMN_NAME");
				}
				if (rs.next()) {
					primaryKeyColumnName = null; // Primary key from multiple columns -> attribute 'primary key' cannot be assigned to a single column
				}
			}

			// Retrieve column meta data
			try (ResultSet rs = dbmd.getColumns(null, schemaName, tableName, null)) {

				while (rs.next()) {
					SqlDbColumn column = table.new SqlDbColumn(table);

					column.order = rs.getInt("ORDINAL_POSITION");
					column.name = rs.getString("COLUMN_NAME").toUpperCase();
					column.datatype = rs.getString("TYPE_NAME").toUpperCase();
					column.maxlen = rs.getInt("COLUMN_SIZE");
					int nullableInt = rs.getInt("NULLABLE");
					column.isNullable = (nullableInt == ResultSetMetaData.columnNullable);
					if (objectsEqual(column.name, primaryKeyColumnName)) {
						column.isPrimaryKey = true;
					}

					table.columns.add(column);
					if (column.isIdentity()) {
						table.identityColumn = column;
					}
				}
			}

			// Retrieve foreign key column meta data
			List<String> keys = new ArrayList<>();
			try (ResultSet rs = dbmd.getImportedKeys(null, schemaName, tableName)) {

				while (rs.next()) {

					String columnName = rs.getString("FKCOLUMN_NAME").toUpperCase();
					if (keys.contains(columnName)) { // On Oracle keys appear twice in result set
						break;
					}
					keys.add(columnName);

					// Build foreign key column from same-name table column
					SqlDbColumn column = table.findColumnByName(columnName);
					if (column == null) {
						throw new SqlDbException("Foreign key column '" + columnName + "' is not a column of '" + tableName + "'");
					}

					SqlDbForeignKeyColumn fkColumn = table.new SqlDbForeignKeyColumn(column);
					table.columns.remove(column);
					table.columns.add(fkColumn);

					// Get reference data
					String referencedTableName = rs.getString("PKTABLE_NAME").toUpperCase();
					String referencedColumnName = rs.getString("PKCOLUMN_NAME").toUpperCase();
					fkColumnMap.put(fkColumn, referencedTableName + "." + referencedColumnName);
				}
			}

			// Retrieve JDBC types of columns using dummy query
			sql = String.format(DUMMY_QUERY_MASK, tableName);
			if (log.isTraceEnabled()) {
				log.trace("SQL: {}", SqlDbHelpers.forSecretLoggingInsertUpdate(sql, null, null));
			}

			try (Statement st = cn.createStatement()) {

				st.setQueryTimeout(queryTimeout);

				try (ResultSet rs = st.executeQuery(sql)) {

					ResultSetMetaData rsmd = rs.getMetaData();

					for (int colIndex = 0; colIndex < rsmd.getColumnCount(); colIndex++) {

						String columnName = rsmd.getColumnName(colIndex + 1).toUpperCase();
						SqlDbColumn column = table.findColumnByName(columnName);
						if (column == null) {
							throw new SqlDbException("Column '" + columnName + "' is not a column of '" + tableName + "'");
						}

						String columnJavaClassName = rsmd.getColumnClassName(colIndex + 1);
						try {
							column.jdbcType = Class.forName(columnJavaClassName);
						}
						catch (ClassNotFoundException e) {
							throw new SqlDbException("Class '" + columnJavaClassName + "' for '" + tableName + "." + columnName + "' cannot be loaded!");
						}
					}
				}

				// Retrieve unique constraints
				try (ResultSet rs = dbmd.getIndexInfo(null, schemaName, tableName, true, true)) {

					while (rs.next()) {

						String indexName = rs.getString("INDEX_NAME");
						if (indexName == null) {
							continue;
						}

						String columnName = rs.getString("COLUMN_NAME");

						// Check if UNIQUE constraint with this name already exist (containing another column) and create new constraint if not
						SqlDbUniqueConstraint constraint = table.findUniqueConstraintByName(indexName);
						if (constraint == null) {
							constraint = table.new SqlDbUniqueConstraint();
							constraint.table = table;
							table.uniqueConstraints.add(constraint);
						}

						constraint.name = indexName;

						SqlDbColumn column = table.findColumnByName(columnName);
						if (column == null) {
							throw new SqlDbException("Column '" + columnName + "' is not a column of '" + tableName + "'");
						}
						constraint.columns.add(column);
					}
				}

				// Assign UNIQUE constraint directly to columns where a single column UNIQUE constraint exist for
				for (SqlDbUniqueConstraint uc : table.uniqueConstraints) {
					if (uc.columns.size() == 1) {
						uc.columns.iterator().next().isUnique = true;
					}
				}
			}
		}
		catch (SQLException sqlex) {
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage().trim(), sql);
			throw sqlex;
		}

		// Add to registered tables before registering referenced tables to allow check if table is already registered
		registeredTables.add(table);

		// Register referenced tables (recursion)
		for (SqlDbForeignKeyColumn fkColumn : fkColumnMap.keySet()) {
			String[] refTableColumn = fkColumnMap.get(fkColumn).split("\\.");

			SqlDbTable referencedTable = registerTable(cn, refTableColumn[0], fkColumn);
			fkColumn.referencedUniqueColumn = referencedTable.findColumnByName(refTableColumn[1]);
		}

		// Add in - as far as possible (circular schema references) - correct hierarchical order
		halfOrderedTables.add(table);

		if (log.isTraceEnabled()) {
			log.trace("SQL: {}", table);
		}

		return table;
	}

}
