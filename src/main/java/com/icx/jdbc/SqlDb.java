package com.icx.jdbc;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
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
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Prop;
import com.icx.common.Reflection;
import com.icx.common.base.CFile;
import com.icx.common.base.CLog;
import com.icx.common.base.CMap;
import com.icx.common.base.Common;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;
import com.icx.jdbc.SqlDbTable.SqlDbForeignKeyColumn;
import com.icx.jdbc.SqlDbTable.SqlDbUniqueConstraint;

/**
 * Based on {@link ConnectionPool} provides high level methods to perform SQL SELECT, INSERT, UPDATE, DELETE statements.
 * <p>
 * {@code SqlDb} object can be created using {@link #SqlDb(Properties)} from {@link Properties} object containing <b>{@code dbConnectionString}</b>, <b>{@code dbUser}</b>, <b>{@code dbPassword}</b>
 * properties.
 * <p>
 * Using these high level methods programmer avoids to have to deal with details of JDBC programming like handling {@code ResultSet} and {@code ResultSetMetaData}. Outputs (of SELECT methods) and
 * inputs (of INSERT and UPDATE methods) are sorted maps or list of sorted maps, where such a map is the representation of one table or view record. A record map contains [ column name (label) : value
 * ] pairs and is sorted by column name (or label). The types of the column values are the JDBC types associated to the columns and therefore database type specific. Use {@link #getDbType()} to check
 * database type programmatically.
 * <p>
 * Most methods come in two types here: one with a parameter for database connection (which can, but not have to be retrieved from connection pool of this {@code SqlDb} object), the other where
 * database connection is retrieved internally from connection pool of {@code SqlDb} object. Methods of first type support database transactions spanning more than one SQL statement (using
 * non-auto-commit connections in this case).
 * <p>
 * Database tables can be <b>registered</b> using {@link #registerTable(Connection, String)} in form of {@link SqlDbTable} objects, which provide meta data of these tables and their columns.
 * {@code SqlDbTable} provides also methods to analyze table reference structure of database. {@link SqlDbTable} objects for registered tables can be retrieved by table name using
 * {@link #findRegisteredTable(String)}. Database tables involved in INSERT and UPDATE methods will be registered internally on first call for performance reasons.
 * <p>
 * Currently database types MS-SQL, Oracle and MySql are supported.
 * 
 * @author baumgrai
 */
public class SqlDb extends Common {

	private static final Logger log = LoggerFactory.getLogger(SqlDb.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	private static final String ORACLE_DATE_TMPL = "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF3')";
	private static final String MS_SQL_DATE_TMPL = "CONVERT(datetime, '%s', 121)";
	private static final String MYSQL_DATE_TMPL = "STR_TO_DATE('%s', '%%Y-%%m-%%d %%T.%%f')";

	/**
	 * Supported database types: <code>ORACLE, MS_SQL, MYSQL</code>
	 */
	public enum DbType {
		ORACLE, MS_SQL, MYSQL;

		public String dateTemplate() {
			return (this == ORACLE ? ORACLE_DATE_TMPL : this == MS_SQL ? MS_SQL_DATE_TMPL : this == MYSQL ? MYSQL_DATE_TMPL : null);
		}
	}

	protected static final Map<DbType, String> DB_DATE_FUNCT = new EnumMap<DbType, String>(DbType.class) {
		static final long serialVersionUID = 1L;
		{
			put(DbType.ORACLE, "SYSDATE");
			put(DbType.MS_SQL, "GETDATE()");
			put(DbType.MYSQL, "SYSDATE()");
		}
	};

	// Query to retrieve result set metadata
	static final String DUMMY_QUERY_MASK = "SELECT * FROM %s WHERE 1=0";

	// Oracle sequences
	public static final String ORACLE_SEQ_NEXTVAL = ".NEXTVAL";
	protected static final String ORACLE_SEQ_CURRVAL = ".CURRVAL";

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// DB type
	protected DbType type = null;

	// Database connection pool
	public ConnectionPool pool = null;

	// Query timeout for SELECT statements
	public int queryTimeout = 60;

	// Registry: Registered tables
	protected List<SqlDbTable> halfOrderedTables = new ArrayList<>();
	private Set<SqlDbTable> registeredTables = new HashSet<>();

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	// Initialize SqlDb object, request and log database meta data
	void init() throws SQLException, ConfigException {

		// Get database meta data
		String dbProdName = null;
		try (SqlConnection connection = SqlConnection.open(pool, true)) {
			DatabaseMetaData databaseMetaData = connection.cn.getMetaData();

			dbProdName = databaseMetaData.getDatabaseProductName();
			log.info("SQL: {}, Version: {}.{}", dbProdName, databaseMetaData.getDatabaseMajorVersion(), databaseMetaData.getDatabaseMinorVersion());
			log.info("SQL: {}, Version: {}.{}", databaseMetaData.getDriverName(), databaseMetaData.getJDBCMajorVersion(), databaseMetaData.getJDBCMinorVersion());
			log.info("SQL: ----------------");
		}

		// Determine database type
		if (dbProdName.contains("Microsoft")) {
			type = DbType.MS_SQL;
		}
		else if (dbProdName.contains("Oracle")) {
			type = DbType.ORACLE;
		}
		else if (dbProdName.contains("MySQL") || dbProdName.contains("MariaDB")) {
			type = DbType.MYSQL;
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
	 *            query timeout in milliseconds
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

		queryTimeout = Prop.getIntProperty(databaseProperties, ConnectionPool.DB_QUERYTIMEOUT_PROP, 60);

		init();
	}

	/**
	 * Close database connection pool and all open database connections
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
	 * Get database type of this connection (Oracle, MS/SQL)
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

	// Build up byte array containing file path and file content . If file cannot be read store only file path in database and set file content to an error message.
	private static byte[] buildFileByteEntry(File file, String columnName) {

		byte[] pathBytes = file.getAbsolutePath().getBytes(StandardCharsets.UTF_8);
		byte[] contentBytes;
		try {
			contentBytes = CFile.readBinary(file);
		}
		catch (IOException ioex) {
			log.error("SQL: File '{}' cannot be read! Therfore column '{}' will be set to file path name but file itself contains an error message. ({})", file, columnName, ioex);
			contentBytes = "File did not exist or could not be read on storing to database!".getBytes(StandardCharsets.UTF_8);
		}
		byte[] entryBytes = new byte[2 + pathBytes.length + contentBytes.length];

		entryBytes[0] = (byte) (pathBytes.length / 0x100);
		entryBytes[1] = (byte) (pathBytes.length % 0x100);

		int b = 2;
		for (; b < pathBytes.length + 2; b++) {
			entryBytes[b] = pathBytes[b - 2];
		}

		for (; b < contentBytes.length + pathBytes.length + 2; b++) {
			entryBytes[b] = contentBytes[b - pathBytes.length - 2];
		}

		return entryBytes;
	}

	// Assign value to store to place holder - convert value to string on special cases and if converter is registered for specific value type, otherwise rely on internal driver conversion
	private static void assignValue(PreparedStatement pst, int c, String tableName, SqlDbColumn column, Object columnValue) throws SQLException {

		try {
			if (columnValue == null) {
				pst.setNull(c + 1, typeIntegerFromJdbcType(column.jdbcType)); // Store null value - provide JDBC type of column
			}
			else {
				Class<?> objectClass = columnValue.getClass();
				if (SqlDbHelpers.isBasicType(objectClass) || objectClass.isArray()) {

					if (objectClass == Character.class || objectClass == Boolean.class || Enum.class.isAssignableFrom(objectClass)) {
						pst.setString(c + 1, columnValue.toString()); // Store values of specific classes as string
					}
					else if (objectClass == byte[].class) {
						pst.setBlob(c + 1, new ByteArrayInputStream((byte[]) columnValue)); // Store other value using JDBC conversion
					}
					else if (objectClass == char[].class) {
						pst.setClob(c + 1, new CharArrayReader((char[]) columnValue)); // Store other value using JDBC conversion
					}
					else if (objectClass == File.class) {
						pst.setObject(c + 1, buildFileByteEntry((File) columnValue, column.name)); // Store File as byte array
					}
					else {
						pst.setObject(c + 1, columnValue); // Store other value using JDBC conversion
					}
				}
				else {
					pst.setObject(c + 1, SqlDbHelpers.tryToBuildStringValueFromColumnValue(columnValue)); // Store value as string using either registered to-string converter or declared toString()
				}
			}
		}
		catch (IllegalArgumentException ex) {
			log.error("SQL: Column '{}' could not be set to value {} ({})", column.name, CLog.forSecretLogging(tableName, column.name, columnValue), ex);
		}
	}

	// -------------------------------------------------------------------------
	// Retrieve records
	// -------------------------------------------------------------------------

	// Rebuild file object from binary coded file entry
	private static File rebuildFile(byte[] entryBytes) throws IOException {

		if (entryBytes == null) {
			return null;
		}

		int pathLength = 0x100 * entryBytes[0] + entryBytes[1];

		int b = 2;
		byte[] pathBytes = new byte[pathLength];
		for (; b < pathLength + 2; b++) {
			pathBytes[b - 2] = entryBytes[b];
		}
		String filepath = new String(pathBytes, StandardCharsets.UTF_8);

		byte[] contentBytes = new byte[entryBytes.length - pathLength - 2];
		for (; b < entryBytes.length; b++) {
			contentBytes[b - pathLength - 2] = entryBytes[b];
		}

		File file = new File(filepath);
		CFile.writeBinary(file, contentBytes);

		return file;
	}

	// Rebuild file name from binary coded file entry (for error logging)
	private static String rebuildFileName(byte[] entryBytes) {

		if (entryBytes == null) {
			return "unknown file";
		}

		int pathLength = 0x100 * entryBytes[0] + entryBytes[1];

		int b = 2;
		byte[] pathBytes = new byte[pathLength];
		for (; b < pathLength + 2; b++) {
			pathBytes[b - 2] = entryBytes[b];
		}

		return new String(pathBytes, StandardCharsets.UTF_8);
	}

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

	// Retrieve column values for one row from result set of SELECT statement
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private SortedMap<String, Object> retrieveRecord(ResultSet rs, ResultSetMetaData rsmd, List<String> qualifiedColumnNames) throws SQLException {

		SortedMap<String, Object> resultRecord = new TreeMap<>();

		// Retrieve all values for one result record - JDBC indexes start by 1
		for (int c = 0; c < rsmd.getColumnCount(); c++) {

			String tableName = "<unknown>";
			String columnName = rsmd.getColumnName(c + 1);
			Class<?> fieldType = null;
			Object columnValue = null;
			byte[] fileEntryBytes = null;
			try {
				// Try to determine type of associated field from registered table column by qualified column name
				SqlDbTable table = null;
				if (qualifiedColumnNames != null && qualifiedColumnNames.size() > c && qualifiedColumnNames.get(c).contains(".")) {
					table = getTableFromQualifiedColumnName(qualifiedColumnNames.get(c));
					if (table != null) {
						tableName = table.name;
						fieldType = getFieldTypeFromQualifiedColumnName(table, qualifiedColumnNames.get(c));
					}
				}
				else if (qualifiedColumnNames == null || qualifiedColumnNames.size() <= c) {
					log.warn("SQL: Field type of column '{}' could not be determined because column name list {} does not contain appropriate entry!", columnName, qualifiedColumnNames);
				}
				else if (log.isDebugEnabled()) {
					log.debug("SQL: Table name and field type of column '{}' could not be determined because column names are not prefixed by table name ({}).", columnName, qualifiedColumnNames);
				}

				// For LOB types retrieve values based on column type
				int columnType = rsmd.getColumnType(c + 1);
				if (columnType == Types.BLOB) { // Assume BLOB is byte[] - max 2GB is supported
					Blob blob = rs.getBlob(c + 1);
					if (blob != null) {
						columnValue = blob.getBytes(1, ((Number) blob.length()).intValue());
						if (fieldType == File.class) {
							columnValue = (columnValue != null ? rebuildFile((byte[]) columnValue) : null);
						}
					}
				}
				else if (columnType == Types.CLOB) { // Assume CLOB is String - max 2G characters is supported
					Clob clob = rs.getClob(c + 1);
					if (clob != null) {
						columnValue = clob.getSubString(1, ((Number) clob.length()).intValue()).toCharArray();
					}
				}
				else if (fieldType == null) {

					// If no field type given retrieve date/time column values as Java 8 date/time objects and other column values based on column type
					if (columnType == Types.TIMESTAMP || columnType == Types.TIMESTAMP_WITH_TIMEZONE) {
						columnValue = rs.getObject(c + 1, LocalDateTime.class);
					}
					else if (columnType == Types.TIME || columnType == Types.TIME_WITH_TIMEZONE) {
						columnValue = rs.getObject(c + 1, LocalTime.class);
					}
					else if (columnType == Types.DATE) {
						columnValue = rs.getObject(c + 1, LocalDate.class);
					}
					else {
						columnValue = rs.getObject(c + 1);
					}
				}
				else {
					// If field type given retrieve value based on field type and convert values if necessary
					if (fieldType == String.class) {
						columnValue = rs.getObject(c + 1, String.class);
					}
					else if (fieldType == Character.class || fieldType == char.class) {

						String string = rs.getObject(c + 1, String.class);
						columnValue = (!isEmpty(string) ? string.charAt(0) : null);
					}
					else if (fieldType == Boolean.class || fieldType == boolean.class) {
						columnValue = Boolean.valueOf(rs.getObject(c + 1, String.class));
					}
					else if (Enum.class.isAssignableFrom(fieldType)) {

						String string = rs.getObject(c + 1, String.class);
						if (string != null) {
							columnValue = Enum.valueOf((Class<? extends Enum>) fieldType, string);
						}
					}
					else if (Number.class.isAssignableFrom(Reflection.getBoxingWrapperType(fieldType))) {

						// Do not use rs.getObject(c + 1, <fieldtype>) on numerical types because 0 may be retrieved for null values!
						columnValue = rs.getObject(c + 1);
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
							columnValue = rs.getObject(c + 1, fieldType);
						}
					}
					else if (fieldType == LocalDateTime.class || fieldType == LocalDate.class || fieldType == LocalTime.class || Date.class.isAssignableFrom(fieldType) || fieldType == byte[].class) {
						columnValue = rs.getObject(c + 1, fieldType);
					}
					else if (fieldType == File.class) {

						fileEntryBytes = rs.getObject(c + 1, byte[].class);
						columnValue = (fileEntryBytes != null ? rebuildFile(fileEntryBytes) : null);
					}
					else if (fieldType == char[].class) {
						columnValue = rs.getObject(c + 1);
						if (columnValue instanceof String) {
							columnValue = ((String) columnValue).toCharArray();
						}
					}
					else {
						columnValue = rs.getObject(c + 1);
						if (columnValue instanceof String) { // Try to convert value using either registered from-string converter or declared valueOf(String) method
							columnValue = SqlDbHelpers.tryToBuildFieldValueFromStringValue(fieldType, (String) columnValue, columnName);
						}
					}
				}

				// Put column value retrieved into result map
				resultRecord.put(rsmd.getColumnLabel(c + 1).toUpperCase(), columnValue);
			}
			catch (IllegalArgumentException iaex) {

				if (fieldType != null && Enum.class.isAssignableFrom(fieldType)) {
					log.error("SQL: Column value {} cannot be converted to enum type '{}' for column '{}.{}'! ({})", CLog.forAnalyticLogging(columnValue),
							((Class<? extends Enum>) fieldType).getName(), tableName, columnName, iaex);
				}
				else {
					log.error("SQL: Value of column '{}.{}' could not be retrieved - {}", tableName, columnName, iaex);
				}
			}
			catch (IOException ioex) {
				log.error("SQL: File '{}' retrieved from column '{}.{}' cannot be written! ({})", rebuildFileName(fileEntryBytes), tableName, columnName, ioex);
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

					// Retrieve results from result set
					while (rs.next()) {
						resultRecords.add(retrieveRecord(rs, rsmd, orderedColumnNames));
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
	 *            maximum records to retrieve
	 * @param values
	 *            values to assign to placeholders for prepared statement
	 * @param requiredResultTypes
	 *            list of required types of results
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

		if (limit > 0 && getDbType() == DbType.MYSQL) {
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
	// Helpers for INSERT and UPDATE
	// -------------------------------------------------------------------------

	// Get JDBC type from Java type
	static int typeIntegerFromJdbcType(Class<?> type) {

		if (type == String.class) {
			return Types.VARCHAR;
		}
		else if (type == BigDecimal.class) {
			return Types.NUMERIC;
		}
		else if (type == Byte.class || type == Short.class) {
			return Types.SMALLINT;
		}
		else if (type == Character.class) {
			return Types.CHAR;
		}
		else if (type == Byte.class || type == Integer.class) {
			return Types.INTEGER;
		}
		else if (type == Long.class) {
			return Types.BIGINT;
		}
		else if (type == Float.class) {
			return Types.FLOAT;
		}
		else if (type == Double.class) {
			return Types.DOUBLE;
		}
		else if (type == java.sql.Timestamp.class) {
			return Types.TIMESTAMP;
		}
		else if (type == byte[].class) {
			return Types.BINARY;
		}
		else if (java.sql.Blob.class.isAssignableFrom(type)) {
			return Types.BLOB;
		}
		else if (java.sql.Clob.class.isAssignableFrom(type)) {
			return Types.CLOB;
		}
		else {
			return Types.OTHER;
		}
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
	 * @param columnValueMaps
	 *            list of maps of: IN: values to set for columns, OUT: real JDBC values set for column after Java -&gt; SQL conversion
	 * 
	 * @return batch execution results
	 * 
	 * @throws SqlDbException
	 *             if table name is null or empty or if column/value map is null or on problems registering database table
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public int[] insertInto(Connection cn, String tableName, List<SortedMap<String, Object>> columnValueMaps) throws SQLException, SqlDbException {

		// Check preconditions
		if (isEmpty(tableName)) {
			throw new SqlDbException("INSERT: Table name is empty or null!");
		}
		else if (columnValueMaps == null) {
			throw new SqlDbException("INSERT: Column/value map is null!");
		}

		if (columnValueMaps.isEmpty()) {
			if (log.isDebugEnabled()) {
				log.debug("SQL: No records to insert");
			}
			return new int[0];
		}

		// Uppercase table and column names
		tableName = tableName.toUpperCase();
		for (Map<String, Object> columnValueMap : columnValueMaps) {
			CMap.upperCaseKeysInMap(columnValueMap);
		}

		// Retrieve table columns metadata and put them into registry (if not already done)
		SqlDbTable table = registerTable(cn, tableName);

		// Build ordered set of columns to insert
		SortedSet<SqlDbColumn> columnsToInsert = new TreeSet<>();
		for (SqlDbColumn column : table.columns) {
			if (columnValueMaps.get(0).containsKey(column.name)) {
				columnsToInsert.add(column);
			}
		}

		// Check for non existent columns
		for (String columnName : (columnValueMaps.get(0).keySet())) {
			if (!table.getColumnNames().contains(columnName)) {
				throw new SQLException("SQL: INSERT: Try to set value for column '" + columnName + "' which does not exist in table '" + table.name + "'");
			}
		}

		long numberOfRecords = columnValueMaps.size();
		if (numberOfRecords > 1 && log.isDebugEnabled()) {
			log.debug("SQL: Batch insert {} records...", numberOfRecords);
		}

		// Build SQL INSERT statement
		List<SqlDbColumn> columnsWithPlaceholders = new ArrayList<>();
		StringBuilder sqlBuilder = new StringBuilder();

		sqlBuilder.append("INSERT INTO " + tableName);
		if (columnsToInsert.isEmpty()) {

			// Insert default values if no value is specified
			sqlBuilder.append(" DEFAULT VALUES");
		}
		else {
			// Columns clause
			sqlBuilder.append(" (");
			for (SqlDbColumn column : columnsToInsert) {
				sqlBuilder.append(column.name);
				sqlBuilder.append(", ");
			}

			// Values clause
			sqlBuilder.delete(sqlBuilder.lastIndexOf(","), sqlBuilder.length());
			sqlBuilder.append(") VALUES (");
			for (SqlDbColumn column : columnsToInsert) {
				Object value = columnValueMaps.get(0).get(column.name);

				if (value instanceof String && ((String) value).toUpperCase().startsWith("SELECT ")) {
					sqlBuilder.append("(" + (String) value + ")");
				}
				else if (SqlDbHelpers.isSqlFunctionCall(value)) {
					sqlBuilder.append(SqlDbHelpers.getSqlColumnExprOrFunctionCall(value));
				}
				else if (SqlDbHelpers.isOraSeqNextvalExpr(value)) {
					sqlBuilder.append(value.toString());
				}
				else {
					columnsWithPlaceholders.add(column);
					sqlBuilder.append("?");
				}
				sqlBuilder.append(", ");
			}
			sqlBuilder.delete(sqlBuilder.lastIndexOf(","), sqlBuilder.length());
			sqlBuilder.append(")");
		}
		String preparedStatementString = sqlBuilder.toString();

		if (log.isTraceEnabled()) {
			log.trace("SQL: {}", preparedStatementString);
		}

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			// Assign values to prepared statement - convert to string if to string converter is given
			for (Map<String, Object> columnValueMap : columnValueMaps) {

				for (int c = 0; c < columnsWithPlaceholders.size(); c++) {

					SqlDbColumn column = columnsWithPlaceholders.get(c);
					Object columnValue = columnValueMap.get(column.name);

					assignValue(pst, c, tableName, column, columnValue);
				}

				if (log.isDebugEnabled()) {
					log.debug("SQL: {}", SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToInsert));
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
				for (Map<String, Object> columnValueMap : columnValueMaps) {
					log.error("SQL: '{}'", SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToInsert));
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
	 * @param columnValueMap
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
	public long update(Connection cn, String tableName, Map<String, Object> columnValueMap, String whereClause) throws SQLException, SqlDbException {

		// Check preconditions
		if (isEmpty(tableName)) {
			throw new SqlDbException("UPDATE: Table name is empty or null!");
		}
		else if (columnValueMap == null) {
			throw new SqlDbException("UPDATE: Column/value map is null!");
		}

		if (columnValueMap.isEmpty()) {
			if (log.isDebugEnabled()) {
				log.debug("SQL: Column/value map empty - nothing to update");
			}
			return 0;
		}

		// Uppercase table and column names
		tableName = tableName.toUpperCase();
		CMap.upperCaseKeysInMap(columnValueMap);

		// Retrieve table columns metadata and put them into registry (if not already done)
		SqlDbTable table = registerTable(cn, tableName);

		// Build ordered set of columns to update
		SortedSet<SqlDbColumn> columnsToUpdate = new TreeSet<>();
		for (SqlDbColumn column : table.columns) {
			if (columnValueMap.containsKey(column.name)) {
				columnsToUpdate.add(column);
			}
		}

		// // Check for non existent columns
		// for (String columnName : columnValueMap.keySet()) {
		// if (!table.getColumnNames().contains(columnName)) {
		// throw new SQLException("SQL: UPDATE: Try to set column '" + columnName + "' which does not exist in table '" + table.name + "'");
		// }
		// }

		// Build SQL update statement
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("UPDATE " + tableName + " SET ");

		// Append placeholders for values
		List<SqlDbColumn> columnsWithPlaceholders = new ArrayList<>();
		for (SqlDbColumn column : columnsToUpdate) {

			sqlBuilder.append(column.name + " = ");

			Object value = columnValueMap.get(column.name);

			if (SqlDbHelpers.isSqlFunctionCall(value)) {
				sqlBuilder.append(SqlDbHelpers.getSqlColumnExprOrFunctionCall(value));
			}
			else if (SqlDbHelpers.isOraSeqNextvalExpr(value)) {
				sqlBuilder.append(value.toString());
			}
			else {
				columnsWithPlaceholders.add(column);
				sqlBuilder.append("?");
			}
			sqlBuilder.append(", ");
		}
		if (sqlBuilder.toString().contains(",")) {
			sqlBuilder.delete(sqlBuilder.lastIndexOf(","), sqlBuilder.length());
		}

		// Where clause ?
		if (whereClause != null && !whereClause.isEmpty()) {
			sqlBuilder.append(" WHERE " + whereClause);
		}

		String preparedStatementString = sqlBuilder.toString();
		if (log.isTraceEnabled()) {
			log.trace("SQL: {}", preparedStatementString);
		}

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			// Prepare update statement
			if (log.isTraceEnabled()) {
				log.trace("SQL: Update statement '{}' prepared", preparedStatementString);
			}

			// Assign values to prepared statement
			for (int c = 0; c < columnsWithPlaceholders.size(); c++) {

				SqlDbColumn column = columnsWithPlaceholders.get(c);
				Object columnValue = columnValueMap.get(column.name);

				assignValue(pst, c, tableName, column, columnValue);
			}

			if (log.isDebugEnabled()) {
				log.debug("SQL: {}", SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToUpdate));
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
					SqlDbHelpers.forSecretLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToUpdate));
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

		if (log.isDebugEnabled()) {
			log.debug("SQL: {}", preparedStatementString);
		}

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

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
	 * <p>
	 * Grabs connection from connection pool.
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
	 * Try to find already registered table by name
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
