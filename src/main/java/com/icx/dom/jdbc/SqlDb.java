package com.icx.dom.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.EnumMap;
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
import com.icx.common.base.CLog;
import com.icx.common.base.CMap;
import com.icx.common.base.Common;
import com.icx.dom.jdbc.SqlDbTable.Column;
import com.icx.dom.jdbc.SqlDbTable.ForeignKeyColumn;
import com.icx.dom.jdbc.SqlDbTable.UniqueConstraint;

/**
 * Based on {@link ConnectionPool} provides high level methods to perform SQL SELECT, INSERT, UPDATE, DELETE statements and to CALL Stored Procedures.
 * <p>
 * {@code SqlDb} object can be created using {@link #SqlDb(Properties)} from {@link Properties} object containing <b>{@code dbConnectionString}</b>, <b>{@code dbUser}</b>, <b>{@code dbPassword}</b>
 * properties.
 * <p>
 * Using these high level methods programmer avoids to have to deal with details of JDBC programming like handling {@code ResultSet} and {@code ResultSetMetaData}. Outputs (of SELECT methods and
 * Stored Procedures) and inputs (of INSERT and UPDATE methods and Stored Procedures) are sorted maps or list of sorted maps, where such a map is the representation of one table or view record. A
 * record map contains [ column name (label) : value ] pairs and is sorted by column name (or label). The types of the column values are the JDBC types associated to the columns and therefore database
 * type specific. Use {@link #getDbType()} to check database type programmatically.
 * <p>
 * Most methods come in two types here: one with a parameter for database connection (which can, but not have to be retrieved from connection pool of this {@code SqlDb} object), the other where
 * database connection is retrieved internally from connection pool of {@code SqlDb} object. Methods of first type support database transactions spanning more than one SQL statement (using
 * non-auto-commit connections in this case).
 * <p>
 * Database tables can be <b>registered</b> using {@link #registerTable(String)} in form of {@link SqlDbTable} objects, which provide meta data of these tables and their columns. {@link SqlDbTable}
 * provides also methods to analyze table reference structure of database. {@link SqlDbTable} objects for registered tables can be retrieved by table name using {@link #findRegisteredTable(String)}.
 * Database tables involved in INSERT and UPDATE methods will be registered internally on first call for performance reasons.
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

	public static final Map<DbType, String> DB_DATE_FUNCT = new EnumMap<DbType, String>(DbType.class) {
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
	 * Behaves like {@link #SqlDb(String, String, String, int)}.
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
	 */
	public void close() throws SQLException {

		if (pool != null) {
			pool.close();
		}
	}

	// -------------------------------------------------------------------------
	// Simple information getters and setters
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
	 * @param requiredResultTypes
	 *            list of required types of results
	 * 
	 * @return result records as list of sorted column/value maps
	 * 
	 * @throws SQLException
	 *             on JDBC or SQL errors
	 */
	public List<SortedMap<String, Object>> select(Connection cn, String sql, List<Object> valuesOfPlaceholders, List<Class<?>> requiredResultTypes) throws SQLException {

		if (log.isDebugEnabled()) {
			log.debug("SQL: {}", JdbcHelpers.forLoggingSelect(sql, valuesOfPlaceholders));
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
							st.close();
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

				// Cancel timer on successful execution
				timer.cancel();

				// Build result structure
				try {
					return JdbcHelpers.buildAndLogResult(rs, requiredResultTypes);
				}
				catch (SQLException sqlex) {
					log.error("SQL: {} '{}' on retrieving results '{}' ({})", sqlex.getClass().getSimpleName(), sqlex.getMessage(), JdbcHelpers.forLoggingSelect(sql, valuesOfPlaceholders),
							exceptionStackToString(sqlex));
					throw sqlex;
				}
			}
		}
		catch (SQLException sqlex) {
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage(), JdbcHelpers.forLoggingSelect(sql, valuesOfPlaceholders)); // Log SQL statement on exception
			throw sqlex;
		}
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

		List<SortedMap<String, Object>> records = selectFrom(cn, tableName, "count(*)", whereClause, null, 0, null, null);

		return ((Number) records.iterator().next().values().iterator().next()).longValue();
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
	public List<SortedMap<String, Object>> selectFrom(Connection cn, String tableExpr, Object colExpr, String whereClause, String orderByClause, int limit, List<Object> values,
			List<Class<?>> requiredResultTypes) throws SQLException, SqlDbException {

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

		return select(cn, sql.toString(), values, requiredResultTypes);
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
	public List<SortedMap<String, Object>> selectFrom(Connection cn, String tableExpr, Object colExpr, String whereClause, String orderByClause, List<Class<?>> requiredResultTypes)
			throws SQLException, SqlDbException {

		return selectFrom(cn, tableExpr, colExpr, whereClause, orderByClause, 0, null, requiredResultTypes);
	}

	// -------------------------------------------------------------------------
	// Helpers for INSERT, UPDATE, stored procedures
	// -------------------------------------------------------------------------

	// Get JDBC type from Java type
	static int typeIntegerFromJdbcType(Class<?> type) {

		if (type == String.class) {
			return Types.VARCHAR;
		}
		else if (type == BigDecimal.class) {
			return Types.NUMERIC;
		}
		else if (type == Integer.class) {
			return Types.INTEGER;
		}
		else if (type == Long.class) {
			return Types.BIGINT;
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
	// Stored procedures
	// -------------------------------------------------------------------------

	// /**
	// * Parameter modes for SQL stored procedures: IN, OUT, INOUT
	// *
	// * @author baumgrai
	// */
	// public enum ParameterMode {
	// IN, OUT, INOUT
	// }
	//
	// /**
	// * Call SQL stored procedure.
	// * <p>
	// * Grabs connection from connection pool.
	// *
	// * @param cn
	// * database connection
	// * @param procedureName
	// * name of stored procedure
	// * @param mssqlReturnsStatus
	// * true if procedure returns a status, false otherwise (only for MS/SQL Server, ignored in Oracle and MySQL)
	// * @param parameterModes
	// * list of ParameterMode elements (IN, OUT, INOUT)
	// * @param parameters
	// * for IN/INOUT parameters: parameter value, for OUT parameters: Java type of output value expected, if output parameter is {@code sys_refcursor} (Oracle) use {@code List.class}
	// * @param outputs
	// * output parameters (empty on call)
	// *
	// * @return list of result sets
	// *
	// * @throws SqlDbException
	// * if procedure name is null or empty or size of parameters and parameter modes differ
	// * @throws SQLException
	// * on JDBC or SQL errors
	// */
	// public List<List<SortedMap<String, Object>>> callStoredProcedure(Connection cn, String procedureName, boolean mssqlReturnsStatus, List<ParameterMode> parameterModes, List<Object> parameters,
	// List<Object> outputs) throws SQLException, SqlDbException {
	//
	// // Check preconditions
	// if (CBase.isEmpty(procedureName)) {
	// throw new SqlDbException("CALL: No stored procedure specified!");
	// }
	// else if (parameters.size() != parameterModes.size()) {
	// throw new SqlDbException("CALL: # of parameter modes and parameters of stored procedure differ!");
	// }
	//
	// // Uppercase procedure name
	// procedureName = procedureName.toUpperCase();
	//
	// // Build callable SQL statement string
	// StringBuilder sqlBuilder = new StringBuilder();
	// sqlBuilder.append("{ ");
	// if (type == DbType.MS_SQL && mssqlReturnsStatus) {
	// sqlBuilder.append("? = ");
	// }
	// sqlBuilder.append("CALL " + procedureName + "(");
	// for (int i = 0; i < parameters.size(); i++) {
	// if (i > 0) {
	// sqlBuilder.append(", ");
	// }
	// sqlBuilder.append("?");
	// }
	// sqlBuilder.append(") }");
	// String callableStatementString = sqlBuilder.toString();
	//
	// if (log.isTraceEnabled()) {
	// log.trace("SQL: \t {}", callableStatementString);
	// }
	//
	// // Prepare callable statement
	// List<Object> inputValuesAndOutputJavaTypes = new ArrayList<>();
	// try (CallableStatement cst = cn.prepareCall(callableStatementString)) {
	//
	// if (log.isDebugEnabled()) {
	// log.debug("SQL: \tStored procedure call statement '{}' prepared", callableStatementString);
	// }
	//
	// List<Integer> outputParameterIndexes = new ArrayList<>();
	// List<Integer> outputParameterTypes = new ArrayList<>();
	//
	// int offset = 1;
	// if (type == DbType.MS_SQL && mssqlReturnsStatus) {
	//
	// // Register procedure return status
	// int jdbcType = Types.INTEGER;
	// cst.registerOutParameter(1, jdbcType);
	// outputParameterIndexes.add(1);
	// outputParameterTypes.add(jdbcType);
	// inputValuesAndOutputJavaTypes.add(Integer.class);
	// offset++;
	// }
	//
	// // Assign parameter values for input parameters and register output parameters
	// for (int i = 0; i < parameters.size(); i++) {
	// ParameterMode mode = parameterModes.get(i);
	// Object param = parameters.get(i);
	//
	// inputValuesAndOutputJavaTypes.add(param);
	//
	// if (mode == ParameterMode.OUT || mode == ParameterMode.INOUT) {
	//
	// int jdbcType = typeIntegerFromJdbcType((mode == ParameterMode.OUT ? (Class<?>) param : param.getClass()));
	// cst.registerOutParameter(i + offset, jdbcType);
	// outputParameterIndexes.add(i + offset);
	// outputParameterTypes.add(jdbcType);
	// }
	//
	// if (mode == ParameterMode.IN || mode == ParameterMode.INOUT) {
	// cst.setObject(i + offset, param);
	// }
	// }
	//
	// if (log.isDebugEnabled()) {
	// log.debug("SQL: {}", JdbcHelpers.forLoggingSql(callableStatementString, inputValuesAndOutputJavaTypes));
	// }
	//
	// // Call stored procedure
	// boolean hasResults = cst.execute();
	//
	// // Retrieve result sets (usually only one) and store in result list
	// List<List<SortedMap<String, Object>>> results = new ArrayList<>();
	// int rsCount = 1;
	// while (hasResults) {
	//
	// if (log.isDebugEnabled()) {
	// log.debug("SQL: {}. result set: ", rsCount++);
	// }
	//
	// // Retrieve result set
	// try (ResultSet rs = cst.getResultSet()) {
	//
	// // Build result for result set and add it to list of result maps
	// results.add(JdbcHelpers.buildAndLogResult(rs, null));
	// hasResults = cst.getMoreResults();
	// }
	// }
	//
	// // Retrieve output parameters and store in output list (in order of declaration)
	// if (outputs != null) {
	//
	// for (int i = 0; i < outputParameterIndexes.size(); i++) {
	// int index = outputParameterIndexes.get(i);
	//
	// if (outputParameterTypes.get(i) == OracleTypes.CURSOR) {
	//
	// // Oracle SYS_REFCURSOR output parameters
	// ResultSet rsn = (ResultSet) cst.getObject(index);
	// results.add(JdbcHelpers.buildAndLogResult(rsn, null));
	// }
	// else {
	// // Normal output parameters
	// outputs.add(cst.getObject(index));
	// }
	// }
	//
	// if (log.isDebugEnabled()) {
	// log.debug("SQL: Outputs (in order of declaration): {}", CLog.forAnalyticLogging(outputs));
	// }
	// }
	//
	// return results;
	// }
	// catch (SQLException sqlex) {
	// log.error("SQL: Exception '{}'on '{}'", sqlex.getMessage(), JdbcHelpers.forLoggingSql(callableStatementString, inputValuesAndOutputJavaTypes)); // Log SQL statement on exception
	// throw sqlex;
	// }
	// }

	// -------------------------------------------------------------------------
	// Insert
	// -------------------------------------------------------------------------

	/**
	 * INSERT one record into a database table on given database connection.
	 * <p>
	 * Behaves like {@link #insertInto(String, SortedMap, boolean)}.
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
	 * <p>
	 * Behaves like {@link #insertInto(String, SortedMap, boolean)}.
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
			log.info("SQL: No records to insert");
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
		SortedSet<Column> columnsToInsert = new TreeSet<>();
		for (Column column : table.columns) {
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
		List<Column> columnsWithPlaceholders = new ArrayList<>();
		StringBuilder sqlBuilder = new StringBuilder();

		sqlBuilder.append("INSERT INTO " + tableName);
		if (columnsToInsert.isEmpty()) {

			// Insert default values if no value is specified
			sqlBuilder.append(" DEFAULT VALUES");
		}
		else {
			// Columns clause
			sqlBuilder.append(" (");
			for (Column column : columnsToInsert) {
				sqlBuilder.append(column.name);
				sqlBuilder.append(", ");
			}

			// Values clause
			sqlBuilder.delete(sqlBuilder.lastIndexOf(","), sqlBuilder.length());
			sqlBuilder.append(") VALUES (");
			for (Column column : columnsToInsert) {
				Object value = columnValueMaps.get(0).get(column.name);

				if (value instanceof String && ((String) value).toUpperCase().startsWith("SELECT ")) {
					sqlBuilder.append("(" + (String) value + ")");
				}
				else if (JdbcHelpers.isSqlFunctionCall(value)) {
					sqlBuilder.append(JdbcHelpers.getSqlColumnExprOrFunctionCall(value));
				}
				else if (JdbcHelpers.isOraSeqNextvalExpr(value)) {
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
			log.trace("SQL:  \t{}", preparedStatementString);
		}

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			// Logically and/or formally convert values to SQL type (if necessary) and assign values to prepared statement
			for (Map<String, Object> columnValueMap : columnValueMaps) {

				if (log.isTraceEnabled()) {
					log.trace("SQL: \tColumn values to assign: {}", table.logColumnValueMap(columnValueMap));
				}

				for (int i = 0; i < columnsWithPlaceholders.size(); i++) {

					Object columnValue = columnValueMap.get(columnsWithPlaceholders.get(i).name);
					try {
						if (columnValue == null) {
							pst.setNull(i + 1, typeIntegerFromJdbcType(columnsWithPlaceholders.get(i).jdbcType));
						}
						else { // Normal value
							pst.setObject(i + 1, columnValue);
						}
					}
					catch (IllegalArgumentException iaex) {
						log.error("SQL: Column {} could not be set to value {} ({})", columnsWithPlaceholders.get(i).name, CLog.forAnalyticLogging(columnValue), iaex);
					}
				}

				if (log.isDebugEnabled()) {
					log.debug("SQL: {}", JdbcHelpers.forLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToInsert));
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
				log.info("In-progress record used for access synchronization in Domain persistence system could not be inserted - associated object is currently used exclusivly by another thread!");
			}
			else {
				log.error("SQL: {} '{}' on... ", sqlex.getClass().getSimpleName(), sqlex.getMessage()); // Log SQL statement(s) on exception
				for (Map<String, Object> columnValueMap : columnValueMaps) {
					log.error("SQL: '{}'", JdbcHelpers.forLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToInsert));
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
			log.info("SQL: Column/value map empty - nothing to update");
			return 0;
		}

		// Uppercase table and column names
		tableName = tableName.toUpperCase();
		CMap.upperCaseKeysInMap(columnValueMap);

		// Retrieve table columns metadata and put them into registry (if not already done)
		SqlDbTable table = registerTable(cn, tableName);

		// Build ordered set of columns to update
		SortedSet<Column> columnsToUpdate = new TreeSet<>();
		for (Column column : table.columns) {
			if (columnValueMap.containsKey(column.name)) {
				columnsToUpdate.add(column);
			}
		}

		// Check for non existent columns
		for (String columnName : columnValueMap.keySet()) {
			if (!table.getColumnNames().contains(columnName)) {
				throw new SQLException("SQL: UPDATE: Try to set column '" + columnName + "' which does not exist in table '" + table.name + "'");
			}
		}

		// Build SQL update statement
		StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("UPDATE " + tableName + " SET ");

		// Append placeholders for values
		List<Column> columnsWithPlaceholders = new ArrayList<>();
		for (Column column : columnsToUpdate) {

			sqlBuilder.append(column.name + " = ");

			Object value = columnValueMap.get(column.name);

			if (JdbcHelpers.isSqlFunctionCall(value)) {
				sqlBuilder.append(JdbcHelpers.getSqlColumnExprOrFunctionCall(value));
			}
			else if (JdbcHelpers.isOraSeqNextvalExpr(value)) {
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
			log.trace("SQL: \t{}", preparedStatementString);
		}

		try (PreparedStatement pst = cn.prepareStatement(preparedStatementString)) {

			// Prepare update statement
			if (log.isTraceEnabled()) {
				log.trace("SQL: \tUpdate statement '{}' prepared", preparedStatementString);
				log.trace("SQL: \tColumn values to assign: {}", table.logColumnValueMap(columnValueMap));
			}

			// Assign values to prepared statement
			for (int i = 0; i < columnsWithPlaceholders.size(); i++) {

				Object columnValue = columnValueMap.get(columnsWithPlaceholders.get(i).name);
				if (columnValue == null) {
					pst.setNull(i + 1, typeIntegerFromJdbcType(columnsWithPlaceholders.get(i).jdbcType));
				}
				else { // Normal value
					pst.setObject(i + 1, columnValue);
				}
			}

			if (log.isTraceEnabled()) {
				log.trace("SQL: \tColumn values assigned: {}", table.logColumnValueMap(columnValueMap));
			}

			if (log.isDebugEnabled()) {
				log.debug("SQL: {}", JdbcHelpers.forLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToUpdate));
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
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage(), JdbcHelpers.forLoggingInsertUpdate(preparedStatementString, columnValueMap, columnsToUpdate));
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
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage(), preparedStatementString); // Log SQL statement on exception
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
	private SqlDbTable registerTable(Connection cn, String tableName, ForeignKeyColumn fkColumnReferencingTable) throws SQLException, SqlDbException {

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
		SortedMap<ForeignKeyColumn, String> fkColumnMap = new TreeMap<>();

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
					Column column = table.new Column(table);

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
			try (ResultSet rs = dbmd.getImportedKeys(null, schemaName, tableName)) {

				while (rs.next()) {

					// Build foreign key column from same-name table column
					String columnName = rs.getString("FKCOLUMN_NAME").toUpperCase();
					Column column = table.findColumnByName(columnName);
					if (column == null) {
						throw new SqlDbException("Foreign key column '" + columnName + "' is not a column of '" + tableName + "'");
					}

					ForeignKeyColumn fkColumn = table.new ForeignKeyColumn(column);
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
				log.trace("SQL: \t{}", JdbcHelpers.forLoggingInsertUpdate(sql, null, null));
			}

			try (Statement st = cn.createStatement()) {

				st.setQueryTimeout(queryTimeout);

				try (ResultSet rs = st.executeQuery(sql)) {

					ResultSetMetaData rsmd = rs.getMetaData();

					for (int colIndex = 0; colIndex < rsmd.getColumnCount(); colIndex++) {

						String columnName = rsmd.getColumnName(colIndex + 1).toUpperCase();
						Column column = table.findColumnByName(columnName);
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
						UniqueConstraint constraint = table.findUniqueConstraintByName(indexName);
						if (constraint == null) {
							constraint = table.new UniqueConstraint();
							constraint.table = table;
							table.uniqueConstraints.add(constraint);
						}

						constraint.name = indexName;

						Column column = table.findColumnByName(columnName);
						if (column == null) {
							throw new SqlDbException("Column '" + columnName + "' is not a column of '" + tableName + "'");
						}
						constraint.columns.add(column);
					}
				}

				// Assign UNIQUE constraint directly to columns where a single column UNIQUE constraint exist for
				for (UniqueConstraint uc : table.uniqueConstraints) {
					if (uc.columns.size() == 1) {
						uc.columns.iterator().next().isUnique = true;
					}
				}
			}
		}
		catch (SQLException sqlex) {
			log.error("SQL: {} '{}' on '{}'", sqlex.getClass().getSimpleName(), sqlex.getMessage(), sql);
			throw sqlex;
		}

		// Add to registered tables before registering referenced tables to allow check if table is already registered
		registeredTables.add(table);

		// Register referenced tables (recursion)
		for (ForeignKeyColumn fkColumn : fkColumnMap.keySet()) {
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
