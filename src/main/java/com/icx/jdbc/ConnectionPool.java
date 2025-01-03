package com.icx.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CProp;
import com.icx.common.Common;

/**
 * JDBC database {@link Connection} pool.
 * <p>
 * Manages pooled database connections for given <b>database connection string</b>, <b>user</b> and <b>password</b> independently of database type. Connection pool contains already opened database
 * connections which then will be (re)used by following connection requests.
 * <p>
 * {@code ConnectionPool} object can be created using {@link #ConnectionPool(Properties)} from {@link Properties} object containing <b>{@code dbConnectionString}</b>, <b>{@code dbUser}</b>,
 * <b>{@code dbPassword}</b> properties or directly using {@link #ConnectionPool(String, String, String, int)}.
 * <p>
 * A maximum of {@code poolSize} open connections will be kept in pool and reused on connection requests using {@link #getConnection()}. If # of open connections in pool equals pool size, additional
 * connections will be closed physically on {@link #returnConnection(Connection)}. Connection request on full pool enforces physically opening new database connections. If pool size is
 * {@code UNLIMITED} (recommended) connections generally won't be closed physically before closing pool (which also means # of open connections in pool equals maximum # of connections used at the same
 * time in the past). Pool size == 0 disables pooling function: every connection requested is immediately closed physically on {@code returnConnection(Connection)}, so no unused connections stay open
 * and a new physical database connection will automatically be opened for any connection request (performance!).
 * 
 * @author baumgrai
 */
public class ConnectionPool extends Common {

	private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	// JDBC properties

	/**
	 * Property name for database connection string property (in 'domain.properties')
	 */
	public static final String DB_CONNECTION_STRING_PROP = "dbConnectionString";

	/**
	 * Property name for database user property (in 'domain.properties')
	 */
	public static final String DB_USER_PROP = "dbUser";

	/**
	 * Property name for database password property (in 'domain.properties')
	 */
	public static final String DB_PASSWORD_PROP = "dbPassword";

	/**
	 * Property name for pool size property (in 'domain.properties')
	 */
	public static final String POOL_SIZE_PROP = "poolSize";

	public static final int UNLIMITED = -1;

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Pool size
	private int poolSize = UNLIMITED;

	// Pooled connections
	private Set<Connection> connectionsInPool = ConcurrentHashMap.newKeySet();
	private Set<Connection> connectionsInUse = ConcurrentHashMap.newKeySet();

	// Database connection string and credentials
	private String dbConnectionString = null;
	private String user = null;
	private String password = null;

	// -------------------------------------------------------------------------
	// Constructor and close
	// -------------------------------------------------------------------------

	private void initPool(String dbConnectionString, String dbUser, String dbPassword, int poolSize) throws ConfigException {

		if (isEmpty(dbConnectionString)) {
			throw new ConfigException("Database connection string is null or empty!");
		}

		this.dbConnectionString = dbConnectionString;
		this.user = dbUser;
		this.password = dbPassword;
		this.poolSize = max(UNLIMITED, poolSize);

		log.info("SQL: Connection pool for database '{}' {} with {} created", this.dbConnectionString, (!isEmpty(user) ? " and user '" + user + "'" : ""),
				(this.poolSize == UNLIMITED ? "unlimited pool size" : "pool size " + this.poolSize));
	}

	/**
	 * Constructor from database parameters.
	 * 
	 * @param dbConnectionString
	 *            database connection string (e.g.: "jdbc:sqlserver://localhost;instanceName=MyInstance;databaseName=MyDb", "jdbc:oracle:thin:@//localhost:1521/xe")
	 * @param dbUser
	 *            database user or null
	 * @param dbPassword
	 *            password or null
	 * @param poolSize
	 *            maximum # of open connections kept in connection pool, -1 for unlimited number of connections
	 * 
	 * @throws ConfigException
	 *             if connection string is null or empty
	 */
	public ConnectionPool(
			String dbConnectionString,
			String dbUser,
			String dbPassword,
			int poolSize) throws ConfigException {

		initPool(dbConnectionString, dbUser, dbPassword, poolSize);
	}

	/**
	 * Constructor from properties object with database properties.
	 * 
	 * @param databaseProperties
	 *            {@code Properties} object which must contain the following properties: {@code dbConnectionString}, {@code dbUser}, {@code dbPassword} and can optionally contain {@code poolSize}
	 *            (defaults to UNLIMITED).
	 * 
	 * @throws ConfigException
	 *             if connection string is null or empty
	 */
	public ConnectionPool(
			Properties databaseProperties) throws ConfigException {

		//@formatter:off
		initPool(
				CProp.getStringProperty(databaseProperties, DB_CONNECTION_STRING_PROP, null), 
				CProp.getStringProperty(databaseProperties, DB_USER_PROP, null),
				CProp.getStringProperty(databaseProperties, DB_PASSWORD_PROP, null), 
				CProp.getIntProperty(databaseProperties, POOL_SIZE_PROP, UNLIMITED));
		//@formatter:on
	}

	/**
	 * Close database connection pool and all open database connections.
	 * 
	 * @throws SQLException
	 *             on database error
	 */
	public void close() throws SQLException {

		log.info("SQL: Close connection pool");

		for (Connection cn : connectionsInPool) {
			cn.close();
			if (log.isDebugEnabled()) {
				log.debug("SQL: Physically closed cached connection");
			}
		}

		for (Connection cn : connectionsInUse) {
			cn.close();
			if (log.isDebugEnabled()) {
				log.debug("SQL: Physically closed connection in use");
			}
		}
	}

	// -------------------------------------------------------------------------
	// Pooled connections
	// -------------------------------------------------------------------------

	// Retrieves physical database connection from JDBC driver manager
	private static Connection getPhysicalConnection(String dbConnectionString, String user, String password) throws SQLException {

		Connection cn = null;

		if (isEmpty(user)) {
			cn = DriverManager.getConnection(dbConnectionString);
		}
		else {
			cn = DriverManager.getConnection(dbConnectionString, user, password);
		}

		log.info("SQL: Established connection to database '{}' {}", dbConnectionString, (!isEmpty(user) ? "' for user '" + user + "'" : ""));

		return cn;
	}

	/**
	 * Get a pooled database connection for given credentials.
	 * 
	 * @return open database connection
	 * 
	 * @throws SQLException
	 *             on database (access) error
	 */
	public synchronized Connection getConnection() throws SQLException {

		if (log.isTraceEnabled()) {
			log.trace("SQL: Connection pool for database '{}' with size: {} has {} unused open connections and {} connections currently in use", dbConnectionString, poolSize, connectionsInPool.size(),
					connectionsInUse.size());
		}

		Connection cn = null;
		Iterator<Connection> it = connectionsInPool.iterator();
		while (it.hasNext()) {

			// Try to use first connection in pool and remove this from pool
			cn = it.next();
			it.remove();

			// Store only valid connections
			if (cn != null && cn.isValid(0)) {

				if (log.isTraceEnabled()) {
					log.trace("SQL: Open connection got from pool");
				}
				break;
			}
			else {
				cn = null;
				log.info("SQL: Connection was invalidated by driver and was now removed from pool");
			}
		}

		// Acquire new physical connection if pool does not contain any reusable connection
		if (cn == null) {
			cn = getPhysicalConnection(dbConnectionString, user, password);
		}

		// Add connection to currently used connections
		connectionsInUse.add(cn);

		return cn;
	}

	// Check if pool contains maximum available connections
	private boolean isPoolOverflow() {
		return (poolSize != UNLIMITED && connectionsInPool.size() >= poolSize);
	}

	/**
	 * Close pooled database connection.
	 * <p>
	 * If pool size = 0 or # of open connections in pool = pool size closes connection physically, otherwise leaves connection open and returns it to pool.
	 * 
	 * @param cn
	 *            database connection to close
	 */
	public synchronized void returnConnection(Connection cn) {

		try {
			// Check preconditions
			if (cn == null) {
				log.error("SQL: Database connection to close is null!");
				return;
			}
			else if (cn.isClosed()) {
				log.warn("SQL: Database connection to close is already closed!");
				return;
			}
			else if (!connectionsInUse.contains(cn)) {
				log.error("SQL: Database connection to close does not belong to this connection pool! Leave connection open.");
				return;
			}

			// Remove from used connection pool
			connectionsInUse.remove(cn);

			boolean physically = false;
			if (isPoolOverflow()) {

				// Close physically on pool overflow
				log.info("SQL: Pool size {} exceeded! Connection to database '{}' physically closed and not returned to pool", poolSize, dbConnectionString);
				physically = true;
			}

			if (physically) {

				// Close connection physically
				log.info("SQL: Connection to database '{}' physically closed", dbConnectionString);
				cn.close();
			}
			else {
				// Add open connection to pool
				connectionsInPool.add(cn);
				if (log.isTraceEnabled()) {
					log.trace("SQL: Connection to database '{}' returned to pool", dbConnectionString);
				}
			}
		}
		catch (SQLException sqlex) {
			log.error("SQL: Exception on returning connection to pool: ", sqlex);
		}
	}

}
