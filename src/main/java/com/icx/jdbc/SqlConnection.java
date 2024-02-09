package com.icx.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Autoclosable database connection class based on {@link ConnectionPool}.
 * 
 * @author baumgrai
 */
public class SqlConnection implements AutoCloseable {

	static final Logger log = LoggerFactory.getLogger(SqlConnection.class);

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	public Connection cn = null;

	private ConnectionPool pool = null;

	// -------------------------------------------------------------------------
	// Constructor
	// -------------------------------------------------------------------------

	private SqlConnection(
			ConnectionPool pool,
			boolean autoCommit) throws SQLException { // Initialization must be done in constructor to satisfy Sonarqube (which complains if code is in open() method)

		this.pool = pool;
		cn = pool.getConnection();
		cn.setAutoCommit(autoCommit);
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * Open autoclosable database connection
	 * 
	 * @param pool
	 *            connection pool
	 * @param autoCommit
	 *            auto commit mode for connection
	 * 
	 * @return autoclosable database connection object
	 * 
	 * @throws SQLException
	 *             on error establishing database connection
	 */
	public static SqlConnection open(ConnectionPool pool, boolean autoCommit) throws SQLException {
		return new SqlConnection(pool, autoCommit);
	}

	@Override
	public void close() {

		if (cn == null) {
			return;
		}

		try {
			// Commit open transaction on non-auto commit connections
			if (!cn.getAutoCommit()) {
				cn.commit();
				if (log.isDebugEnabled()) {
					log.debug("SCO: Transaction committed on returning connection to pool");
				}
			}
		}
		catch (SQLException e) {
			log.error("SCO: Exception occurred on committing: ", e);
		}

		pool.returnConnection(cn);
	}
}
