package com.icx.jdbc;

/**
 * Exceptions thrown using SqlDB methods. Either on registering database table using JDBC methods or if prerequisites for performing SQL statements are not fulfilled (empty table, column names, wrong
 * parameter count or types, etc.) or if data cannot be converted from or to JDBC data types on SELECT, INSERT or UPDATE.
 * 
 * @author baumgrai
 */
public class SqlDbException extends Exception {

	private static final long serialVersionUID = 1L;

	public SqlDbException(
			String msg) {

		super(msg);
	}

	public SqlDbException(
			String msg,
			Throwable t) {

		super(msg, t);
	}
}
