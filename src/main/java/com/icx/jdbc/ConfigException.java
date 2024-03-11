package com.icx.jdbc;

/**
 * Exceptions thrown on missing or wrong configuration (e.g.: missing database connection string)
 * 
 * @author baumgrai
 */
public class ConfigException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor with message.
	 * 
	 * @param msg
	 *            exception message
	 */
	public ConfigException(
			String msg) {

		super(msg);
	}
}
