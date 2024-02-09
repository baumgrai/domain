package com.icx.jdbc;

/**
 * Exceptions thrown on missing or wrong configuration (e.g. missing database connection string)
 * 
 * @author baumgrai
 */
public class ConfigException extends Exception {

	private static final long serialVersionUID = 1L;

	public ConfigException(
			String msg) {

		super(msg);
	}

	public ConfigException(
			String msg,
			Exception ex) {

		super(msg, ex);
	}
}
