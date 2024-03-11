package com.icx.domain;

/**
 * Exceptions thrown on domain class registration errors.
 * 
 * @author baumgrai
 */
public class DomainException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructor
	 * 
	 * @param msg
	 *            exception message
	 */
	public DomainException(
			String msg) {

		super(msg);
	}
}
