package com.icx.dom.domain;

/**
 * Exceptions thrown on domain class registration
 * 
 * @author baumgrai
 */
public class DomainException extends Exception {

	private static final long serialVersionUID = 1L;

	public DomainException(
			String msg) {

		super(msg);
	}

	public DomainException(
			String msg,
			Exception ex) {

		super(msg, ex);
	}
}
