package com.icx.dom.domain;

/**
 * Non system critical exceptions thrown using domain object persistence mechanism
 * 
 * @author baumgrai
 */
public class ObjectNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public ObjectNotFoundException(String msg) {

		super(msg);
	}

	public ObjectNotFoundException(String msg,
			Exception ex) {

		super(msg, ex);
	}
}
