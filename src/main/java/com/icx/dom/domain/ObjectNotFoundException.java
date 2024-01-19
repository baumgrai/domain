package com.icx.dom.domain;

/**
 * Exception thrown if an object is not contained in object store ('registered') on try to access this object using {@link DomainController#get(Class, long)}.
 * 
 * @author baumgrai
 */
public class ObjectNotFoundException extends Exception {

	private static final long serialVersionUID = 1L;

	public ObjectNotFoundException(
			String msg) {

		super(msg);
	}

	public ObjectNotFoundException(
			String msg,
			Exception ex) {

		super(msg, ex);
	}
}
