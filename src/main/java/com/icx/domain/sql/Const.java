package com.icx.domain.sql;

/**
 * Constants names of columns and fields
 * 
 * @author baumgrai
 */
public abstract class Const {

	/**
	 * Name of field containing object id.
	 */
	public static final String ID_FIELD = "id";

	/**
	 * Name of field containing object's last modification date.
	 */
	public static final String LAST_MODIFIED_IN_DB_FIELD = "lastModifiedInDb";

	// Standard columns of object tables

	/**
	 * Name of column containing domain object id
	 */
	public static final String ID_COL = "ID";

	/**
	 * Name of column containing domain object class (highest class in inheritance stack) of domain object
	 */
	public static final String DOMAIN_CLASS_COL = "DOMAIN_CLASS";

	/**
	 * Name of column containing last modified date
	 */
	public static final String LAST_MODIFIED_COL = "LAST_MODIFIED";

	// Columns of entry tables

	/**
	 * Name of column containing element of collection
	 */
	public static final String ELEMENT_COL = "ELEMENT";

	/**
	 * Name of column containing element order (for lists)
	 */
	public static final String ORDER_COL = "ELEMENT_ORDER";

	/**
	 * Name of column containing key of map
	 */
	public static final String KEY_COL = "ENTRY_KEY";

	/**
	 * Name of column containing value of map
	 */
	public static final String VALUE_COL = "ENTRY_VALUE";

}
