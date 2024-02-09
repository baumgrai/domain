package com.icx.domain.sql;

/**
 * Constants names of columns and fields
 * 
 * @author baumgrai
 */
public abstract class Const {

	// Fields
	public static final String LAST_MODIFIED_IN_DB_FIELD = "lastModifiedInDb";

	// Standard columns of object tables
	public static final String ID_COL = "ID";
	public static final String DOMAIN_CLASS_COL = "DOMAIN_CLASS";
	public static final String LAST_MODIFIED_COL = "LAST_MODIFIED";

	// Columns of entry tables
	public static final String ELEMENT_COL = "ELEMENT";
	public static final String ORDER_COL = "ELEMENT_ORDER";
	public static final String KEY_COL = "ENTRY_KEY";
	public static final String VALUE_COL = "ENTRY_VALUE";

}
