package com.icx.common.base;

import java.util.List;

/**
 * Array helpers
 * 
 * @author baumgrai
 */
public class CArray {

	// -------------------------------------------------------------------------
	// Manipulation
	// -------------------------------------------------------------------------

	/**
	 * Build object array from one object array and another object array
	 * 
	 * @param additionalObjectArray
	 *            second objects array
	 * @param objectArray
	 *            first objects array
	 * 
	 * @return objects array = first array + second array
	 */
	public static Object[] sum(Object[] additionalObjectArray, Object... objectArray) {

		Object[] allAttributes = new Object[objectArray.length + additionalObjectArray.length];

		for (int a = 0; a < objectArray.length; a++) {
			allAttributes[a] = objectArray[a];
		}

		for (int a = 0; a < additionalObjectArray.length; a++) {
			allAttributes[objectArray.length + a] = additionalObjectArray[a];
		}

		return allAttributes;
	}

	// -------------------------------------------------------------------------
	// Create arrays
	// -------------------------------------------------------------------------

	/**
	 * Create and initializes object array
	 * 
	 * @param objects
	 *            objects to build array from
	 * 
	 * @return object array containing given objects in given order
	 */
	public static Object[] newObjectArray(Object... objects) {
		return objects;
	}

	/**
	 * Create and initializes string array
	 * 
	 * @param strings
	 *            strings to build array from
	 * 
	 * @return string array containing given strings in given order
	 */
	public static String[] newStringArray(String... strings) {
		return strings;
	}

	// -------------------------------------------------------------------------
	// Conversion
	// -------------------------------------------------------------------------

	/**
	 * Convert a string list to a string array
	 * 
	 * @param strings
	 *            string list
	 * 
	 * @return string array
	 */
	public static String[] toStringArray(List<String> strings) {
		return strings.toArray(new String[0]);
	}

	/**
	 * Convert a object list to a object array
	 * 
	 * @param objects
	 *            object list
	 * 
	 * @return object array
	 */
	public static Object[] toArray(List<Object> objects) {
		return objects.toArray(new Object[0]);
	}

}
