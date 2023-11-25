package com.icx.dom.common;

import java.util.Collection;

public abstract class CCollection {

	/**
	 * Check if collection is null or empty
	 * 
	 * @param collection
	 *            set
	 * 
	 * @return true if set is null or empty, false otherwise
	 */
	public static boolean isEmpty(Collection<?> collection) {
		return (collection == null || collection.isEmpty());
	}

	/**
	 * Check if all elements of second collection are contained in first collection
	 * 
	 * @param c1
	 *            first collection
	 * @param c2
	 *            second collection
	 * 
	 * @return true if all elements of second collection are contained in first collection, false otherwise
	 */
	public static boolean containsAll(Collection<?> c1, Collection<?> c2) {

		for (Object o2 : c2) {
			if (!c1.contains(o2))
				return false;
		}

		return true;
	}

	/**
	 * Check if at least one element of second collection is contained in first collection
	 * 
	 * @param c1
	 *            first collection
	 * @param c2
	 *            second collection
	 * 
	 * @return true if at least one element of second collection are contained in first collection, false otherwise
	 */
	public static boolean containsAny(Collection<?> c1, Collection<?> c2) {

		for (Object o2 : c2) {
			if (c1.contains(o2))
				return true;
		}

		return false;
	}
}
