package com.icx.common.base;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Set helpers
 * 
 * @author baumgrai
 */
public abstract class CSet {

	/**
	 * Create and initialize generic set (HashSet)
	 * 
	 * @param <E>
	 *            type of set
	 * @param firstElement
	 *            first element
	 * @param furtherElements
	 *            further elements
	 * 
	 * @return set with given element
	 */
	@SuppressWarnings("unchecked")
	public static <E> Set<E> newSet(E firstElement, Object... furtherElements) {

		Set<E> set = new HashSet<>();
		set.add(firstElement);
		for (int i = 0; i < furtherElements.length; i++) {
			set.add((E) furtherElements[i]);
		}

		return set;
	}

	/**
	 * Create and initialize generic sorted set (TreeSet)
	 * 
	 * @param <E>
	 *            type of set
	 * @param firstElement
	 *            first element
	 * @param furtherElements
	 *            further elements
	 * 
	 * @return set with given element
	 */
	@SuppressWarnings("unchecked")
	public static <E> SortedSet<E> newSortedSet(E firstElement, Object... furtherElements) {

		SortedSet<E> set = new TreeSet<>();
		set.add(firstElement);
		for (int i = 0; i < furtherElements.length; i++) {
			set.add((E) furtherElements[i]);
		}

		return set;
	}

}
