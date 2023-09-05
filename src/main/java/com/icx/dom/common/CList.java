package com.icx.dom.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * List helpers
 * 
 * @author baumgrai
 */
public abstract class CList {

	// -------------------------------------------------------------------------
	// Primitives
	// -------------------------------------------------------------------------

	/**
	 * Check if list is null or empty
	 * 
	 * @param list
	 *            list
	 * 
	 * @return true if list is null or empty, false otherwise
	 */
	public static <T extends Object> boolean isEmpty(List<T> list) {
		return (list == null || list.isEmpty());
	}

	/**
	 * Get last element of a list
	 * 
	 * @param list
	 *            list
	 * 
	 * @return last element of list or null if list is empty
	 */
	public static <T extends Object> T getLast(List<T> list) {

		if (isEmpty(list)) {
			return null;
		}
		else {
			return list.get(list.size() - 1);
		}
	}

	// -------------------------------------------------------------------------
	// Create lists
	// -------------------------------------------------------------------------

	/**
	 * Create and initialize generic list
	 * 
	 * @param objects
	 *            elements
	 * 
	 * @return list of elements
	 */
	@SafeVarargs
	public static <T extends Object> List<T> newList(T... objects) {
		return new ArrayList<>(Arrays.asList(objects));
	}

	// -------------------------------------------------------------------------
	// Sort
	// -------------------------------------------------------------------------

	/**
	 * Reverse list
	 * 
	 * @param <T>
	 *            Element type
	 * @param list
	 *            list to reverse
	 * 
	 * @return new list with elements of given list in reverse order
	 */
	public static <T extends Object> List<T> reverse(List<T> list) {

		List<T> reversedList = new ArrayList<>(list);
		Collections.reverse(reversedList);

		return reversedList;
	}

	/**
	 * Sort object stream collection by given comparator
	 * 
	 * @param objectStream
	 *            stream of objects
	 * @param comparator
	 *            comparator for objects
	 * @param isAscending
	 *            true for ascending sort, false for descending sort
	 * 
	 * @return sorted list of domain objects
	 */
	public static <T extends Object> List<T> sort(Stream<T> objectStream, Comparator<T> comparator, boolean isAscending) {

		if (objectStream == null) {
			return new ArrayList<>();
		}

		if (comparator == null) {
			return objectStream.collect(Collectors.toList());
		}

		return objectStream.sorted(isAscending ? comparator : comparator.reversed()).collect(Collectors.toList());
	}

	/**
	 * Sort object collection by given comparator
	 * 
	 * @param objects
	 *            collection of objects
	 * @param comparator
	 *            comparator for objects
	 * @param isAscending
	 *            true for ascending sort, false for descending sort
	 * 
	 * @return sorted list of domain objects
	 */
	public static <T extends Object> List<T> sort(Collection<T> objects, Comparator<T> comparator, boolean isAscending) {

		if (objects == null) {
			return new ArrayList<>();
		}

		return sort(objects.stream(), comparator, isAscending);
	}

	/**
	 * Sort object collection by {@code Comparable} attribute retrieved using given getter
	 * 
	 * @param objectStream
	 *            collection of objects
	 * @param getter
	 *            object getter providing {@code Comparable} attribute for sort
	 * @param isAscending
	 *            true for ascending sort, false for descending sort
	 * 
	 * @return sorted list of domain objects
	 */
	public static <T extends Object> List<T> sort(Stream<T> objectStream, Function<T, Comparable<?>> getter, boolean isAscending) {

		if (objectStream == null) {
			return new ArrayList<>();
		}

		if (getter == null) {
			return objectStream.collect(Collectors.toList());
		}

		return objectStream.sorted((x, y) -> CBase.compare(getter.apply(isAscending ? x : y), getter.apply(isAscending ? y : x))).collect(Collectors.toList());
	}

	/**
	 * Sort object collection by {@code Comparable} attribute retrieved using given getter
	 * 
	 * @param objects
	 *            collection of objects
	 * @param getter
	 *            object getter providing {@code Comparable} attribute for sort
	 * @param isAscending
	 *            true for ascending sort, false for descending sort
	 * 
	 * @return sorted list of domain objects
	 */
	public static <T extends Object> List<T> sort(Collection<T> objects, Function<T, Comparable<?>> getter, boolean isAscending) {

		if (objects == null) {
			return new ArrayList<>();
		}

		return sort(objects.stream(), getter, isAscending);
	}

}
