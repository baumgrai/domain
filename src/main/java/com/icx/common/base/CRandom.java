package com.icx.dom.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random helpers
 * 
 * @author baumgrai
 */
public abstract class CRandom {

	// -------------------------------------------------------------------------
	// Random selection
	// -------------------------------------------------------------------------

	/**
	 * Get random integer between 0 and bound - 1
	 * 
	 * @param bound
	 *            excluding upper limit
	 * 
	 * @return random integer
	 */
	public static int randomInt(int bound) {

		return ThreadLocalRandom.current().nextInt(0, bound);
	}

	/**
	 * Return 'true' with given probability
	 * 
	 * @param probability
	 *            probability between 0.0 and 1.0
	 * 
	 * @return true with given probability, false for 1 - probability
	 */
	public static boolean trueWithProbability(Double probability) {

		int random = randomInt(1000000);

		return (random < 1000000 * probability);
	}

	/**
	 * Random selection of k-from-n different elements
	 * 
	 * @param elements
	 *            element collection
	 * @param count
	 *            # of elements to select randomly
	 * 
	 * @return list of randomly selected elements containing min(elements.size(), count) elements
	 */
	public static <T extends Object> List<T> randomSelect(Collection<T> elements, int count) {

		// Build (random ordered) list from collection
		List<T> elementList = new ArrayList<>();
		elementList.addAll(elements);

		// Build ordered number list
		List<Integer> orderedNumbers = new ArrayList<>();
		for (int i = 0; i < elements.size(); i++) {
			orderedNumbers.add(i);
		}

		List<T> selectedElements = new ArrayList<>();
		for (int i = 0; i < Common.min(elements.size(), count); i++) {
			int random = randomInt(orderedNumbers.size());
			random = orderedNumbers.remove(random);
			selectedElements.add(elementList.get(random));
		}

		return selectedElements;
	}

	static final String CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static final int CHARSIZE = CHARS.length();

	/**
	 * Build string from n characters/digits (e.g.for initial passwords)
	 * 
	 * @param length
	 *            length of string to return
	 * 
	 * @return list of randomly selected elements containing min(elements.size(), count) elements
	 */
	public static String randomString(int length) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < Common.min(CHARSIZE, length); i++) {
			sb.append(CHARS.charAt(randomInt(CHARSIZE)));
		}

		return sb.toString();
	}

}
