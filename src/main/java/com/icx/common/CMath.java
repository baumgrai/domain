package com.icx.common;

import java.util.Collection;

/**
 * Numerical helpers
 * 
 * @author baumgrai
 */
public abstract class CMath {

	/**
	 * Build sum of numbers
	 * 
	 * @param numbers
	 *            numbers to sum up
	 * 
	 * @return sum
	 */
	public static Number sum(Collection<?> numbers) {

		Number sum = 0;
		for (Object o : numbers) {
			sum = sum.doubleValue() + ((Number) o).doubleValue();
		}

		return sum;
	}

	/**
	 * Build double percentage value of part from total.
	 * <p>
	 * Avoids divide by zero exception.
	 * 
	 * @param part
	 *            absolute value of part
	 * @param total
	 *            absolute value of total
	 * 
	 * @return percentage value of part of total (may be &gt; 100 or &lt; 0) or 0 if total is 0
	 */
	public static double percentage(Number part, Number total) {

		if (total.doubleValue() == 0L) {
			return 0L;
		}

		return (part.doubleValue() / total.doubleValue()) * 100;
	}

	/**
	 * Build rounded integer percentage value of part from total.
	 * 
	 * @param part
	 *            absolute value of part
	 * @param total
	 *            part absolute value of total
	 * 
	 * @return rounded integer percentage value of part of total (may be &gt; 100 or &lt; 0) or 0 if total is 0
	 */
	public static int intPercentage(Number part, Number total) {
		return (int) (Math.round(percentage(part, total)));
	}

	/**
	 * Determine maximum value of integers
	 * 
	 * @param integers
	 *            integers to build max of
	 * 
	 * @return max
	 */
	public static int max(Collection<Integer> integers) {

		int max = Integer.MIN_VALUE;
		for (int i : integers) {
			if (i > max) {
				max = i;
			}
		}

		return max;
	}

	/**
	 * Determine maximum value of doubles
	 * 
	 * @param doubles
	 *            doubles to build max of
	 * 
	 * @return max
	 */
	public static double maxDouble(Collection<Double> doubles) {

		double max = Double.MIN_VALUE;
		for (double d : doubles) {
			if (d > max) {
				max = d;
			}
		}

		return max;
	}

	/**
	 * Determine minimum value of integers
	 * 
	 * @param integers
	 *            integers to build min of
	 * 
	 * @return min
	 */
	public static int min(Collection<Integer> integers) {

		int min = Integer.MAX_VALUE;
		for (int i : integers) {
			if (i < min) {
				min = i;
			}
		}

		return min;
	}

	/**
	 * Determine minimum value of doubles
	 * 
	 * @param doubles
	 *            doubles to build min of
	 * 
	 * @return min
	 */
	public static double minDouble(Collection<Double> doubles) {

		double min = Double.MAX_VALUE;
		for (double d : doubles) {
			if (d < min) {
				min = d;
			}
		}

		return min;
	}

	/**
	 * Format double value with maximum precision and eventually given decimal separator.
	 * 
	 * @param d
	 *            double value to format
	 * @param maxPrecision
	 *            maximum precision
	 * @param decimalSeparatorOrNull
	 *            "." or "," to explicitly define decimal separator or null to use locale-specific separator
	 * 
	 * @return double value formatted as string
	 */
	public static String formatDouble(Double d, int maxPrecision, String decimalSeparatorOrNull) {

		if (d == null) {
			return "NaN";
		}

		String doubleString = null;

		int n = 0;
		for (n = 0; n < maxPrecision; n++) {

			Double nthPowerOf10 = Math.pow(10, n);
			Double d0 = Double.valueOf(((Double) (nthPowerOf10 * d)).longValue());
			Double d1 = nthPowerOf10 * d;

			if (d0.equals(d1)) {
				break;
			}
		}

		doubleString = String.format("%." + n + "f", d);
		if (decimalSeparatorOrNull != null)
			doubleString = doubleString.replace(".", decimalSeparatorOrNull).replace(",", decimalSeparatorOrNull);

		return doubleString;
	}
}
