package com.icx.common.base;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Calendar helpers
 * 
 * @author baumgrai
 */
public abstract class CDateTime {

	// Finals

	// Date/time formatters
	public static final String INTERNATIONAL_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
	public static final String INTERNATIONAL_DATETIME_MS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String DATE_FORMAT_DE = "dd.MM.yyyy";

	public static final String TIME_FORMAT_HM = "HH:mm";
	public static final String TIME_FORMAT = "HH:mm:ss";
	public static final String TIME_MS_FORMAT = "HH:mm:ss.SSS";

	public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DATETIME_FORMAT_DE = "dd.MM.yyyy HH:mm:ss";
	public static final String DATETIME_MS_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

	// Methods

	/**
	 * Add an interval defined as string ("500ms", "2m30s", "1y3M") to a LocalDateTime object
	 * 
	 * @param datetime
	 *            LocalDateTime object
	 * @param intervalString
	 *            interval string
	 * 
	 * @return LocalDateTime object with computed value
	 */
	public static LocalDateTime add(LocalDateTime datetime, String intervalString) {

		LocalDateTime changedDateTime = (datetime != null ? LocalDateTime.parse(datetime.toString()) : LocalDateTime.now());

		if (Common.isEmpty(intervalString)) {
			return changedDateTime;
		}

		int sign = 1;
		if (intervalString.startsWith("-")) {
			sign = -1;
			intervalString = intervalString.substring(1);
		}

		intervalString = intervalString.replace("ms", "f");

		List<ChronoUnit> calUnits = CList.newList(ChronoUnit.YEARS, ChronoUnit.MONTHS, ChronoUnit.DAYS, ChronoUnit.HOURS, ChronoUnit.MINUTES, ChronoUnit.SECONDS, ChronoUnit.MILLIS);
		List<String> calUnitNames = CList.newList("y", "M", "d", "h", "m", "s", "f");

		for (int i = 0; i < calUnits.size() && !Common.isEmpty(intervalString); i++) {

			String[] array = intervalString.split(calUnitNames.get(i), 2);
			if (array.length > 1) {
				changedDateTime = changedDateTime.plus((long) sign * Common.parseInt(array[0], 0), calUnits.get(i));
				intervalString = array[1];
			}
		}

		return changedDateTime;
	}

	/**
	 * Subtract an interval defined as string ("500ms", "2m30s", "1y3M") from a LocalDateTime object
	 * 
	 * @param datetime
	 *            LocalDateTime object
	 * @param intervalString
	 *            interval string
	 * 
	 * @return LocalDateTime object with computed value
	 */
	public static LocalDateTime subtract(LocalDateTime datetime, String intervalString) {
		return add(datetime, (intervalString.startsWith("-") ? intervalString.substring(1) : "-" + intervalString));
	}

	/**
	 * Return the amount of a given chrono unit determined by an interval string ("500ms", "2m30s", "1y3M")
	 * 
	 * @param unit
	 *            chrono unit (seconds, days, etc.)
	 * @param intervalString
	 *            interval string
	 * 
	 * @return amount of given chrono unit determined by the interval string
	 */
	public static long intervalIn(ChronoUnit unit, String intervalString) {

		LocalDateTime now = LocalDateTime.now();
		LocalDateTime added = add(now, intervalString);

		return unit.between(now, added);
	}

}
