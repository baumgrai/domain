package com.icx.dom.common;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Calendar helpers
 * 
 * @author baumgrai
 */
public abstract class CDateTime {

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

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

	public static LocalDateTime subtract(LocalDateTime datetime, String intervalString) {
		return add(datetime, (intervalString.startsWith("-") ? intervalString.substring(1) : "-" + intervalString));
	}

}
