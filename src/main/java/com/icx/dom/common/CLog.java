package com.icx.dom.common;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collection;

/**
 * Logging helpers
 * 
 * @author baumgrai
 */
public abstract class CLog {

	// Secret field name prefix to avoid logging value of this field
	public static final String SECRET = "secret_";

	/**
	 * Convert object value to string containing also type of value for analytic logging.
	 * <p>
	 * null: null, char: 'v', String: "v", Date/Time/Calendar types: <type>@<formatted v>, others: <type>@v.toString()
	 * <p>
	 * Method should only be used for trace and error logging because there is no mechanism to hide security critical values!
	 * 
	 * @param value
	 *            value to log
	 * 
	 * @return string representation containing value's type
	 */
	public static String forAnalyticLogging(Object value) {

		String logString = null;

		if (value == null) {
			logString = "null";
		}
		else if (value instanceof Class) {
			return "Class@" + value.toString().replace("class ", "");
		}
		else if (value instanceof Character) {
			logString = "'" + value.toString() + "'";
		}
		else if (value instanceof Boolean) {
			logString = value.getClass().getName() + "@" + value.toString();
		}
		else if (value instanceof String) {
			logString = "\"" + value.toString() + "\"";
		}
		else if (value instanceof java.sql.Date) {
			logString = value.getClass().getName() + "@" + new SimpleDateFormat(CDateTime.DATE_FORMAT).format(((java.sql.Date) value));
		}
		else if (value instanceof java.sql.Time) {
			logString = value.getClass().getName() + "@" + new SimpleDateFormat(CDateTime.TIME_MS_FORMAT).format(((java.sql.Time) value));
		}
		else if (value instanceof java.sql.Timestamp) {
			logString = value.getClass().getName() + "@" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format(((java.sql.Timestamp) value));
		}
		else if (value instanceof oracle.sql.TIMESTAMP)
			try {
				logString = value.getClass().getName() + "@" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format(((oracle.sql.TIMESTAMP) value).timestampValue());
			}
			catch (SQLException e) {
				logString = value.toString();
			}
		else if (value instanceof java.util.Date) {
			logString = value.getClass().getName() + "@" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format((java.util.Date) value);
		}
		else if (value instanceof LocalDate) {
			logString = value.getClass().getName() + "@" + ((LocalDate) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
		}
		else if (value instanceof LocalTime) {
			logString = value.getClass().getName() + "@" + ((LocalTime) value).format(DateTimeFormatter.ISO_LOCAL_TIME);
		}
		else if (value instanceof LocalDateTime) {
			logString = value.getClass().getName() + "@" + ((LocalDateTime) value).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
		}
		else if (value instanceof Calendar) {
			logString = value.getClass().getName() + "@" + new SimpleDateFormat(CDateTime.DATETIME_MS_FORMAT).format(((Calendar) value).getTime());
		}
		else if (value.toString().contains("@")) {
			logString = value.toString();
		}
		else {
			logString = value.getClass().getSimpleName() + "@" + value.toString();
		}

		return logString.substring(0, CBase.min(1024, logString.length()));
	}

	/**
	 * Convert collection of values to string for analytic logging.
	 * <p>
	 * See {@link #forAnalyticLogging(Object)}.
	 * <p>
	 * Method should only be used for trace and error logging because there is no mechanism to hide security critical values!
	 * 
	 * @param values
	 *            values to log
	 * 
	 * @return string in '[' and ']' containing comma separated string representations of given values
	 */
	public static String forAnalyticLogging(Collection<Object> values) {

		String valuesString = "[ ";
		boolean first = true;
		for (Object value : values) {
			if (!first) {
				valuesString += ", ";
			}
			first = false;
			valuesString += forAnalyticLogging(value);
		}
		valuesString += " ]";

		return valuesString;
	}

	/**
	 * Grey out secret info
	 * 
	 * @param secretInfo
	 *            secret info to replace by '*'
	 * 
	 * @return '***' of length of secret info with max length 32
	 */
	public static String greyOut(String secretInfo) {

		if (CBase.isEmpty(secretInfo)) {
			return "******";
		}
		else {
			return "********************************".substring(0, CBase.min(32, secretInfo.length()));
		}
	}

	public static boolean isSecret(String expr) {
		return (expr != null && (expr.contains(SECRET) || expr.toLowerCase().contains("passwor") || expr.toLowerCase().contains("pwd")));
	}

	/**
	 * Build string for logging of value or '***' if expression given contains 'secret_'
	 * 
	 * @param expr
	 *            expression (may be name of value to log)
	 * @param value
	 *            value to log
	 * 
	 * @return string representation for logging of '***' for secret value
	 */
	public static String forSecretLogging(String expr, Object value) {
		return forAnalyticLogging((value instanceof String && isSecret(expr) ? greyOut((String) value) : value));
	}

}
