package com.icx.common.base;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Logging helpers
 * 
 * @author baumgrai
 */
public abstract class CLog {

	/**
	 * Check if logging level is enabled for logger.
	 * 
	 * @param logger
	 *            logger
	 * @param level
	 *            level to check
	 * 
	 * @return true if level is enabled, false otherwise
	 */
	public static boolean isEnabled(Logger logger, Level level) {

		if (level == Level.TRACE) {
			return logger.isTraceEnabled();
		}
		else if (level == Level.DEBUG) {
			return logger.isDebugEnabled();
		}
		else if (level == Level.INFO) {
			return logger.isInfoEnabled();
		}
		else if (level == Level.WARN) {
			return logger.isWarnEnabled();
		}
		else if (level == Level.ERROR) {
			return logger.isErrorEnabled();
		}
		else {
			return false;
		}
	}

	/**
	 * Log text message using given logging level.
	 * 
	 * @param logger
	 *            logger
	 * @param level
	 *            logging level
	 * @param text
	 *            text message to log
	 */
	public static void log(Logger logger, Level level, String text) {

		if (level == Level.TRACE && logger.isTraceEnabled()) {
			logger.trace(text);
		}
		else if (level == Level.DEBUG && logger.isDebugEnabled()) {
			logger.debug(text);
		}
		else if (level == Level.INFO && logger.isInfoEnabled()) {
			logger.info(text);
		}
		else if (level == Level.WARN && logger.isWarnEnabled()) {
			logger.warn(text);
		}
		else if (level == Level.ERROR && logger.isErrorEnabled()) {
			logger.error(text);
		}
	}

	// Secret field name prefix to avoid logging value of this field
	public static final String SECRET = "secret_";

	/**
	 * Convert object value to string containing also type of value for analytic logging.
	 * <p>
	 * null: null, char: 'v', String: "v", Date/Time/Calendar types: type@formatted_value, others: type@v.toString()
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

		return logString.substring(0, Common.min(1024, logString.length()));
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
	 * Grey out secret info.
	 * 
	 * @param secretInfo
	 *            secret info to replace by '*'
	 * 
	 * @return '***' of length of secret info with max length 32
	 */
	public static String greyOut(String secretInfo) {

		if (Common.isEmpty(secretInfo)) {
			return "******";
		}
		else {
			return "********************************".substring(0, Common.min(32, secretInfo.length()));
		}
	}

	/**
	 * Check if a name forces secret logging of value.
	 * <p>
	 * Secret logging of value is forced for names containing '_secret', 'pwd' and 'passwor'.
	 * 
	 * @param name
	 *            name to check
	 * 
	 * @return true if secret logging is forced, false otherwise
	 */
	public static boolean isSecret(String name) {
		return (name != null && (name.contains(SECRET) || name.toLowerCase().contains("passwor") || name.toLowerCase().contains("pwd")));
	}

	/**
	 * Build string for logging of value or '***' if expression given contains 'secret_' or 'passwor' or 'pwd'.
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

	/**
	 * Returns indentation string of count tabs.
	 * 
	 * @param count
	 *            number of tabs for indentation
	 * 
	 * @return indentation string
	 */
	public static String tabs(int count) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) {
			sb.append("\t");
		}
		return sb.toString();
	}
}
