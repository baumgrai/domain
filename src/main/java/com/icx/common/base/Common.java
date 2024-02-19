package com.icx.common.base;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * General and string helpers
 * 
 * @author baumgrai
 */
public abstract class Common {

	// -------------------------------------------------------------------------
	// Equal
	// -------------------------------------------------------------------------

	/**
	 * Check if two - may be null - objects are equal. If objects are arrays check if arrays equal (not array objects).
	 * 
	 * @param o1
	 *            first object
	 * @param o2
	 *            second object
	 * 
	 * @return true if objects both are null or objects equal, false otherwise
	 */
	public static boolean objectsEqual(Object o1, Object o2) {

		if (o1 == null && o2 == null || o1 != null && (o1.equals(o2))) {
			return true;
		}
		else if (o1 == null || o2 == null) {
			return false;
		}
		else if (o1.getClass().isArray() && o2.getClass().isArray()) {

			if (o1 instanceof byte[] && o2 instanceof byte[]) {
				return Arrays.equals((byte[]) o1, (byte[]) o2);
			}
			else if (o1 instanceof boolean[] && o2 instanceof boolean[]) {
				return Arrays.equals((boolean[]) o1, (boolean[]) o2);
			}
			else if (o1 instanceof char[] && o2 instanceof char[]) {
				return Arrays.equals((char[]) o1, (char[]) o2);
			}
			else if (o1 instanceof short[] && o2 instanceof short[]) {
				return Arrays.equals((short[]) o1, (short[]) o2);
			}
			else if (o1 instanceof int[] && o2 instanceof int[]) {
				return Arrays.equals((int[]) o1, (int[]) o2);
			}
			else if (o1 instanceof long[] && o2 instanceof long[]) {
				return Arrays.equals((long[]) o1, (long[]) o2);
			}
			else if (o1 instanceof float[] && o2 instanceof float[]) {
				return Arrays.equals((float[]) o1, (float[]) o2);
			}
			else if (o1 instanceof double[] && o2 instanceof double[]) {
				return Arrays.equals((double[]) o1, (double[]) o2);
			}
			else {
				Method m;
				try {
					m = Arrays.class.getMethod("equals", o1.getClass(), o2.getClass()); // TODO: Caching
					return (Boolean) m.invoke(null, o1, o2);
				}
				catch (NoSuchMethodException | SecurityException | IllegalAccessException | InvocationTargetException e) {
					return false;
				}
			}
		}
		else {
			return false;
		}
	}

	/**
	 * Compare two - may be null - objects
	 * 
	 * @param <T>
	 *            object type
	 * @param o1
	 *            first object
	 * @param o2
	 *            second object
	 * 
	 * @return result of o1.compareTo(o2) if both o1 and o2 are not null, 0 if both objects are null, 1 if only o2 is null and -1 if only o1 is null
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T extends Comparable> int compare(T o1, T o2) {

		if (o1 == null) {
			return (o2 == null ? 0 : -1);
		}
		else {
			return (o2 == null ? 1 : o1.compareTo(o2));
		}
	}

	// -------------------------------------------------------------------------
	// Equality checks
	// -------------------------------------------------------------------------

	/**
	 * Check if two - may be null - objects have same string representation
	 * 
	 * @param o1
	 *            first object
	 * @param o2
	 *            second object
	 * 
	 * @return true if objects both are null or toString() invocation of both objects is equal, false otherwise
	 */
	public static boolean toStringEqual(Object o1, Object o2) {
		return (o1 == null && o2 == null || o1 != null && o2 != null && o1.toString().equals(o2.toString()));
	}

	/**
	 * Check equality of two objects ignoring formal differences: (1) treat null values and empty strings, collections, maps as equal, (2) treat float, double values as equal if they are equal rounded
	 * to 5 decimals.
	 * 
	 * @param o1
	 *            first object
	 * @param o2
	 *            second object
	 * 
	 * @return true if objects logically equal, false otherwise
	 */
	public static boolean logicallyEqual(Object o1, Object o2) {

		if (objectsEqual(o1, o2)) {
			return true;
		}
		else if (o1 instanceof String && ((String) o1).isEmpty() && o2 == null || o1 == null && o2 instanceof String && ((String) o2).isEmpty()) {
			return true;
		}
		else if (o1 instanceof Collection && ((Collection<?>) o1).isEmpty() && o2 == null || o1 == null && o2 instanceof Collection && ((Collection<?>) o2).isEmpty()) {
			return true;
		}
		else if (o1 instanceof Map && ((Map<?, ?>) o1).isEmpty() && o2 == null || o1 == null && o2 instanceof Map && ((Map<?, ?>) o2).isEmpty()) {
			return true;
		}
		else if (o1 instanceof Number && o2 instanceof Number) {
			String s1 = String.format("%.5f", ((Number) o1).doubleValue());
			String s2 = String.format("%.5f", ((Number) o2).doubleValue());
			return s1.equals(s2);
		}
		else if (o1 instanceof LocalDateTime && o2 instanceof LocalDateTime) { // Skip milliseconds which will be suppressed by (date) times retrieved from database rounding 'second' value
			LocalDateTime dt1 = (LocalDateTime) o1;
			LocalDateTime dt2 = (LocalDateTime) o2;
			return (dt1.getYear() == dt2.getYear() && dt1.getMonth() == dt2.getMonth() && dt1.getDayOfMonth() == dt2.getDayOfMonth() && dt1.getHour() == dt2.getHour()
					&& dt1.getMinute() == dt2.getMinute() && (dt1.getSecond() == dt2.getSecond() || dt1.getSecond() == dt2.getSecond() + 1 || dt1.getSecond() + 1 == dt2.getSecond()));
		}
		else if (o1 instanceof LocalTime && o2 instanceof LocalTime) {
			LocalTime t1 = (LocalTime) o1;
			LocalTime t2 = (LocalTime) o2;
			return (t1.getHour() == t2.getHour() && t1.getMinute() == t2.getMinute()
					&& (t1.getSecond() == t2.getSecond() || t1.getSecond() == t2.getSecond() + 1 || t1.getSecond() + 1 == t2.getSecond()));
		}
		else {
			return toStringEqual(o1, o2);
		}
	}

	// -------------------------------------------------------------------------
	// Min/max for integers (Only because Sonarqube code analyzer has problems with Math.max()/min()
	// -------------------------------------------------------------------------

	public static int min(int a, int b) {
		return (a < b ? a : b);
	}

	public static int max(int a, int b) {
		return (a > b ? a : b);
	}

	// -------------------------------------------------------------------------
	// Check strings
	// -------------------------------------------------------------------------

	/**
	 * Check if string is null or empty
	 * 
	 * @param s
	 *            string
	 * 
	 * @return true if string is null or empty, false otherwise
	 */
	public static boolean isEmpty(String s) {
		return (s == null || s.isEmpty());
	}

	/**
	 * Check if string is a boolean value
	 * 
	 * @param str
	 *            string
	 * 
	 * @return true if string equals - independent of case - 'true' or 'false', false otherwise
	 */
	public static boolean isBoolean(String str) {
		return (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false"));
	}

	/**
	 * Check if string is an integer value
	 * 
	 * @param str
	 *            string
	 * 
	 * @return true if string can be converted to an integer, false otherwise
	 */
	public static boolean isInteger(String str) {
		try {
			Integer.parseInt(str);
		}
		catch (NumberFormatException nfe) {
			return false;
		}

		return true;
	}

	/**
	 * Check if string is a long value
	 * 
	 * @param str
	 *            string
	 * 
	 * @return true if string can be converted to an long, false otherwise
	 */
	public static boolean isLong(String str) {
		try {
			Long.parseLong(str);
		}
		catch (NumberFormatException nfe) {
			return false;
		}

		return true;
	}

	/**
	 * Check if string is a numeric (double) value
	 * 
	 * @param str
	 *            string
	 * 
	 * @return true if string can be converted to a double, false otherwise
	 */
	public static boolean isDouble(String str) {
		try {
			Double.parseDouble(str);
		}
		catch (NumberFormatException nfe) {
			return false;
		}

		return true;
	}

	/**
	 * Check if string is a date string
	 * 
	 * @param string
	 *            string
	 * @param dateFormat
	 *            date format, if null default date format will be used for parsing
	 * 
	 * @return true if string can be parsed to a date using given date format or default date format, false otherwise
	 */
	public static boolean isDate(String string, String dateFormat) {
		try {
			if (dateFormat != null)
				new SimpleDateFormat(dateFormat).parse(string);
			else
				new SimpleDateFormat().parse(string);
		}
		catch (ParseException cpex) {
			return false;
		}

		return true;
	}

	// -------------------------------------------------------------------------
	// Parse & format
	// -------------------------------------------------------------------------

	/**
	 * Parse string to Boolean
	 * <p>
	 * Uses {@link Boolean#parseBoolean(String)}.
	 * 
	 * @param booleanValueString
	 *            boolean value string
	 * @param defaultValue
	 *            value returned if value string provided is null or empty
	 * 
	 * @return Boolean value
	 */
	public static Boolean parseBoolean(String booleanValueString, Boolean defaultValue) {

		if (isEmpty(booleanValueString)) {
			return defaultValue;
		}

		return Boolean.parseBoolean(booleanValueString);
	}

	/**
	 * Format boolean value to string
	 * 
	 * @param value
	 *            boolean value
	 * @param defaultValue
	 *            value returned if value provided is null
	 * 
	 * @return boolean value string
	 */
	public static String formatBoolean(Boolean value, Boolean defaultValue) {

		if (value == null) {
			value = defaultValue;
		}

		return Boolean.toString(value);
	}

	private static final DecimalFormatSymbols symbols = ((DecimalFormat) DecimalFormat.getInstance()).getDecimalFormatSymbols();

	/**
	 * Parse string to Integer
	 * 
	 * @param intValueString
	 *            integer value string
	 * @param defaultValue
	 *            value returned if value string provided is null or empty
	 * 
	 * @return Integer value
	 * 
	 * @throws NumberFormatException
	 *             on parse error
	 */
	public static Integer parseInt(String intValueString, Integer defaultValue) throws NumberFormatException {

		if (isEmpty(intValueString)) {
			return defaultValue;
		}

		if (intValueString.contains(".") && symbols.getGroupingSeparator() == '.') {
			intValueString = intValueString.replace(".", "");
		}
		else if (intValueString.contains(",") && symbols.getGroupingSeparator() == ',') {
			intValueString = intValueString.replace(",", "");
		}

		return Integer.parseInt(intValueString);
	}

	/**
	 * Format integer value to string
	 * 
	 * @param value
	 *            integer value
	 * @param defaultValue
	 *            value returned if value provided is null
	 * 
	 * @return integer value string
	 */
	public static String formatInt(Integer value, Integer defaultValue) {

		if (value == null) {
			value = defaultValue;
		}

		return Integer.toString(value);
	}

	/**
	 * Parse string to Long
	 * 
	 * @param longValueString
	 *            long value string
	 * @param defaultValue
	 *            value returned if value string provided is null or empty
	 * 
	 * @return Long value
	 * 
	 * @throws NumberFormatException
	 *             on parse error
	 */
	public static Long parseLong(String longValueString, Long defaultValue) throws NumberFormatException {

		if (isEmpty(longValueString)) {
			return defaultValue;
		}

		if (longValueString.contains(".") && symbols.getGroupingSeparator() == '.') {
			longValueString = longValueString.replace(".", "");
		}
		else if (longValueString.contains(",") && symbols.getGroupingSeparator() == ',') {
			longValueString = longValueString.replace(",", "");
		}

		return Long.parseLong(longValueString);
	}

	/**
	 * Format long value to string
	 * 
	 * @param value
	 *            long value
	 * @param defaultValue
	 *            value returned if value provided is null
	 * 
	 * @return long value string
	 */
	public static String formatLong(Long value, Long defaultValue) {

		if (value == null) {
			value = defaultValue;
		}

		return Long.toString(value);
	}

	/**
	 * Parse string to Double
	 * 
	 * @param doubleValueString
	 *            double value string
	 * @param defaultValue
	 *            value returned if value string provided is null or empty
	 * 
	 * @return Double value
	 * 
	 * @throws NumberFormatException
	 *             on parse error
	 */
	public static Double parseDouble(String doubleValueString, Double defaultValue) throws NumberFormatException {

		if (isEmpty(doubleValueString)) {
			return defaultValue;
		}

		return Double.parseDouble(doubleValueString.replace(",", "."));
	}

	/**
	 * Format double value to string
	 * 
	 * @param value
	 *            double value
	 * @param defaultValue
	 *            value returned if value provided is null
	 * 
	 * @return double value string
	 */
	public static String formatDouble(Double value, Double defaultValue) {

		if (value == null) {
			value = defaultValue;
		}

		return Double.toString(value).replace(",", ".");
	}

	// -------------------------------------------------------------------------
	// String primitives
	// -------------------------------------------------------------------------

	/**
	 * Return substring until first occurrence of part or whole string if part is not contained
	 * 
	 * @param s
	 *            string
	 * @param p
	 *            part
	 * 
	 * @return substring
	 */
	public static String untilFirst(String s, String p) {

		if (isEmpty(s)) {
			return "";
		}

		if (!isEmpty(p) && s.contains(p)) {
			return s.substring(0, s.indexOf(p));
		}
		else {
			return s;
		}
	}

	/**
	 * Return substring from first occurrence of part or whole string if part is not contained
	 * 
	 * @param s
	 *            string
	 * @param p
	 *            part
	 * 
	 * @return substring
	 */
	public static String fromFirst(String s, String p) {

		if (isEmpty(s)) {
			return "";
		}

		if (!isEmpty(p) && s.contains(p)) {
			return s.substring(s.indexOf(p));
		}
		else {
			return s;
		}
	}

	/**
	 * Return substring behind first occurrence of part or whole string if part is not contained
	 * 
	 * @param s
	 *            string
	 * @param p
	 *            part
	 * 
	 * @return substring
	 */
	public static String behindFirst(String s, String p) {

		if (isEmpty(s)) {
			return "";
		}

		if (!isEmpty(p) && s.contains(p)) {
			return s.substring(s.indexOf(p) + p.length());
		}
		else {
			return s;
		}
	}

	/**
	 * Return substring until last occurrence of part or whole string if part is not contained
	 * 
	 * @param s
	 *            string
	 * @param p
	 *            part
	 * 
	 * @return substring
	 */
	public static String untilLast(String s, String p) {

		if (isEmpty(s)) {
			return "";
		}

		if (!isEmpty(p) && s.contains(p)) {
			return s.substring(0, s.lastIndexOf(p));
		}
		else {
			return s;
		}
	}

	/**
	 * Return substring from last occurrence of part or whole string if part is not contained
	 * 
	 * @param s
	 *            string
	 * @param p
	 *            part
	 * 
	 * @return substring
	 */
	public static String fromLast(String s, String p) {

		if (isEmpty(s)) {
			return "";
		}

		if (!isEmpty(p) && s.contains(p)) {
			return s.substring(s.lastIndexOf(p));
		}
		else {
			return s;
		}
	}

	/**
	 * Return substring behind last occurrence of part or whole string if part is not contained
	 * 
	 * @param s
	 *            string
	 * @param p
	 *            part
	 * 
	 * @return substring
	 */
	public static String behindLast(String s, String p) {

		if (isEmpty(s)) {
			return "";
		}

		if (!isEmpty(p) && s.contains(p)) {
			return s.substring(s.lastIndexOf(p) + p.length());
		}
		else {
			return s;
		}
	}

	/**
	 * Insert spaces between non-whitespace characters in string
	 * 
	 * @param string
	 *            string to insert spaces
	 * 
	 * @return string with spaces between non-whitespace characters
	 */
	public static String insertSpaces(String string) {

		if (string == null) {
			return null;
		}

		String singleCharString = "";
		string = string.trim();
		for (int i = 0; i < string.length(); i++) {
			char c = string.charAt(i);
			if (!Character.isWhitespace(c)) {
				singleCharString += c + " ";
			}
		}

		return singleCharString.trim();
	}

	/**
	 * Capitalize first letter of a string
	 * 
	 * @param s
	 *            string
	 * 
	 * @return changed string
	 */
	public static String capitalizeFirstLetter(String s) {

		String cs = null;

		if (s == null) {
			return null;
		}
		else if (s.length() > 1) {
			cs = s.substring(0, 1).toUpperCase() + s.substring(1);
		}
		else if (s.length() > 0) {
			cs = s.substring(0, 1).toUpperCase();
		}
		else {
			cs = "";
		}

		return cs;
	}

	/**
	 * Remove non-digit characters from string
	 * 
	 * @param string
	 *            string
	 * 
	 * @return string without any other characters than digits
	 */
	public static String removeNonDigits(String string) {

		if (string == null) {
			return null;
		}

		String onlyDigitString = "";
		for (int i = 0; i < string.length(); i++) {
			if (Character.isDigit(string.charAt(i))) {
				onlyDigitString += string.charAt(i);
			}
		}

		return onlyDigitString;
	}

	// -------------------------------------------------------------------------
	// String <-> List conversion
	// -------------------------------------------------------------------------

	/**
	 * String separator type: SPACE, LINE_SEPERATOR, SEMICOLON, AND, SLASH, DOT, COMMA
	 */
	public enum StringSep {
		SPACE, LINE_SEPERATOR, SEMICOLON, AND, SLASH, DOT, COMMA;

		/**
		 * Get separator as string
		 * 
		 * @return separator as string
		 */
		public String separator() {

			if (this == StringSep.SPACE) {
				return " ";
			}
			else if (this == StringSep.LINE_SEPERATOR) {
				return System.lineSeparator();
			}
			else if (this == StringSep.SEMICOLON) {
				return ";";
			}
			else if (this == StringSep.AND) {
				return "&";
			}
			else if (this == StringSep.SLASH) {
				return "/";
			}
			else if (this == StringSep.DOT) {
				return ".";
			}
			else {
				return ",";
			}
		}

		/**
		 * Get regex for separator
		 * 
		 * @return regex for separator
		 */
		public String regex() {
			if (this == StringSep.SPACE) {
				return "\\s";
			}
			else if (this == StringSep.LINE_SEPERATOR) {
				return System.lineSeparator();
			}
			else if (this == StringSep.SEMICOLON) {
				return ";";
			}
			else if (this == StringSep.AND) {
				return "&";
			}
			else if (this == StringSep.SLASH) {
				return "/";
			}
			else if (this == StringSep.DOT) {
				return "\\.";
			}
			else {
				return ",";
			}
		}
	}

	/**
	 * Convert string of separated strings to list of strings
	 * 
	 * @param string
	 *            String object containing separated strings
	 * @param stringSep
	 *            type of string separator
	 * 
	 * @return list of strings
	 */
	public static List<String> stringToList(String string, StringSep stringSep) {

		List<String> stringList = new ArrayList<>();

		if (isEmpty(string)) {
			return stringList;
		}

		if (string.startsWith("[") && string.endsWith("]")) {
			string = string.substring(1, string.length() - 1);
		}

		String[] stringArray = string.split(stringSep.regex(), -1);
		for (String s : stringArray) {
			if ("null".equals(s)) {
				stringList.add(null);
			}
			else {
				stringList.add(s.trim());
			}
		}

		return stringList;
	}

	/**
	 * Convert string of comma separated strings to list of strings
	 * 
	 * @param string
	 *            String object containing separated strings
	 * 
	 * @return list of strings
	 */
	public static List<String> stringToList(String string) {
		return stringToList(string, StringSep.COMMA);
	}

	/**
	 * Convert list of strings to string of separated strings
	 * 
	 * @param strings
	 *            list of strings
	 * @param stringSep
	 *            type of string separator
	 * 
	 * @return String object containing separated strings
	 */
	public static String listToString(List<String> strings, StringSep stringSep) {

		if (strings == null) {
			return "";
		}

		if (stringSep == null) {
			stringSep = StringSep.COMMA;
		}

		String separator = stringSep.separator();

		StringBuilder sb = new StringBuilder();
		for (String s : strings) {
			if (sb.length() == 0) {
				sb.append(s);
			}
			else {
				sb.append(separator + s);
			}
		}

		return sb.toString();
	}

	/**
	 * Convert list of strings to string of comma separated strings
	 * 
	 * @param strings
	 *            list of strings
	 * 
	 * @return String object containing separated strings
	 */
	public static String listToString(List<String> strings) {
		return listToString(strings, StringSep.COMMA);
	}

	// -------------------------------------------------------------------------
	// String <-> Map conversion
	// -------------------------------------------------------------------------

	/**
	 * Key/value separator type: EQUAL_SIGN, COLON
	 */
	public enum KeyValueSep {
		EQUAL_SIGN, COLON;

		/**
		 * Get separator as string
		 * 
		 * @return separator as string
		 */
		public String separator() {
			return (this == KeyValueSep.EQUAL_SIGN ? "=" : ":");
		}

		/**
		 * Get regex for separator
		 * 
		 * @return regex for separator
		 */
		public String regex() {
			return (this == KeyValueSep.EQUAL_SIGN ? "=" : ":");
		}
	}

	/**
	 * Convert string containing key/value pairs to key/value map
	 * 
	 * @param string
	 *            string containing key/value pairs
	 * @param elementSep
	 *            element separator type
	 * @param keyValueSep
	 *            key/value separator type
	 * 
	 * @return key/value map
	 */
	public static Map<String, String> stringToMap(String string, StringSep elementSep, KeyValueSep keyValueSep) {

		Map<String, String> map = new HashMap<>();

		if (isEmpty(string)) {
			return map;
		}

		if (string.startsWith("{") && string.endsWith("}")) {
			string = string.substring(1, string.length() - 1);
		}

		String eRegex = elementSep.regex();
		String kvRegex = keyValueSep.regex();

		String[] elements = string.split(eRegex, -1);
		for (String element : elements) {

			String[] keyValue = element.split(kvRegex, 2);
			if (keyValue.length > 0) {

				String key = keyValue[0];
				String value = (keyValue.length > 1 ? keyValue[1] : "");
				if (value.equals("null")) {
					map.put(key.trim(), null);
				}
				else {
					map.put(key.trim(), value);
				}
			}
		}

		return map;
	}

	/**
	 * Convert string containing key/value pairs to key/value map using comma (,) as element separator and equal sign (=) as key/value separator
	 * 
	 * @param string
	 *            string containing key/value pairs
	 * 
	 * @return key/value map
	 */
	public static Map<String, String> stringToMap(String string) {
		return stringToMap(string, StringSep.COMMA, KeyValueSep.EQUAL_SIGN);
	}

	/**
	 * Convert key/value map to string containing key/value pairs
	 * 
	 * @param map
	 *            key/value map
	 * @param elementSep
	 *            element separator type
	 * @param keyValueSep
	 *            key/value separator type
	 * 
	 * @return string containing key/value pairs
	 */
	public static String mapToString(Map<String, String> map, StringSep elementSep, KeyValueSep keyValueSep) {

		if (map == null)
			return "";

		String eSep = elementSep.separator();
		String kvSep = keyValueSep.separator();

		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (Entry<String, String> entry : map.entrySet()) {

			String key = entry.getKey();
			String value = entry.getValue();

			if (!first) {
				sb.append(eSep);
			}
			first = false;

			sb.append(key + kvSep + value);
		}

		return sb.toString();
	}

	/**
	 * Convert key/value map to string containing key/value pairs using comma (,) as element separator and equal sign (=) as key/value separator
	 * 
	 * @param map
	 *            key/value map
	 * 
	 * @return string containing key/value pairs
	 */
	public static String mapToString(Map<String, String> map) {
		return mapToString(map, StringSep.COMMA, KeyValueSep.EQUAL_SIGN);
	}

	/**
	 * Convert string containing key/value pairs to key/value map where value for one key may be a comma separated list if string argument contains this key multiple
	 * 
	 * @param string
	 *            string containing key/value pairs
	 * @param elementSep
	 *            element separator type
	 * @param keyValueSep
	 *            key/value separator type
	 * 
	 * @return key/value map with possibly comma separated (multi)values
	 */
	public static Map<String, String> stringToMultivalueMap(String string, StringSep elementSep, KeyValueSep keyValueSep) {

		Map<String, String> map = new HashMap<>();

		if (isEmpty(string))
			return map;

		String eRegex = elementSep.regex();
		String kvRegex = keyValueSep.regex();

		String[] elements = string.split(eRegex);
		for (String element : elements) {

			String[] keyValue = element.split(kvRegex, 2);
			if (keyValue.length > 0) {

				String key = keyValue[0].trim();
				String value = (keyValue.length > 1 ? keyValue[1] : "");
				if (value.equals("(null)")) {
					value = null;
				}
				else {
					value = value.trim();
				}

				if (map.containsKey(key)) {
					map.put(key, map.get(key) + "," + (value != null ? value : ""));
				}
				else {
					map.put(key, value);
				}
			}
		}

		return map;
	}

	// -------------------------------------------------------------------------
	// Hex string <-> byte array conversion
	// -------------------------------------------------------------------------

	/**
	 * Get byte array from string using UTF-8 encoding
	 * 
	 * @param s
	 *            string
	 * 
	 * @return bytes
	 */
	public static byte[] getBytesUTF8(String s) {

		if (s == null) {
			return new byte[0];
		}

		return s.getBytes(StandardCharsets.UTF_8);
	}

	/**
	 * Build string from byte array using UTF-8 encoding
	 * 
	 * @param bytes
	 *            bytes
	 * 
	 * @return string
	 */
	public static String getStringUTF8(byte[] bytes) {

		if (bytes == null) {
			return null;
		}

		return new String(bytes, StandardCharsets.UTF_8);
	}

	/**
	 * Build string from byte array using ANSII encoding
	 * 
	 * @param bytes
	 *            bytes
	 * 
	 * @return string
	 */
	public static String getStringANSI(byte[] bytes) {

		if (bytes == null) {
			return null;
		}

		return new String(bytes, StandardCharsets.ISO_8859_1);
	}

	/**
	 * Byte array to hex string
	 * 
	 * @param bytes
	 *            byte array
	 * 
	 * @return hex string
	 */
	public static String byteArrayToHexString(byte[] bytes) {

		if (bytes == null) {
			return null;
		}

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bytes.length; i++) {
			sb.append(String.format("%02x", bytes[i]));
		}

		return sb.toString();
	}

	/**
	 * Hex string to byte array
	 * 
	 * @param string
	 *            hex string
	 * 
	 * @return byte array
	 */
	public static byte[] hexStringToByteArray(String string) {

		if (string == null) {
			return new byte[0];
		}

		byte[] bytes = new byte[string.length() / 2];

		for (int i = 0; i < string.length() / 2; i++) {
			bytes[i] = (byte) Integer.parseInt(string.substring(2 * i, 2 * i + 2), 16);
		}

		return bytes;
	}

	// -------------------------------------------------------------------------
	// Exception stack
	// -------------------------------------------------------------------------

	/**
	 * Get exception stack as string from exception
	 * 
	 * @param t
	 *            Throwable (Exception) object
	 * 
	 * @return exception stack string
	 */
	public static String exceptionStackToString(Throwable t) {

		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		t.printStackTrace(pw);

		return sw.toString();
	}

}
