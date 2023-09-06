package com.icx.dom.common;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Basic general and string helpers
 * 
 * @author baumgrai
 */
public abstract class Common {

	// -------------------------------------------------------------------------
	// Equal
	// -------------------------------------------------------------------------

	/**
	 * Check if two - may be null - objects are equal
	 * 
	 * @param o1
	 *            first object
	 * @param o2
	 *            second object
	 * 
	 * @return true if objects both are null or objects equal, false otherwise
	 */
	public static boolean objectsEqual(Object o1, Object o2) {
		return (o1 == null && o2 == null || o1 != null && o1.equals(o2));
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
	 * @return result of o1.compareTo(o2) if o1 is not null, 0 if both objects are null and -1 if only o1 is null
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
		else if (o1 instanceof byte[] && o2 instanceof byte[]) {
			return Arrays.equals((byte[]) o1, (byte[]) o2);
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
		else {
			return false;
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

	private static final DecimalFormatSymbols symbols = ((DecimalFormat) NumberFormat.getInstance()).getDecimalFormatSymbols();

	/**
	 * Parse string to Integer
	 * 
	 * @param intValueString
	 *            integer value string
	 * @param defaultValue
	 *            value returned if value string provided is null or empty
	 * 
	 * @return Integer value
	 */
	public static Integer parseInt(String intValueString, Integer defaultValue) {

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
	public static Long parseLong(String longValueString, Long defaultValue) {

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
	public static Double parseDouble(String doubleValueString, Double defaultValue) {

		if (isEmpty(doubleValueString)) {
			return defaultValue;
		}

		return Double.parseDouble(doubleValueString.replace(",", "."));
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
