package com.icx.dom.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CBase.KeyValueSep;
import com.icx.dom.common.CBase.StringSep;

/**
 * Enhanced property management supporting properties for different runtime environments.
 * <p>
 * Provides general property helpers and methods supporting environment specific properties - which means multiple properties with same name but with different values for different runtime
 * environments. To mark a property as environment specific environment path (like 'local/oracle' or 'customer1/prod') must precede property name separated by '/'. For Servlet based web applications
 * current environment path can be defined in {@code web.xml} context parameter {@code environment}.
 */
public abstract class Prop {

	static final Logger log = LoggerFactory.getLogger(Prop.class);

	// -------------------------------------------------------------------------
	// Retrieve properties
	// -------------------------------------------------------------------------

	/**
	 * Read properties from properties file.
	 * 
	 * @param propertiesFile
	 *            properties File object
	 * 
	 * @return Properties object (empty if file is null)
	 * 
	 * @throws IOException
	 *             if if properties file is does not exist or is not accessible
	 */
	public static Properties readProperties(File propertiesFile) throws IOException {

		if (propertiesFile == null) {
			return new Properties();
		}

		Properties properties = new Properties();
		try (InputStream is = new FileInputStream(propertiesFile)) {
			properties.load(is);
		}

		return properties;
	}

	/**
	 * Read properties which best match given environment.
	 * <p>
	 * Selects properties which match most of the given environment path parts in their given order.
	 * 
	 * @param propertiesFile
	 *            properties File
	 * @param environmentPath
	 *            environment path (e.g. from context parameter {@code environment} of {@code web.xml}) - used to select properties for given runtime environment
	 * @param mandatoryPropertyNames
	 *            IN: null or names of properties which should exist - missing one or more leads to warning message, OUT: names of properties missed
	 * 
	 * @return best matching properties or empty properties object if file is null
	 * 
	 * @throws IOException
	 *             if if properties file is does not exist or is not accessible
	 */
	public static Properties readEnvironmentSpecificProperties(File propertiesFile, String environmentPath, List<String> mandatoryPropertyNames) throws IOException {

		if (propertiesFile == null) {
			return new Properties();
		}

		// Read all properties
		Properties properties = readProperties(propertiesFile);

		// Find matching properties for environment
		Properties[] matchingProperties = new Properties[properties.size()];
		for (Object key : properties.keySet()) {
			String qualifiedPropertyName = (String) key;

			List<String> environmentSelectors = CBase.stringToList(qualifiedPropertyName, StringSep.SLASH);
			String propertyName = environmentSelectors.remove(environmentSelectors.size() - 1); // Ignore property name itself

			List<String> environmentInfos = CBase.stringToList(environmentPath, StringSep.SLASH);
			int i;
			for (i = 0; i < CBase.min(environmentInfos.size(), environmentSelectors.size()); i++) {
				if (!environmentSelectors.get(i).equalsIgnoreCase(environmentInfos.get(i))) {
					break;
				}
			}

			if (i == environmentSelectors.size()) {
				if (matchingProperties[i] == null) {
					matchingProperties[i] = new Properties();
				}
				matchingProperties[i].put(propertyName, properties.get(qualifiedPropertyName));
			}
		}

		// Select best matching properties - these are the ones with the maximum (matching) index
		Properties bestMatchingProperties = new Properties();
		for (int i = 0; i < matchingProperties.length; i++) {
			if (matchingProperties[i] != null) {
				for (Object key : matchingProperties[i].keySet()) {
					bestMatchingProperties.put(key, matchingProperties[i].get(key));
				}
			}
		}

		// Check if all required properties are given
		if (mandatoryPropertyNames != null) {
			for (String mandatoryPropertyName : mandatoryPropertyNames) {
				if (!bestMatchingProperties.containsKey(mandatoryPropertyName)) {
					log.warn("PRP: Missing mandatory property: {}' in '{}'!", mandatoryPropertyName, propertiesFile.getName());
				}
			}
		}

		// Log properties, suppress passwords and keys
		if (!CBase.isEmpty(environmentPath)) {
			log.info("PRP: Best matching properties for environment '{}' in '{}:", environmentPath, propertiesFile.getName());
		}
		else {
			log.info("PRP: Properties in '{}:", propertiesFile.getName());
		}
		for (Object prop : CList.sort(bestMatchingProperties.keySet(), k -> ((String) k).toLowerCase(), true)) {
			String lowercasePropName = ((String) prop).toLowerCase();
			if (lowercasePropName.contains("password") || lowercasePropName.contains("pwd") || lowercasePropName.contains("key")) {
				log.info("PRP: \t\t{}: ******", prop);
			}
			else {
				log.info("PRP: \t\t{}: {}", prop, bestMatchingProperties.get(prop));
			}

		}

		return bestMatchingProperties;
	}

	/**
	 * Find one-and-only properties file in class path
	 * 
	 * @param name
	 *            properties file name
	 * 
	 * @return properties file found or null if no or multiple files were found in class path
	 * 
	 * @throws IOException
	 *             on IO error
	 */
	public static File findPropertiesFile(String name) throws IOException {

		List<File> propertyFiles = CResource.findJavaResourceFiles(name);
		if (propertyFiles.isEmpty()) {

			// No file found
			log.info("PRP: No property file with name '{}' found in class path!", name);
			return null;
		}
		else if (propertyFiles.size() > 1) {

			// Multiple files found - check if properties files are identical
			String text = CFile.readText(propertyFiles.get(0), StandardCharsets.UTF_8.name());
			boolean filesEqual = true;
			for (int i = 1; i < propertyFiles.size() && filesEqual; i++) {
				if (!CBase.objectsEqual(text.trim(), CFile.readText(propertyFiles.get(i), StandardCharsets.UTF_8.name()).trim())) {
					filesEqual = false;
				}
			}

			if (filesEqual) {
				log.info("PRP: Multiple equal property files with name '{}' found in class path ({})!", name, propertyFiles);
				return propertyFiles.get(0);
			}
			else {
				log.warn("PRP: Multiple different property files with name '{}' found in class path ({})!", name, propertyFiles);
				return null;
			}
		}
		else {
			// One file found
			log.info("PRP: Use property file '{}'", propertyFiles.get(0));
			return propertyFiles.get(0);
		}
	}

	// -------------------------------------------------------------------------
	// Get properties
	// -------------------------------------------------------------------------

	/**
	 * Parse (property) value to object of given type
	 * 
	 * @param valueString
	 *            string value
	 * @param type
	 *            destination object type
	 * 
	 * @return object of given type parsed from string value
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Object parseValue(Class<?> type, String valueString) {

		if (type == String.class) {
			return valueString;
		}
		else if (type == Boolean.class || type == boolean.class) {
			return CBase.parseBoolean(valueString, null);
		}
		else if (type == Integer.class || type == int.class) {
			return CBase.parseInt(valueString, null);
		}
		else if (type == Long.class || type == long.class) {
			return CBase.parseLong(valueString, null);
		}
		else if (type == Double.class || type == double.class) {
			return CBase.parseDouble(valueString, null);
		}
		else if (List.class.isAssignableFrom(type)) {
			return CBase.stringToList(valueString, StringSep.COMMA);
		}
		else if (Map.class.isAssignableFrom(type)) {
			return CBase.stringToMap(valueString, StringSep.COMMA, KeyValueSep.EQUAL_SIGN);
		}
		else if (Enum.class.isAssignableFrom(type)) {
			Class<? extends Enum> enumType = (Class<? extends Enum>) type;
			return Enum.valueOf(enumType, valueString);
		}
		else {
			return valueString;
		}
	}

	/**
	 * Get context qualified property of given type.
	 * <p>
	 * If property name is extended by context information his method recursively tries to find property value for configured property with all, some or at last non of the context infos (e.g. if given
	 * property name is 'text.de' and property 'text.de' exists the value of this property is returned. If only property 'text' exists the value of this - non-qualified - property is returned).
	 * 
	 * @param properties
	 *            Properties object
	 * @param type
	 *            type into which property value found shall be converted
	 * @param propertyName
	 *            name of property - may contain dot-separated context information ({@code <base-property-name>[.<context-info>]}*)
	 * @param defaultValue
	 *            value returned if property is not configured
	 * 
	 * @return object of given type parsed from property string value
	 */
	public static Object getProperty(Properties properties, Class<?> type, String propertyName, Object defaultValue) {

		// Assume property name to be of form <base-property-name>[.context-info]* and recursively try to find property with all or some of the context infos
		String valueString = properties.getProperty(propertyName);
		while (valueString == null && propertyName.contains(".")) {
			propertyName = CBase.untilLast(propertyName, ".");
			valueString = properties.getProperty(propertyName);
		}

		// Return default value if property is not configured or parsed object of destination type
		if (valueString == null) {
			return defaultValue;
		}
		else {
			try {
				return parseValue(type, valueString);
			}
			catch (Exception nfex) {
				log.warn("PRP: Value of property '{}' is of {}! (but '{}')", propertyName, type, valueString);
				return defaultValue;
			}
		}
	}

	/**
	 * Get a property of type string (see {@link #getProperty(Properties, Class, String, Object)})
	 * 
	 * @param properties
	 *            Properties object
	 * @param propertyName
	 *            name of property
	 * @param defaultValue
	 *            value returned if property is not configured
	 * 
	 * @return configured or default value of property (string)
	 */
	public static String getStringProperty(Properties properties, String propertyName, String defaultValue) {
		return (String) getProperty(properties, String.class, propertyName, defaultValue);
	}

	/**
	 * Get a property of type boolean (see {@link #getProperty(Properties, Class, String, Object)})
	 * 
	 * @param properties
	 *            Properties object
	 * @param propertyName
	 *            name of property
	 * @param defaultValue
	 *            value returned if property is not configured
	 * 
	 * @return configured or default boolean value of property
	 */
	public static boolean getBooleanProperty(Properties properties, String propertyName, boolean defaultValue) {
		return (boolean) getProperty(properties, boolean.class, propertyName, defaultValue);
	}

	/**
	 * Get a property of type int (see {@link #getProperty(Properties, Class, String, Object)})
	 * 
	 * @param properties
	 *            Properties object
	 * @param propertyName
	 *            name of property
	 * @param defaultValue
	 *            value returned if property is not configured
	 * 
	 * @return configured or default integer value of property
	 */
	public static int getIntProperty(Properties properties, String propertyName, int defaultValue) {
		return (int) getProperty(properties, int.class, propertyName, defaultValue);
	}

	/**
	 * Get a property of type long (see {@link #getProperty(Properties, Class, String, Object)})
	 * 
	 * @param properties
	 *            Properties object
	 * @param propertyName
	 *            name of property
	 * @param defaultValue
	 *            value returned if property is not configured
	 * 
	 * @return configured or default long value of property
	 */
	public static long getLongProperty(Properties properties, String propertyName, long defaultValue) {
		return (long) getProperty(properties, long.class, propertyName, defaultValue);
	}

	/**
	 * Get a property of type double
	 * 
	 * @param properties
	 *            Properties object
	 * @param propertyName
	 *            name of property
	 * @param defaultValue
	 *            value returned if property is not configured
	 * 
	 * @return configured or default double value of property
	 */
	public static double getDoubleProperty(Properties properties, String propertyName, double defaultValue) {
		return (double) getProperty(properties, double.class, propertyName, defaultValue);
	}

}
