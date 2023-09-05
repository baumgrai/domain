package com.icx.dom.common;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * Java resource helpers
 * 
 * @author baumgrai
 */
public abstract class CResource {

	/**
	 * Get localized message by given resource bundle
	 * 
	 * @param bundle
	 *            resource bundle (got from {@code ResourceBundle.getBundle(message file name, locale)})
	 * @param key
	 *            text to get localized message for
	 * 
	 * @return localized message if configured, key otherwise
	 */
	public static String i18n(ResourceBundle bundle, String key) {

		if (key == null) {
			return "";
		}

		if (bundle == null) {
			return key;
		}

		try {
			return bundle.getString(key.trim().toUpperCase());
		}
		catch (MissingResourceException mrex) {
			return key;
		}
	}

	// -------------------------------------------------------------------------
	// Find Java resource files (in class path)
	// -------------------------------------------------------------------------

	/**
	 * Find all (resource) files with given path/name in Java class path.
	 * <p>
	 * Resource file(s) must be situated exactly under given path/name in one of the possibly multiple class path root entries (e.g.: class path: {@code <TOMCAT>/lib,<WEB-INF>/classes} and search for
	 * {@code log4j.properties}: where {@code log4j.properties} is directly under {@code <TOMCAT>/lib} and under {@code <WEB-INF>/classes/de/common} - than it will be found only in
	 * {@code <TOMCAT>/lib}).
	 * <p>
	 * Do not find files in jars!
	 * 
	 * @param fileName
	 *            resource file name - can be preceded by path relative to one of the class path root entries
	 * 
	 * @return List of resource files of given name
	 * 
	 * @throws IOException
	 *             on IO error
	 */
	public static List<File> findJavaResourceFiles(String fileName) throws IOException {

		Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(fileName);
		List<File> resourceFiles = new ArrayList<>();
		while (resources.hasMoreElements()) {

			String resourceFileName = URLDecoder.decode(resources.nextElement().getPath(), StandardCharsets.UTF_8.name());
			if (!resourceFileName.contains(".jar!")) {
				resourceFiles.add(new File(resourceFileName));
			}
		}

		return resourceFiles;
	}

	/**
	 * Find first (resource) file with given path/name in Java class path.
	 * <p>
	 * Behaves like {@link #findJavaResourceFiles(String)} but returns only first file found.
	 * 
	 * @param fileName
	 *            resource file name - can be preceded by path relative to one of the class path root entries
	 * 
	 * @return first resource files of given name found or null
	 * 
	 * @throws IOException
	 *             on IO error
	 */
	public static File findFirstJavaResourceFile(String fileName) throws IOException {

		List<File> resourceFiles = findJavaResourceFiles(fileName);
		if (resourceFiles.isEmpty()) {
			return null;
		}
		else {
			return resourceFiles.get(0);
		}
	}

}