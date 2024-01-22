package com.icx.common.base;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Java resource helpers
 * 
 * @author baumgrai
 */
public abstract class CResource extends Common {

	// -------------------------------------------------------------------------
	// Localized messages
	// -------------------------------------------------------------------------

	// Location and name of resource file containing localized messages
	public static String localizedMessageFileName = null;

	/**
	 * Set location and name of resource file containing localized messages
	 * <p>
	 * Use '&lt;package_path&gt;.messages' if 'messages_&lt;locale&gt;.properties' files exist in package &lt;package_path&gt;.
	 * 
	 * @param qualifiedPackagePath
	 *            fully qualified base name of localized message file (e.g. 'de.icx.v4j.app.xxx.i18n.messages')
	 */
	public static void setLocalizedMessageFileName(String qualifiedPackagePath) {
		localizedMessageFileName = qualifiedPackagePath;
	}

	/**
	 * Get resource bundle for locale (for localized text messages)
	 * 
	 * @param locale
	 *            locale for localization
	 * 
	 * @return resource bundle for given locale
	 */
	public static ResourceBundle getBundleForLocale(Locale locale) {

		if (locale == null || localizedMessageFileName == null) {
			return null;
		}

		try {
			return ResourceBundle.getBundle(localizedMessageFileName, locale);
		}
		catch (MissingResourceException mex) {
			return null;
		}
	}

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

	/**
	 * Get localized message by given location
	 * 
	 * @param locale
	 *            locale for localization
	 * @param key
	 *            text to get localized message for
	 * 
	 * @return localized message if configured, key otherwise
	 */
	public static String i18n(Locale locale, String key) {
		return i18n(getBundleForLocale(locale), key);
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
			String resourceFileName = URLDecoder.decode(resources.nextElement().getPath(), UTF_8);
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

	// -------------------------------------------------------------------------
	// Manifest / jar files
	// -------------------------------------------------------------------------

	/**
	 * Get manifest attribute
	 * 
	 * @param manifest
	 *            Manifest object
	 * @param name
	 *            attribute name
	 * @param defaultValue
	 *            default value
	 * 
	 * @return attribute value or default value if attribute not set
	 */
	public static String getManifestAttr(Manifest manifest, Attributes.Name name, String defaultValue) {

		// null pointer check
		if (manifest == null) {
			return defaultValue;
		}

		// Check main attributes
		Attributes attrs = manifest.getMainAttributes();
		if (attrs == null) {
			return defaultValue;
		}

		String attrValue = attrs.getValue(name);
		if (attrValue != null) {
			return attrValue;
		}

		// Check manifest entries
		Map<String, Attributes> mattrs = manifest.getEntries();
		Iterator<String> it = mattrs.keySet().iterator();
		while (it.hasNext()) {
			Attributes eattrs = mattrs.get(it.next());
			attrValue = eattrs.getValue(name);
			if (attrValue != null) {
				return attrValue;
			}
		}

		return defaultValue;
	}

	/**
	 * Get manifest attribute of a manifest file
	 * 
	 * @param manifestFile
	 *            File object of manifest file
	 * @param name
	 *            attribute name
	 * @param defaultValue
	 *            default value
	 * 
	 * @return attribute value or default value if not set or on IO exception
	 */
	public static String getManifestAttr(File manifestFile, Attributes.Name name, String defaultValue) {

		try {
			Manifest manifest = new Manifest(new FileInputStream(manifestFile));
			return getManifestAttr(manifest, name, defaultValue);
		}
		catch (IOException ex) {
			return defaultValue;
		}
	}

	/**
	 * Get manifest attribute of a jar file
	 * 
	 * @param jarFile
	 *            File object of jar file
	 * @param name
	 *            attribute name
	 * @param defaultValue
	 *            default value
	 * 
	 * @return attribute value or default value if not set or on IO exception
	 */
	public static String getJarManifestAttr(File jarFile, Attributes.Name name, String defaultValue) {

		try (JarFile jar = new JarFile(jarFile)) {
			Manifest manifest = jar.getManifest();
			return getManifestAttr(manifest, name, defaultValue);
		}
		catch (IOException ex) {
			return defaultValue;
		}
	}

	/**
	 * Get version info from of manifest file or from jar file
	 * 
	 * @param manifestOrJarFile
	 *            manifest or jar file object
	 * 
	 * @return version or 'unknown' if version attribute not set or o on IO exception
	 */
	public static String getVersion(File manifestOrJarFile) {

		if (manifestOrJarFile.getName().endsWith(".jar")) {
			return getJarManifestAttr(manifestOrJarFile, Attributes.Name.IMPLEMENTATION_VERSION, "unknown");
		}
		else {
			return getManifestAttr(manifestOrJarFile, Attributes.Name.IMPLEMENTATION_VERSION, "unknown");
		}
	}

}
