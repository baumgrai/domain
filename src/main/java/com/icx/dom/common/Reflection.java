package com.icx.dom.common;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Java Reflection helpers.
 * 
 * @author baumgrai
 */
public abstract class Reflection {

	static final Logger log = LoggerFactory.getLogger(Reflection.class);

	// -------------------------------------------------------------------------
	// Statics
	// -------------------------------------------------------------------------

	// Internal caches
	private static Queue<String> loadedPackageNames = new ConcurrentLinkedQueue<>();
	private static Map<String, Class<?>> loadedClassesCache = new ConcurrentHashMap<>();

	// -------------------------------------------------------------------------
	// General helpers
	// -------------------------------------------------------------------------

	/**
	 * Get boxing wrapper object type for primitive types.
	 * 
	 * @param type
	 *            type
	 * 
	 * @return Associated object type if type is primitive, type itself otherwise
	 */
	public static <T> Class<?> getBoxingWrapperType(Class<T> type) {

		if (type == boolean.class) {
			return Boolean.class;
		}
		else if (type == char.class) {
			return Character.class;
		}
		else if (type == short.class) {
			return Short.class;
		}
		else if (type == int.class) {
			return Integer.class;
		}
		else if (type == long.class) {
			return Long.class;
		}
		else if (type == float.class) {
			return Float.class;
		}
		else if (type == double.class) {
			return Double.class;
		}
		else {
			return type;
		}
	}

	/**
	 * Get original exception of invocation target exception
	 * 
	 * @param itex
	 *            invocation target exception
	 * 
	 * @return exception causing invocation target exception
	 */
	public static Exception getOriginalException(InvocationTargetException itex) {
		return (itex.getCause() instanceof Exception ? (Exception) itex.getCause() : itex);
	}

	// Recursive declaring/inner class prefix
	private static String declaringClassPrefix(Field field) {

		Class<?> declaringClass = field.getDeclaringClass();
		String prefix = declaringClass.getSimpleName();

		while (declaringClass.isMemberClass()) {
			declaringClass = declaringClass.getDeclaringClass();
			prefix = declaringClass.getSimpleName() + "$" + prefix;
		}

		return prefix;
	}

	/**
	 * Get field declaration string
	 * 
	 * @param field
	 *            field
	 * 
	 * @return field field declaration string
	 */
	public static String fieldDeclaration(Field field) {
		return (field != null ? field.getGenericType().getTypeName().replaceAll("(\\p{Lower}+\\.)+", "") + " " + declaringClassPrefix(field) + "#" + field.getName() : "(field is null)");
	}

	/**
	 * Field name - including declaring domain class name except for given class
	 * 
	 * @param field
	 *            field
	 * 
	 * @return field name - qualified by declaring simple class' name if declaring class is not given class, simple field name otherwise
	 */
	public static String qualifiedName(Field field) {
		return (field != null ? field.getType().getSimpleName() + " " + declaringClassPrefix(field) + "#" + field.getName() : "(field is null)");
	}

	/**
	 * Create new collection of specified type
	 * 
	 * @param <T>
	 *            element type
	 * @param collectionClass
	 *            collection type (Set, List, ...)
	 * 
	 * @return new collection of given type
	 */
	public static <E> Collection<E> newCollection(Class<? extends Collection<E>> collectionClass) {

		if (SortedSet.class.isAssignableFrom(collectionClass)) {
			return new TreeSet<>();
		}
		else if (Set.class.isAssignableFrom(collectionClass)) {
			return new HashSet<>();
		}
		else if (List.class.isAssignableFrom(collectionClass)) {
			return new ArrayList<>();
		}
		else {
			throw new IllegalArgumentException("Unupported class '" + collectionClass.getName() + "' to construct collection");
		}
	}

	/**
	 * Create new map of specified type
	 * 
	 * @param <K>
	 *            key type
	 * @param <V>
	 *            value type
	 * @param mapClass
	 *            map type (Set, List, ...)
	 * 
	 * @return new map of given type
	 */
	public static <K, V> Map<K, V> newMap(Class<? extends Map<K, V>> mapClass) {

		if (SortedMap.class.isAssignableFrom(mapClass)) {
			return new TreeMap<>();
		}
		else if (Map.class.isAssignableFrom(mapClass)) {
			return new HashMap<>();
		}
		else {
			throw new IllegalArgumentException("Unupported class '" + mapClass.getName() + "' to construct map");
		}
	}

	/**
	 * Create new collection or map of given type
	 * 
	 * @param type
	 *            collection or map type
	 * 
	 * @return new collection or map
	 */
	@SuppressWarnings("unchecked")
	public static Object newComplex(Class<?> type) {

		if (Collection.class.isAssignableFrom(type)) {
			return newCollection((Class<? extends Collection<Object>>) type);
		}
		else if (Map.class.isAssignableFrom(type)) {
			return newMap((Class<? extends Map<Object, Object>>) type);
		}
		else {
			throw new IllegalArgumentException("Unupported class '" + type.getName() + "' to construct collection or map");
		}
	}

	// -------------------------------------------------------------------------
	// Classes
	// -------------------------------------------------------------------------

	/**
	 * Get name of absolute 'classes' directory or jar file where given class is located.
	 * 
	 * @param cls
	 *            class to check
	 * 
	 * @return 'classes' directory or jar file where class is located
	 */
	public static File getLocation(Class<?> cls) {

		ProtectionDomain domain = cls.getProtectionDomain();
		CodeSource source = domain.getCodeSource();
		URL sourceUrl = source.getLocation();
		String path = sourceUrl.getPath();
		try {
			path = URLDecoder.decode(sourceUrl.getPath(), StandardCharsets.UTF_8.name());
		}
		catch (UnsupportedEncodingException e) {
			log.error("UnsupportedEncodingException: {}", StandardCharsets.UTF_8.name());
		}

		log.info("RFL: Got location of class '{}'. Source URL: '{}', path: '{}'", cls.getSimpleName(), sourceUrl, path);

		return new File(path);
	}

	/**
	 * Get absolute 'classes' directory where class is located.
	 * <p>
	 * If class is part of a jar file null is returned.
	 * 
	 * @param anyClassInClassesDir
	 *            class in 'classes' directory
	 * 
	 * @return 'classes' directory or null if class is not located there
	 */
	public static File getClassesDir(Class<?> anyClassInClassesDir) {

		File location = getLocation(anyClassInClassesDir);

		return (location.isDirectory() ? location : null);
	}

	/**
	 * Retrieve and sort names of loaded packages.
	 * <p>
	 * Internally caches loaded package names, so subsequent calls of {@link #getLoadedPackageNames()} do not call Reflection method to get packages).
	 */
	public static synchronized void retrieveLoadedPackageNames() {

		List<String> loadedPackageNamesTmp = new ArrayList<>();
		for (Package p : Package.getPackages()) {
			if (!CBase.isEmpty(p.getName()))
				loadedPackageNamesTmp.add(p.getName());
		}

		Collections.sort(loadedPackageNamesTmp);
		for (String packageName : loadedPackageNamesTmp) {
			loadedPackageNames.add(packageName);
		}
	}

	/**
	 * Get names of loaded packages in alphabetical order - retrieve loaded packages only on first call.
	 * 
	 * @return sorted list of names of currently loaded packages
	 */
	public static Collection<String> getLoadedPackageNames() {

		if (loadedPackageNames.isEmpty()) {
			retrieveLoadedPackageNames();
		}

		return loadedPackageNames;
	}

	/**
	 * Load inner class of given class.
	 * <p>
	 * Once loaded classes using this method are cached internally, so subsequent calls of this method with same parameters do not load class again.
	 * 
	 * @param innerClassName
	 *            inner class name
	 * @param outerClass
	 *            outer class
	 * 
	 * @return class object found
	 * 
	 * @throws ClassNotFoundException
	 *             if class could not be found or parameters are null or empty
	 */
	public static synchronized Class<?> loadInnerClass(String innerClassName, Class<?> outerClass) throws ClassNotFoundException {
		return loadClass(outerClass.getName() + "$" + innerClassName);
	}

	// NoClass - to cache instead of null if class could not be loaded because Concurrent map does not support null values
	static class NoClass {
	}

	/**
	 * Load class by qualified or simple name including caching of already loaded and non-loadable classes.
	 * <p>
	 * If name is not qualified the first class of given name found in any of the loaded packages will be returned.
	 * <p>
	 * Once loaded classes using this method are cached internally, so subsequent calls of this method with same parameter do not load class again. If any class could not be loaded name will also be
	 * cached to avoid subsequent unsuccessful load tries.
	 * 
	 * @param className
	 *            qualified or simple class name
	 * 
	 * @return class object
	 * 
	 * @throws ClassNotFoundException
	 *             if no class could be found for given name
	 */
	public static synchronized Class<?> loadClass(String className) throws ClassNotFoundException {

		if (!loadedClassesCache.containsKey(className)) {

			Class<?> cls = null;
			if (className.contains(".")) {

				// Qualified class name: try loading class
				try {
					cls = Class.forName(className);
				}
				catch (ClassNotFoundException cnfexn) {
					cls = NoClass.class; // Assign NoClass (instead of null) if class could not be loaded because Concurrent map does not support null values
				}
			}
			else {
				// Simple class name: search class in all loaded packages - use first one found
				for (String packageName : getLoadedPackageNames()) {
					try {
						cls = Class.forName(packageName + "." + className);
						break;
					}
					catch (ClassNotFoundException cnfexn) {
						cls = NoClass.class;
					}
				}
			}

			loadedClassesCache.put(className, cls);
		}

		if (loadedClassesCache.get(className) == NoClass.class) {
			throw new ClassNotFoundException("Class '" + className + "' could not be loaded from any of the loaded packages!");
		}
		else {
			return loadedClassesCache.get(className);
		}
	}
}
