package com.icx.domain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.Common;

/**
 * Helper class for methods originally contained in Google's Guava library and cloned here to avoid dependence on Guava.
 * 
 * @author baumgrai
 */
public abstract class GuavaReplacements {

	static final Logger log = LoggerFactory.getLogger(GuavaReplacements.class);

	// Case format conversion

	public enum CaseFormat {
		UPPER_CAMEL, UPPER_UNDERSCORE;

		public String to(CaseFormat caseFormat, String string) {

			if (Common.isEmpty(string)) {
				return string;
			}
			else if (this == UPPER_CAMEL && caseFormat == UPPER_UNDERSCORE) {
				return (string.substring(0, 1) + string.substring(1).replaceAll("(\\p{javaUpperCase})", "_$1")).toUpperCase();
			}
			// else if (this == UPPER_UNDERSCORE && caseFormat == UPPER_CAMEL) {
			//
			// StringBuilder sb = new StringBuilder();
			// while (string.contains("_")) {
			// String part = CBase.untilFirst(string, "_");
			// sb.append(part.substring(0, 1).toUpperCase());
			// sb.append(part.substring(1).toLowerCase());
			// string = CBase.behindFirst(string, "_");
			// }
			// sb.append(string.substring(0, 1).toUpperCase());
			// sb.append(string.substring(1).toLowerCase());
			//
			// return sb.toString();
			// }
			else {
				return string;
			}
		}
	}

	// Top level classes

	public static class ClassInfo {

		private String name;

		public ClassInfo(
				String name) {

			this.name = name;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	public static class ClassPath {

		ClassLoader classloader = null;

		public static ClassPath from(ClassLoader classloader) {

			ClassPath classPath = new ClassPath();
			classPath.classloader = classloader;

			return classPath;
		}

		public void getTopLevelClassesRecursive(String packageName, Set<String> classNames) {

			String packageResourceName = packageName.replace(".", "/");

			try {
				Enumeration<URL> resources = classloader.getResources(packageResourceName);
				if (resources.hasMoreElements()) {
					try (BufferedReader reader = new BufferedReader(new InputStreamReader(resources.nextElement().openStream()))) {

						Set<String> subResources = reader.lines().collect(Collectors.toSet());
						for (String subResource : subResources) {

							String qualifiedSubResource = packageName + "." + subResource;

							if (subResource.endsWith(".class") && !subResource.contains("$")) { // Class
								classNames.add(qualifiedSubResource.substring(0, qualifiedSubResource.length() - 6));
							}
							else if (!subResource.contains(".")) { // Package or anything else
								getTopLevelClassesRecursive(qualifiedSubResource, classNames);
							}
						}
					}
				}
			}
			catch (IOException e) {
				log.error("GUR: Package resource '{}' could not be opened - {} occurred!", packageResourceName, e);
			}
		}

		public Set<ClassInfo> getTopLevelClassesRecursive(String packageName) {

			Set<String> classNames = new HashSet<>();
			getTopLevelClassesRecursive(packageName, classNames);

			return classNames.stream().map(ClassInfo::new).collect(Collectors.toSet());
		}
	}
}
