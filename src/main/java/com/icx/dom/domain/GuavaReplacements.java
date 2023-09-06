package com.icx.dom.domain;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.icx.dom.common.Common;

public abstract class GuavaReplacements {

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

			String resource = packageName.replaceAll("[.]", "/");

			InputStream stream = classloader.getResourceAsStream(resource); // Code for reading resources adapted from Baeldung
			BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

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

		public Set<ClassInfo> getTopLevelClassesRecursive(String packageName) {

			Set<String> classNames = new HashSet<>();
			getTopLevelClassesRecursive(packageName, classNames);

			return classNames.stream().map(r -> new ClassInfo(r)).collect(Collectors.toSet());
		}
	}
}
