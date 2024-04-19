package com.icx.domain.sql;

import java.io.File;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Common;

/**
 * General helpers
 * 
 * @author baumgrai
 */
public abstract class Helpers extends Common {

	static final Logger log = LoggerFactory.getLogger(Helpers.class);

	// Count new and changed objects grouped by object domain classes (for logging only)
	static <T extends SqlDomainObject> Set<Entry<String, Integer>> groupCountsByDomainClassName(Set<T> objects) {

		return objects.stream().collect(Collectors.groupingBy(Object::getClass)).entrySet().stream().map(e -> new SimpleEntry<>(e.getKey().getSimpleName(), e.getValue().size()))
				.collect(Collectors.toSet());
	}

	// Build string lists with a maximum of max elements (Oracle limit for # of elements in WHERE IN (...) clause = 1000)
	static List<String> buildStringLists(Set<?> elements, int max) {

		List<String> stringLists = new ArrayList<>();
		if (elements == null || elements.isEmpty()) {
			return stringLists;
		}

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (Object element : elements) {

			if (i % max != 0) {
				sb.append(",");
			}

			if (element instanceof String || element instanceof Enum || element instanceof Boolean || element instanceof File) {
				sb.append("'" + element + "'");
			}
			else {
				sb.append(element);
			}

			if (i % max == max - 1) {
				stringLists.add(sb.toString());
				sb.setLength(0);
			}

			i++;
		}

		if (sb.length() > 0) {
			stringLists.add(sb.toString());
		}

		return stringLists;
	}

}
