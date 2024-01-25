package com.icx.dom.domain.sql;

import java.io.File;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.Common;

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

	// Build "(<ids>)" lists with at maximum 1000 ids (Oracle limit for # of elements in WHERE IN (...) clause = 1000)
	static List<String> buildMax1000IdsLists(Set<Long> ids) {

		List<String> idStringLists = new ArrayList<>();
		if (ids == null || ids.isEmpty()) {
			return idStringLists;
		}

		StringBuilder sb = new StringBuilder();
		int i = 0;
		for (long id : ids) {

			if (i % 1000 != 0) {
				sb.append(",");
			}

			sb.append(id);

			if (i % 1000 == 999) {
				idStringLists.add(sb.toString());
				sb.setLength(0);
			}

			i++;
		}

		if (sb.length() > 0) {
			idStringLists.add(sb.toString());
		}

		return idStringLists;
	}

	// Build string list of elements for WHERE clause (of DELETE statement)
	static String buildElementList(Set<Object> elements) {

		StringBuilder sb = new StringBuilder();
		sb.append("(");

		for (Object element : elements) {

			// element = FieldColumnConversion.field2ColumnValue(element);
			if (element instanceof String || element instanceof Enum || element instanceof Boolean || element instanceof File) {
				sb.append("'" + element + "'");
			}
			else {
				sb.append(element);
			}

			sb.append(",");
		}

		sb.replace(sb.length() - 1, sb.length(), ")");

		return sb.toString();
	}

}
