package com.icx.dom.junit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.icx.common.Common;

public class TestHelpers extends Common {

	protected static List<Field> fields(Class<?> c, String... fieldNames) throws NoSuchFieldException, SecurityException {

		List<Field> fields = new ArrayList<>();
		for (String fieldName : fieldNames) {
			fields.add(c.getDeclaredField(fieldName));
		}

		return fields;
	}

	protected static String name(Object o) {

		if (o instanceof Class) {
			return (((Class<?>) o).getSimpleName());
		}
		else if (o instanceof Field) {
			return (((Field) o).getName());
		}
		else if (o != null) {
			return o.toString();
		}
		else {
			return "null";
		}
	}

	protected static void assertListsEqualButOrder(List<?> l1, List<?> l2, String listName) {

		Set<?> l1Names = l1.stream().map(e -> name(e)).collect(Collectors.toSet());
		Set<?> l2Names = l2.stream().map(e -> name(e)).collect(Collectors.toSet());

		assertEquals(l1Names, l2Names, listName);
		assertEquals(l1.size(), l2.size(), listName + " has doublets");
	}

}
