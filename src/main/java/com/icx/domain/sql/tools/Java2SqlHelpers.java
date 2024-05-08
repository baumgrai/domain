package com.icx.domain.sql.tools;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CList;
import com.icx.common.Common;
import com.icx.domain.Registry;
import com.icx.domain.sql.Const;
import com.icx.domain.sql.Annotations;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.SqlRegistry;
import com.icx.domain.sql.Annotations.Changed;
import com.icx.domain.sql.Annotations.Created;
import com.icx.domain.sql.Annotations.Removed;
import com.icx.domain.sql.tools.FkConstraint.ConstraintType;
import com.icx.jdbc.SqlDbHelpers;
import com.icx.jdbc.SqlDb.DbType;

/**
 * Helpers for {@link Java2Sql} tool.
 * <p>
 * Class, methods and fields are 'public' only for formal reasons. Java2Sql class can be copied and must be runnable in any application 'domain' package to generate SQL scripts for application's
 * domain classes.
 * 
 * @author baumgrai
 */
public class Java2SqlHelpers extends SqlDbHelpers {

	static final Logger log = LoggerFactory.getLogger(Java2SqlHelpers.class);

	// Indentation
	public static String tabs(String name) {

		int len = (name != null ? name.length() / 2 : 8);
		if (len > 8) {
			len = 8;
		}

		return "  \t\t\t\t\t\t\t\t".substring(len);
	}

	private static Registry<SqlDomainObject> registry = new Registry<>();

	// Fields

	public static boolean wasCreatedInVersion(Created created, String version) {

		if (created == null) {
			return false;
		}
		else {
			return Common.untilFirst(created.version(), ":").equals(version);
		}
	}

	public static String getCreatedVersion(Created created) {

		if (created == null) {
			return "1.0";
		}
		else {
			return Common.untilFirst(created.version(), ":");
		}
	}

	public static Map<String, String> getCreateInfo(Created created) {

		if (created == null) {
			return new HashMap<>();
		}
		else {
			Map<String, String> changeInfoMap = new HashMap<>();

			String changeInfoString = Common.behindFirst(created.version(), ":");
			String[] changeEntryStrings = changeInfoString.split("\\;");
			for (String changeEntryString : changeEntryStrings) {

				String[] changeEntry = changeEntryString.split("\\=");
				String changedAttribute = changeEntry[0];
				String value = "";
				if (changeEntry.length > 1) {
					value = changeEntry[1];
				}

				changeInfoMap.put(changedAttribute, value);
			}

			return changeInfoMap;
		}
	}

	// Check if field was created in version
	public static boolean wasCreatedInVersion(Field field, String version) {
		return (wasCreatedInVersion(field.getAnnotation(Created.class), version));
	}

	// Get version where field was created in
	public static String getCreatedVersion(Field field) {
		return (getCreatedVersion(field.getAnnotation(Created.class)));
	}

	// Get create information for field
	public static Map<String, String> getCreateInfo(Field field) {
		return getCreateInfo(field.getAnnotation(Created.class));
	}

	public static boolean wasChangedInVersion(Changed changed, String version) {

		if (changed == null) {
			return false;
		}
		else {
			return Stream.of(changed.versions()).map(vi -> Common.untilFirst(vi, ":")).anyMatch(v -> v.equals(version));
		}
	}

	public static List<String> getVersionsWithChanges(Changed changed) {

		if (changed == null) {
			return new ArrayList<>();
		}
		else {
			return Stream.of(changed.versions()).map(vi -> Common.untilFirst(vi, ":")).collect(Collectors.toList());
		}
	}

	public static Map<String, String> getChangeInfo(Changed changed, String version) {

		if (changed == null) {
			return new HashMap<>();
		}
		else {
			Map<String, String> changeInfoMap = new HashMap<>();

			String changeInfoString = Common.behindFirst(Stream.of(changed.versions()).filter(vi -> Common.untilFirst(vi, ":").equals(version)).findAny().orElse(""), ":");
			String[] changeEntryStrings = changeInfoString.split("\\;");
			for (String changeEntryString : changeEntryStrings) {

				String[] changeEntry = changeEntryString.split("\\=");
				String changedAttribute = changeEntry[0];
				String value = "";
				if (changeEntry.length > 1) {
					value = changeEntry[1];
				}

				changeInfoMap.put(changedAttribute, value);
			}

			return changeInfoMap;
		}
	}

	// Check if field was changed in version
	public static boolean wasChangedInVersion(Field field, String version) {
		return (wasChangedInVersion(field.getAnnotation(Changed.class), version));
	}

	// Get versions in which field was changed
	public static List<String> getVersionsWithChanges(Field field) {
		return getVersionsWithChanges(field.getAnnotation(Changed.class));
	}

	// Get change information for field and version
	public static Map<String, String> getChangeInfo(Field field, String version) {
		return getChangeInfo(field.getAnnotation(Changed.class), version);
	}

	public static boolean wasRemovedInVersion(Removed removed, String version) {

		if (removed == null) {
			return false;
		}
		else {
			return removed.version().equals(version);
		}
	}

	public static String getRemovedVersion(Removed removed) {

		if (removed == null) {
			return "";
		}
		else {
			return removed.version();
		}
	}

	// Check if field was removed in version
	public static boolean wasRemovedInVersion(Field field, String version) {
		return (wasRemovedInVersion(field.getAnnotation(Removed.class), version));
	}

	// Get version where field was created in
	public static String getRemovedVersion(Field field) {
		return (getRemovedVersion(field.getAnnotation(Removed.class)));
	}

	// Check if any changes to given field column are to consider for given version
	public static boolean isFieldAffected(Field field, String version) {
		return (wasCreatedInVersion(field, version) || wasChangedInVersion(field, version) || wasRemovedInVersion(field, version));
	}

	// Domain classes

	// Check if domain class was created in version
	public static boolean wasCreatedInVersion(Class<?> cls, String version) {
		return (wasCreatedInVersion(cls.getAnnotation(Created.class), version));
	}

	// Get version where domain class was created in
	public static String getCreatedVersion(Class<?> cls) {
		return (getCreatedVersion(cls.getAnnotation(Created.class)));
	}

	// Get create information for domain class
	public static Map<String, String> getCreateInfo(Class<?> cls) {
		return getCreateInfo(cls.getAnnotation(Created.class));
	}

	// Check if domain class was changed in version
	public static boolean wasChangedInVersion(Class<?> cls, String version) {
		return (wasChangedInVersion(cls.getAnnotation(Changed.class), version));
	}

	// Get versions in which domain class was changed
	public static List<String> getVersionsWithChanges(Class<?> cls) {
		return getVersionsWithChanges(cls.getAnnotation(Changed.class));
	}

	// Get change information for domain class and version
	public static Map<String, String> getChangeInfo(Class<?> cls, String version) {
		return getChangeInfo(cls.getAnnotation(Changed.class), version);
	}

	// Check if domain class was removed in version
	public static boolean wasRemovedInVersion(Class<?> cls, String version) {
		return (wasRemovedInVersion(cls.getAnnotation(Removed.class), version));
	}

	// Get version where domain class was created in
	public static String getRemovedVersion(Class<?> cls) {
		return (getRemovedVersion(cls.getAnnotation(Removed.class)));
	}

	// Incremental update scripts

	public static void updateColumnAttributes(Column column, Map<String, String> changeInfoMap) {

		if (changeInfoMap.containsKey(Annotations.CHARSIZE)) {
			column.charsize = Integer.valueOf(changeInfoMap.get(Annotations.CHARSIZE));
			column.isText = false; // If charsize is given in version info column can only be a 'normal' text column, not a CLOB, etc. in this version
			log.info("J2S: \t\t\tColumn '{}': Got charsize {} from version info", column.name, column.charsize);
		}

		if (changeInfoMap.containsKey(Annotations.NOT_NULL)) {
			column.notNull = (column.field.getType().isPrimitive() || Boolean.valueOf(changeInfoMap.get(Annotations.NOT_NULL)));
			log.info("J2S: \t\t\tColumn '{}': Got NOT NULL {} from version info", column.name, column.notNull);
		}

		if (changeInfoMap.containsKey(Annotations.UNIQUE)) { // Do not set column attribute here - to allow subsequent check for change of UNIQUE state
			log.info("J2S: \t\t\tColumn '{}': Got UNIQUE {} from version info", column.name, Boolean.valueOf(changeInfoMap.get(Annotations.UNIQUE)));
		}

		if (changeInfoMap.containsKey(Annotations.IS_TEXT)) {
			column.isText = Boolean.valueOf(changeInfoMap.get(Annotations.IS_TEXT));
			log.info("J2S: \t\t\tColumn '{}': Got 'isText' {} from version info", column.name, column.isText);
		}

		if (changeInfoMap.containsKey(Annotations.NUMERICAL_TYPE)) {
			String numericalType = changeInfoMap.get(Annotations.NUMERICAL_TYPE);
			try {
				column.fieldRelatedType = Class.forName("java.lang." + numericalType);
			}
			catch (ClassNotFoundException ex) {
				try {
					column.fieldRelatedType = Class.forName("java.math." + numericalType);
				}
				catch (ClassNotFoundException ex1) {
					log.error("Numerical type {} does not exist!", numericalType);
				}
			}
			log.info("J2S: \t\t\tColumn '{}': Got 'numericalType' {} from version info", column.name, column.fieldRelatedType.getSimpleName());
		}
	}

	// Build entry table for Set, List or Map field
	public static Table buildEntryTable(Field field, String currentCollectionTypeString, DbType dbType) {

		// Create table
		Table entryTable = new Table(field, dbType);

		log.info("J2S: \t\tCreate entry table '{}' for field '{}' of type '{}'", entryTable.name, field.getName(), field.getType().getSimpleName());

		// Create main table reference column and associated foreign key constraint
		Column mainTableRefColumn = entryTable.addStandardColumn(SqlRegistry.buildMainTableRefColumnName(field, dbType), Long.class);
		mainTableRefColumn.notNull = true;
		mainTableRefColumn.addFkConstraintForObjectId(ConstraintType.MAIN_TABLE, registry.getCastedDeclaringDomainClass(field));

		// Create entry columns
		if (field.getType().isArray()) {

			// Create element column - do not support collections or maps as elements of array! - and order column
			entryTable.addStandardColumn(Const.ELEMENT_COL, field.getType().getComponentType());
			entryTable.addStandardColumn(Const.ORDER_COL, Long.class);
		}
		else if (Collection.class.isAssignableFrom(field.getType())) { // Collection field

			Type elementType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

			// Create element column - support also collections or maps of simple objects as element type - convert with string2list and string2map and vice versa
			Column elementColumn = null;
			if (elementType instanceof Class<?>) {
				elementColumn = entryTable.addStandardColumn(Const.ELEMENT_COL, (Class<?>) elementType);
			}
			else { // if element itself is collection or map
				elementColumn = entryTable.addStandardColumn(Const.ELEMENT_COL, String.class);
				elementColumn.isText = true;
			}

			// Avoid null pointer exception on following tests
			if (currentCollectionTypeString == null) {
				currentCollectionTypeString = "";
			}

			// Add UNIQUE constraint for element sets (or if type is unknown)
			if (Set.class.isAssignableFrom(field.getType()) || currentCollectionTypeString.equalsIgnoreCase("set") || currentCollectionTypeString.equalsIgnoreCase("both")) {
				elementColumn.charsize = 512; // Less bytes allowed for keys in UNIQUE constraints (differs for different database types)
				entryTable.addUniqueConstraintFor(mainTableRefColumn, elementColumn);
			}

			// Add add order column for element lists (or if type is unknown)
			if (List.class.isAssignableFrom(field.getType()) || currentCollectionTypeString.equalsIgnoreCase("list") || currentCollectionTypeString.equalsIgnoreCase("both")) {
				Column orderColumn = entryTable.addStandardColumn(Const.ORDER_COL, Long.class);
				entryTable.addUniqueConstraintFor(mainTableRefColumn, orderColumn);
			}
		}
		else if (Map.class.isAssignableFrom(field.getType())) { // Map field

			Type keyType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
			Type valueType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];

			// Create key column - may not be a collection or map itself
			Column keyColumn = null;
			if (!(keyType instanceof Class<?>)) {
				log.error("J2S: Key type of map {} must be a simple type (not a collection or a map)!", keyType);
			}
			else { // if value type instanceof ParameterizedType
				keyColumn = entryTable.addStandardColumn(Const.KEY_COL, (Class<?>) keyType);
				keyColumn.charsize = 512; // Less bytes allowed for keys in UNIQUE constraints
			}

			// Create value column - differ between simple objects and collections or maps as values
			if (valueType instanceof Class<?>) {
				entryTable.addStandardColumn(Const.VALUE_COL, (Class<?>) valueType);
			}
			else { // if value itself is collection or map
				Column valueColumn = entryTable.addStandardColumn(Const.VALUE_COL, String.class);
				valueColumn.isText = true;
			}

			// Add UNIQUE constraint on key
			entryTable.addUniqueConstraintFor(mainTableRefColumn, keyColumn);
		}

		return entryTable;
	}

	// Get UNIQUE constraint for elements for later dropping (if type of collection type changed from Set to List)
	public static UniqueConstraint getUniqueConstraintForElements(Table entryTable) {
		return (!CList.isEmpty(entryTable.uniqueConstraints) ? entryTable.uniqueConstraints.get(0) : null);
	}

	// Get UNIQUE constraint for element order (list fields)
	public static Column getElementOrderColumn(Table entryTable) {
		return (entryTable.columns.size() > 2 ? entryTable.columns.get(2) : null);
	}

	// Versions

	// Build ordered set of versions found in class or field annotations
	public static SortedSet<String> findAllVersions(List<Class<? extends SqlDomainObject>> domainClasses) {

		SortedSet<String> versions = new TreeSet<>();

		for (Class<? extends SqlDomainObject> cls : domainClasses) {

			// Class
			if (getCreatedVersion(cls).compareTo("1.0") > 0) {
				versions.add(getCreatedVersion(cls));
			}

			versions.addAll(getVersionsWithChanges(cls));

			if (!getRemovedVersion(cls).isEmpty()) {
				versions.add(getRemovedVersion(cls));
			}

			// Fields
			for (Field field : cls.getDeclaredFields()) {

				if (getCreatedVersion(field).compareTo("1.0") > 0) {
					versions.add(getCreatedVersion(field));
				}

				versions.addAll(getVersionsWithChanges(field));

				if (!getRemovedVersion(field).isEmpty()) {
					versions.add(getRemovedVersion(field));
				}
			}
		}

		log.info("J2S: \t\tFound versions {}", versions);

		return versions;
	}

	// TODO: Allow incremental adding of NOT NULL columns (create as NULL, UPDATE records by default value, set NOT NULL)
	// For adding NOT NULL columns: Get assumed column Jdbc type from field type (for Java2Sql where database and tables are not present)
	// public static Class<?> assumedJdbcTypeForNotNullColumn(Class<?> fieldType) {
	//
	// if (fieldType == String.class || fieldType == File.class || Enum.class.isAssignableFrom(fieldType)) {
	// return String.class;
	// }
	// else if (fieldType == boolean.class || fieldType == Boolean.class) {
	// return String.class; // 'TRUE' or 'FALSE' for true or false
	// }
	// else if (fieldType == int.class || fieldType == Integer.class) {
	// return Integer.class;
	// }
	// else if (fieldType == long.class || fieldType == Long.class) {
	// return Long.class;
	// }
	// else if (fieldType == double.class || fieldType == Double.class) {
	// return Double.class;
	// }
	// else {
	// return null;
	// }
	// }
	//
	// // For adding NOT NULL columns: Get default column value from column annotation by double conversion: String -> Object by field type and Object of field type -> Object of column's JDBC type
	// public static Object getDefaultValueForNotNullColumn(SqlColumn ca, Column column) {
	//
	// String defaultValue = ca.defaultValue();
	// if (defaultValue == null) {
	// log.error("J2S: Default value of new NOT NULL field '{}' not specified in @SqlColumn() annotation! Use: defaultValue = '<default value as string>'", column.field.getName());
	// return null;
	// }
	//
	// Class<?> columnType = assumedJdbcTypeForNotNullColumn(column.fieldRelatedType);
	// if (columnType == null) {
	// log.warn("J2S: Type '{}' of new NOT NULL field '{}' does not allow conversion of default value!", column.fieldRelatedType.getName(), column.field.getName());
	// return null;
	// }
	//
	// Object columnValue = null;
	// try {
	// columnValue = java2Sql(columnType, string2SimpleObject(column.fieldRelatedType, defaultValue));
	// return columnValue;
	// }
	// catch (SqlDbException e) {
	// log.warn("J2S: Default value '{}' cannot be converted to field type '{}' for new NOT NULL field '{}'!", defaultValue, column.fieldRelatedType.getName(), column.field.getName());
	// return null;
	// }
	// }
}
