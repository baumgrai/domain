package com.icx.domain.sql.tools;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.CList;
import com.icx.domain.DomainAnnotations;
import com.icx.domain.DomainAnnotations.Changed;
import com.icx.domain.DomainAnnotations.Created;
import com.icx.domain.DomainAnnotations.Removed;
import com.icx.domain.Registry;
import com.icx.domain.sql.Const;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.SqlRegistry;
import com.icx.domain.sql.tools.FkConstraint.ConstraintType;
import com.icx.jdbc.JdbcHelpers;
import com.icx.jdbc.SqlDb.DbType;

/**
 * Helpers for {@link Java2Sql} tool
 * 
 * @author baumgrai
 */
public class Java2SqlHelpers extends JdbcHelpers {

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

	// Check if field was created in version
	public static boolean wasCreatedInVersion(Field field, String version) {
		return (DomainAnnotations.wasCreatedInVersion(field.getAnnotation(Created.class), version));
	}

	// Get version where field was created in
	public static String getCreatedVersion(Field field) {
		return (DomainAnnotations.getCreatedVersion(field.getAnnotation(Created.class)));
	}

	// Get create information for field
	public static Map<String, String> getCreateInfo(Field field) {
		return DomainAnnotations.getCreateInfo(field.getAnnotation(Created.class));
	}

	// Check if field was changed in version
	public static boolean wasChangedInVersion(Field field, String version) {
		return (DomainAnnotations.wasChangedInVersion(field.getAnnotation(Changed.class), version));
	}

	// Get versions in which field was changed
	public static List<String> getVersionsWithChanges(Field field) {
		return DomainAnnotations.getVersionsWithChanges(field.getAnnotation(Changed.class));
	}

	// Get change information for field and version
	public static Map<String, String> getChangeInfo(Field field, String version) {
		return DomainAnnotations.getChangeInfo(field.getAnnotation(Changed.class), version);
	}

	// Check if field was removed in version
	public static boolean wasRemovedInVersion(Field field, String version) {
		return (DomainAnnotations.wasRemovedInVersion(field.getAnnotation(Removed.class), version));
	}

	// Get version where field was created in
	public static String getRemovedVersion(Field field) {
		return (DomainAnnotations.getRemovedVersion(field.getAnnotation(Removed.class)));
	}

	// Check if any changes to given field column are to consider for given version
	public static boolean isFieldAffected(Field field, String version) {
		return (wasCreatedInVersion(field, version) || wasChangedInVersion(field, version) || wasRemovedInVersion(field, version));
	}

	// Domain classes

	// Check if domain class was created in version
	public static boolean wasCreatedInVersion(Class<?> cls, String version) {
		return (DomainAnnotations.wasCreatedInVersion(cls.getAnnotation(Created.class), version));
	}

	// Get version where domain class was created in
	public static String getCreatedVersion(Class<?> cls) {
		return (DomainAnnotations.getCreatedVersion(cls.getAnnotation(Created.class)));
	}

	// Get create information for domain class
	public static Map<String, String> getCreateInfo(Class<?> cls) {
		return DomainAnnotations.getCreateInfo(cls.getAnnotation(Created.class));
	}

	// Check if domain class was changed in version
	public static boolean wasChangedInVersion(Class<?> cls, String version) {
		return (DomainAnnotations.wasChangedInVersion(cls.getAnnotation(Changed.class), version));
	}

	// Get versions in which domain class was changed
	public static List<String> getVersionsWithChanges(Class<?> cls) {
		return DomainAnnotations.getVersionsWithChanges(cls.getAnnotation(Changed.class));
	}

	// Get change information for domain class and version
	public static Map<String, String> getChangeInfo(Class<?> cls, String version) {
		return DomainAnnotations.getChangeInfo(cls.getAnnotation(Changed.class), version);
	}

	// Check if domain class was removed in version
	public static boolean wasRemovedInVersion(Class<?> cls, String version) {
		return (DomainAnnotations.wasRemovedInVersion(cls.getAnnotation(Removed.class), version));
	}

	// Get version where domain class was created in
	public static String getRemovedVersion(Class<?> cls) {
		return (DomainAnnotations.getRemovedVersion(cls.getAnnotation(Removed.class)));
	}

	// Incremental update scripts

	public static void updateColumnAttributes(Column column, Map<String, String> changeInfoMap) {

		if (changeInfoMap.containsKey(DomainAnnotations.CHARSIZE)) {
			column.charsize = Integer.valueOf(changeInfoMap.get(DomainAnnotations.CHARSIZE));
			column.isText = false; // If charsize is given in version info column can only be a 'normal' text column, not a CLOB, etc. in this version
			log.info("J2S: \t\t\tColumn '{}': Got charsize {} from version info", column.name, column.charsize);
		}

		if (changeInfoMap.containsKey(DomainAnnotations.NOT_NULL)) {
			column.notNull = (column.field.getType().isPrimitive() || Boolean.valueOf(changeInfoMap.get(DomainAnnotations.NOT_NULL)));
			log.info("J2S: \t\t\tColumn '{}': Got NOT NULL {} from version info", column.name, column.notNull);
		}

		if (changeInfoMap.containsKey(DomainAnnotations.UNIQUE)) { // Do not set column attribute here - to allow subsequent check for change of UNIQUE state
			log.info("J2S: \t\t\tColumn '{}': Got UNIQUE {} from version info", column.name, Boolean.valueOf(changeInfoMap.get(DomainAnnotations.UNIQUE)));
		}

		if (changeInfoMap.containsKey(DomainAnnotations.IS_TEXT)) {
			column.isText = Boolean.valueOf(changeInfoMap.get(DomainAnnotations.IS_TEXT));
			log.info("J2S: \t\t\tColumn '{}': Got 'isText' {} from version info", column.name, column.isText);
		}

		if (changeInfoMap.containsKey(DomainAnnotations.NUMERICAL_TYPE)) {
			String numericalType = changeInfoMap.get(DomainAnnotations.NUMERICAL_TYPE);
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

		// Use explicitly specified type if given, field type otherwise
		Class<?> currentType = field.getType();
		if (currentCollectionTypeString != null) {
			if (currentCollectionTypeString.equalsIgnoreCase("list")) {
				currentType = List.class;
			}
			else if (currentCollectionTypeString.equalsIgnoreCase("set")) {
				currentType = Set.class;
			}
			else if (currentCollectionTypeString.equalsIgnoreCase("both")) {
				currentType = null;
			}
		}

		// Create table
		Table entryTable = new Table(field, dbType);

		log.info("J2S: \t\tCreate entry table '{}' for field '{}' of type '{}'", entryTable.name, field.getName(), field.getType().getSimpleName());

		// Create main table reference column and associated foreign key constraint
		Column mainTableRefColumn = entryTable.addStandardColumn(SqlRegistry.buildMainTableRefColumnName(field, dbType), Long.class);
		mainTableRefColumn.notNull = true;
		mainTableRefColumn.addFkConstraint(ConstraintType.MAIN_TABLE, registry.getCastedDeclaringDomainClass(field));

		// Create entry columns
		if (currentType == null || currentType.isArray() || Collection.class.isAssignableFrom(currentType)) { // Collection field

			Type elementType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];

			// Create element column - allow collections or maps of simple objects as element type - convert with string2list and string2map and vice versa
			Column elementColumn = null;
			if (elementType instanceof Class<?>) {
				elementColumn = entryTable.addStandardColumn(Const.ELEMENT_COL, (Class<?>) elementType);
			}
			else { // if elementType instanceof ParameterizedType
				elementColumn = entryTable.addStandardColumn(Const.ELEMENT_COL, String.class);
			}

			// Add UNIQUE constraint for element sets (or if type is unknown)
			if (currentType == null || Set.class.isAssignableFrom(currentType)) {
				elementColumn.charsize = 512; // Less bytes allowed for keys in UNIQUE constraints (differs by database type)
				entryTable.addUniqueConstraintFor(mainTableRefColumn, elementColumn);
			}

			// Add add order column for element lists (or if type is unknown)
			if (currentType == null || currentType.isArray() || List.class.isAssignableFrom(currentType)) {
				entryTable.addStandardColumn(Const.ORDER_COL, Integer.class);
			}
		}
		else { // Map field

			Type keyType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
			Type valueType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[1];

			// Create key column - may not be a collection or map itself
			Column keyColumn = null;
			if (!(keyType instanceof Class<?>)) {
				log.error("J2S: Key type of map {} must be a simple type (not a collection or a map)!", keyType);
			}
			else { // if elementType instanceof ParameterizedType
				keyColumn = entryTable.addStandardColumn(Const.KEY_COL, (Class<?>) keyType);
				keyColumn.charsize = 512; // Less bytes allowed for keys in UNIQUE constraints
			}

			// Create value column - differ between simple objects and collections or maps as values
			if (valueType instanceof Class<?>) {
				entryTable.addStandardColumn(Const.VALUE_COL, (Class<?>) valueType);
			}
			else { // if elementType instanceof ParameterizedType
				entryTable.addStandardColumn(Const.VALUE_COL, String.class);
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

	// Get UNIQUE constraint for elements for later dropping (if type of collection type changed from Set to List)
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

	// NOT NULL

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
