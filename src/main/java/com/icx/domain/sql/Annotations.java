package com.icx.domain;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.icx.common.base.Common;
import com.icx.domain.sql.SqlDomainController;
import com.icx.domain.sql.tools.Java2Sql;

/**
 * Annotations for domain classes and fields for Domain object persistence mechanism.
 * 
 * @author baumgrai
 */
public abstract class DomainAnnotations {

	// ----------------------------------------------------------------------
	// Data horizon
	// ----------------------------------------------------------------------

	/**
	 * For object domain classes: Defines loading strategy for domain class objects by domain controller. If a domain class misses this annotation all persisted objects will be loaded and registered
	 * ({@link SqlDomainController#synchronize(Class...)} on startup of domain controller. If - otherwise - this annotation is defined for a domain class, 'older' objects (which were not changed
	 * within a 'data horizon period' - specified as {@code dataHorizonPeriod} in {@code domain.properties}) will not be loaded and registered on startup, and objects will be unregistered
	 * automatically during runtime if they fall out of the data horizon period on subsequent call of {@code SqlDomainController#synchronize(Class...)}. Reference integrity is guaranteed, what means
	 * that objects referenced by objects within data horizon period are loaded even if they are out of this period themselves. This feature is to avoid data overload on over time growing object
	 * numbers.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface UseDataHorizon {
	}

	// ----------------------------------------------------------------------
	// Version control
	// ----------------------------------------------------------------------

	// Allowed table changes for version control
	public static final String UNIQUE_CONSTRAINTS = "unique";
	public static final String UNIQUE_CONSTRAINTS_TO_DROP = "notUnique";
	public static final String INDEXES = "indexes";
	public static final String INDEXES_TO_DROP = "indexesToDrop";

	// Allowed field changes for version control
	public static final String NOT_NULL = "notNull";
	public static final String UNIQUE = "unique";
	public static final String CHARSIZE = "charsize";
	public static final String IS_TEXT = "isText";
	public static final String NUMERICAL_TYPE = "numericalType";
	public static final String COLLECTION_TYPE = "collectionType"; // 'list' or 'map'

	/**
	 * Definitions for version control - creation
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Created {

		/**
		 * @return version in which field/class was created, e.g. "1.1:notNull=false;unique=false" - (specification of attributes on creation is necessary for incremental update script if these
		 *         attributes are later were changed
		 */
		public String version() default "1.0";
	}

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

	/**
	 * Definitions for version control - changes
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Changed {

		/**
		 * @return versions in which field/class was changed including modification, e.g. for field: <code> { "1.2:notNull=true;unique=true", "2.0.1:numericalType=BigInteger" }, for domain class: {
		 *         "1.5:unique=firstName&lastName;indexes=id,age" } </code>
		 */
		public String[] versions();
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

	/**
	 * Definitions for version control - removal
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Removed {

		/**
		 * @return version in which field/class was created
		 */
		public String version();
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

	// ----------------------------------------------------------------------
	// Tables
	// ----------------------------------------------------------------------

	/**
	 * For main tables of domain classes: (Non default) table name, UNIQUE constraints and INDEXes of SQL table associated with domain class
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface SqlTable {

		/**
		 * @return non default name of persistence table associated with this domain class - to define non default table name and allowing changing field name afterwards
		 */
		public String name() default "";

		/**
		 * @return UNIQUE constraints of persistence table containing comma separated lists of field names which are unique together - to define multi-column unique constraints. Names of constraints
		 *         are automatically build from field names contained.
		 */
		public String[] uniqueConstraints() default {};

		/**
		 * @return column INDEXes of persistence table containing comma separated lists of field names where indexes are to build on - to define these indexes. Names of indexes are automatically build
		 *         from field names contained.
		 */
		public String[] indexes() default {};
	}

	/**
	 * For entry tables which are associated to collection or map fields
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface SqlEntryTable {

		/**
		 * @return non default name of persistence table associated with this domain class - to define non default table name
		 */
		public String name() default "";
	}

	// ----------------------------------------------------------------------
	// Fields and accumulations
	// ----------------------------------------------------------------------

	/**
	 * For fields of domain classes which are associated to columns: (Non default) column name, NOT NULL and UNIQUE constraint, text length of column associated with field of a domain class
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface SqlColumn {

		/**
		 * @return non default name of column in SQL table associated with this field of Java domain class
		 */
		public String name() default "";

		/**
		 * @return true if database table column values must be unique - to define UNIQUE constraint for column
		 */
		public boolean unique() default false;

		/**
		 * @return size of character column associated with this (String, enum, List, Map) field (default 256) - to define column size
		 */
		public int charsize() default Java2Sql.DEFAULT_CHARSIZE;

		/**
		 * @return true if content of (character) column can be treated as SQL text (cannot be used in where clause) - to set TEXT/CLOB data type for column
		 */
		public boolean isText() default false;

		/**
		 * @return true if database table column may contain null values - to define NULL constraint for column
		 */
		public boolean notNull() default false;

		/**
		 * @return default value to set for existing records if non-null column is added after creating table or is modified to NOT NULL in a specific version (version control)
		 */
		public String defaultValue() default "";

		/**
		 * @return true if foreign key constraint has 'ON DELETE CASCADE' feature - to force ON DELETE CASCADE on this foreign key
		 */
		public boolean onDeleteCascade() default false;
	}

	/**
	 * Forces that values of annotated field shall be converted to string before storing in database and therefore forces {@link Java2Sql} tool to generate a string ((N)VARCHAR(size)) column
	 * associated to this field.
	 * <p>
	 * Charsize of column for annotated field can be defined with {@link SqlColumn} annotation if it shall differ from default charsize 1024.
	 * <p>
	 * Note: For any type of fields to store as string a to-string and a from-string converter has to be registered using
	 * {@link SqlDomainController#registerStringConvertersForType(Class, java.util.function.Function, java.util.function.Function)} (the to-string converter may also be {@link Object#toString()}
	 * itself).
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface StoreAsString {
	}

	/**
	 * Defines <b>accumulation</b> field.
	 * <p>
	 * An accumulation contains all children and therefore refers to a reference field of the child domain class referencing the domain class where the accumulation field is defined. Accumulation
	 * fields are managed automatically and should only be read by applications.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Accumulation {

		/**
		 * @return name of reference field of child objects to accumulate used for this accumulation. Must only be defined if multiple fields reference same type of objects.
		 */
		public String refField() default "";
	}

	/**
	 * Defines a field as secret.
	 * <p>
	 * Suppresses logging of value of annotated field or of values of all registered fields of annotated domain class for all log levels.
	 * <p>
	 * Attention! This annotation cannot suppress logging of complex field values like lists, sets and maps
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Secret {
	}

	/**
	 * Defines that value of this string field will be stored encrypted in database.
	 * <p>
	 * Uses properties {@code encryption_password} and {@code encryption_salt} for AES encryption
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.FIELD })
	public @interface Crypt {
	}

}
