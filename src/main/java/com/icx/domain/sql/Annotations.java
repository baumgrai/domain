package com.icx.domain.sql;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.icx.domain.sql.tools.Java2Sql;

/**
 * Annotations for SQL domain classes and fields for Domain object persistence mechanism.
 * 
 * @author baumgrai
 */
public abstract class Annotations {

	// ----------------------------------------------------------------------
	// Data horizon
	// ----------------------------------------------------------------------

	/**
	 * For object domain classes: Defines strategy for keeping domain class objects in local object store (heap) by SQL domain controller.
	 * <p>
	 * If a domain class misses this annotation all persisted objects will be loaded and registered on {@link SqlDomainController#synchronize(Class...)}. If, otherwise, this annotation is defined for
	 * a domain class, 'older' objects (which were not changed within a 'data horizon period' - specified as {@code dataHorizonPeriod} in {@code domain.properties}) will not be loaded and registered
	 * on startup, and objects will be unregistered automatically during runtime if they fall out of the data horizon period on subsequent call of {@code SqlDomainController#synchronize(Class...)}.
	 * <p>
	 * Reference integrity is guaranteed, which means that objects referenced by loaded objects are loaded even if they are out of data horizon period.
	 * <p>
	 * This feature is to avoid local data overload on over time growing object numbers.
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
	 * Definitions for version control: Creation
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Created {

		/**
		 * For domain classes and fields: Annotate version in which domain class or field was created.
		 * 
		 * @return version in which field/class was created, e.g. "1.1:notNull=false;unique=false" - (specification of attributes on creation is necessary for incremental update script if these
		 *         attributes are later were changed
		 */
		public String version() default "1.0";
	}

	/**
	 * Definitions for version control: Changes
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Changed {

		/**
		 * For domain classes and fields: Annotate versions in which domain class or field were modified.
		 * 
		 * @return versions in which field/class was changed including modification, e.g. for field: <code> { "1.2:notNull=true;unique=true", "2.0.1:numericalType=BigInteger" }, for domain class: {
		 *         "1.5:unique=firstName&amp;lastName;indexes=id,age" } </code>
		 */
		public String[] versions();
	}

	/**
	 * Definitions for version control: Removal
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Removed {

		/**
		 * For domain classes and fields: Annotate version in which domain class or field was removed.
		 * 
		 * @return version in which field/class was created
		 */
		public String version();
	}

	// ----------------------------------------------------------------------
	// For tables
	// ----------------------------------------------------------------------

	/**
	 * For domain classes: (Non default) table name, UNIQUE constraints and INDEXes of SQL table associated with domain class.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface SqlTable {

		/**
		 * Define name of associated persistence table to generate for domain class.
		 * 
		 * @return non default name of persistence table associated with this domain class
		 */
		public String name() default "";

		/**
		 * Define multi-column UNIQUE constraints for associated persistence table.
		 * <p>
		 * Multi-column UNIQUE constraint descriptions consists of comma separated lists of field names which are unique together (e.g.: { "a,b", "c,d" }).
		 * <p>
		 * SQL name of multi-column UNIQUE constraint is automatically built from field names contained.
		 * 
		 * @return UNIQUE constraints of persistence table
		 */
		public String[] uniqueConstraints() default {};

		/**
		 * Define INDEXs for associated persistence table.
		 * <p>
		 * INDEX descriptions consists of comma separated lists of field names which are unique together (e.g.: { "a", "c,d" }).
		 * <p>
		 * SQL name of INDEX is automatically built from field names contained.
		 * 
		 * @return UNIQUE constraints of persistence table
		 */
		public String[] indexes() default {};
	}

	/**
	 * For array, collection or map fields: name of (non-default) entry table which are associated to complex field
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface SqlEntryTable {

		/**
		 * Define name of associated entry persistence table to generate for array, collection or map field.
		 * 
		 * @return non default name of entry persistence table associated with this field
		 */
		public String name() default "";
	}

	// ----------------------------------------------------------------------
	// For fields
	// ----------------------------------------------------------------------

	public static final int DEFAULT_CHARSIZE = 1024;

	/**
	 * For fields which are associated to columns (not array, Collection or Map fields): (Non default) column name, NOT NULL and UNIQUE constraint, text length, etc. of column associated with field
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface SqlColumn {

		/**
		 * Define name of associated column in persistence table to generate field.
		 * 
		 * @return non default name of column in SQL table associated with this field of Java domain class
		 */
		public String name() default "";

		/**
		 * Define UNIQUE constraint for column associated to field.
		 * 
		 * @return true if database table column values must be unique, false otherwise
		 */
		public boolean unique() default false;

		/**
		 * Define char size of text column associated to field (default 1024).
		 * 
		 * @return size of character column associated with this field
		 */
		public int charsize() default DEFAULT_CHARSIZE;

		/**
		 * Define if field is to persist as 'text' field in database, which means a CLOB type is to use instead a simple TEXT type.
		 * 
		 * @return true if content of (character) column can be treated as SQL text (cannot be used in where clause) - to set TEXT/CLOB data type for column
		 */
		public boolean isText() default false;

		/**
		 * Define NOT NULL constraint for column associated to field.
		 * 
		 * @return true if database table column may contain null values - to define NULL constraint for column
		 */
		public boolean notNull() default false;

		/**
		 * Currently not supported!
		 * 
		 * @return default value to set for existing records if non-null column is added after creating table or is modified to NOT NULL in a specific version (version control)
		 */
		public String defaultValue() default "";

		/**
		 * Defines, that foreign key constraint associated to an object reference field has 'ON DELETE CASCADE' function.
		 * <p>
		 * Defining {@link #onDeleteCascade()} forces immediate deletion of child objects in database if parent object will be deleted.
		 * 
		 * @return true if foreign key constraint has 'ON DELETE CASCADE' feature
		 */
		public boolean onDeleteCascade() default false;
	}

	/**
	 * For fields: Forces that values of annotated field will be converted to String before they are persisted in database (works only for field types which are not natively supported by Domain object
	 * persistence mechanism).
	 * <p>
	 * Natively supported data types are Java basic data types, Date and LocalDate/Time types, Enum, File, collections and maps of these types and char[], byte[].
	 * <p>
	 * If a field has {@link StoreAsString} annotation so called 'from-string'- and 'to-string'-converters have to be defined for field type using
	 * {@link SqlDomainController#registerStringConvertersForType(Class, java.util.function.Function, java.util.function.Function)}, or alternatively field type must contain public methods
	 * {@code toString()} and {@code valueOf(String)} which do the job of to- and from-string-conversion.
	 * <p>
	 * {@link StoreAsString} annotation forces {@link Java2Sql} tool to generate a string ((N)VARCHAR(size)) column associated with this field. Charsize of column can be defined using
	 * {@link SqlColumn#charsize()} if it shall differ from default charsize 1024.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface StoreAsString {
	}

	/**
	 * For fields of type Set&lt;? extends SqlDomainObject&gt;: Defines field as managed 'accumulation' field.
	 * <p>
	 * An accumulation field in a parent domain class contains all child domain objects defined by a reference field of the child domain class.
	 * <p>
	 * Accumulation fields are managed automatically and may not be written by application.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.FIELD)
	public @interface Accumulation {

		/**
		 * Assign reference field of child domain class for parent/child relation.
		 * <p>
		 * Note: explicit defining of reference field {@link #refField()} is only necessary if child domain class has multiple reference fields to parent domain class. Otherwise reference field for
		 * accumulation will be determined automatically from generic type of Set.
		 * 
		 * @return name of reference field of child domain class used for this accumulation
		 */
		public String refField() default "";
	}

	/**
	 * For (simple) fields and also domain classes: Defines a field (or class) as secret, which means content will never be written to logs (independent of log level).
	 * <p>
	 * Suppresses logging of value of annotated field or of values of all registered fields of annotated domain class for all log levels.
	 * <p>
	 * Note: On INFO log level generally no data (content of any field) will be logged.
	 * <p>
	 * Attention! This annotation cannot suppress logging of complex field values: arrays, collections and maps.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.FIELD })
	public @interface Secret {
	}

	/**
	 * For fields: Defines that value of this string field will be stored encrypted in database.
	 * <p>
	 * Uses properties {@code encryption_password} (and {@code encryption_salt} too if wished) in configured in {@code domain.properties} for AES encryption.
	 */
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.FIELD })
	public @interface Crypt {
	}

}
