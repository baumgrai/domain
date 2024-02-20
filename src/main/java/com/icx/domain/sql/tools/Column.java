package com.icx.domain.sql.tools;

import java.io.File;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.domain.DomainAnnotations.StoreAsString;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.SqlRegistry;
import com.icx.domain.sql.tools.FkConstraint.ConstraintType;
import com.icx.jdbc.SqlDb.DbType;

/**
 * Modeling a table column in context of {@link Java2Sql} tool
 * 
 * @author baumgrai
 */
public class Column {

	static final Logger log = LoggerFactory.getLogger(Column.class);

	// Members

	public Table table = null;

	public String name = null;
	public boolean isPrimaryKey = false;
	public boolean notNull = false;
	public boolean isUnique = false;
	public boolean isText = false;
	public int charsize = Java2Sql.DEFAULT_CHARSIZE;

	public Class<?> fieldRelatedType = null;
	public Field field = null;

	// Constructors

	// Only for domain class standard columns
	public Column(
			Table table,
			String name,
			Class<?> fieldType) {

		this.table = table;
		this.table.columns.add(this);

		this.name = name;

		fieldRelatedType = fieldType;
	}

	// For application specific columns
	public Column(
			Table table,
			Field field) {

		this.table = table;
		this.table.columns.add(this);

		this.field = field;

		name = SqlRegistry.buildColumnName(field, table.dbType);

		if (SqlDomainObject.class.isAssignableFrom(field.getType())) { // Foreign key columns
			fieldRelatedType = Long.class;
		}
		else if (field.isAnnotationPresent(StoreAsString.class)) {
			fieldRelatedType = String.class;
		}
		else {
			fieldRelatedType = field.getType();
		}

		if (fieldRelatedType.isPrimitive()) {
			notNull = true;
		}
	}

	// Methods

	public String definitionClause() {

		String type = null;
		DbType dbType = table.dbType;

		if (fieldRelatedType == boolean.class || fieldRelatedType == Boolean.class) {
			type = (dbType == DbType.ORACLE ? "NVARCHAR2(5)" : dbType == DbType.MS_SQL ? "NVARCHAR(5)" : dbType == DbType.MYSQL ? "VARCHAR(5)" : "");
		}
		else if (fieldRelatedType == char.class || fieldRelatedType == Character.class) {
			type = (dbType == DbType.ORACLE ? "NVARCHAR2(1)" : dbType == DbType.MS_SQL ? "NVARCHAR(1)" : dbType == DbType.MYSQL ? "VARCHAR(1)" : "");
		}
		// Note: TODO: Byte type is not supported because negative byte value will not be stored correctly using with Jdbc driver for MS/SQL database (stored as positive value) - use 'short' instead
		else if (fieldRelatedType == short.class || fieldRelatedType == Short.class) {
			type = (dbType == DbType.ORACLE ? "NUMBER" : dbType == DbType.MS_SQL ? "SMALLINT" : dbType == DbType.MYSQL ? "SMALLINT" : "");
		}
		else if (fieldRelatedType == int.class || fieldRelatedType == Integer.class) {
			type = (dbType == DbType.ORACLE ? "NUMBER" : dbType == DbType.MS_SQL ? "INTEGER" : dbType == DbType.MYSQL ? "INTEGER" : "");
		}
		else if (fieldRelatedType == long.class || fieldRelatedType == Long.class) {
			type = (dbType == DbType.ORACLE ? "NUMBER" : dbType == DbType.MS_SQL ? "BIGINT" : dbType == DbType.MYSQL ? "BIGINT" : "");
		}
		// Note: Float type is not supported because no JDBC driver stores and /or retrieves 'float' values correctly - use 'double' instead
		else if (fieldRelatedType == double.class || fieldRelatedType == Double.class) {
			type = (dbType == DbType.ORACLE ? "NUMBER" : dbType == DbType.MS_SQL ? "FLOAT" : dbType == DbType.MYSQL ? "DOUBLE" : "");
		}
		else if (fieldRelatedType == BigInteger.class) {
			type = (dbType == DbType.ORACLE ? "NUMBER" : dbType == DbType.MS_SQL ? "BIGINT" : dbType == DbType.MYSQL ? "BIGINT" : "");
		}
		else if (fieldRelatedType == BigDecimal.class) {
			type = (dbType == DbType.ORACLE ? "NUMBER" : dbType == DbType.MS_SQL ? "FLOAT" : dbType == DbType.MYSQL ? "DOUBLE" : "");
		}
		else if (fieldRelatedType == String.class) {
			if (isText) {
				type = (dbType == DbType.ORACLE ? "CLOB" : dbType == DbType.MS_SQL ? "NVARCHAR(MAX)" : dbType == DbType.MYSQL ? "LONGTEXT CHARACTER SET UTF8MB4" : "");
			}
			else {
				type = (dbType == DbType.ORACLE ? "NVARCHAR2" : dbType == DbType.MS_SQL ? "NVARCHAR" : dbType == DbType.MYSQL ? "VARCHAR" : "") + "(" + charsize + ")"
						+ (dbType == DbType.MYSQL ? " CHARACTER SET UTF8MB4" : "");
			}
		}
		else if (Enum.class.isAssignableFrom(fieldRelatedType)) {

			// Max enum length or MAX_ENUM_VALUE_LENGTH
			int length = Arrays.asList(fieldRelatedType.getDeclaredFields()).stream().mapToInt(f -> f.getName().length()).max().getAsInt();
			if (length < Java2Sql.MAX_ENUM_VALUE_LENGTH) {
				length = Java2Sql.MAX_ENUM_VALUE_LENGTH;
			}
			type = (dbType == DbType.ORACLE ? "NVARCHAR2" : dbType == DbType.MS_SQL ? "NVARCHAR" : dbType == DbType.MYSQL ? "VARCHAR" : "") + "(" + length + ")"
					+ (dbType == DbType.MYSQL ? " CHARACTER SET UTF8MB4" : "");
		}
		else if (fieldRelatedType == byte[].class || fieldRelatedType == File.class) {
			type = (dbType == DbType.ORACLE ? "BLOB" : dbType == DbType.MS_SQL ? "VARBINARY(MAX)" : dbType == DbType.MYSQL ? "LONGBLOB" : "");
		}
		else if (fieldRelatedType == char[].class) {
			type = (dbType == DbType.ORACLE ? "CLOB" : dbType == DbType.MS_SQL ? "NVARCHAR(MAX)" : dbType == DbType.MYSQL ? "LONGTEXT" : "");
		}
		else if (LocalDate.class.isAssignableFrom(fieldRelatedType)) {
			type = "DATE";
		}
		else if (LocalTime.class.isAssignableFrom(fieldRelatedType)) {
			type = (dbType == DbType.ORACLE ? "TIMESTAMP" : "TIME");
		}
		else if (LocalDateTime.class.isAssignableFrom(fieldRelatedType) || Date.class.isAssignableFrom(fieldRelatedType)) {
			type = (dbType == DbType.ORACLE ? "TIMESTAMP" : "DATETIME");
		}
		else {
			log.error("J2S: Field type '{}' is not supported for auto-conversion! Supported types are boolean, Boolean, short, Short, int, Integer, long, Long, double, Double, "
					+ "BigInteger, BigDecimal, String, List, Set, Map, Enum, File, byte[], LocalDateTime, LocalDate, LocalTime. "
					+ "Fields of other types must be annotated with @SaveAsString, and to-string and from-string converters must be provided using SqlDomainController.registerStringConvertersForType()",
					fieldRelatedType.getName());
			return "";
		}

		return "\t" + name + Java2SqlHelpers.tabs(name) + type + (isPrimaryKey ? Java2SqlHelpers.tabs(type) + "PRIMARY KEY" : notNull ? Java2SqlHelpers.tabs(type) + "NOT NULL" : "");
	}

	public String alterTableAddColumnStatement() {
		return ("ALTER TABLE " + table.name + " ADD " + definitionClause() + ";\n");
	}

	public String alterTableModifyColumnStatement(DbType dbType) {
		return ("ALTER TABLE " + table.name + (dbType == DbType.ORACLE ? " MODIFY " : dbType == DbType.MS_SQL ? " ALTER COLUMN " : " MODIFY COLUMN ") + definitionClause() + ";\n");
	}

	public String alterTableDropColumnStatement() {
		return ("ALTER TABLE " + table.name + " DROP COLUMN " + name + ";\n");
	}

	public FkConstraint addFkConstraint(ConstraintType type, Class<? extends SqlDomainObject> referencedDomainClass) {
		return new FkConstraint(this, type, SqlRegistry.buildTableName(referencedDomainClass, table.dbType));
	}

}
