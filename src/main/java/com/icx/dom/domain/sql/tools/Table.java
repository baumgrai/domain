package com.icx.dom.domain.sql.tools;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.sql.SqlDomainObject;
import com.icx.dom.domain.sql.SqlRegistry;
import com.icx.dom.jdbc.SqlDb.DbType;

/**
 * Modeling a database table in context of {@link Java2Sql} tool
 * 
 * @author baumgrai
 */
public class Table {

	static final Logger log = LoggerFactory.getLogger(Table.class);

	// Members

	public DbType dbType = null;

	public String name = null;

	public List<Column> columns = new ArrayList<>();

	public List<FkConstraint> fkConstraints = new ArrayList<>();
	public List<UniqueConstraint> uniqueConstraints = new ArrayList<>();
	public List<Index> indexes = new ArrayList<>();

	public Class<? extends SqlDomainObject> domainClass = null; // Not for entry tables

	// Constructors

	// Constructor for main table
	public Table(
			Class<? extends SqlDomainObject> domainClass,
			DbType dbType) {

		this.name = SqlRegistry.buildTableName(domainClass, dbType);
		this.dbType = dbType;
		this.domainClass = domainClass;
	}

	// Constructor for entry table (related to collection or map field)
	public Table(
			Field field,
			DbType dbType) {

		this.name = SqlRegistry.buildEntryTableName(field, dbType);
		this.dbType = dbType;
	}

	// Methods

	public Column addStandardColumn(String columnName, Class<?> fieldType) {
		return new Column(this, columnName, fieldType);
	}

	public Column addColumnForRegisteredField(Field field) {

		Column column = new Column(this, field);

		SqlColumn ca = field.getAnnotation(SqlColumn.class);
		if (ca != null) {

			if (!field.getType().isPrimitive()) {
				column.notNull = ca.notNull();
			}

			if (ca.unique()) {
				column.isUnique = true;
				addUniqueConstraintFor(column);
			}

			if (ca.isText()) {
				column.isText = true; // Forces CLOB/NVARCHAR(MAX)/LONGTEXT data type
			}
			else if (ca.charsize() != Java2Sql.DEFAULT_CHARSIZE) {
				column.charsize = ca.charsize();
			}

			boolean nameAttributeDefined = false;
			if (!ca.name().isEmpty()) { // Redundant here because SqlRegistry.buildColumnName() already considers 'name' attribute of @SqlColumn annotation - used here for logging
				column.name = ca.name();
				nameAttributeDefined = true;
			}

			log.info("J2S: \t\t\tColumn '{}': attributes got from @SqlColumn: {}{}{}{}{}", column.name, (nameAttributeDefined ? "name: '" + column.name + "' " : ""),
					(column.notNull ? "NOT NULL " : ""), (column.isUnique ? "UNIQUE " : ""), (column.charsize != 256 ? "charsize: " + column.charsize + " " : ""), (column.isText ? "isText" : ""));
		}

		return column;
	}

	public UniqueConstraint addUniqueConstraintFor(Column... columns) {
		return new UniqueConstraint(this, columns);
	}

	public UniqueConstraint addUniqueConstraintFor(List<String> fieldNames) {
		return new UniqueConstraint(this, fieldNames);
	}

	public Index addIndexFor(Column... columns) {
		return new Index(this, columns);
	}

	public Index addIndexFor(List<String> fieldNames) {
		return new Index(this, fieldNames);
	}

	public String createScript() {

		StringBuilder sb = new StringBuilder();

		sb.append("CREATE TABLE " + name + "\n(\n");

		for (Column column : columns) {
			sb.append(column.definitionClause() + ",\n");
		}

		for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
			sb.append("\n\t" + uniqueConstraint.definitionClause() + ",\n");
		}
		sb.replace(sb.length() - 2, sb.length(), "\n);\n"); // ",\n" -> "\n);\n"

		for (Index index : indexes) {
			sb.append(index.createStatement());
		}

		sb.append("\n");

		return sb.toString();
	}

	public String dropScript() {

		StringBuilder sb = new StringBuilder();

		sb.append("DROP TABLE " + name + ";\n");

		return sb.toString();
	}
}
