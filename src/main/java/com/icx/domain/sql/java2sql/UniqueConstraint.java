package com.icx.domain.sql.java2sql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Common;
import com.icx.domain.sql.SqlRegistry;
import com.icx.jdbc.SqlDbHelpers;

/**
 * Modeling a unique constraint in context of {@link Java2Sql} tool.
 * <p>
 * Class, methods and fields are 'public' only for formal reasons. Java2Sql class can be copied and must be runnable in any application 'domain' package to generate SQL scripts for application's
 * domain classes.
 * 
 * @author baumgrai
 */
public class UniqueConstraint {

	static final Logger log = LoggerFactory.getLogger(UniqueConstraint.class);

	Table table;
	List<String> columnNames = new ArrayList<>();

	public UniqueConstraint(
			Table table,
			Column... columns) {

		this.table = table;
		this.table.uniqueConstraints.add(this);

		columnNames.addAll(Stream.of(columns).map(c -> c.name).collect(Collectors.toList()));
	}

	public UniqueConstraint(
			Table table,
			List<String> fieldNamesFromAnnotation) {

		this.table = table;
		this.table.uniqueConstraints.add(this);

		for (String fieldName : fieldNamesFromAnnotation) {
			Field field = null;
			try {
				field = table.domainClass.getDeclaredField(fieldName);
				columnNames.add(SqlRegistry.buildColumnName(field, table.dbType));
			}
			catch (NoSuchFieldException | SecurityException e) {
				log.error("J2S: Column for field '{}' to add to UNIQUE constraint '{}' does not exist in table '{}'", fieldName, name(), table.name);
			}
		}
	}

	public String name() {

		String constraintName = "UNIQUE_" + table.name.substring(SqlRegistry.TABLE_PREFIX.length());
		for (String columnName : columnNames) {
			constraintName += (table.dbType.isMySql() ? "$" : "#") + columnName;
		}

		return SqlDbHelpers.identifier(constraintName, table.dbType);
	}

	public String definitionClause() {
		return "CONSTRAINT " + name() + " UNIQUE (" + Common.listToString(columnNames) + ")";
	}

	public String alterTableAddConstraintStatement() {
		return "ALTER TABLE " + table.name + " ADD " + definitionClause() + ";\n";
	}

	public String alterTableDropConstraintStatement() {
		return "ALTER TABLE " + table.name + " DROP " + (table.dbType.isMySql() ? "INDEX " : "CONSTRAINT ") + name() + ";\n";
	}
}
