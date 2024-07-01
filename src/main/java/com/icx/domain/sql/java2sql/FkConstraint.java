package com.icx.domain.sql.java2sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.domain.sql.SqlRegistry;
import com.icx.jdbc.SqlDbHelpers;

/**
 * Modeling a foreign key constraint for a table column in context of {@link Java2Sql} tool.
 * <p>
 * Class, methods and fields are 'public' only for formal reasons. Java2Sql class can be copied and must be runnable in any application 'domain' package to generate SQL scripts for application's
 * domain classes.
 * 
 * @author baumgrai
 */
public class FkConstraint {

	static final Logger log = LoggerFactory.getLogger(FkConstraint.class);

	public enum ConstraintType {
		REFERENCE, INHERITANCE, MAIN_TABLE
	}

	public Column column = null;
	public String referencedTableName = null;
	public String referencedColumnName = null;
	public ConstraintType type = null;

	public boolean onDeleteCascade = false;

	public FkConstraint(
			Column column,
			ConstraintType type,
			String referencedTableName,
			String referencedColumnName) {

		this.column = column;
		this.column.table.fkConstraints.add(this);

		this.type = type;
		this.referencedTableName = referencedTableName;
		this.referencedColumnName = referencedColumnName;

		if (this.type == ConstraintType.INHERITANCE) {
			this.onDeleteCascade = true;
		}
	}

	public String name() {

		String part1 = "FK_" + column.table.name.substring(SqlRegistry.TABLE_PREFIX.length());
		String part2 = (type == ConstraintType.INHERITANCE ? ConstraintType.INHERITANCE.name() : column.name.substring(0, column.name.length() - ("_" + referencedColumnName).length()));

		return SqlDbHelpers.identifier(part1 + (column.table.dbType.isMySql() ? "$" : "#") + part2, column.table.dbType);
	}

	public String alterTableAddForeignKeyStatement() {

		StringBuilder sb = new StringBuilder();

		sb.append("ALTER TABLE " + column.table.name + " ADD CONSTRAINT " + name() + " FOREIGN KEY (" + column.name + ") REFERENCES " + referencedTableName + "(" + referencedColumnName + ")");

		if (onDeleteCascade) {
			sb.append(" ON DELETE CASCADE");
		}

		sb.append(";\n");

		return sb.toString();
	}

	public String alterTableDropForeignKeyStatement() {

		StringBuilder sb = new StringBuilder();

		sb.append("ALTER TABLE " + column.table.name + " DROP " + (column.table.dbType.isMySql() ? "FOREIGN KEY " : "CONSTRAINT ") + name() + ";\n");

		return sb.toString();
	}
}
