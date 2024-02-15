package com.icx.domain.sql.tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.domain.sql.SqlRegistry;
import com.icx.jdbc.SqlDbHelpers;
import com.icx.jdbc.SqlDb.DbType;

/**
 * Modeling a foreign key constraint for a table column in context of {@link Java2Sql} tool
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
	public ConstraintType type = null;

	public boolean onDeleteCascade = false;

	public FkConstraint(
			Column column,
			ConstraintType type,
			String referencedTableName) {

		this.column = column;
		this.column.table.fkConstraints.add(this);

		this.type = type;
		this.referencedTableName = referencedTableName;

		if (this.type == ConstraintType.INHERITANCE) {
			this.onDeleteCascade = true;
		}
	}

	public String name() {

		String part1 = "FK_" + column.table.name.substring(SqlRegistry.TABLE_PREFIX.length());
		String part2 = (type == ConstraintType.INHERITANCE ? ConstraintType.INHERITANCE.name() : column.name.substring(0, column.name.length() - "_ID".length()));

		return SqlDbHelpers.identifier(part1 + (column.table.dbType == DbType.MYSQL ? "$" : "#") + part2, column.table.dbType);
	}

	public String alterTableAddForeignKeyStatement() {

		StringBuilder sb = new StringBuilder();

		sb.append("ALTER TABLE " + column.table.name + " ADD CONSTRAINT " + name() + " FOREIGN KEY (" + column.name + ") REFERENCES " + referencedTableName + "(ID)");

		if (onDeleteCascade) {
			sb.append(" ON DELETE CASCADE");
		}

		sb.append(";\n");

		return sb.toString();
	}

	public String alterTableDropForeignKeyStatement() {

		StringBuilder sb = new StringBuilder();

		sb.append("ALTER TABLE " + column.table.name + " DROP " + (column.table.dbType == DbType.MYSQL ? "FOREIGN KEY " : "CONSTRAINT ") + name() + ";\n");

		return sb.toString();
	}
}
