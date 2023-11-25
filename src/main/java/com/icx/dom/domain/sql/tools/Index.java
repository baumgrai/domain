package com.icx.dom.domain.sql.tools;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.Common;
import com.icx.dom.domain.sql.SqlRegistry;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlDb.DbType;

public class Index {

	static final Logger log = LoggerFactory.getLogger(Index.class);

	Table table;
	List<String> columnNames = new ArrayList<>();

	public Index(
			Table table,
			Column... columns) {

		this.table = table;
		this.table.indexes.add(this);

		columnNames.addAll(Stream.of(columns).map(c -> c.name).collect(Collectors.toList()));
	}

	public Index(
			Table table,
			List<String> fieldNamesFromAnnotation) {

		this.table = table;
		this.table.indexes.add(this);

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

		String indexName = SqlRegistry.TABLE_PREFIX + "IDX_" + table.name.substring(SqlRegistry.TABLE_PREFIX.length()); // Skip 'DOM_'
		for (String columnName : columnNames) {
			indexName += (table.dbType == DbType.MYSQL ? "$" : "#") + columnName;
		}

		return JdbcHelpers.identifier(indexName, table.dbType);
	}

	public String createStatement() {
		return "CREATE INDEX " + name() + " ON " + table.name + " (" + Common.listToString(columnNames) + ");\n";
	}

	public String dropStatement() {
		return "DROP INDEX " + name() + ";\n";
	}
}
