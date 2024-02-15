package com.icx.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.icx.common.base.Common;

/**
 * Objects of this class are associated to SQL database tables and contain table and column meta data.
 * <p>
 * {@code SqlDbTable} objects are created on registering database tables using {@link SqlDb#registerTable(Connection, String)} and can then be retrieved by table name using
 * {@link SqlDb#findRegisteredTable(String)}.
 * <p>
 * Table metadata is stored in fields of {@code SqlDbTable}, column metadata in fields of {@link SqlDbColumn}, {@link SqlDbForeignKeyColumn}, {@link SqlDbUniqueConstraint} subclasses.
 * <p>
 * Provides also methods to inspect table and column reference structure of database {@link #getReachableTables()}, {@link #reaches(SqlDbTable)}, {@link #getTablesWhichCanReach()},
 * {@link #isReachableFrom(SqlDbTable)}, {@link SqlDbForeignKeyColumn#reaches(SqlDbTable)}.
 * 
 * @author baumgrai
 */
public class SqlDbTable extends Common implements Comparable<SqlDbTable> {

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	/**
	 * Class containing SQL table column meta information and associated methods
	 */
	public class SqlDbColumn implements Comparable<SqlDbColumn> {

		/**
		 * SQL table containing this column
		 */
		public SqlDbTable table = null;

		/**
		 * Column order by table creation statement
		 */
		public int order = -1;

		/**
		 * Column name
		 */
		@SuppressWarnings("hiding")
		public String name = null;

		/**
		 * SQL data type (varies for different database systems)
		 */
		public String datatype = null;

		/**
		 * Maximum (character) length
		 */
		public Integer maxlen = 0;

		/**
		 * Is column primary key?
		 */
		public boolean isPrimaryKey = false;

		/**
		 * May column be null?
		 */
		public boolean isNullable = true;

		/**
		 * Must column be UNIQUE?
		 */
		public boolean isUnique = false;

		/**
		 * JDBC type of column
		 */
		public Class<?> jdbcType = null;

		/**
		 * Type of field associated with column (in known)
		 */
		public Class<?> fieldType = null;

		// Constructor
		protected SqlDbColumn() {
		}

		// Constructor from table
		protected SqlDbColumn(
				SqlDbTable table) {

			this.table = table;
		}

		@Override
		public final int compareTo(SqlDbColumn column) {
			return this.order - column.order;
		}

		@Override
		public final boolean equals(Object obj) {
			return super.equals(obj);
		}

		@Override
		public final int hashCode() {
			return super.hashCode();
		}

		protected String toBaseString() {
			return name + " " + datatype + ((datatype.contains("char") || datatype.contains("CHAR")) && maxlen != null ? "(" + maxlen + ")" : "")
					+ (isPrimaryKey ? " PRIMARY KEY" : (isUnique ? " UNIQUE" : "") + (!isNullable ? " NOT NULL" : ""));
		}

		// For registry logging
		public String toStringWithoutTable() {
			return toBaseString() + " (" + SqlDbHelpers.getTypeString(jdbcType) + ")";
		}

		public String toStringWithoutTable(Class<?> requiredType) {

			if (objectsEqual(SqlDbHelpers.getTypeString(jdbcType), requiredType.getName())) {
				return toStringWithoutTable();
			}
			else {
				String s = toStringWithoutTable();
				return s.substring(0, s.length() - 1) + " -> " + requiredType.getName() + ")";
			}
		}

		@Override
		public String toString() {
			return table.name + "." + toStringWithoutTable();
		}

		/**
		 * Check if column is a FOREIGN KEY column
		 * 
		 * @return true if column is a FOREIGN KEY column, false otherwise
		 */
		public boolean isForeignKey() {
			return (this instanceof SqlDbForeignKeyColumn);
		}

		/**
		 * Check if column is IDENTITY column (means values are auto-generated on INSERT)
		 * 
		 * @return true if FOREIGN KEY column is IDENTITY, false otherwise
		 */
		public boolean isIdentity() {
			return datatype.toUpperCase().contains("IDENTITY");
		}
	}

	/**
	 * FOREIGN KEY column class
	 */
	public class SqlDbForeignKeyColumn extends SqlDbColumn {

		/**
		 * UNIQUE column referenced by this FOREIGN KEY column
		 */
		public SqlDbColumn referencedUniqueColumn = null;

		// Constructor from table
		protected SqlDbForeignKeyColumn(
				SqlDbTable table) {

			super(table);
		}

		// Constructor from column
		protected SqlDbForeignKeyColumn(
				SqlDbColumn column) {

			table = column.table;
			order = column.order;
			name = column.name;
			datatype = column.datatype;
			maxlen = column.maxlen;
			isNullable = column.isNullable;
			isPrimaryKey = column.isPrimaryKey;
		}

		// For registry logging
		@Override
		public String toStringWithoutTable() {
			return toBaseString() + " FOREIGN KEY REFERENCES " + referencedUniqueColumn.table.name + "(" + referencedUniqueColumn.name + ") (" + SqlDbHelpers.getTypeString(jdbcType) + ")";
		}

		@Override
		public String toString() {
			return table.name + "." + toStringWithoutTable();
		}

		// Check if this FOREIGN KEY column references directly or indirectly given table
		private boolean reaches(SqlDbTable otherTable, List<SqlDbForeignKeyColumn> alreadyCheckedFkColumns) {

			if (referencedUniqueColumn.table.equals(otherTable))
				return true;

			if (alreadyCheckedFkColumns.contains(this))
				return false;
			else
				alreadyCheckedFkColumns.add(this);

			return referencedUniqueColumn.table.reaches(otherTable, alreadyCheckedFkColumns);
		}

		/**
		 * Check if this FOREIGN KEY column references directly or indirectly given table
		 * 
		 * @param otherTable
		 *            table to check
		 * 
		 * @return true if at least one reference exists, false otherwise
		 */
		public boolean reaches(SqlDbTable otherTable) {
			return reaches(otherTable, new ArrayList<>());
		}
	}

	/**
	 * Class for table's UNIQUE constraints
	 */
	public class SqlDbUniqueConstraint {

		/**
		 * SQL table containing this column
		 */
		public SqlDbTable table = null;

		/**
		 * Constraint name
		 */
		@SuppressWarnings("hiding")
		public String name = null;

		/**
		 * Columns which are UNIQUE together
		 */
		@SuppressWarnings("hiding")
		public Set<SqlDbColumn> columns = new HashSet<>();

		public String toStringWithoutTable() {

			StringBuilder s = new StringBuilder();
			s.append("CONSTRANT " + name + " UNIQUE (");
			for (SqlDbColumn column : columns) {
				s.append(column.name);
				s.append(",");
			}
			s.replace(s.length() - 1, s.length(), "");
			s.append(")");

			return s.toString();
		}

		@Override
		public String toString() {
			return table.name + "." + toStringWithoutTable();
		}
	}

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Database connection object
	private SqlDb db = null;

	/**
	 * Database table name
	 */
	public String name = null;

	/**
	 * Sorted set of columns of this table ordered by column order from table creation statement
	 */
	public SortedSet<SqlDbColumn> columns = new TreeSet<>();

	/**
	 * IDENTITY column (if such a column exists) or null
	 */
	public SqlDbColumn identityColumn = null;

	/**
	 * UNIQUE constraints of table
	 */
	public Set<SqlDbUniqueConstraint> uniqueConstraints = new HashSet<>();

	/**
	 * FOREIGN KEY columns of this table or other tables referencing this table
	 */
	public Set<SqlDbForeignKeyColumn> fkColumnsReferencingThisTable = new HashSet<>();

	// -------------------------------------------------------------------------
	// Constructor
	// -------------------------------------------------------------------------

	/**
	 * Constructor
	 * 
	 * @param name
	 *            table name
	 * @param db
	 *            Database connection object
	 */
	public SqlDbTable(
			String name,
			SqlDb db) {

		this.name = name;
		this.db = db;
	}

	// -------------------------------------------------------------------------
	// Overrides
	// -------------------------------------------------------------------------

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();
		sb.append("\nTABLE " + name + "\n(\n");
		for (SqlDbColumn c : columns) {
			sb.append("\t" + c.toStringWithoutTable() + "\n");
		}
		if (!uniqueConstraints.isEmpty()) {
			sb.append("\n");
			for (SqlDbUniqueConstraint uc : uniqueConstraints) {
				sb.append("\t" + uc.toStringWithoutTable() + "\n");
			}
		}
		sb.append(")\n");

		return sb.toString();
	}

	// Compare tables by reference relation and order of registration
	@Override
	public final int compareTo(SqlDbTable other) {
		return db.halfOrderedTables.indexOf(this) - db.halfOrderedTables.indexOf(other);
	}

	@Override
	public final boolean equals(Object obj) {
		return super.equals(obj);
	}

	@Override
	public final int hashCode() {
		return super.hashCode();
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * Get FOREIGN KEY columns of this table
	 * 
	 * @return FOREIGN KEY columns
	 */
	public List<SqlDbForeignKeyColumn> getFkColumns() {

		List<SqlDbForeignKeyColumn> fkColumns = new ArrayList<>();
		for (SqlDbColumn column : this.columns) {
			if (column.isForeignKey()) {
				fkColumns.add((SqlDbForeignKeyColumn) column);
			}
		}

		return fkColumns;
	}

	/**
	 * Get FOREIGN KEY columns (of this table) which are referencing given table
	 * 
	 * @param table
	 *            table which is referenced
	 * 
	 * @return FOREIGN KEY columns referencing given table - may be empty
	 */
	public List<SqlDbForeignKeyColumn> getFkColumnsReferencingTable(SqlDbTable table) {

		List<SqlDbForeignKeyColumn> fkColumns = new ArrayList<>();
		for (SqlDbForeignKeyColumn fkColumn : getFkColumns()) {
			if (fkColumn.referencedUniqueColumn.table.equals(table)) {
				fkColumns.add(fkColumn);
			}
		}

		return fkColumns;
	}

	/**
	 * Find column of this table by name
	 * 
	 * @param columnName
	 *            column name
	 * 
	 * @return column or null if no column with given name exists
	 */
	public SqlDbColumn findColumnByName(String columnName) {

		for (SqlDbColumn column : columns) {
			if (objectsEqual(column.name, columnName)) {
				return column;
			}
		}

		return null;
	}

	/**
	 * Get column names in definition order
	 * 
	 * @return column names
	 */
	public List<String> getColumnNames() {
		return columns.stream().map(c -> c.name).collect(Collectors.toList());
	}

	/**
	 * Find UNIQUE constraint by name
	 * 
	 * @param ucName
	 *            name of UNIQUE constraint
	 * 
	 * @return UNIQUE constraint or null if no UNIQUE constraint with given name exists
	 */
	public SqlDbUniqueConstraint findUniqueConstraintByName(String ucName) {

		for (SqlDbUniqueConstraint uniqueConstraint : uniqueConstraints) {
			if (objectsEqual(uniqueConstraint.name, ucName)) {
				return uniqueConstraint;
			}
		}

		return null;
	}

	/**
	 * Find UNIQUE constraints where column is involved by column name
	 * 
	 * @param columnName
	 *            name of column
	 * 
	 * @return UNIQUE constraints containing column - may be empty
	 */
	public Set<SqlDbUniqueConstraint> findUniqueConstraintsByColumnName(String columnName) {

		Set<SqlDbUniqueConstraint> uniqueConstraintsWithColumn = new HashSet<>();

		for (SqlDbUniqueConstraint uniqueConstraint : uniqueConstraints) {
			for (SqlDbColumn column : uniqueConstraint.columns) {
				if (objectsEqual(column.name, columnName)) {
					uniqueConstraintsWithColumn.add(uniqueConstraint);
				}
			}
		}

		return uniqueConstraintsWithColumn;
	}

	// Check if table references another table directly or indirectly by one of the FOREIGN KEYs
	private boolean reaches(SqlDbTable table, List<SqlDbForeignKeyColumn> alreadyCheckedFkColumns) {

		for (SqlDbForeignKeyColumn fkColumn : getFkColumns()) {
			if (fkColumn.reaches(table, alreadyCheckedFkColumns)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Check if this table references another table directly or indirectly
	 * 
	 * @param table
	 *            other table to check
	 * 
	 * @return true if at least one reference path exists, false otherwise
	 */
	public boolean reaches(SqlDbTable table) {
		return reaches(table, new ArrayList<>());
	}

	/**
	 * Check if this table is referenced directly or indirectly by another table
	 * 
	 * @param table
	 *            table to check
	 * 
	 * @return true if at least one reference path exists, false otherwise
	 */
	public boolean isReachableFrom(SqlDbTable table) {
		return table.reaches(this);
	}

	/**
	 * Get (registered) tables which are referencing directly or indirectly this table
	 * 
	 * @return tables which are directly or indirectly referencing this table
	 */
	public Set<SqlDbTable> getTablesWhichCanReach() {

		SortedSet<SqlDbTable> referencingTables = new TreeSet<>();

		for (SqlDbTable table : db.halfOrderedTables) {
			if (table.reaches(this)) {
				referencingTables.add(table);
			}
		}

		return referencingTables;
	}

	/**
	 * Get (registered) tables which are referenced directly or indirectly by this table
	 * 
	 * @return tables which are directly or indirectly referencing this table
	 */
	public Set<SqlDbTable> getReachableTables() {

		SortedSet<SqlDbTable> referencedTables = new TreeSet<>();

		for (SqlDbTable table : db.halfOrderedTables) {
			if (table.isReachableFrom(this)) {
				referencedTables.add(table);
			}
		}

		return referencedTables;
	}

}
