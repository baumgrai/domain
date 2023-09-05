package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CBase;
import com.icx.dom.common.Reflection;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.SqlTable;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.GuavaReplacements.CaseFormat;
import com.icx.dom.domain.Registry;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDb.DbType;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

/**
 * Singleton to register Java class/SQL table and field/column associations. Based on domain class registration of {@link Registry}. Only used internally.
 * 
 * @author RainerBaumgärtel
 */
public abstract class SqlRegistry extends Registry {

	static final Logger log = LoggerFactory.getLogger(SqlRegistry.class);

	public static final String TABLE_PREFIX = "DOM_";

	// -------------------------------------------------------------------------
	// Statics
	// -------------------------------------------------------------------------

	private static Field idField = null;
	private static Field lastModifiedInDbField = null;

	static {
		try {
			idField = DomainObject.class.getDeclaredField(DomainObject.ID_FIELD);
			lastModifiedInDbField = SqlDomainObject.class.getDeclaredField(SqlDomainObject.LAST_MODIFIED_IN_DB_FIELD);
		}
		catch (Exception e) {
			log.error("SRG: Internal error: 'DomainObject' class does not contain field {} or 'SqlDomainObject' class does not contain field {}!", DomainObject.ID_FIELD,
					SqlDomainObject.LAST_MODIFIED_IN_DB_FIELD);
		}
	}

	// Basic domain class/table and field/column association
	private static Map<Class<? extends DomainObject>, SqlDbTable> sqlTableByDomainClassMap = new HashMap<>();
	private static Map<Field, Column> sqlColumnByFieldMap = new HashMap<>();
	private static Map<Column, Class<?>> sqlReqiredJdbcTypeByColumnMap = new HashMap<>();

	// Table and column associations for both element collection and key/value map (together called 'table related') fields
	private static Map<Field, SqlDbTable> sqlTableByComplexFieldMap = new HashMap<>();
	private static Map<Field, Column> sqlMainRecordRefIdColumnByComplexFieldMap = new HashMap<>();

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	// Build SQL table name from domain class name considering eventually existing table name annotations
	public static String buildTableName(Class<?> domainClass, DbType dbType) {

		String tableName = null;
		if (domainClass.isAnnotationPresent(SqlTable.class) && !CBase.isEmpty(domainClass.getAnnotation(SqlTable.class).name())) {
			tableName = domainClass.getAnnotation(SqlTable.class).name().toUpperCase();
		}
		else {
			String prefix = "";
			if (domainClass.isMemberClass()) {
				prefix = CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, domainClass.getDeclaringClass().getSimpleName()) + "_";
			}
			tableName = TABLE_PREFIX + prefix + CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, domainClass.getSimpleName());
		}

		return JdbcHelpers.identifier(tableName, dbType);
	}

	// Builds SQL database column name from Java domain class field considering eventually existing column name annotations
	public static String buildColumnName(Field field, DbType dbType) {

		if (field.isAnnotationPresent(SqlColumn.class) && !CBase.isEmpty(field.getAnnotation(SqlColumn.class).name())) { // Name from annotation

			return JdbcHelpers.identifier(field.getAnnotation(SqlColumn.class).name().toUpperCase(), dbType);
		}
		else {
			String columnName = JdbcHelpers.identifier(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.getName()), dbType);

			if (isReferenceField(field)) {
				columnName += "_ID";
			}
			else if (columnName.equals("START") || columnName.equals("END") || columnName.equals("COUNT") || columnName.equals("COMMENT") || columnName.equals("DATE") || columnName.equals("TYPE")
					|| columnName.equals("GROUP")) {
				columnName = TABLE_PREFIX + columnName;
			}

			return columnName;
		}
	}

	// Build SQL element list or key/value table name from set/map field name
	public static String buildEntryTableName(Field complexField, DbType dbType) {
		return JdbcHelpers.identifier(buildTableName(complexField.getDeclaringClass(), dbType) + "_" + buildColumnName(complexField, dbType), dbType);
	}

	// Builds SQL database column name from table related field (element collection or key/value map)
	public static String buildMainTableRefColumnName(Field refFieldToMainTable, DbType dbType) {
		return JdbcHelpers.identifier(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, refFieldToMainTable.getDeclaringClass().getSimpleName()) + "_ID", dbType);
	}

	// Table name for domain class
	public static SqlDbTable getTableFor(Class<? extends DomainObject> domainClass) {

		SqlDbTable table = sqlTableByDomainClassMap.get(domainClass);
		if (table == null) {
			log.error("SRG: Internal error! No table associated to domain class '{}'!", domainClass.getName());
			return null;
		}

		return table;
	}

	// Get associated column for field
	public static Column getColumnFor(Field field) {

		Column column = sqlColumnByFieldMap.get(field);
		if (column == null) {
			log.error("SRG: Internal error! No column associated to field '{}'!", Reflection.qualifiedName(field));
			return null;
		}

		return column;
	}

	// Element or key/value table for table related field
	public static SqlDbTable getEntryTableFor(Field complexField) {

		SqlDbTable elementTable = sqlTableByComplexFieldMap.get(complexField);
		if (elementTable == null) {
			log.error("SRG: Internal error! No element or key/value table associated to field '{}'!", Reflection.qualifiedName(complexField));
			return null;
		}

		return elementTable;
	}

	// Column with referenced object's id for table related field
	public static Column getMainTableRefIdColumnFor(Field complexField) {

		Column refIdColumn = sqlMainRecordRefIdColumnByComplexFieldMap.get(complexField);
		if (refIdColumn == null) {
			log.error("SRG: Internal error! No referenced id column associated to element collection or key/value map field '{}'!", Reflection.qualifiedName(complexField));
			return null;
		}

		return refIdColumn;
	}

	// Get field type for column - to retrieve SELECT results with correct type
	public static Class<?> getRequiredJdbcTypeFor(Column column) {

		if (column == null) {
			log.error("SRG: Internal error! Column to get field for is null!");
			return null;
		}

		if (SqlDomainObject.ID_COL.equals(column.name)) {
			return idField.getType();
		}
		else if (SqlDomainObject.LAST_MODIFIED_COL.equals(column.name)) {
			return lastModifiedInDbField.getType();
		}
		else if (SqlDomainObject.DOMAIN_CLASS_COL.equals(column.name)) {
			return String.class;
		}

		Class<?> fieldClass = sqlReqiredJdbcTypeByColumnMap.get(column);
		if (fieldClass == null) {
			log.error("SRG: Internal error! No field associated to column  '{}'!", column.name);
			return null;
		}

		return fieldClass;
	}

	// Get field for column - only used in error handling
	public static Field getFieldFor(Column column) {

		for (Entry<Field, Column> entry : sqlColumnByFieldMap.entrySet()) {
			if (CBase.objectsEqual(column, entry.getValue())) {
				return entry.getKey();
			}
		}

		return null;
	}

	// Associate column and field - only [ column -> field ] is unique because fields 'id' and 'lastModificatioInDb' are not fields of individual domain object class but of base class 'DomainObject'
	private static void associateColumnAndField(Column column, Field field) {

		sqlColumnByFieldMap.put(field, column);

		if (isReferenceField(field)) {
			sqlReqiredJdbcTypeByColumnMap.put(column, Reflection.getBoxingWrapperType(idField.getType())); // Do not use idField.getType() - long - here because references may be null
		}
		else {
			sqlReqiredJdbcTypeByColumnMap.put(column, Helpers.requiredJdbcTypeFor(field.getType()));
		}
	}

	// -------------------------------------------------------------------------
	// Register association of domain classes and SQL tables
	// -------------------------------------------------------------------------

	// Register database tables for all domain classes and detect Java/SQL inconsistencies
	// Stop registration immediately on serious inconsistency
	static void registerDomainClassTableAssociation(Connection cn, SqlDb sqlDb) throws SQLException, SqlDbException {

		log.info("SRG: [ Domain class : table associations ]:...");
		for (Class<? extends DomainObject> domainClass : getRegisteredDomainClasses()) {

			// Register SQL database table for objects and store class/table relation
			SqlDbTable registeredTable = sqlDb.registerTable(cn, buildTableName(domainClass, sqlDb.getDbType()));
			sqlTableByDomainClassMap.put(domainClass, registeredTable);

			log.info("SRG: \t[ {} : {} ]:...", domainClass.getSimpleName(), registeredTable.name);

			List<Field> deprecatedFields = new ArrayList<>();
			boolean javaSqlInconsistence = false;

			// Check static [ 'id' : ID ] field : column relation
			Column idColumn = registeredTable.findColumnByName(SqlDomainObject.ID_COL);
			if (idColumn != null) {
				log.info("SRG: \t\t[ {} ({}) : {} ]", Reflection.qualifiedName(idField), idField.getType().getSimpleName(), idColumn.toStringWithoutTable(idField.getType()));
			}
			else {
				throw new SqlDbException(
						"Detected Java : SQL inconsistency! Table '" + registeredTable.toString() + "' associated to domain class '" + domainClass.getName() + "' does not have ID column!");
			}

			// Check column 'DOMAIN_CLASS'
			Column domainClassColumn = registeredTable.findColumnByName(SqlDomainObject.DOMAIN_CLASS_COL);
			if (domainClassColumn == null) {
				throw new SqlDbException(
						"Detected Java : SQL inconsistency! Table '" + registeredTable.toString() + "' associated to domain class '" + domainClass.getName() + "' does not have DOMAIN_CLASS column!");
			}

			// For base (or only) domain classes check static [ 'lastModifiedInDb' : LAST_MODIFIED ] field : column relation
			if (isBaseDomainClass(domainClass)) {

				Column lastModifiedColumn = registeredTable.findColumnByName(SqlDomainObject.LAST_MODIFIED_COL);
				if (lastModifiedColumn != null) {
					log.info("SRG: \t\t[ {} ({}) : {} ]", Reflection.qualifiedName(lastModifiedInDbField), lastModifiedInDbField.getType().getSimpleName(),
							lastModifiedColumn.toStringWithoutTable(lastModifiedInDbField.getType()));
				}
				else {
					throw new SqlDbException(
							"Detected Java : SQL inconsistency! Table " + registeredTable + " associated to domain class '" + domainClass.getName() + "' does not have LAST_MODIFIED column!");
				}
			}

			// Register [ field : column ] relation for data and reference fields
			for (Field field : getDataAndReferenceFields(domainClass)) {

				// Find column for field in object table
				String columnName = buildColumnName(field, sqlDb.getDbType());
				Column column = registeredTable.findColumnByName(columnName);
				if (column != null) {

					// Check if field is primitive and column is nullable
					if (field.getType().isPrimitive() && column.isNullable) {

						// Stop registration
						javaSqlInconsistence = true;
						log.error("SRG: Nullable column '{}' of table '{}' cannot be associated to primitive field '{}' of domain class '{}' (which do not allow null values)!", columnName,
								registeredTable.name, Reflection.qualifiedName(field), domainClass.getName());
					}

					// Store [ field : column ] relation
					associateColumnAndField(column, field);
					log.info("SRG: \t\t[ {} ({}) : {} ]", Reflection.qualifiedName(field), field.getType().getSimpleName(), column.toStringWithoutTable(field.getType()));
				}
				else { // If associated column does not exist...

					if (field.isAnnotationPresent(Deprecated.class)) {

						// Warn on deprecated fields
						log.warn(
								"SRG: Table '{}' associated to domain class '{}' does not have column '{}' associated to deprecated field '{}'! Unregister and further ignore this field in context of persistance.",
								registeredTable.name, domainClass.getSimpleName(), columnName, Reflection.qualifiedName(field));

						// Mark field to unregister
						deprecatedFields.add(field);
					}
					else {
						// Stop registration on serious inconsistency
						javaSqlInconsistence = true;
						log.error("SRG: Table '{}' associated to domain class '{}' does not have column '{}' associated to field '{}'!", registeredTable, domainClass.getName(), columnName,
								Reflection.qualifiedName(field));
					}
				}
			}

			// Register entry table and object reference column for element list or key/value map field and store [ table related field : entry table ] relation
			for (Field complexField : getComplexFields(domainClass)) {

				String tableName = buildEntryTableName(complexField, sqlDb.getDbType());
				try {
					SqlDbTable entryTable = sqlDb.registerTable(cn, tableName);
					String refIdColumnName = buildMainTableRefColumnName(complexField, sqlDb.getDbType());
					Column refIdColumn = entryTable.findColumnByName(refIdColumnName);
					if (refIdColumn == null) {

						// If object reference column does not exist...
						log.error("SRG: Entry table '{}' misses column referencing object id '{}'!", entryTable.name, refIdColumnName);
						javaSqlInconsistence = true;
					}
					else {
						// Register entry table
						sqlTableByComplexFieldMap.put(complexField, entryTable);
						sqlMainRecordRefIdColumnByComplexFieldMap.put(complexField, refIdColumn);

						if (Collection.class.isAssignableFrom(complexField.getType())) {
							Type elementType = ((ParameterizedType) complexField.getGenericType()).getActualTypeArguments()[0];

							log.info("SRG: \t\t[ {} : {}.{} ({}) ]", Reflection.fieldDeclaration(complexField), entryTable.name, refIdColumn.name, elementType.getTypeName());
						}
						else {
							Type keyType = ((ParameterizedType) complexField.getGenericType()).getActualTypeArguments()[0];
							Type valueType = ((ParameterizedType) complexField.getGenericType()).getActualTypeArguments()[1];

							log.info("SRG: \t\t[ {} : {}.{} ({}/{}) ]", Reflection.fieldDeclaration(complexField), entryTable.name, refIdColumn.name, keyType.getTypeName(), valueType.getTypeName());
						}
					}
				}
				catch (SqlDbException sqldbex) {

					// If entry table does not exist...
					if (complexField.isAnnotationPresent(Deprecated.class)) {

						// Warn on deprecated fields
						log.warn(
								"SRG: Table '{}' associated to deprecated element collection or key/value map field '{}' probably does not exist! Unregister and further ignore this field in context of persistance.",
								tableName, Reflection.qualifiedName(complexField));

						// Mark field to unregister
						deprecatedFields.add(complexField);
					}
					else {
						// Stop registration on serious inconsistency
						javaSqlInconsistence = true;
						log.error("SRG: ", sqldbex);
					}
				}
			}

			// Check Java : SQL inconsistency
			if (javaSqlInconsistence) {
				throw new SqlDbException("Detected Java : SQL inconsistency!");
			}

			// Unregister deprecated fields where no column or entry table is associated to
			deprecatedFields.forEach(f -> unregisterField(f));

			// Check if field for data and reference column exists
			for (Column column : registeredTable.columns) {
				if (!CBase.objectsEqual(column.name, SqlDomainObject.ID_COL) && !CBase.objectsEqual(column.name, SqlDomainObject.LAST_MODIFIED_COL)
						&& !CBase.objectsEqual(column.name, SqlDomainObject.DOMAIN_CLASS_COL)
						&& getDataAndReferenceFields(domainClass).stream().map(f -> getColumnFor(f).name).noneMatch(n -> CBase.objectsEqual(column.name, n))) {

					log.warn("SRG: Table '{}' associated to domain class '{}' has column '{}' where no field of this class is associated to!", registeredTable.name, domainClass.getSimpleName(),
							column.name);
				}
			}
		}
	}

}