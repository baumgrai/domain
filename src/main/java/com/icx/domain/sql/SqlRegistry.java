package com.icx.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CReflection;
import com.icx.domain.DomainObject;
import com.icx.domain.GuavaReplacements.CaseFormat;
import com.icx.domain.sql.Annotations.Secret;
import com.icx.domain.sql.Annotations.SqlColumn;
import com.icx.domain.sql.Annotations.SqlTable;
import com.icx.domain.Registry;
import com.icx.jdbc.SqlDbHelpers;
import com.icx.jdbc.SqlDb;
import com.icx.jdbc.SqlDb.DbType;
import com.icx.jdbc.SqlDbException;
import com.icx.jdbc.SqlDbTable;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;

/**
 * Register Java class/SQL table and field/column associations. Based on domain class registration of {@link Registry}. Only used internally.
 * 
 * @author baumgrai
 */
public class SqlRegistry extends Registry<SqlDomainObject> {

	static final Logger log = LoggerFactory.getLogger(SqlRegistry.class);

	public static final String TABLE_PREFIX = "DOM_";
	static final String SECRET_PREFIX = "SEC_";

	// -------------------------------------------------------------------------
	// Statics
	// -------------------------------------------------------------------------

	private static Field idField = null;
	private static Field lastModifiedInDbField = null;

	static {
		try {
			idField = DomainObject.class.getDeclaredField(Const.ID_FIELD);
			lastModifiedInDbField = SqlDomainObject.class.getDeclaredField(Const.LAST_MODIFIED_IN_DB_FIELD);
		}
		catch (Exception e) {
			log.error("SRG: Internal error: 'DomainObject' class does not contain field {} or 'SqlDomainObject' class does not contain field {}!", Const.ID_FIELD, Const.LAST_MODIFIED_IN_DB_FIELD);
		}
	}

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Basic domain class/table and field/column association
	private Map<Class<? extends SqlDomainObject>, SqlDbTable> sqlTableByDomainClassMap = new HashMap<>();
	private Map<Field, SqlDbColumn> sqlColumnByFieldMap = new HashMap<>();

	// Table and column associations for both element collection and key/value map (together called 'table related') fields
	private Map<Field, SqlDbTable> sqlTableByComplexFieldMap = new HashMap<>();
	private Map<Field, SqlDbColumn> sqlMainRecordRefIdColumnByComplexFieldMap = new HashMap<>();

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	// Reference field -> column with foreign key constraint
	private static boolean isSqlReferenceField(Field field) { // Static method here to use in tools
		return SqlDomainObject.class.isAssignableFrom(field.getType());
	}

	// Build SQL table name from domain class name considering eventually existing table name annotations
	public static String buildTableName(Class<? extends SqlDomainObject> domainClass, DbType dbType) {

		String tableName = null;
		if (domainClass.isAnnotationPresent(SqlTable.class) && !isEmpty(domainClass.getAnnotation(SqlTable.class).name())) {
			tableName = domainClass.getAnnotation(SqlTable.class).name().toUpperCase();
		}
		else {
			String prefix = "";
			if (domainClass.isMemberClass()) {
				prefix = CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, domainClass.getDeclaringClass().getSimpleName()) + "_";
			}
			if (domainClass.isAnnotationPresent(Secret.class)) {
				prefix += SECRET_PREFIX;
			}
			tableName = TABLE_PREFIX + prefix + CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, domainClass.getSimpleName());
		}

		return SqlDbHelpers.identifier(tableName, dbType);
	}

	// Builds SQL database column name from Java domain class field considering eventually existing column name annotations
	public static String buildColumnName(Field field, DbType dbType) {

		if (field.isAnnotationPresent(SqlColumn.class) && !isEmpty(field.getAnnotation(SqlColumn.class).name())) { // Name from annotation
			return SqlDbHelpers.identifier(field.getAnnotation(SqlColumn.class).name().toUpperCase(), dbType);
		}
		else {
			String columnName = SqlDbHelpers.identifier(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, field.getName()), dbType);
			if (isSqlReferenceField(field)) {
				columnName += "_ID";
			}
			else if (field.isAnnotationPresent(Secret.class) && !(field.getGenericType() instanceof ParameterizedType)) { // Ignore @Secret annotation on List, Set, Map fields
				columnName = SECRET_PREFIX + columnName;
			}
			else if (columnName.equals("START") || columnName.equals("END") || columnName.equals("COUNT") || columnName.equals("NUMBER") || columnName.equals("COMMENT") || columnName.equals("DATE")
					|| columnName.equals("TYPE") || columnName.equals("GROUP") || columnName.equals("FILE") || columnName.equals("LONGTEXT")) {
				columnName = TABLE_PREFIX + columnName;
			}

			return columnName;
		}
	}

	// Build SQL element list or key/value table name from set/map field name
	@SuppressWarnings("unchecked")
	public static String buildEntryTableName(Field complexField, DbType dbType) {
		return SqlDbHelpers.identifier(buildTableName((Class<? extends SqlDomainObject>) complexField.getDeclaringClass(), dbType) + "_" + buildColumnName(complexField, dbType), dbType);
	}

	// Builds SQL database column name from table related field (element collection or key/value map)
	public static String buildMainTableRefColumnName(Field refFieldToMainTable, DbType dbType) {
		return SqlDbHelpers.identifier(CaseFormat.UPPER_CAMEL.to(CaseFormat.UPPER_UNDERSCORE, refFieldToMainTable.getDeclaringClass().getSimpleName()) + "_ID", dbType);
	}

	// Table name for domain class
	public SqlDbTable getTableFor(Class<? extends SqlDomainObject> domainClass) {
		SqlDbTable table = sqlTableByDomainClassMap.get(domainClass);
		if (table == null) {
			log.error("SRG: Internal error! No table associated to domain class '{}'!", domainClass.getName());
			return null;
		}
		return table;
	}

	// Get associated column for field
	public SqlDbColumn getColumnFor(Field field) {
		SqlDbColumn column = sqlColumnByFieldMap.get(field);
		if (column == null) {
			log.error("SRG: Internal error! No column associated to field '{}'!", CReflection.qualifiedName(field));
			return null;
		}
		return column;
	}

	// Element or key/value table for table related field
	public SqlDbTable getEntryTableFor(Field complexField) {
		SqlDbTable elementTable = sqlTableByComplexFieldMap.get(complexField);
		if (elementTable == null) {
			log.error("SRG: Internal error! No element or key/value table associated to field '{}'!", CReflection.qualifiedName(complexField));
			return null;
		}
		return elementTable;
	}

	// Column with referenced object's id for table related field
	public SqlDbColumn getMainTableRefIdColumnFor(Field complexField) {
		SqlDbColumn refIdColumn = sqlMainRecordRefIdColumnByComplexFieldMap.get(complexField);
		if (refIdColumn == null) {
			log.error("SRG: Internal error! No referenced id column associated to element collection or key/value map field '{}'!", CReflection.qualifiedName(complexField));
			return null;
		}
		return refIdColumn;
	}

	// Get field for column - only used in error handling
	public Field getFieldFor(SqlDbColumn column) {
		for (Entry<Field, SqlDbColumn> entry : sqlColumnByFieldMap.entrySet()) {
			if (objectsEqual(column, entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	// -------------------------------------------------------------------------
	// Register association of domain classes and SQL tables
	// -------------------------------------------------------------------------

	// Register database tables for all domain classes and detect Java/SQL inconsistencies
	// Stop registration immediately on serious inconsistency
	void registerDomainClassTableAssociation(Connection cn, SqlDb sqlDb) throws SQLException, SqlDbException {

		log.info("SRG: [ Domain class : table associations ]:...");
		for (Class<? extends SqlDomainObject> domainClass : getRegisteredDomainClasses()) {

			// Register SQL database table for objects and store class/table relation
			SqlDbTable registeredTable = sqlDb.registerTable(cn, buildTableName(domainClass, sqlDb.getDbType()));
			sqlTableByDomainClassMap.put(domainClass, registeredTable);
			log.info("SRG: \t[ {} : {} ]:...", domainClass.getSimpleName(), registeredTable.name);

			List<Field> deprecatedFields = new ArrayList<>();
			boolean javaSqlInconsistence = false;

			// Check static [ 'id' : ID ] field : column relation
			SqlDbColumn idColumn = registeredTable.findColumnByName(Const.ID_COL);
			if (idColumn == null) {
				throw new SqlDbException(
						"Detected Java : SQL inconsistency! Table '" + registeredTable.toString() + "' associated to domain class '" + domainClass.getName() + "' does not have ID column!");
			}
			idColumn.fieldType = Long.class;
			log.info("SRG: \t\t[ {} ({}) : {} ]", CReflection.qualifiedName(idField), idField.getType().getSimpleName(), idColumn.toStringWithoutTable(idField.getType()));

			// Check column 'DOMAIN_CLASS'
			SqlDbColumn domainClassColumn = registeredTable.findColumnByName(Const.DOMAIN_CLASS_COL);
			if (domainClassColumn == null) {
				throw new SqlDbException(
						"Detected Java : SQL inconsistency! Table '" + registeredTable.toString() + "' associated to domain class '" + domainClass.getName() + "' does not have DOMAIN_CLASS column!");
			}
			domainClassColumn.fieldType = String.class;

			// For base (or only) domain classes check static [ 'lastModifiedInDb' : LAST_MODIFIED ] field : column relation
			if (isBaseDomainClass(domainClass)) {

				SqlDbColumn lastModifiedColumn = registeredTable.findColumnByName(Const.LAST_MODIFIED_COL);
				if (lastModifiedColumn == null) {
					throw new SqlDbException(
							"Detected Java : SQL inconsistency! Table " + registeredTable + " associated to domain class '" + domainClass.getName() + "' does not have LAST_MODIFIED column!");
				}
				lastModifiedColumn.fieldType = LocalDateTime.class;
				log.info("SRG: \t\t[ {} ({}) : {} ]", CReflection.qualifiedName(lastModifiedInDbField), lastModifiedInDbField.getType().getSimpleName(),
						lastModifiedColumn.toStringWithoutTable(lastModifiedInDbField.getType()));
			}

			// Register [ field : column ] relation for data and reference fields
			for (Field field : getDataAndReferenceFields(domainClass)) {

				// Find column for field in object table
				String columnName = buildColumnName(field, sqlDb.getDbType());
				SqlDbColumn column = registeredTable.findColumnByName(columnName);
				if (column != null) {

					// Check if field is primitive and column is nullable
					if (field.getType().isPrimitive() && column.isNullable) {

						// Stop registration
						javaSqlInconsistence = true;
						log.error("SRG: Nullable column '{}' of table '{}' cannot be associated to primitive field '{}' of domain class '{}' (which do not allow null values)!", columnName,
								registeredTable.name, CReflection.qualifiedName(field), domainClass.getName());
					}

					// Store [ field : column ] relation
					sqlColumnByFieldMap.put(field, column);

					// Assign field type to column
					if (isReferenceField(field)) {
						column.fieldType = Long.class;
					}
					else {
						column.fieldType = field.getType();
					}

					log.info("SRG: \t\t[ {} ({}) : {} ]", CReflection.qualifiedName(field), field.getType().getSimpleName(), column.toStringWithoutTable(field.getType()));
				}
				else { // If associated column does not exist...

					if (field.isAnnotationPresent(Deprecated.class)) {
						log.warn(
								"SRG: Table '{}' associated to domain class '{}' does not have column '{}' associated to deprecated field '{}'! Unregister and further ignore this field in context of persistance.",
								registeredTable.name, domainClass.getSimpleName(), columnName, CReflection.qualifiedName(field));
						deprecatedFields.add(field); // Mark to unregister
					}
					else {
						// Stop registration on serious inconsistency
						javaSqlInconsistence = true;
						log.error("SRG: Table '{}' associated to domain class '{}' does not have column '{}' associated to field '{}'!", registeredTable, domainClass.getName(), columnName,
								CReflection.qualifiedName(field));
					}
				}
			}

			// Register entry table and object reference column for element list or key/value map field and store [ table related field : entry table ] relation
			for (Field complexField : getComplexFields(domainClass)) {

				String tableName = buildEntryTableName(complexField, sqlDb.getDbType());
				try {
					SqlDbTable entryTable = sqlDb.registerTable(cn, tableName);
					String refIdColumnName = buildMainTableRefColumnName(complexField, sqlDb.getDbType());
					SqlDbColumn refIdColumn = entryTable.findColumnByName(refIdColumnName);
					if (refIdColumn == null) { // Object reference column does not exist
						log.error("SRG: Entry table '{}' misses column referencing object id '{}'!", entryTable.name, refIdColumnName);
						javaSqlInconsistence = true;
					}
					else {
						// Register entry table
						sqlTableByComplexFieldMap.put(complexField, entryTable);
						sqlMainRecordRefIdColumnByComplexFieldMap.put(complexField, refIdColumn);

						if (complexField.getType().isArray()) {
							Class<?> elementType = complexField.getType().getComponentType();
							entryTable.findColumnByName(Const.ELEMENT_COL).fieldType = elementType;
							log.info("SRG: \t\t[ {} : {}.{} ({}) ]", CReflection.fieldDeclaration(complexField), entryTable.name, refIdColumn.name, elementType.getTypeName());
						}
						else if (Collection.class.isAssignableFrom(complexField.getType())) {
							Type elementType = ((ParameterizedType) complexField.getGenericType()).getActualTypeArguments()[0];
							entryTable.findColumnByName(Const.ELEMENT_COL).fieldType = (elementType instanceof ParameterizedType ? String.class : (Class<?>) elementType);
							log.info("SRG: \t\t[ {} : {}.{} ({}) ]", CReflection.fieldDeclaration(complexField), entryTable.name, refIdColumn.name, elementType.getTypeName());
						}
						else {
							Type keyType = ((ParameterizedType) complexField.getGenericType()).getActualTypeArguments()[0];
							Type valueType = ((ParameterizedType) complexField.getGenericType()).getActualTypeArguments()[1];
							entryTable.findColumnByName(Const.KEY_COL).fieldType = (Class<?>) keyType;
							entryTable.findColumnByName(Const.VALUE_COL).fieldType = (valueType instanceof ParameterizedType ? String.class : (Class<?>) valueType);
							log.info("SRG: \t\t[ {} : {}.{} ({}/{}) ]", CReflection.fieldDeclaration(complexField), entryTable.name, refIdColumn.name, keyType.getTypeName(), valueType.getTypeName());
						}
					}
				}
				catch (SqlDbException sqldbex) {

					// If entry table does not exist...
					if (complexField.isAnnotationPresent(Deprecated.class)) {
						log.warn(
								"SRG: Table '{}' associated to deprecated element collection or key/value map field '{}' probably does not exist! Unregister and further ignore this field in context of persistance.",
								tableName, CReflection.qualifiedName(complexField));
						deprecatedFields.add(complexField); // Mark to unregister
					}
					else {
						// Stop registration on serious inconsistency
						javaSqlInconsistence = true;
						log.error("SRG: ", sqldbex);
					}
				}
			}
			if (javaSqlInconsistence) {
				throw new SqlDbException("Detected Java : SQL inconsistency!");
			}

			// Unregister deprecated fields where no column or entry table is associated to
			deprecatedFields.forEach(f -> unregisterField(f));

			// Check if field for data and reference column exists
			for (SqlDbColumn column : registeredTable.columns) {
				if (!objectsEqual(column.name, Const.ID_COL) && !objectsEqual(column.name, Const.LAST_MODIFIED_COL) && !objectsEqual(column.name, Const.DOMAIN_CLASS_COL)
						&& getDataAndReferenceFields(domainClass).stream().map(f -> getColumnFor(f).name).noneMatch(n -> objectsEqual(column.name, n))) {
					log.warn("SRG: Table '{}' associated to domain class '{}' has column '{}' where no field is associated with!", registeredTable.name, domainClass.getSimpleName(), column.name);
				}
			}
		}
	}
}
