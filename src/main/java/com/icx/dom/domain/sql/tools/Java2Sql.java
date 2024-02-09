package com.icx.dom.domain.sql.tools;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.CFile;
import com.icx.common.base.Common;
import com.icx.dom.domain.DomainAnnotations;
import com.icx.dom.domain.DomainAnnotations.Changed;
import com.icx.dom.domain.DomainAnnotations.Created;
import com.icx.dom.domain.DomainAnnotations.Crypt;
import com.icx.dom.domain.DomainAnnotations.Removed;
import com.icx.dom.domain.DomainAnnotations.Secret;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.SqlTable;
import com.icx.dom.domain.Registry;
import com.icx.dom.domain.sql.Const;
import com.icx.dom.domain.sql.SqlDomainObject;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlDb.DbType;

/**
 * Java program to generate SQL scripts for generation of persistence database using defined domain classes - for Domain persistence mechanism.
 * <p>
 * Copy {@code Java2Sql.java} into domain package of application (where all domain classes must reside) and start it there without parameters (before starting you may have to change the package name
 * in the first line of code).
 * <p>
 * {@code Java2Sql} generates three SQL table generation scripts: {@code create_ms_sql.sql}, {@code create_mysql.sql} and {@code create_oracle.sql} in directory {@code sql} parallel to {@code src}.
 * <p>
 * {@code Java2Sql} supports version control: Using annotations {@link Created}, {@link Changed}, {@link Removed} the <b>product version</b>s where domain classes and/or single fields were created,
 * changed or removed can be defined. {@code Java2Sql} then produces additional database update scripts for any product version defined in one of these annotations.
 * 
 * @author baumgrai
 */
public abstract class Java2Sql extends JdbcHelpers {

	static final Logger log = LoggerFactory.getLogger(Java2Sql.class);

	// ----------------------------------------------------------------------
	// Finals & statics
	// ----------------------------------------------------------------------

	public static final int DEFAULT_CHARSIZE = 1024;
	public static final int MAX_CHARSIZE = 2000;
	public static final int MAX_ENUM_VALUE_LENGTH = 64;
	public static final int MAX_CLASSNAME_LENGTH = 64;

	private static Registry<SqlDomainObject> registry = new Registry<>();

	// ----------------------------------------------------------------------
	// Script helpers
	// ----------------------------------------------------------------------

	// Add all foreign key constraints for given tables to script
	static void addForeignKeyConstraints(StringBuilder script, List<com.icx.dom.domain.sql.tools.Table> tables) {

		for (com.icx.dom.domain.sql.tools.Table table : tables) {
			for (com.icx.dom.domain.sql.tools.FkConstraint fkConstraint : table.fkConstraints) {
				script.append(fkConstraint.alterTableAddForeignKeyStatement());
			}
		}
	}

	// Drop all foreign key constraints for given tables at begin of script
	static void dropForeignKeyConstraints(StringBuilder script, List<com.icx.dom.domain.sql.tools.Table> tables) {

		script.insert(0, "\n");
		for (com.icx.dom.domain.sql.tools.Table table : tables) {
			for (com.icx.dom.domain.sql.tools.FkConstraint fkConstraint : table.fkConstraints) {
				script.insert(0, fkConstraint.alterTableDropForeignKeyStatement());
			}
		}
	}

	// Build SQL statements to drop table for given domain class
	static void dropTables(StringBuilder script, List<com.icx.dom.domain.sql.tools.Table> tables) {

		script.insert(0, "\n");
		for (com.icx.dom.domain.sql.tools.Table table : tables) {
			script.insert(0, table.dropScript());
		}
	}

	// ----------------------------------------------------------------------
	// Create tables
	// ----------------------------------------------------------------------

	// Build SQL statements to create main table and potentially existing entry tables (for collection and map fields) for given domain class
	static List<com.icx.dom.domain.sql.tools.Table> createTablesForDomainClass(StringBuilder script, String version, Class<? extends SqlDomainObject> domainClass, DbType dbType) {

		// Initialize table object for domain class
		com.icx.dom.domain.sql.tools.Table mainTable = new com.icx.dom.domain.sql.tools.Table(domainClass, dbType);

		log.info("J2S: \t\tCreate table {} for '{}'", mainTable.name, domainClass.getSimpleName());

		// New tables to generate build scripts for - containing this table and potentially containing entry tables for collection or map fields
		List<com.icx.dom.domain.sql.tools.Table> newTables = new ArrayList<>();
		newTables.add(mainTable);

		// Generate column definition for object class name (to know exact object type even on access to inherited record)
		com.icx.dom.domain.sql.tools.Column domainClassColumn = mainTable.addStandardColumn(Const.DOMAIN_CLASS_COL, String.class);
		domainClassColumn.charsize = MAX_CLASSNAME_LENGTH;

		// Generate ID column definition
		com.icx.dom.domain.sql.tools.Column idColumn = mainTable.addStandardColumn(Const.ID_COL, Long.class);
		idColumn.isPrimaryKey = true;

		// Inheritance
		if (registry.isBaseDomainClass(domainClass)) {

			// For base domain classes generate LAST_MODIFIED column definition and force building INDEX on this column for performance reasons
			com.icx.dom.domain.sql.tools.Column lastModifiedColumn = mainTable.addStandardColumn(Const.LAST_MODIFIED_COL, LocalDateTime.class);
			mainTable.addIndexFor(lastModifiedColumn);
		}
		else {
			// For extended domain classes add foreign key constraint to reference ID of superclass record to reflect inheritance relation
			idColumn.addFkConstraint(com.icx.dom.domain.sql.tools.FkConstraint.ConstraintType.INHERITANCE, registry.getCastedSuperclass(domainClass));
		}

		// Generate column definitions and foreign key constraints for registered fields of domain class - include dropped fields here because 'registerAlsoDroppedFields' is true for Java2Sql
		for (Field field : registry.getRegisteredFields(domainClass)) {

			if (version != null && com.icx.dom.domain.sql.tools.Java2SqlHelpers.getCreatedVersion(field).compareTo(version) > 0) {
				// Table creation in incremental update script: ignore fields created in a newer version
				log.info("J2S: \t\t\tField '{}' was created in version {} but incremental update script is for version {}", field.getName(),
						com.icx.dom.domain.sql.tools.Java2SqlHelpers.getCreatedVersion(field), version);
				continue; // Incremental scripts: Do not create column for field which is created in a newer version on incremental table creation
			}

			// Add tables or columns related to fields
			if (registry.isComplexField(field)) {

				// Warn on useless @Secret annotation of complex field
				if (field.isAnnotationPresent(Secret.class)) {
					log.warn(
							"J2S: @Secret annotation is useless for {} field '{}'! Suppressing logging of values for all log levels is not supported for List, Set and Map fields, but values generally will not be logged using INFO log level.",
							field.getType().getSimpleName(), field.getName());
				}

				// Table related field: create entry table - use field type
				newTables.add(com.icx.dom.domain.sql.tools.Java2SqlHelpers.buildEntryTable(field, null, dbType));
			}
			else {
				// Define column from field
				com.icx.dom.domain.sql.tools.Column column = mainTable.addColumnForRegisteredField(field);

				if (registry.isReferenceField(field)) { // Reference field

					// Check if reference is part of circular reference cycle
					String className = domainClass.getSimpleName();
					String refClassName = field.getType().getSimpleName();
					boolean isPartOfCircularReference = false;

					for (List<String> circle : circularReferences) {

						if (!objectsEqual(className, refClassName) && circle.contains(className) && circle.contains(refClassName)
								|| objectsEqual(className, refClassName) && circle.contains(className) && circle.size() == 1) {

							log.info("J2S:\t\t\tReference '{}' -> '{}' is part of reference circle {}", className, refClassName, circle);
							isPartOfCircularReference = true;
						}
					}

					// Add FOREIGN KEY constraint for reference field if field type is a domain class
					com.icx.dom.domain.sql.tools.FkConstraint fkConstraint = column.addFkConstraint(com.icx.dom.domain.sql.tools.FkConstraint.ConstraintType.REFERENCE,
							registry.getCastedReferencedDomainClass(field));

					// Determine if ON DELETE CASCADE shall be set on foreign key constraint - do this if it is explicitly specified in column annotation or if class uses 'data horizon' mechanism
					// to allow deleting parent objects even if children were not loaded
					SqlColumn ca = field.getAnnotation(SqlColumn.class);
					if (registry.isDataHorizonControlled(domainClass) || ca != null && ca.onDeleteCascade()) {

						if (isPartOfCircularReference && dbType == DbType.MS_SQL) {
							log.warn("J2S:\t\t\tDid not set ON DELETE CASCADE for FOREIGN KEY constraint '{}' on SQL Server because this is not supported for circular references!",
									fkConstraint.name());
							fkConstraint.onDeleteCascade = false;
						}
						else {
							fkConstraint.onDeleteCascade = true;
						}
					}
				}
				else {
					if (field.getType() != String.class && field.isAnnotationPresent(Crypt.class)) {
						log.warn("J2S: @Crypt annotation is useless for {} field '{}'! @Crypt is only supported for string fields.", field.getType().getSimpleName(), field.getName());
					}
				}
			}
		}

		// Add UNIQUE constraints and INDEXes specified in table annotation if exists
		SqlTable tableAnnotation = domainClass.getAnnotation(SqlTable.class);
		if (tableAnnotation != null) {

			for (String fieldNamesString : tableAnnotation.uniqueConstraints()) {
				mainTable.addUniqueConstraintFor(Common.stringToList(fieldNamesString, StringSep.COMMA));
			}

			for (String fieldNamesString : tableAnnotation.indexes()) {
				mainTable.addIndexFor(Common.stringToList(fieldNamesString, StringSep.COMMA));
			}
		}

		// Generate SQL statement
		for (com.icx.dom.domain.sql.tools.Table newTable : newTables) {
			script.append(newTable.createScript());
		}

		return newTables;
	}

	// ----------------------------------------------------------------------
	// Alter tables (for incremental version update scripts only)
	// ----------------------------------------------------------------------

	// Build SQL statements to alter main table and create or drop potentially existing entry tables (for collection and map fields) for given domain class for given version
	static List<com.icx.dom.domain.sql.tools.Table> alterTablesForDomainClass(StringBuilder script, String version, Class<? extends SqlDomainObject> domainClass, DbType dbType) {

		log.info("J2S:\t\tAlter table for: '{}' for version '{}'", domainClass.getSimpleName(), version);

		// Initialize table object for domain class as anchor for ALTER TABLE statements
		com.icx.dom.domain.sql.tools.Table table = new com.icx.dom.domain.sql.tools.Table(domainClass, dbType);

		// Append ALTER TABLE statements for adding, modifying and dropping columns to script - check all relevant fields, not only registered ones
		List<com.icx.dom.domain.sql.tools.Table> createdEntryTables = new ArrayList<>();
		for (Field field : registry.getRelevantFields(domainClass)) {

			if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasCreatedInVersion(field, version)) {

				// New fields

				// Get attributes to override by information from @Created annotation if specified
				Map<String, String> createInfoMap = com.icx.dom.domain.sql.tools.Java2SqlHelpers.getCreateInfo(field);

				if (registry.isComplexField(field)) {

					// Build entry table associated to collection/map field - use collection type if specified in create info or field type
					com.icx.dom.domain.sql.tools.Table entryTable = com.icx.dom.domain.sql.tools.Java2SqlHelpers.buildEntryTable(field, createInfoMap.get(DomainAnnotations.COLLECTION_TYPE), dbType);
					createdEntryTables.add(entryTable);

					// Add create script for entry table
					script.append(entryTable.createScript());
				}
				else {
					// Column associated to data or reference field
					com.icx.dom.domain.sql.tools.Column column = table.addColumnForRegisteredField(field);

					// Override column attributes according to values specified in create info
					com.icx.dom.domain.sql.tools.Java2SqlHelpers.updateColumnAttributes(column, createInfoMap);

					// Add ALTER TABLE script to create column
					script.append(column.alterTableAddColumnStatement());

					if (registry.isReferenceField(field)) { // Reference field

						// Add FOREIGN KEY constraint for reference field if field type is a domain class
						com.icx.dom.domain.sql.tools.FkConstraint fkConstraint = column.addFkConstraint(com.icx.dom.domain.sql.tools.FkConstraint.ConstraintType.REFERENCE,
								registry.getCastedReferencedDomainClass(field));

						// Determine if ON DELETE CASCADE shall be set on foreign key constraint
						SqlColumn ca = field.getAnnotation(SqlColumn.class);
						fkConstraint.onDeleteCascade = (registry.isDataHorizonControlled(domainClass) || ca != null && ca.onDeleteCascade());

						script.append(fkConstraint.alterTableAddForeignKeyStatement());
					}

					// Add UNIQUE constraint if column is specified as unique either in @SqlColumn or in @Created annotation
					if (column.isUnique) {
						script.append(new com.icx.dom.domain.sql.tools.UniqueConstraint(table, column).alterTableAddConstraintStatement());
					}
				}
			}
			else if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasChangedInVersion(field, version)) {

				// Changed fields

				// Get changes from version annotation
				Map<String, String> changeInfoMap = com.icx.dom.domain.sql.tools.Java2SqlHelpers.getChangeInfo(field, version);

				if (registry.isComplexField(field)) {

					// Table related field: only change of collection type for Set or List fields are allowed

					if (changeInfoMap.containsKey(DomainAnnotations.COLLECTION_TYPE)) {

						// Collection type changed: use additional 'order' column for Lists and UNIQUE constraint for elements for Sets

						// Create entry table associated to collection/map field as anchor for subsequent ADD/DROP statements - add both UNIQUE constraint for Set type and 'order' column for List type
						com.icx.dom.domain.sql.tools.Table entryTable = com.icx.dom.domain.sql.tools.Java2SqlHelpers.buildEntryTable(field, "both", dbType);
						com.icx.dom.domain.sql.tools.Column orderColumnForList = com.icx.dom.domain.sql.tools.Java2SqlHelpers.getElementOrderColumn(entryTable);
						com.icx.dom.domain.sql.tools.UniqueConstraint uniqueConstraintForSet = com.icx.dom.domain.sql.tools.Java2SqlHelpers.getUniqueConstraintForElements(entryTable);

						String collectionType = changeInfoMap.get(DomainAnnotations.COLLECTION_TYPE);
						if (collectionType.equalsIgnoreCase("list")) {

							// Add 'order' column and drop unique constraint if collection type changed to List
							script.append(orderColumnForList.alterTableAddColumnStatement());
							script.append(uniqueConstraintForSet.alterTableDropConstraintStatement());
						}
						else { // "set"
								// Drop 'order' column and add unique constraint if collection type changed to Set
							script.append(orderColumnForList.alterTableDropColumnStatement());
							script.append(uniqueConstraintForSet.alterTableAddConstraintStatement());
						}
					}
				}
				else {
					// Column related field: MODIFY column according to changes noted in change info map

					// Add modify column statement and retrieve information from @SqlColumn annotation
					com.icx.dom.domain.sql.tools.Column column = table.addColumnForRegisteredField(field);

					// Retrieve information from @Changed annotation and override information from @SqlColumn annotation if both are given
					com.icx.dom.domain.sql.tools.Java2SqlHelpers.updateColumnAttributes(column, changeInfoMap);

					// Modify column
					script.append(column.alterTableModifyColumnStatement(dbType));

					if (changeInfoMap.containsKey(DomainAnnotations.UNIQUE)) {

						// Add or drop UNIQUE constraint if unique value specified in changed info differs from value specified in @SqlColumn annotation
						boolean isUnique = Boolean.parseBoolean(changeInfoMap.get(DomainAnnotations.UNIQUE));
						if (isUnique && !column.isUnique) {
							script.append(new com.icx.dom.domain.sql.tools.UniqueConstraint(table, column).alterTableAddConstraintStatement());
						}
						else if (!isUnique && column.isUnique) {
							script.append(new com.icx.dom.domain.sql.tools.UniqueConstraint(table, column).alterTableDropConstraintStatement());
						}
					}
				}
			}
			else if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasRemovedInVersion(field, version)) {

				// Removed fields

				if (registry.isComplexField(field)) {

					// Drop entry table for removed field
					script.append(com.icx.dom.domain.sql.tools.Java2SqlHelpers.buildEntryTable(field, null, dbType).dropScript());
				}
				else {
					// Drop column (and foreign key constraint if exists) for removed field
					com.icx.dom.domain.sql.tools.Column column = table.addColumnForRegisteredField(field);
					if (registry.isReferenceField(field)) {
						script.append(column.addFkConstraint(com.icx.dom.domain.sql.tools.FkConstraint.ConstraintType.REFERENCE, registry.getCastedReferencedDomainClass(field))
								.alterTableDropForeignKeyStatement());
					}

					script.append(column.alterTableDropColumnStatement());
				}
			}
		}

		return createdEntryTables;
	}

	// "a?b,c&d&e" -> [[a,b],[c,d,e]]
	private static List<List<String>> getNameLists(String versionInfoString) {

		List<List<String>> nameLists = new ArrayList<>();

		String[] listStrings = versionInfoString.split("\\,");
		for (String listString : listStrings) {
			nameLists.add(Common.stringToList(listString, StringSep.AND));
		}

		return nameLists;
	}

	// Change multi column unique constraints and indexes according to version infos for specific version
	static void changeUniqueConstraintsAndIndexesForDomainClass(StringBuilder script, String version, Class<? extends SqlDomainObject> domainClass, DbType dbType) {

		log.info("J2S:\t\tChange multi column unique constraints and indexes of table '{}' for version '{}'", domainClass.getSimpleName(), version);

		// Initialize table object for domain class as anchor for ALTER TABLE statements
		com.icx.dom.domain.sql.tools.Table table = new com.icx.dom.domain.sql.tools.Table(domainClass, dbType);

		// Table changes (UNIQUE constraints and INDEXes)
		if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasChangedInVersion(domainClass, version)) {
			script.append("\n");

			SqlTable tableAnnotation = domainClass.getAnnotation(SqlTable.class);
			Map<String, String> tableChangeInfo = com.icx.dom.domain.sql.tools.Java2SqlHelpers.getChangeInfo(domainClass, version);

			if (tableChangeInfo.containsKey(DomainAnnotations.UNIQUE_CONSTRAINTS_TO_DROP)) {

				// Drop UNIQUE constraints defined in table change info
				for (List<String> fieldNames : getNameLists(tableChangeInfo.get(DomainAnnotations.UNIQUE_CONSTRAINTS_TO_DROP))) {
					script.append(new com.icx.dom.domain.sql.tools.UniqueConstraint(table, fieldNames).alterTableDropConstraintStatement());
				}
			}

			if (tableChangeInfo.containsKey(DomainAnnotations.UNIQUE_CONSTRAINTS)) {

				// Create UNIQUE constraints defined in table change info
				for (List<String> fieldNames : getNameLists(tableChangeInfo.get(DomainAnnotations.UNIQUE_CONSTRAINTS))) {
					script.append(new com.icx.dom.domain.sql.tools.UniqueConstraint(table, fieldNames).alterTableAddConstraintStatement());
				}
			}
			else if (tableAnnotation != null) {

				// Assume UNIQUE constraints defined in @SqlTable annotation are created in this version and create these UNIQUE constraints
				for (String fieldNamesString : tableAnnotation.uniqueConstraints()) {
					script.append(new com.icx.dom.domain.sql.tools.UniqueConstraint(table, Common.stringToList(fieldNamesString)).alterTableAddConstraintStatement());
				}
			}

			if (tableChangeInfo.containsKey(DomainAnnotations.INDEXES_TO_DROP)) {

				// Create INDEXes defined in table change info
				for (List<String> fieldNames : getNameLists(tableChangeInfo.get(DomainAnnotations.INDEXES_TO_DROP))) {
					script.append(new com.icx.dom.domain.sql.tools.Index(table, fieldNames).dropStatement());
				}
			}

			if (tableChangeInfo.containsKey(DomainAnnotations.INDEXES)) {

				// Drop INDEXes defined in table change info
				for (List<String> fieldNames : getNameLists(tableChangeInfo.get(DomainAnnotations.INDEXES))) {
					script.append(new com.icx.dom.domain.sql.tools.Index(table, fieldNames).createStatement());
				}
			}
			else if (tableAnnotation != null) {

				// Assume INDEXes defined in @SqlTable annotation are created in this version and create these INDEXes
				for (String fieldNamesString : tableAnnotation.indexes()) {
					script.append(new com.icx.dom.domain.sql.tools.Index(table, Common.stringToList(fieldNamesString)).createStatement());
				}
			}
		}
	}

	// ----------------------------------------------------------------------
	// Generate scripts for DB type
	// ----------------------------------------------------------------------

	// Generate SQL scripts for DB of given type
	public static void generateSqlScripts(String name, DbType dbType) throws IOException {

		log.info("J2S: Generate SQL scripts for {}...", dbType);

		// Get all domain classes where tables are to create for
		List<Class<? extends SqlDomainObject>> domainClasses = registry.getRegisteredDomainClasses();

		// DB create script

		StringBuilder createScript = new StringBuilder();

		// Create tables (do before dropping)
		List<com.icx.dom.domain.sql.tools.Table> createdTables = new ArrayList<>();
		for (Class<? extends SqlDomainObject> domainClass : domainClasses) {
			createdTables.addAll(createTablesForDomainClass(createScript, null, domainClass, dbType));
		}
		createScript.append("\n");

		// Drop tables (insert DROP TABLE statements at start of script)
		dropTables(createScript, createdTables);

		// Drop and add foreign key constraints (insert DROP CONSTRAINT statements at start of script)
		dropForeignKeyConstraints(createScript, createdTables);
		addForeignKeyConstraints(createScript, createdTables);

		// Write SQL script files for different DB types
		File createScriptFile = CFile.checkOrCreateFile(new File("sql/create_" + name + "_" + dbType.toString().toLowerCase() + ".sql"));
		log.info("J2S: \t\tWrite complete SQL script to: {}", createScriptFile.getAbsolutePath());
		CFile.writeText(createScriptFile, createScript.toString(), false, StandardCharsets.UTF_8.name());

		// Version specific DB increment scripts

		// For all versions defined within table or column annotations of all (also deprecated) classes and fields
		SortedSet<String> versions = com.icx.dom.domain.sql.tools.Java2SqlHelpers.findAllVersions(domainClasses);
		for (String version : versions) {

			log.info("J2S: Generate incremental SQL scripts for {} and version {}...", dbType, version);

			StringBuilder alterScriptForVersion = new StringBuilder();
			List<com.icx.dom.domain.sql.tools.Table> createdTablesForVersion = new ArrayList<>();

			// CREATE, DROP or ALTER tables for version... - consider also removed domain classes
			for (Class<? extends SqlDomainObject> domainClass : registry.getRelevantDomainClasses()) {

				if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasCreatedInVersion(domainClass, version)) {
					createdTablesForVersion.addAll(createTablesForDomainClass(alterScriptForVersion, version, domainClass, dbType));
					alterScriptForVersion.append("\n");
				}
				else if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasRemovedInVersion(domainClass, version)) {
					alterScriptForVersion.append(new com.icx.dom.domain.sql.tools.Table(domainClass, dbType).dropScript());
					alterScriptForVersion.append("\n");
				}
				else {
					// Check version related annotations for all relevant (also removed) fields
					if (registry.getRelevantFields(domainClass).stream().anyMatch(f -> com.icx.dom.domain.sql.tools.Java2SqlHelpers.isFieldAffected(f, version))) {
						createdTablesForVersion.addAll(alterTablesForDomainClass(alterScriptForVersion, version, domainClass, dbType));
					}

					// Check if changes on multi column UNIQUE constrains or indexes were made
					if (com.icx.dom.domain.sql.tools.Java2SqlHelpers.wasChangedInVersion(domainClass, version)) {
						changeUniqueConstraintsAndIndexesForDomainClass(alterScriptForVersion, version, domainClass, dbType);
						alterScriptForVersion.append("\n");
					}
				}
			}

			// Add foreign key constraints for new tables (on altering table constraints will be added or dropped within alterTableForDomainClass(), on dropping table constraints belonging to this
			// table will be automatically dropped)
			addForeignKeyConstraints(alterScriptForVersion, createdTablesForVersion);

			// Write incremental SQL script files
			File alterScriptFileForVersion = CFile.checkOrCreateFile(new File("sql/alter_" + name + "_" + dbType.toString().toLowerCase() + "_for_v" + version + ".sql"));
			log.info("J2S: \t\tWrite incremental SQL script for version {} to: '{}'", version, alterScriptFileForVersion.getAbsolutePath());
			CFile.writeText(alterScriptFileForVersion, alterScriptForVersion.toString(), false, StandardCharsets.UTF_8.name());
		}
	}

	// ----------------------------------------------------------------------
	// Main
	// ----------------------------------------------------------------------

	static Set<List<String>> circularReferences = null;

	public static void main(String[] args) throws Exception {

		// Determine domain class package and supposed app name (second last part of package name)
		String packageName = (args.length > 0 ? Stream.of(Package.getPackages()).map(Package::getName).filter(n -> objectsEqual(n, args[0])).findAny().orElse(null)
				: Java2Sql.class.getPackage().getName());
		String supposedAppName = Common.behindLast(Common.untilLast(packageName, "."), ".");

		// Register domain classes
		// Registry.register(SqlDomainObject.class, Xxx.class, Y.class, Z.class); // Only for test!
		registry.registerDomainClasses(SqlDomainObject.class, packageName);

		// Determine circular references - to avoid setting ON DELETE CASCADE for foreign keys within circular references
		circularReferences = registry.determineCircularReferences();
		log.info("J2S: Circular references: {}", circularReferences);

		// Generate SQL scripts for database generation and version specific updates for all supported database types
		for (DbType dbType : DbType.values()) {
			generateSqlScripts(supposedAppName, dbType);
		}
	}
}
