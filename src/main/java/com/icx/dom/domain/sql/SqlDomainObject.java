package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CList;
import com.icx.dom.common.CLog;
import com.icx.dom.common.CMap;
import com.icx.dom.common.Reflection;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;

/**
 * Base class for objects managed by {@link SqlDomainController}. Includes methods for saving and deleting domain objects. Supports SQL error handling.
 * <p>
 * Any SQL persisted domain object must extend this class.
 * 
 * @author RainerBaumgärtel
 */
public abstract class SqlDomainObject extends DomainObject {

	static final Logger log = LoggerFactory.getLogger(SqlDomainObject.class);

	// -------------------------------------------------------------------------
	// Finals
	// -------------------------------------------------------------------------

	public static final String LAST_MODIFIED_IN_DB_FIELD = "lastModifiedInDb";

	public static final String ID_COL = "ID";
	public static final String DOMAIN_CLASS_COL = "DOMAIN_CLASS";
	public static final String LAST_MODIFIED_COL = "LAST_MODIFIED";
	public static final String ELEMENT_COL = "ELEMENT";
	public static final String ORDER_COL = "ELEMENT_ORDER";
	public static final String KEY_COL = "ENTRY_KEY";
	public static final String VALUE_COL = "ENTRY_VALUE";

	// -------------------------------------------------------------------------
	// Members
	// -------------------------------------------------------------------------

	// Date of last modification
	LocalDateTime lastModifiedInDb = null;

	// Is already saved to database?
	private transient boolean isStored = false;

	public boolean isStored() {
		return isStored;
	}

	public void setIsStored() {
		isStored = true;
	}

	// -------------------------------------------------------------------------
	// Field errors
	// -------------------------------------------------------------------------

	// SQL exception occurred during last try to save object
	public transient Exception currentException = null;

	// Field errors which prohibit object saving or warnings which force truncating field content on saving
	private transient Map<Field, FieldError> fieldErrorMap = new ConcurrentHashMap<>();

	void setFieldError(Field field, String message) {
		fieldErrorMap.put(field, new FieldError(true, this, field, message));
	}

	void setFieldWarning(Field field, String message) {
		fieldErrorMap.put(field, new FieldError(false, this, field, message));
	}

	void clearErrors() {
		currentException = null;
		fieldErrorMap.clear();
	}

	/**
	 * Check if domain object could be saved without critical errors (or is still not saved to database).
	 * 
	 * @return true if domain object was saved without critical errors or is still not saved to database, false otherwise
	 */
	public boolean isValid() {
		return (currentException == null && fieldErrorMap.values().stream().noneMatch(fe -> fe.isCritical));
	}

	/**
	 * Check if warnings or errors were generated during last saving of domain object.
	 * <p>
	 * A warning will be generated if size of string contained in a String field exceeds size of associated text column in database table and string therefore was truncated on saving.
	 * 
	 * @return true if any (String) field had to be truncated on last object saving, false otherwise
	 */
	public boolean hasErrorsOrWarnings() {
		return (!fieldErrorMap.isEmpty());
	}

	/**
	 * Get fields with errors or warnings.
	 * 
	 * @return field error map
	 */
	public Set<Field> getInvalidFields() {
		return fieldErrorMap.keySet();
	}

	/**
	 * Get field error (or warning) for given field.
	 * 
	 * @param field
	 *            field
	 * 
	 * @return field error object or null
	 */
	public FieldError getErrorOrWarning(Field field) {
		return fieldErrorMap.get(field);
	}

	/**
	 * Get field errors for this object.
	 * 
	 * @return field errors
	 */
	public List<FieldError> getErrorsAndWarnings() {
		return new ArrayList<>(fieldErrorMap.values());
	}

	// -------------------------------------------------------------------------
	// Registration
	// -------------------------------------------------------------------------

	// Unregister object
	@Override
	protected void unregister() {

		// Remove object record from record map
		if (SqlDomainController.recordMap.containsKey(getClass())) {
			SqlDomainController.recordMap.get(getClass()).remove(getId());
		}

		// Unregister object from object store
		super.unregister();
	}

	// Only for unit tests
	public void unregisterForTest() {
		unregister();
	}

	// Re-register object (on failed deletion)
	private void reregister() {

		// Re-register object in object store
		registerById(id);

		// Collect field changes: because object record was removed from object record map this call finds all field/value pairs for object
		Map<Class<? extends DomainObject>, Map<Field, Object>> fieldChangesMapByDomainClassMap = collectFieldChanges();

		// Re-generate object record from field/value pairs of all inherited domain classes
		SortedMap<String, Object> objectRecord = new TreeMap<>();
		SqlDomainController.recordMap.get(getClass()).put(id, objectRecord);

		for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(getClass())) {

			SortedMap<String, Object> columnValueMap = buildColumnValueMapForInsertOrUpdate(fieldChangesMapByDomainClassMap.get(domainClass));
			objectRecord.putAll(columnValueMap);
		}

		log.info("DC: Re-registered {} (by original id)", name());
	}

	// Only for unit tests
	public void reregisterForTest() {
		reregister();
	}

	// -------------------------------------------------------------------------
	// Delete object
	// -------------------------------------------------------------------------

	// Note: Deletion of an object means unregister object (remove it from object store) and DELETE associated records from database and do so for all direct and indirect children.

	// Indentation for logging
	private static String tabs(int count) {

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < count; i++) {
			sb.append("\t");
		}
		return sb.toString();
	}

	// Check if any of objects to check has a reference to this object - which indicates a circular reference in calling context
	private void checkForAndResetCircularReferences(Connection cn, List<SqlDomainObject> objectsToCheck, int stackSize) throws SQLException, SqlDbException {

		// Check for - circular - references to this object and reset these references to allow deletion
		for (DomainObject objectToCheck : objectsToCheck) {

			SortedMap<String, Object> columnValueMap = new TreeMap<>();
			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(objectToCheck.getClass())) {
				for (Field refField : Registry.getReferenceFields(domainClass)) {

					if (objectsEqual(objectToCheck.getFieldValue(refField), this)) {

						log.info("SDO: {}Circular reference detected: {}.{} references {}! Reset reference before deleting object.", tabs(stackSize), objectToCheck.name(), refField.getName(), name());

						objectToCheck.setFieldValue(refField, null);

						columnValueMap.put(SqlRegistry.getColumnFor(refField).name, null);
						SqlDbTable referencingTable = SqlRegistry.getTableFor(Registry.getDeclaringDomainClass(refField)); // Table where FOREIGN KEY column for reference is defined
						SqlDomainController.sqlDb.update(cn, referencingTable.name, columnValueMap, ID_COL + "=" + objectToCheck.getId());
					}
				}
			}
		}
	}

	// DELETE object records locally and from database
	private synchronized void deleteFromDatabase(Connection cn) throws SQLException, SqlDbException {

		// Delete records belonging to this object: object records for domain class(es) and potentially existing element or key/value records
		for (Class<? extends DomainObject> domainClass : CList.reverse(Registry.getInheritanceStack(getClass()))) {

			// Delete possibly existing element or key/value records from entry tables before deleting object record for domain class itself
			for (Field complexField : Registry.getComplexFields(domainClass)) {
				SqlDb.deleteFrom(cn, SqlRegistry.getEntryTableFor(complexField).name, SqlRegistry.getMainTableRefIdColumnFor(complexField).name + "=" + id);
			}

			// Delete object record for inherited domain class
			SqlDb.deleteFrom(cn, SqlRegistry.getTableFor(domainClass).name, ID_COL + "=" + id);
		}
	}

	// Delete object and all of its children from database
	private synchronized void deleteRecursiveFromDatabase(Connection cn, List<SqlDomainObject> objectsToCheck, int stackSize) throws SQLException, SqlDbException {

		log.info("SDO: {}Delete {}", tabs(stackSize), name());

		// Unregister this object (from object store and from object record map) before DELETing from database - to avoid that this object can be found while deletion process runs
		unregister();

		// Add object to objects which potentially have circular references
		objectsToCheck.add(this);

		// Delete children
		for (DomainObject child : getDirectChildren()) {
			((SqlDomainObject) child).deleteRecursiveFromDatabase(cn, objectsToCheck, stackSize + 1);
		}

		// Delete object itself from database if it was already saved...
		if (isStored) {

			// Check for circular references and reset them
			checkForAndResetCircularReferences(cn, objectsToCheck, stackSize);

			// DELETE object related records from database
			deleteFromDatabase(cn);

			// Avoid further checking for references of deleted object
			objectsToCheck.remove(this);

			if (log.isTraceEnabled()) {
				log.trace("SDO: {}{} was deleted", tabs(stackSize), name());
			}
		}
	}

	private void handleDeleteException(Connection cn, Exception sqlex, List<SqlDomainObject> objectsToCheck) throws SQLException {

		log.error("SDO: Delete: Object {} cannot be deleted", name());
		log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

		// Assign exception to object
		currentException = sqlex;

		// Rollback changes on deletion error
		cn.rollback();

		// Re-register already unregistered objects and re-generate object record
		for (SqlDomainObject obj : objectsToCheck) {
			obj.reregister();
		}

		log.warn("SDO: Whole delete transaction rolled back!");
	}

	/**
	 * Delete object and child objects using existing database connection without can-be-deleted check.
	 * 
	 * @param cn
	 *            database connection
	 * 
	 * @throws SQLException
	 *             exceptions thrown on executing SQL DELETE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public synchronized void delete(Connection cn) throws SQLException, SqlDbException {

		// Delete object and children
		List<SqlDomainObject> objectsToCheck = new ArrayList<>();
		try {
			deleteRecursiveFromDatabase(cn, objectsToCheck, 0);
		}
		catch (SQLException | SqlDbException sqlex) {
			handleDeleteException(cn, sqlex, objectsToCheck);
			throw sqlex;
		}
	}

	/**
	 * Check if object can be deleted and if so unregisters object and all direct and indirect child objects, delete associated records from database and removes object from existing accumulations of
	 * parent objects.
	 * <p>
	 * No object will be unregistered, no database record will be deleted and no accumulation will be changed if deletion of any child object is not possible (complete SQL transaction will be ROLLed
	 * BACK and already unregistered objects will be re-registered in this case).
	 * <p>
	 * Note: Database records of old 'data horizon' controlled child objects, which were not loaded, will be deleted by ON DELETE CASCADE (which is automatically set for FOREIGN KEYs in tables of
	 * 'data horizon' controlled domain classes in SQL scripts for database generation).
	 * 
	 * @return true if deletion was successful, false if object or at least one of its direct or indirect children cannot be deleted by can-be-deleted check.
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL DELETE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public synchronized boolean delete() throws SQLException, SqlDbException {

		LocalDateTime now = LocalDateTime.now();

		// Recursively check if this object and all direct and indirect children can be deleted
		if (!canBeDeletedRecursive(new ArrayList<>())) {
			return false;
		}

		try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) { // Use one transaction for all DELETE operations to allow ROLLBACK of whole transaction on error

			// Recursively delete all object and child object records from database - transaction will automatically be committed later on closing connection
			delete(sqlcn.cn);

			if (log.isDebugEnabled()) {
				log.debug("SDO: Deletion of {} and all children took: {}", name(), ChronoUnit.MILLIS.between(now, LocalDateTime.now()) + "ms");
			}

			return true;
		}
		catch (SQLException sqlex) {
			log.error("SDO: Delete: Object {} cannot be deleted", name());
			log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

			currentException = sqlex;

			throw sqlex;
		}
	}

	// -------------------------------------------------------------------------
	// Load object from database
	// -------------------------------------------------------------------------

	// SELECT records and build domain object record for this object - returns empty map if object could not be loaded
	@SuppressWarnings("unchecked")
	private <S extends SqlDomainObject> Map<Class<S>, Map<Long, SortedMap<String, Object>>> selectObjectRecord(Connection cn) {

		SqlDbTable table = SqlRegistry.getTableFor(getClass());

		Map<Long, SortedMap<String, Object>> loadedRecordsMap = LoadHelpers.retrieveRecordsForObjectDomainClass(cn, 0, getClass(), table.name + "." + ID_COL + "=" + id);

		Map<Class<S>, Map<Long, SortedMap<String, Object>>> loadedRecordsMapByDomainClassMap = new HashMap<>();
		if (!loadedRecordsMap.isEmpty()) {
			loadedRecordsMapByDomainClassMap.put((Class<S>) getClass(), loadedRecordsMap);
		}

		return loadedRecordsMapByDomainClassMap;
	}

	/**
	 * (Re)load object from database.
	 * <p>
	 * If object is not initially saved or is not registered this method does nothing.
	 * <p>
	 * If direct or indirect parent objects exist which are not yet loaded due to data horizon control these object will be loaded (and instantiated) too.
	 * <p>
	 * Attention: Overrides unsaved changes of this object.
	 * 
	 * @throws SQLException
	 *             on error establishing database connection
	 */
	public boolean reload() throws SQLException {

		if (!isStored) {
			log.warn("SDO: {} is not yet stored in database and therefore cannot be loaded!", name());
			return false;
		}
		else if (!isRegistered()) {
			log.warn("SDO: {} is not registered in object store and cannot be loaded from database!", name());
			return false;
		}

		if (log.isDebugEnabled()) {
			log.debug("SDO: Load {} from database", name());
		}

		return SqlDomainController.loadAssuringReferentialIntegrity(this::selectObjectRecord, new HashSet<>());
	}

	/**
	 * Perform update function on exclusively on this object and save changed object immediately.
	 * 
	 * @param inProgressClass
	 *            class for shadow records to ensure exclusivity of this operation
	 * @param update
	 *            update function
	 * 
	 * @throws SQLException
	 *             exceptions thrown establishing connection or on executing SQL UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public void updateExclusively(Class<? extends SqlDomainObject> inProgressClass, Consumer<? super SqlDomainObject> update) throws SQLException, SqlDbException {
		SqlDomainController.updateExclusively((Class<? extends SqlDomainObject>) getClass(), inProgressClass, SqlRegistry.getTableFor(getClass()).name + ".ID=" + id, update);
	}

	public void release(Class<? extends SqlDomainObject> inProgressClass, Consumer<? super SqlDomainObject> update) throws SQLException, SqlDbException {
		SqlDomainController.release(this, inProgressClass, update);
	}

	// -------------------------------------------------------------------------
	// Save object to database
	// -------------------------------------------------------------------------

	// Save unsaved referenced objects before saving this object or collect them here to save them after saving this object.
	// Note: If reference is nullable, referenced object may also reference this object directly or indirectly. In this case an infinite loop would occur on saving referenced object before
	// saving object. Therefore nullable references will be temporarily reset to allow saving object and referenced objects will be saved after saving object and references will be restored.
	private void saveOrStoreUnsavedReferecedObjectsBeforeSavingObject(Connection cn, Class<? extends DomainObject> domainClass, Map<Field, DomainObject> unsavedReferencedObjectMap,
			List<DomainObject> objectsToSaveBefore, List<DomainObject> savedObjects) throws SQLException, SqlDbException {

		// Avoid infinite recursion on - actually impossible - circular object references using only non nullable foreign keys (these references could not be saved in case of circularity)
		objectsToSaveBefore.add(this);
		int stackSize = objectsToSaveBefore.size();

		// For all reference fields of (derived) domain class...
		for (Field refField : Registry.getReferenceFields(domainClass)) {

			// Get referenced object, ignore null reference
			SqlDomainObject refObj = (SqlDomainObject) getFieldValue(refField);
			if (refObj == null) {
				continue;
			}

			// Re-register and reload meanwhile unregistered referenced object
			if (!refObj.isRegistered()) { // Referenced object was unregistered before saving this object (during synchronization due to data horizon condition)

				log.warn("SDO: {}Referenced {} is not registered anymore (probably out of data horizon) - reregister object", tabs(stackSize), DomainObject.name(refObj));
				refObj.reregister();
				continue;
			}

			// Ignore referenced object if it was unregistered and reloaded or meanwhile saved
			if (refObj.isStored) {
				continue;
			}

			// Decide if referenced object is to save after this object (nullable reference) or if it can be saved now (not nullable reference)
			if (SqlRegistry.getColumnFor(refField).isNullable) {

				// Store referenced object for latter saving and reset reference to allow saving this object
				unsavedReferencedObjectMap.put(refField, refObj);
				setFieldValue(refField, null);

				log.info("SDO: {}Collected unsaved referenced object {} for subsequent saving", tabs(stackSize), DomainObject.name(refObj));
			}
			else {
				if (objectsToSaveBefore.contains(refObj)) {
					log.error("SDO: {}Detected impossible circular reference using non nullable foreign keys between referenced object {} and this object {}.{}", tabs(stackSize),
							DomainObject.name(refObj), name(), refField.getName());
				}
				else {
					// Save referenced object before saving object
					log.info("SDO: {}Save unsaved referenced object {} before saving this object {}", tabs(stackSize), DomainObject.name(refObj), name());
					try {
						// Save unsaved referenced object before saving object (circular references are not possible on non-nullable foreign keys)
						refObj.save(cn, refObj.collectFieldChanges(), objectsToSaveBefore, savedObjects);
					}
					catch (SQLException sqlex) {
						log.error("SDO: {}INSERT failed! Object {} cannot be saved because unsaved parent object {} could not be saved!", tabs(stackSize), name(), DomainObject.name(refObj));
						log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

						// Set field error on reference field
						setFieldError(refField, "REFERENCED_OBJECT_COULD_NOT_BE_SAVED");

						// Restore references which were reset before save error occurred
						for (Field rf : unsavedReferencedObjectMap.keySet()) {
							setFieldValue(rf, unsavedReferencedObjectMap.get(rf)); // Use setter without throw clause because we know the values here
						}

						// Throw exception to force ROLL BACK of complete transaction
						throw sqlex;
					}
				}
			}
		}

		objectsToSaveBefore.remove(this);
	}

	// Save collected referenced objects (on nullable references) after saving this object and restore (UPDATE) references in database
	private SortedMap<String, Object> saveUnsavedReferecedObjectsAfterSavingObject(Connection cn, Map<Field, DomainObject> unsavedReferencedObjectMap, List<DomainObject> objectsToSaveBefore,
			List<DomainObject> savedObjects) throws SqlDbException {

		int stackSize = objectsToSaveBefore.size();
		SortedMap<String, Object> columnValueMap = new TreeMap<>();

		// For all referenced objects which could not be saved before saving this object...
		for (Entry<Field, DomainObject> unsavedReferenceEntry : unsavedReferencedObjectMap.entrySet()) {

			// Get referencing field and referenced object
			Field refField = unsavedReferenceEntry.getKey();
			SqlDomainObject refObj = (SqlDomainObject) unsavedReferenceEntry.getValue();

			// Ignore if meanwhile unregistered
			if (!refObj.isRegistered()) {// Referenced object was deleted during saving this object
				log.warn("SDO: {}Referenced {} is not registered anymore!", tabs(stackSize), DomainObject.name(refObj));
				continue;
			}

			// Save referenced object if not done meanwhile
			if (!refObj.isStored) {

				log.info("SDO: {}Save unsaved referenced object {} after saving this object {}", tabs(stackSize), DomainObject.name(refObj), name());

				try {
					refObj.save(cn, refObj.collectFieldChanges(), objectsToSaveBefore, savedObjects);
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}INSERT failed! Unsaved parent object {} of object to save {} cannot not be saved!", tabs(stackSize), DomainObject.name(refObj), name());
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

					// Set field error on reference field
					setFieldError(refField, "REFERENCED_OBJECT_COULD_NOT_BE_SAVED");
				}
			}

			if (refObj.isStored) {

				// Re-establish reference to parent object (was reset to null before to allow saving object)
				setFieldValue(refField, refObj); // Use setter without throw clause because we know the values here

				// UPDATE reference in database
				SortedMap<String, Object> oneCVMap = new TreeMap<>();
				oneCVMap.put(SqlRegistry.getColumnFor(refField).name, refObj.id);

				try {
					SqlDbTable referencingTable = SqlRegistry.getTableFor(Registry.getDeclaringDomainClass(refField)); // Table of saved object where FOREIGN KEY column for this reference is defined
					SqlDomainController.sqlDb.update(cn, referencingTable.name, oneCVMap, ID_COL + "=" + id);
					columnValueMap.putAll(oneCVMap); // Store change to subsequently update object record

					log.info("SDO: {}Restored reference '{}' to {} for {}", tabs(stackSize), refField.getName(), DomainObject.name(refObj), name());
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}UPDATE failed! Exception on restoring reference field '{}' for object {}", tabs(stackSize), Reflection.qualifiedName(refField), name());
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

					this.currentException = sqlex;
					setFieldError(refField, sqlex.getMessage());
				}
			}
		}

		return columnValueMap;
	}

	// Collect changed field values (in respect to object record) or all field values if object is still unsaved
	@SuppressWarnings("unchecked")
	private Map<Class<? extends DomainObject>, Map<Field, Object>> collectFieldChanges() {

		Map<Class<? extends DomainObject>, Map<Field, Object>> fieldChangesMapByDomainClassMap = new HashMap<>();

		// Try to find object record
		SortedMap<String, Object> objectRecord = SqlDomainController.recordMap.get(getClass()).get(id);
		if (objectRecord == null) {

			// New object: add { field , field value } entry to changes map for all registered fields
			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(getClass())) {
				for (Field dataField : Registry.getRegisteredFields(domainClass)) {
					fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(dataField, getFieldValue(dataField));
				}
			}
		}
		else {
			// Check fields and add { field , field value } entry to changes map if value was changed
			for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(getClass())) {

				// Data fields
				for (Field dataField : Registry.getDataFields(domainClass)) {

					Column column = SqlRegistry.getColumnFor(dataField);
					Object fieldValue = getFieldValue(dataField);
					Object columnValue = objectRecord.get(column.name);

					if (!logicallyEqual(Helpers.field2ColumnValue(fieldValue), columnValue)) {
						fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(dataField, fieldValue);
					}
				}

				// Reference fields
				for (Field refField : Registry.getReferenceFields(domainClass)) {

					Column fkColumn = SqlRegistry.getColumnFor(refField);
					DomainObject refObj = (DomainObject) getFieldValue(refField);
					Long refObjIdFromField = (refObj == null ? null : refObj.getId());
					Long refObjFromColumn = (Long) objectRecord.get(fkColumn.name);

					if (!objectsEqual(refObjIdFromField, refObjFromColumn)) {
						fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(refField, refObj);
					}
				}

				// Element collection and key/value map fields
				for (Field complexField : Registry.getComplexFields(domainClass)) {

					SqlDbTable entryTable = SqlRegistry.getEntryTableFor(complexField);
					List<SortedMap<String, Object>> entries = (List<SortedMap<String, Object>>) objectRecord.get(entryTable.name);
					ParameterizedType genericFieldType = ((ParameterizedType) complexField.getGenericType());

					if (Collection.class.isAssignableFrom(complexField.getType())) {

						Collection<Object> fieldCollection = (Collection<Object>) getFieldValue(complexField);
						Collection<Object> columnCollection = Helpers.entryRecords2Collection(genericFieldType, entries);

						if (!objectsEqual(fieldCollection, columnCollection)) {
							fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(complexField, fieldCollection);
						}
					}
					else {
						Map<Object, Object> fieldMap = (Map<Object, Object>) getFieldValue(complexField);
						Map<Object, Object> columnMap = Helpers.entryRecords2Map(genericFieldType, entries);

						if (!objectsEqual(fieldMap, columnMap)) {
							fieldChangesMapByDomainClassMap.computeIfAbsent(domainClass, dc -> new HashMap<>()).put(complexField, fieldMap);
						}
					}
				}
			}
		}

		return fieldChangesMapByDomainClassMap;
	}

	// Build column value map for SQL INSERT or UPDATE from field changes map
	private SortedMap<String, Object> buildColumnValueMapForInsertOrUpdate(Map<Field, Object> fieldChangesMap) {

		SortedMap<String, Object> columnValueMap = new TreeMap<>();

		if (CMap.isEmpty(fieldChangesMap)) {
			return columnValueMap;
		}

		for (Entry<Field, Object> entry : fieldChangesMap.entrySet()) {

			// Handle only column related fields here
			Field field = entry.getKey();
			if (!Registry.isDataField(field) && !Registry.isReferenceField(field)) {
				continue;
			}

			// Build column/value entry for data or reference field
			Column column = SqlRegistry.getColumnFor(field);
			if (Registry.isReferenceField(field)) { // Reference field

				DomainObject refObj = (DomainObject) entry.getValue();
				columnValueMap.put(column.name, refObj == null ? null : refObj.getId());
			}
			else { // Data field
				Object columnValue = null;
				Object fieldValue = entry.getValue();

				if (fieldValue instanceof String && ((String) fieldValue).length() > column.maxlen) {

					log.warn("SDO: Value '{}' exceeds maximum size {} of column '{}' for object {}! Truncate before saving object...", fieldValue, column.maxlen, column.name, name());
					setFieldWarning(field, "CONTENT_TRUNCATED_IN_DATABASE");

					columnValue = ((String) fieldValue).substring(0, column.maxlen);
				}
				else {
					columnValue = Helpers.field2ColumnValue(fieldValue);
				}

				columnValueMap.put(column.name, columnValue);
			}
		}

		return columnValueMap;
	}

	// Build string list of elements for WHERE clause (of DELETE statement)
	private static String buildElementList(Set<Object> elements) {

		StringBuilder sb = new StringBuilder();
		sb.append("(");

		for (Object element : elements) {

			element = Helpers.field2ColumnValue(element);
			if (element instanceof String) {
				sb.append("'" + element + "'");
			}
			else {
				sb.append(element);
			}

			sb.append(",");
		}

		sb.replace(sb.length() - 1, sb.length(), ")");

		return sb.toString();
	}

	// DELETE, UPDATE or/and INSERT entry records (reflecting element collection or key/value map fields) and update object record
	@SuppressWarnings("unchecked")
	private void updateEntryTables(Connection cn, Map<Field, Object> fieldChangesMap, SortedMap<String, Object> objectRecord) throws SqlDbException, SQLException {

		if (CMap.isEmpty(fieldChangesMap)) {
			return;
		}

		// For all changed complex fields...
		for (Field complexField : fieldChangesMap.keySet()) {

			// Ignore changes of column related fields here
			if (!Registry.isComplexField(complexField)) {
				continue;
			}

			// Get entry table and column referencing domain object
			SqlDbTable entryTable = SqlRegistry.getEntryTableFor(complexField);
			Column mainTableRefIdColumn = SqlRegistry.getMainTableRefIdColumnFor(complexField);

			// Perform database operations
			boolean isMap = Map.class.isAssignableFrom(complexField.getType());
			try {
				// Determine generic field type for element conversion and get existing entry records from object record for to find changes
				ParameterizedType genericFieldType = ((ParameterizedType) complexField.getGenericType());
				List<SortedMap<String, Object>> existingEntryRecords = (List<SortedMap<String, Object>>) objectRecord.get(entryTable.name);

				if (isMap) {

					// Map field -> get existing map from object record and new map from field
					Map<?, ?> oldMap = Helpers.entryRecords2Map(genericFieldType, existingEntryRecords);
					Map<?, ?> newMap = (Map<?, ?>) fieldChangesMap.get(complexField);

					// Ignore unchanged map
					if (objectsEqual(oldMap, newMap)) {
						continue;
					}

					// Determine map entries which do not exist anymore
					Set<Object> mapKeysToRemove = new HashSet<>();
					boolean hasNullKey = false;
					for (Entry<?, ?> oldMapEntry : oldMap.entrySet()) {
						if (!newMap.containsKey(oldMapEntry.getKey())) {
							if (oldMapEntry.getKey() == null) {
								hasNullKey = true;
							}
							else {
								mapKeysToRemove.add(oldMapEntry.getKey());
							}
						}
					}

					// DELETE entry records for non-existing map entries
					if (!mapKeysToRemove.isEmpty()) {
						// TODO: Multiple DELETES with lists of max 1000 elements (Oracle)
						SqlDb.deleteFrom(cn, entryTable.name, mainTableRefIdColumn.name + "=" + id + " AND " + KEY_COL + " IN " + buildElementList(mapKeysToRemove));
					}
					if (hasNullKey) {
						SqlDb.deleteFrom(cn, entryTable.name, mainTableRefIdColumn.name + "=" + id + " AND " + KEY_COL + " IS NULL");
					}

					// Determine new and changes map entries
					Map<Object, Object> mapEntriesToInsert = new HashMap<>();
					Map<Object, Object> mapEntriesToChange = new HashMap<>();
					for (Entry<?, ?> newMapEntry : newMap.entrySet()) {
						if (!oldMap.containsKey(newMapEntry.getKey())) {
							mapEntriesToInsert.put(newMapEntry.getKey(), newMapEntry.getValue());
						}
						else if (!objectsEqual(oldMap.get(newMapEntry.getKey()), newMapEntry.getValue())) {
							mapEntriesToChange.put(newMapEntry.getKey(), newMapEntry.getValue());
						}
					}

					// INSERT new records for new map entries
					if (!mapEntriesToInsert.isEmpty()) {
						SqlDomainController.sqlDb.insertInto(cn, entryTable.name, Helpers.map2EntryRecords(mainTableRefIdColumn.name, id, mapEntriesToInsert));
					}

					// UPDATE records for changed map entries
					if (!mapEntriesToChange.isEmpty()) {
						for (Object key : mapEntriesToChange.keySet()) {

							SortedMap<String, Object> changedValueMap = new TreeMap<>();
							changedValueMap.put(VALUE_COL, Helpers.field2ColumnValue(mapEntriesToChange.get(key)));

							key = Helpers.field2ColumnValue(key);
							SqlDomainController.sqlDb.update(cn, entryTable.name, changedValueMap,
									mainTableRefIdColumn.name + "=" + id + " AND " + KEY_COL + "=" + (key instanceof String ? "'" + key + "'" : key));
						}
					}

					// Update object record
					objectRecord.put(entryTable.name, Helpers.map2EntryRecords(mainTableRefIdColumn.name, id, newMap));
				}
				else if (Set.class.isAssignableFrom(complexField.getType())) {

					// Set field -> get existing set from object record and new set from field
					Set<?> oldSet = (Set<?>) Helpers.entryRecords2Collection(genericFieldType, existingEntryRecords);
					Set<?> newSet = (Set<?>) fieldChangesMap.get(complexField);

					// Ignore unchanged set
					if (objectsEqual(oldSet, newSet)) {
						continue;
					}

					// Determine elements which do not exist anymore
					Set<Object> elementsToRemove = new HashSet<>();
					boolean hasNullElement = false;
					for (Object oldElement : oldSet) {
						if (!newSet.contains(oldElement)) {
							if (oldElement == null) {
								hasNullElement = true;
							}
							else {
								elementsToRemove.add(oldElement);
							}
						}
					}

					// DELETE entry records for non-existing elements
					if (!elementsToRemove.isEmpty()) {
						SqlDb.deleteFrom(cn, entryTable.name, mainTableRefIdColumn.name + "=" + id + " AND ELEMENT IN " + buildElementList(elementsToRemove));
					}
					if (hasNullElement) {
						SqlDb.deleteFrom(cn, entryTable.name, mainTableRefIdColumn.name + "=" + id + " AND ELEMENT IS NULL");
					}

					// Determine new elements (null element is no exception from default handling on insertInto())
					Set<Object> elementsToInsert = new HashSet<>();
					for (Object newElement : newSet) {
						if (!oldSet.contains(newElement)) {
							elementsToInsert.add(newElement);
						}
					}

					// INSERT new records for new elements
					if (!elementsToInsert.isEmpty()) {
						SqlDomainController.sqlDb.insertInto(cn, entryTable.name, Helpers.collection2EntryRecords(mainTableRefIdColumn.name, id, elementsToInsert));
					}

					// Update object record
					objectRecord.put(entryTable.name, Helpers.collection2EntryRecords(mainTableRefIdColumn.name, id, newSet));
				}
				else {
					// List field -> get existing list from object record and new list from field
					List<?> oldList = (List<?>) Helpers.entryRecords2Collection(genericFieldType, existingEntryRecords);
					List<?> newList = (List<?>) fieldChangesMap.get(complexField);

					// Ignore unchanged list
					if (objectsEqual(oldList, newList)) {
						continue;
					}

					// DELETE old entry records and INSERT new ones (to simplify operation on list where order-only changes must be reflected too)
					SqlDb.deleteFrom(cn, entryTable.name, mainTableRefIdColumn.name + "=" + id);
					SqlDomainController.sqlDb.insertInto(cn, entryTable.name, Helpers.collection2EntryRecords(mainTableRefIdColumn.name, id, newList));

					// Update object record
					objectRecord.put(entryTable.name, Helpers.collection2EntryRecords(mainTableRefIdColumn.name, id, newList));
				}
			}
			catch (SQLException sqlex) {

				log.error("SDO: SQLException trying to update entry table '{}' for {} field '{}' of object '{}'", entryTable.name, (isMap ? "map" : "collection"), complexField.getName(), name());
				setFieldError(complexField, "Entries for this " + (isMap ? "map" : "collection") + " field could not be updated in database");

				throw sqlex;
			}
		}
	}

	// Try to UPDATE every field of domain object separately after 'normal' UPDATE containing all changed fields failed
	private void tryAnalyticUpdate(Connection cn, SqlDbTable table, SortedMap<String, Object> columnValueMap) throws SqlDbException {

		SortedMap<String, Object> oneCVMap = new TreeMap<>();
		currentException = null;

		Iterator<String> it = columnValueMap.keySet().iterator();
		while (it.hasNext()) {

			String columnName = it.next();
			Object columnValue = columnValueMap.get(columnName);

			oneCVMap.clear();
			oneCVMap.put(columnName, columnValue);

			try {
				// Update one column
				SqlDomainController.sqlDb.update(cn, table.name, oneCVMap, ID_COL + "=" + id);
			}
			catch (SQLException sqlex) {
				log.error("SDO: UPDATE failed by exception! Column '{}' cannot be updated to {} for object {}", columnName, CLog.forAnalyticLogging(oneCVMap.get(columnName)), name());
				log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

				// Assign exception to object
				currentException = sqlex;

				// Set field error for object and field failed to UPDATE
				Field field = SqlRegistry.getFieldFor(table.findColumnByName(columnName));
				if (field != null) {
					setFieldError(field, "CANNOT_UPDATE_COLUMN - " + sqlex.getMessage());
				}
			}
		}
	}

	// -------------------------------------------------------------------------
	// Save object to database
	// -------------------------------------------------------------------------

	// Save object in one transaction to database
	synchronized void save(Connection cn, Map<Class<? extends DomainObject>, Map<Field, Object>> fieldChangesMapByDomainClassMap, List<DomainObject> objectsToSaveBefore,
			List<DomainObject> savedObjects) throws SQLException, SqlDbException {

		// Return immediately if no field changes were detected on already stored object
		if (isStored && CMap.isEmpty(fieldChangesMapByDomainClassMap)) {
			return;
		}

		// Initialize object list for recursion control
		if (objectsToSaveBefore == null) {
			objectsToSaveBefore = new ArrayList<>();
		}
		int stackSize = objectsToSaveBefore.size();

		if (log.isDebugEnabled()) {
			log.debug("SDO: {}Save{} object {}", tabs(stackSize), (isStored ? "" : " new"), name());
		}

		if (savedObjects == null) {
			savedObjects = new ArrayList<>();
		}

		// Update accumulations for pending reference changes (do this on saving object to ensure accumulation integrity after save())
		updateAccumulationsOfParentObjects();

		// Reset exception and field errors/warnings here (if exist) - exception and/or field errors/warning will be generated again on saving if object is still invalid or has invalid fields
		clearErrors();

		// Get or create object record
		SortedMap<String, Object> objectRecord = null;
		if (!isStored) {
			objectRecord = new TreeMap<>();
			SqlDomainController.recordMap.get(getClass()).put(id, objectRecord);
		}
		else {
			objectRecord = SqlDomainController.recordMap.get(getClass()).get(id);
		}

		Map<Field, DomainObject> unsavedReferencedObjectMap = new HashMap<>();

		// For all (inherited) domain classes of object...
		for (Class<? extends DomainObject> domainClass : Registry.getInheritanceStack(getClass())) {

			// Save unsaved referenced objects before saving this object or collect them here to save them after saving this object (avoid infinite loop on circular reference)
			saveOrStoreUnsavedReferecedObjectsBeforeSavingObject(cn, domainClass, unsavedReferencedObjectMap, objectsToSaveBefore, savedObjects);

			// Do not save object again if it was already saved (in case of circular reference)
			if (savedObjects.contains(this)) {
				break;
			}

			// Get database table related to domain class
			SqlDbTable table = SqlRegistry.getTableFor(domainClass);

			// If unsaved referenced objects exist references to these objects were reset to allow saving this object - field changes must be collected again in this case to reflect these changes
			if (!CMap.isEmpty(unsavedReferencedObjectMap)) {
				fieldChangesMapByDomainClassMap = collectFieldChanges();
			}

			// Build column value map for SQL INSERT or UPDATE from field changes map
			SortedMap<String, Object> columnValueMap = buildColumnValueMapForInsertOrUpdate(fieldChangesMapByDomainClassMap.get(domainClass));

			// Update modification date if object is still unsaved or if any changes are detected
			if (Registry.isBaseDomainClass(domainClass) && (!isStored || !CMap.isEmpty(fieldChangesMapByDomainClassMap))) {

				// Update 'last modified in database' field and add LAST_MODIFIED item to column/value map (for object domain class table)
				lastModifiedInDb = LocalDateTime.now();
				columnValueMap.put(LAST_MODIFIED_COL, lastModifiedInDb);
			}

			if (!isStored) { // INSERT

				// Add ID and DOMAIN_CLASS column (latter one to identify object's domain class also from non-object records)
				columnValueMap.put(ID_COL, id);
				columnValueMap.put(DOMAIN_CLASS_COL, getClass().getSimpleName());

				try {
					// INSERT record into table associated to this domain class
					// Note: During INSERT values in column/value map will potentially be converted to JDBC specific type, so that data types after INSERT and after SELECT are the same
					SqlDomainController.sqlDb.insertInto(cn, table.name, columnValueMap);
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}INSERT failed by exception! Object {} cannot be saved in table '{}'", tabs(stackSize), name(), table.name);
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

					// Set exception and field errors on constraint violation(s) (check violations in ascending order of severity to have the most critical ones assigned to field(s) if multiple
					// violations exist for one field)
					this.currentException = sqlex;
					FieldError.hasColumnSizeViolations(this, domainClass);
					FieldError.hasUniqueConstraintViolations(this, domainClass);
					FieldError.hasNotNullConstraintViolations(this, domainClass);

					// Restore references which were reset before trying to save object
					for (Field refField : unsavedReferencedObjectMap.keySet()) {
						setFieldValue(refField, unsavedReferencedObjectMap.get(refField));
					}

					cn.rollback();

					log.warn("SDO: Whole save transaction rolled back!");

					throw sqlex;
				}
			}
			else { // UPDATE

				// Ignore domain class if no changes detected for their fields
				if (columnValueMap.isEmpty()) {
					continue;
				}

				// UPDATE record in table associated to this domain class
				try {
					long count = SqlDomainController.sqlDb.update(cn, table.name, columnValueMap, ID_COL + "=" + id);
					if (count == 0) {
						throw new SqlDbException("Object '" + this + "' could not be saved because record for this object does not exist in table '" + table.name + "'");
					}
				}
				catch (SQLException sqlex) {
					log.error("SDO: {}UPDATE failed by {}! Not all changed fields can be saved for object {}!\nTry to update columns separetely...", tabs(stackSize), sqlex.getClass().getSimpleName(),
							name());
					log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

					// UPDATE columns separately - so all but one (or more) columns can be updated - set SQL exception and field error according to (first) failing column UPDATE
					tryAnalyticUpdate(cn, table, columnValueMap);
				}
			}

			// Update local object record
			objectRecord.putAll(columnValueMap);

			// Handle table related (collection and map) fields: DELETE old entry records and INSERT new ones. Update object record
			updateEntryTables(cn, fieldChangesMapByDomainClassMap.get(domainClass), objectRecord);
		}

		// Mark object as saved - not until after all records have been inserted but before saving collected unsaved referenced objects - to avoid infinite recursion on circular references
		isStored = true;

		// Save collected referenced objects (on nullable foreign key columns) after saving this object and restore (UPDATE) references in database
		if (!unsavedReferencedObjectMap.isEmpty()) {
			objectRecord.putAll(saveUnsavedReferecedObjectsAfterSavingObject(cn, unsavedReferencedObjectMap, objectsToSaveBefore, savedObjects));
		}

		if (log.isDebugEnabled()) {
			log.debug("SDO: {}Object {} saved", tabs(stackSize), name());
		}

		savedObjects.add(this);
	}

	/**
	 * Save object on existing database connection.
	 * 
	 * @param cn
	 *            database connection
	 * 
	 * @throws SQLException
	 *             exception thrown during execution of INSERT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public void save(Connection cn) throws SQLException, SqlDbException {

		if (!isRegistered()) {
			log.warn("SDO: Object {} cannot be saved because it was not registered before!", name());
			return;
		}

		save(cn, collectFieldChanges(), null, null);
	}

	/**
	 * Save changed object to database.
	 * <p>
	 * INSERTs object records for new object or UPDATE columns associated to changed fields for existing objects. UPDATEs element and key/value tables associated to entry fields of object too.
	 * <p>
	 * On failed saving the SQL exception thrown and the field error(s) recognized will be assigned to object. In this case object is marked as invalid and will not be found using
	 * {@link SqlDomainController#allValid()}. Invalid field content remains (to allow using it as base for correction). If invalid field content shall be overridden by existing valid content one can
	 * use {@link #reload()}. If object was already saved UPDATE will be tried for every column separately to keep impact of failure small.
	 * <p>
	 * If initial saving (INSERTing object records) fails whole transaction will be ROLLed BACK
	 * 
	 * @throws SQLException
	 *             exception thrown during establishing database connection or execution of INSERT or UPDATE statement
	 * @throws SqlDbException
	 *             on internal errors
	 */
	public void save() throws SQLException, SqlDbException {

		try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) { // Use one transaction for all INSERTs and UPDATEs to allow ROLLBACK of whole transaction on error

			try {
				// Save this object - transaction will automatically be committed on closing connection
				save(sqlcn.cn);
			}
			catch (SqlDbException sqldbex) {
				log.error("SDO: Object {} cannot be saved", name());
				log.info("SDO: {}: {}", sqldbex.getClass().getSimpleName(), sqldbex.getMessage());

				currentException = sqldbex;
				sqlcn.cn.rollback();

				log.warn("SDO: Whole save transaction rolled back!");

				throw sqldbex;
			}
		}
		catch (SQLException sqlex) {
			log.error("SDO: Object {} cannot be saved (database connection could not be established)", name());
			log.info("SDO: {}: {}", sqlex.getClass().getSimpleName(), sqlex.getMessage());

			throw sqlex;
		}
	}
}
