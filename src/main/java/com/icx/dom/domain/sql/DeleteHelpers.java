package com.icx.dom.domain.sql;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.CList;
import com.icx.common.base.CLog;
import com.icx.common.base.CMap;
import com.icx.common.base.Common;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDbException;

/**
 * Helpers for deleting objects.
 * <p>
 * Note: Deletion of a domain object means unregistering this object (removing it from object store), DELETEing associated records from database and do so for all direct and indirect children.
 * 
 * @author baumgrai
 */
public class DeleteHelpers extends Common {

	static final Logger log = LoggerFactory.getLogger(DeleteHelpers.class);

	// Check if any of objects to check has a reference to this object - which indicates a circular reference in calling context
	private static void checkForAndResetCircularReferences(Connection cn, SqlDomainController sdc, SqlDomainObject objectToDelete, List<SqlDomainObject> objectsToCheck, int stackSize)
			throws SQLException, SqlDbException {

		for (SqlDomainObject objectToCheck : objectsToCheck) {

			for (Class<? extends SqlDomainObject> domainClass : sdc.registry.getDomainClassesFor(objectToCheck.getClass())) {
				for (Field refField : sdc.registry.getReferenceFields(domainClass)) {

					if (objectsEqual(objectToCheck.getFieldValue(refField), objectToDelete)) {

						log.info("SDC: {}Circular reference detected: {}.{} references {}! Reset reference before deleting object.", CLog.tabs(stackSize), objectToCheck.name(), refField.getName(),
								objectToDelete.name());

						// Set object's field value to null
						objectToCheck.setFieldValue(refField, null);

						// UPDATE object SET column value to NULL
						String referencingTableName = ((SqlRegistry) sdc.registry).getTableFor(sdc.registry.getCastedDeclaringDomainClass(refField)).name;
						String foreignKeyColumnName = ((SqlRegistry) sdc.registry).getColumnFor(refField).name;
						sdc.sqlDb.update(cn, referencingTableName, CMap.newSortedMap(foreignKeyColumnName, null), Const.ID_COL + "=" + objectToCheck.getId());
					}
				}
			}
		}
	}

	// DELETE object records from database
	private static void deleteFromDatabase(Connection cn, SqlDomainController sdc, SqlDomainObject obj) throws SQLException, SqlDbException {

		// Delete records belonging to this object: object records for domain class(es) and potentially existing entry records
		for (Class<? extends SqlDomainObject> domainClass : CList.reverse(sdc.registry.getDomainClassesFor(obj.getClass()))) {

			// Delete possibly existing element or key/value records from entry tables before deleting object record for domain class itself
			for (Field complexField : sdc.registry.getComplexFields(domainClass)) {
				SqlDb.deleteFrom(cn, ((SqlRegistry) sdc.registry).getEntryTableFor(complexField).name, ((SqlRegistry) sdc.registry).getMainTableRefIdColumnFor(complexField).name + "=" + obj.getId());
			}

			// Delete object record for domain class
			SqlDb.deleteFrom(cn, ((SqlRegistry) sdc.registry).getTableFor(domainClass).name, Const.ID_COL + "=" + obj.getId());
		}
	}

	// Delete object and all of its children from database
	static void deleteRecursiveFromDatabase(Connection cn, SqlDomainController sdc, SqlDomainObject obj, List<SqlDomainObject> unregisteredObjects, List<SqlDomainObject> objectsToCheck, int stackSize)
			throws SQLException, SqlDbException {

		if (objectsToCheck == null) {
			objectsToCheck = new ArrayList<>();
		}
		log.info("SDC: {}Delete {}", CLog.tabs(stackSize), obj.name());

		// Unregister this object
		sdc.unregister(obj); // to avoid that this object can be found while deletion process runs
		unregisteredObjects.add(obj); // to allow re-registration on failure
		objectsToCheck.add(obj); // objects which are in process of deletion (and which shall be checked for circular references)

		// Delete children
		for (SqlDomainObject child : sdc.getDirectChildren(obj)) {
			deleteRecursiveFromDatabase(cn, sdc, child, unregisteredObjects, objectsToCheck, stackSize + 1);
		}

		// Delete object itself from database (if it was already stored)
		if (obj.isStored) {
			checkForAndResetCircularReferences(cn, sdc, obj, objectsToCheck, stackSize);
			deleteFromDatabase(cn, sdc, obj);

			objectsToCheck.remove(obj); // object was DELETED in database and does not need be checked for circular references anymore
			if (log.isTraceEnabled()) {
				log.trace("SDC: {}{} was deleted", CLog.tabs(stackSize), obj.name());
			}
		}
	}
}
