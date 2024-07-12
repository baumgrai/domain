package com.icx.domain.sql;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CList;
import com.icx.common.CLog;
import com.icx.common.CMap;
import com.icx.common.Common;
import com.icx.jdbc.SqlDb;
import com.icx.jdbc.SqlDbException;

/**
 * Helpers for deleting objects.
 * <p>
 * Note: Deletion of a domain object means unregistering this object (removing it from object store), DELETEing associated records from database and do so for all direct and indirect children.
 * 
 * @author baumgrai
 */
public class Deleter extends Common {

	static final Logger log = LoggerFactory.getLogger(Deleter.class);

	// Members & constructor

	public SqlDomainController sdc = null;

	public Connection cn = null;

	public Deleter(
			SqlDomainController sdc,
			Connection cn) {

		this.sdc = sdc;
		this.cn = cn;
	}

	// Check if any of objects to check has a reference to this object - which indicates a circular reference in calling context
	private void checkForAndResetCircularReferences(SqlDomainObject objectToDelete, List<SqlDomainObject> objectsToCheck, int stackSize) throws SQLException, SqlDbException {

		for (SqlDomainObject objectToCheck : objectsToCheck) {

			for (Class<? extends SqlDomainObject> domainClass : sdc.getRegistry().getDomainClassesFor(objectToCheck.getClass())) {
				for (Field refField : sdc.getRegistry().getReferenceFields(domainClass)) {

					if (objectsEqual(objectToCheck.getFieldValue(refField), objectToDelete)) {

						if (log.isDebugEnabled()) {
							log.debug("SDC: {}Circular reference detected: {}.{} references {}! Reset reference before deleting object.", CLog.tabs(stackSize), objectToCheck.universalId(),
									refField.getName(), objectToDelete.universalId());
						}

						// Set object's field value to null
						objectToCheck.setFieldValue(refField, null);

						// UPDATE object SET column value to NULL
						String referencingTableName = sdc.getSqlRegistry().getTableFor(sdc.getRegistry().getCastedDeclaringDomainClass(refField)).name;
						String foreignKeyColumnName = sdc.getSqlRegistry().getColumnFor(refField).name;
						sdc.sqlDb.update(cn, referencingTableName, CMap.newSortedMap(foreignKeyColumnName, null), Const.ID_COL + "=" + objectToCheck.getId());
					}
				}
			}
		}
	}

	// DELETE object records from database
	private void deleteFromDatabase(SqlDomainObject obj, int stackSize) throws SQLException, SqlDbException {

		// Delete records belonging to this object: object records for domain class(es) and potentially existing entry records
		for (Class<? extends SqlDomainObject> domainClass : CList.reverse(sdc.getRegistry().getDomainClassesFor(obj.getClass()))) {

			// Delete possibly existing element or key/value records from entry tables before deleting object record for domain class itself
			for (Field complexField : sdc.getRegistry().getComplexFields(domainClass)) {
				SqlDb.deleteFrom(cn, sdc.getSqlRegistry().getEntryTableFor(complexField).name, sdc.getSqlRegistry().getMainTableRefIdColumnFor(complexField).name + "=" + obj.getId());
			}

			// Delete object record for domain class
			long count = SqlDb.deleteFrom(cn, sdc.getSqlRegistry().getTableFor(domainClass).name, Const.ID_COL + "=" + obj.getId());
			if (count != 1) {
				log.warn("SDC: Record for domain class '{}' of {} was not deleted (did not exist)", obj.universalId(), domainClass);
			}
			else if (log.isTraceEnabled()) {
				log.info("SDC: {}Record for domain class '{}' of {} was deleted", CLog.tabs(stackSize), obj.universalId(), domainClass);
			}
		}
	}

	// Delete object and all of its children from database
	void deleteRecursiveFromDatabase(SqlDomainObject obj, List<SqlDomainObject> unregisteredObjects, List<SqlDomainObject> objectsToCheck, int stackSize) throws SQLException, SqlDbException {

		if (objectsToCheck == null) {
			objectsToCheck = new ArrayList<>();
		}
		if (log.isTraceEnabled()) {
			log.trace("SDC: {}Delete {}", CLog.tabs(stackSize), obj.name());
		}

		// Unregister this object
		sdc.unregister(obj); // to avoid that this object can be found while deletion process runs
		unregisteredObjects.add(obj); // to allow re-registration on failure
		objectsToCheck.add(obj); // objects which are in process of deletion (and which shall be checked for circular references)

		// Delete children
		for (SqlDomainObject child : sdc.getDirectChildren(obj)) {
			deleteRecursiveFromDatabase(child, unregisteredObjects, objectsToCheck, stackSize + 1);
		}

		// Delete object itself from database (if it was already stored)
		if (obj.isStored) {
			checkForAndResetCircularReferences(obj, objectsToCheck, stackSize);

			deleteFromDatabase(obj, stackSize);

			objectsToCheck.remove(obj); // object was DELETED in database and does not need be checked for circular references anymore
			if (log.isTraceEnabled()) {
				log.trace("SDC: {}{} was deleted", CLog.tabs(stackSize), obj.universalId());
			}
		}
		else {
			log.info("SDC: {}{} was not stored in database and therefore object record(s) cannot be deleted", CLog.tabs(stackSize), obj.universalId());
		}
	}
}
