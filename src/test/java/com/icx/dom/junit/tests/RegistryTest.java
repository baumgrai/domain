package com.icx.dom.junit.tests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CList;
import com.icx.common.CProp;
import com.icx.dom.junit.TestHelpers;
import com.icx.dom.junit.domain.A;
import com.icx.dom.junit.domain.AA;
import com.icx.dom.junit.domain.AB;
import com.icx.dom.junit.domain.B;
import com.icx.dom.junit.domain.C;
import com.icx.dom.junit.domain.O;
import com.icx.dom.junit.domain.RemovedClass;
import com.icx.dom.junit.domain.sub.X;
import com.icx.dom.junit.domain.sub.Y;
import com.icx.dom.junit.domain.sub.Z;
import com.icx.domain.Registry;
import com.icx.domain.sql.Const;
import com.icx.domain.sql.SqlDomainController;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.java2sql.Java2Sql;
import com.icx.jdbc.ConnectionPool;
import com.icx.jdbc.SqlDbTable;
import com.icx.jdbc.SqlDbTable.SqlDbColumn;
import com.icx.jdbc.SqlDbTable.SqlDbUniqueConstraint;

@TestMethodOrder(OrderAnnotation.class)
class RegistryTest extends TestHelpers {

	static final Logger log = LoggerFactory.getLogger(RegistryTest.class);

	SqlDomainController sdc = new SqlDomainController();

	@SuppressWarnings("static-method")
	@Test
	@Order(1)
	void register() throws Exception {

		log.info("\tTEST 1: register()");

		// Expected values

		List<Class<? extends SqlDomainObject>> registeredDomainClasses = CList.newList(O.class, AA.class, A.class, A.Inner.class, AB.class, B.class, C.class, X.class, X.InProgress.class, Y.class,
				Z.class);
		List<Class<? extends SqlDomainObject>> registeredObjectDomainClasses = CList.newList(O.class, AA.class, A.Inner.class, AB.class, B.class, C.class, X.class, X.InProgress.class, Y.class,
				Z.class);
		List<Class<? extends SqlDomainObject>> relevantDomainClasses = new ArrayList<>(registeredDomainClasses);
		relevantDomainClasses.add(RemovedClass.class);

		List<Field> dataFieldsOfA = fields(A.class, "bool", "booleanValue", "c", "charValue", "sh", "shortValue", "i", "integerValue", "l", "longValue", "d", "doubleValue", "bigIntegerValue",
				"bigDecimalValue", "datetime", "date", "time", "s", "structure", "deprecatedField", "picture", "longtext", "file", "type", "secretString", "pwd");
		List<Field> complexFieldsOfA = fields(A.class, "stringArray", "strings", "doubleSet", "bigDecimalMap", "listOfLists", "listOfMaps", "mapOfLists", "mapOfMaps");
		List<Field> referenceFieldsOfA = fields(A.class, "o");
		List<Field> accumulationFieldsOfA = fields(A.class, "inners", "xs");
		List<Field> allFieldsReferencingA = fields(A.Inner.class, "a");
		allFieldsReferencingA.addAll(fields(X.class, "a"));

		List<Field> dataAndReferenceFieldsOfA = new ArrayList<>(dataFieldsOfA);
		dataAndReferenceFieldsOfA.addAll(referenceFieldsOfA);

		List<Field> registeredFieldsOfA = new ArrayList<>(dataAndReferenceFieldsOfA);
		registeredFieldsOfA.addAll(complexFieldsOfA);

		List<Field> relevantFieldsOfA = new ArrayList<>(registeredFieldsOfA);
		relevantFieldsOfA.add(A.class.getDeclaredField("nonRegisterableField"));
		relevantFieldsOfA.add(A.class.getDeclaredField("removedField"));
		relevantFieldsOfA.add(A.class.getDeclaredField("removedRefField"));
		relevantFieldsOfA.add(A.class.getDeclaredField("removedCollectionField"));

		List<Field> referenceFieldsOfX = fields(X.class, "a", "y");

		log.info("\tRegister by explicitly defined domain classes...");

		SqlDomainController sdc = new SqlDomainController();

		assertDoesNotThrow(() -> sdc.registerDomainClasses(SqlDomainObject.class, X.class, AA.class)); // AA as sub class of A must explicitly be defined

		assertEquals(O.class, sdc.getDomainClassByName("O"), "register inherited domain class");
		assertEquals(Z.class, sdc.getDomainClassByName("Z"), "register referenced domain class");

		assertDoesNotThrow(() -> sdc.registerDomainClasses(SqlDomainObject.class, O.class, AA.class, A.Inner.class, AB.class, B.class, C.class, X.class, RemovedClass.class));

		assertListsEqualButOrder(registeredDomainClasses, sdc.getRegistry().getRegisteredDomainClasses(), "register domain classes by class list");

		log.info("\tUnregister...");

		sdc.getRegistry().unregisterField(A.class.getDeclaredField("s"));
		assertFalse(sdc.getRegistry().getDataFields(A.class).contains(A.class.getDeclaredField("s")));
		sdc.getRegistry().unregisterField(A.class.getDeclaredField("strings"));
		assertFalse(sdc.getRegistry().getComplexFields(A.class).contains(A.class.getDeclaredField("strings")));
		sdc.getRegistry().unregisterField(A.class.getDeclaredField("o"));
		assertFalse(sdc.getRegistry().getReferenceFields(A.class).contains(A.class.getDeclaredField("o")));

		log.info("\tRegister by domain classes in specific package...");

		assertDoesNotThrow(() -> sdc.getRegistry().registerDomainClasses(SqlDomainObject.class, A.class.getPackage().getName()));

		log.info("\tClass related checks...");

		assertListsEqualButOrder(registeredDomainClasses, sdc.getRegistry().getRegisteredDomainClasses(), "register domain classes by package name");
		assertListsEqualButOrder(relevantDomainClasses, sdc.getRegistry().getRelevantDomainClasses(), "relevant (also removed) domain classes");
		assertListsEqualButOrder(registeredObjectDomainClasses, sdc.getRegistry().getRegisteredObjectDomainClasses(), "registered object domain classes");

		assertTrue(sdc.getRegistry().isRegisteredDomainClass(Z.class));
		assertFalse(sdc.getRegistry().isRegisteredDomainClass(RegistryTest.class));
		assertTrue(sdc.getRegistry().isObjectDomainClass(AA.class));
		assertFalse(sdc.getRegistry().isObjectDomainClass(A.class));
		assertFalse(sdc.getRegistry().isBaseDomainClass(AA.class));
		assertTrue(sdc.getRegistry().isBaseDomainClass(A.class));

		log.info("\tField related checks...");

		assertEquals(A.class.getDeclaredField("i"), sdc.getRegistry().getFieldByName(A.class, "i"), "get field by name");
		assertEquals(AA.class.getDeclaredConstructor(), sdc.getRegistry().getConstructor(AA.class), "get constructor");

		assertListsEqualButOrder(dataFieldsOfA, sdc.getRegistry().getDataFields(A.class), "data fields");
		assertListsEqualButOrder(complexFieldsOfA, sdc.getRegistry().getComplexFields(A.class), "collection and map fields");
		assertListsEqualButOrder(referenceFieldsOfX, sdc.getRegistry().getReferenceFields(X.class), "reference fields");
		assertListsEqualButOrder(accumulationFieldsOfA, sdc.getRegistry().getAccumulationFields(A.class), "accumulation fields");
		assertListsEqualButOrder(dataAndReferenceFieldsOfA, sdc.getRegistry().getDataAndReferenceFields(A.class), "data and reference fields");
		assertListsEqualButOrder(registeredFieldsOfA, sdc.getRegistry().getRegisteredFields(A.class), "registered fields");
		assertListsEqualButOrder(allFieldsReferencingA, sdc.getRegistry().getAllReferencingFields(A.class), "referencing fields");

		assertTrue(Registry.isDataField(A.class.getDeclaredField("l")));
		assertTrue(sdc.getRegistry().isReferenceField(X.class.getDeclaredField("a")));
		assertTrue(sdc.getRegistry().isComplexField(A.class.getDeclaredField("mapOfMaps")));
		assertEquals(A.class.getDeclaredField("xs"), sdc.getRegistry().getAccumulationFieldForReferenceField(X.class.getDeclaredField("a")));

		assertEquals(A.class, sdc.getRegistry().getFieldByName(A.class, "i").getDeclaringClass());
		assertListsEqualButOrder(relevantFieldsOfA, sdc.getRegistry().getRelevantFields(A.class), "relevant fields");

		log.info("\tCircular references...");

		Set<List<String>> circularReferences = sdc.getRegistry().determineCircularReferences();
		assertEquals(3, circularReferences.size(), "circular references");
		assertEquals(2, circularReferences.stream().filter(cr -> cr.size() == 1).count());
		assertEquals(1, circularReferences.stream().filter(cr -> cr.size() == 3).count());

		log.info("\t" + circularReferences);
	}

	@Test
	@Order(2)
	void domainClassDatabaseTableAssociation() throws Exception {

		log.info("\tTEST 2: domainClassDatabaseTableAssociation()");

		log.info("\tRegister domain classes and associate with database tables...");

		SqlDomainController sdc = new SqlDomainController();

		Properties dbProps = CProp.readEnvironmentSpecificProperties(CProp.findPropertiesFile("db.properties"), "local/mysql/junit",
				CList.newList(ConnectionPool.DB_CONNECTION_STRING_PROP, ConnectionPool.DB_USER_PROP));
		assertEquals("jdbc:mysql://localhost/junit?useSSL=false", CProp.getStringProperty(dbProps, ConnectionPool.DB_CONNECTION_STRING_PROP, ""));

		Properties domainProps = CProp.readProperties(CProp.findPropertiesFile(SqlDomainController.DOMAIN_PROPERIES_FILE));
		assertNotNull(CProp.getStringProperty(domainProps, "dataHorizonPeriod", null));

		assertDoesNotThrow(() -> sdc.initialize(dbProps, domainProps, A.class.getPackage().getName()));

		log.info("\tAssertions on class/table association...");

		SqlDbTable tableA = sdc.getSqlDb().findRegisteredTable("DOM_A");
		SqlDbTable tableAA = sdc.getSqlDb().findRegisteredTable("DOM_AA");
		SqlDbTable tableX = sdc.getSqlDb().findRegisteredTable("DOM_X");
		SqlDbTable tableZ = sdc.getSqlDb().findRegisteredTable("DOM_Z");

		assertEquals("DOM_A", sdc.getSqlRegistry().getTableFor(A.class).name, "Associated table for class A");
		assertEquals("DOM_AA", sdc.getSqlRegistry().getTableFor(AA.class).name, "Associated table for class AA (name specified in SqlTable annotation");
		assertEquals("BOOLEAN", sdc.getSqlRegistry().getColumnFor(A.class.getDeclaredField("bool")).name, "Associated column for data field (name specified in SqlColumn annotation");
		assertEquals("O_ID", sdc.getSqlRegistry().getColumnFor(A.class.getDeclaredField("o")).name, "Associated column for reference field");
		assertEquals("DOM_A_STRINGS", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("strings")).name, "Associated entry table for list");
		assertEquals("DOM_A_DOUBLE_SET", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("doubleSet")).name, "Associated entry table for set");
		assertEquals("DOM_A_BIG_DECIMAL_MAP", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("bigDecimalMap")).name, "Associated entry table map");
		assertEquals("DOM_A_LIST_OF_LISTS", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("listOfLists")).name, "Associated entry table for list of lists");
		assertEquals("DOM_A_LIST_OF_MAPS", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("listOfMaps")).name, "Associated entry table for list of maps");
		assertEquals("DOM_A_MAP_OF_LISTS", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("mapOfLists")).name, "Associated entry table for map of lists");
		assertEquals("DOM_A_MAP_OF_MAPS", sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("mapOfMaps")).name, "Associated entry table for map of maps");

		assertEquals("doubleValue", sdc.getSqlRegistry().getFieldFor(sdc.getSqlRegistry().getColumnFor(A.class.getDeclaredField("doubleValue"))).getName(), "Associated field for column");

		Set<SqlDbUniqueConstraint> ucs = tableA.findUniqueConstraintsByColumnName("S");
		assertEquals(1, ucs.size(), "find unique constraints by columm name");
		SqlDbUniqueConstraint uc = ucs.iterator().next();
		assertEquals(uc, tableA.findUniqueConstraintByName(uc.name), "find unique constraint by name");

		ucs = tableA.findUniqueConstraintsByColumnName("I");
		assertEquals(1, ucs.size(), "find multiple column unique constraints by columm name");

		assertEquals(tableZ.findColumnByName("X_ID"), tableZ.getFkColumnsReferencingTable(tableX).iterator().next(), "find foreign key columns referencing table");
		assertTrue(tableA.isReachableFrom(tableZ), "is reachable");
		assertFalse(tableZ.isReachableFrom(tableA), "is not reachable");
		assertTrue(tableA.isReachableFrom(tableX), "is reachable");
		assertTrue(tableAA.reaches(tableA), "reaches");

		assertEquals(5, tableX.getReachableTables().size(), "reachable tables");
		assertEquals(4, tableX.getTablesWhichCanReach().size(), "tables which can reach");
	}

	@Test
	@Order(3)
	void java2sql() throws Exception {

		log.info("\tTEST 3: java2sql()");

		Java2Sql.main(new String[] { "com.icx.dom.junit.domain" });

		log.info("\tError cases...");

		File dbPropsFile = CProp.findPropertiesFile("db.properties");
		String localConf = "local/" + LoadAndSaveTest.dbType.toString().toLowerCase() + "/junit";
		Properties dbProps = CProp.readEnvironmentSpecificProperties(dbPropsFile, localConf, CList.newList(ConnectionPool.DB_CONNECTION_STRING_PROP, ConnectionPool.DB_USER_PROP));
		Properties domainProps = CProp.readProperties(CProp.findPropertiesFile(SqlDomainController.DOMAIN_PROPERIES_FILE));
		sdc.initialize(dbProps, domainProps, "com.icx.dom.junit.domain");

		assertNull(sdc.getSqlRegistry().getTableFor(SqlDomainObject.class));
		assertNull(sdc.getSqlRegistry().getColumnFor(A.class.getDeclaredField("strings")));
		assertNull(sdc.getSqlRegistry().getEntryTableFor(A.class.getDeclaredField("l")));
		assertNull(sdc.getSqlRegistry().getMainTableRefIdColumnFor(A.class.getDeclaredField("i")));
		SqlDbColumn domainClassColumn = sdc.getSqlRegistry().getTableFor(A.class).columns.stream().filter(c -> Const.DOMAIN_CLASS_COL.equals(c.name)).findAny().orElse(null);
		assertNull(sdc.getSqlRegistry().getFieldFor(domainClassColumn));
	}
}
