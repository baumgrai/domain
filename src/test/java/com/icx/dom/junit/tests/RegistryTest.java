package com.icx.dom.junit.tests;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import com.icx.dom.common.CList;
import com.icx.dom.common.Prop;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.Registry;
import com.icx.dom.domain.sql.SqlDomainController;
import com.icx.dom.domain.sql.SqlDomainObject;
import com.icx.dom.domain.sql.SqlRegistry;
import com.icx.dom.domain.sql.tools.Java2Sql;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.jdbc.SqlDbTable.Column;
import com.icx.dom.jdbc.SqlDbTable.UniqueConstraint;
import com.icx.dom.junit.TestHelpers;
import com.icx.dom.junit.domain.A;
import com.icx.dom.junit.domain.AA;
import com.icx.dom.junit.domain.B;
import com.icx.dom.junit.domain.C;
import com.icx.dom.junit.domain.O;
import com.icx.dom.junit.domain.RemovedClass;
import com.icx.dom.junit.domain.sub.X;
import com.icx.dom.junit.domain.sub.Y;
import com.icx.dom.junit.domain.sub.Z;

@TestMethodOrder(OrderAnnotation.class)
public class RegistryTest extends TestHelpers {

	static final Logger log = LoggerFactory.getLogger(RegistryTest.class);

	@SuppressWarnings("static-method")
	@Test
	@Order(1)
	void register() throws Exception {

		log.info("\tTEST 1: register()");

		// Expected values

		List<Class<? extends SqlDomainObject>> registeredDomainClasses = CList.newList(O.class, AA.class, A.class, A.Inner.class, B.class, C.class, X.class, Y.class, Z.class);
		List<Class<? extends SqlDomainObject>> registeredObjectDomainClasses = CList.newList(O.class, AA.class, A.Inner.class, B.class, C.class, X.class, Y.class, Z.class);
		List<Class<? extends SqlDomainObject>> relevantDomainClasses = new ArrayList<>(registeredDomainClasses);
		relevantDomainClasses.add(RemovedClass.class);

		List<Field> dataFieldsOfA = fields(A.class, "bool", "booleanValue", "i", "integerValue", "l", "longValue", "d", "doubleValue", "bigIntegerValue", "bigDecimalValue", "s", "deprecatedField",
				"bytes", "file", "type");
		List<Field> complexFieldsOfA = fields(A.class, "strings", "doubleSet", "bigDecimalMap", "listOfLists", "listOfMaps", "mapOfLists", "mapOfMaps");
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

		assertDoesNotThrow(() -> SqlDomainController.registerDomainClasses(X.class, AA.class)); // AA as sub class of A must explicitly be defined because subclasses cannot be determined using
																								// Reflection

		assertEquals(O.class, DomainController.getDomainClassByName("O"), "register inherited domain class");
		assertEquals(Z.class, DomainController.getDomainClassByName("Z"), "register referenced domain class");

		assertDoesNotThrow(() -> Registry.registerDomainClasses(SqlDomainObject.class, O.class, AA.class, A.Inner.class, B.class, C.class, X.class, RemovedClass.class));

		assertListsEqualButOrder(registeredDomainClasses, Registry.getRegisteredDomainClasses(), "register domain classes by class list");

		log.info("\tUnregister...");

		Registry.unregisterField(A.class.getDeclaredField("s"));
		assertFalse(Registry.getDataFields(A.class).contains(A.class.getDeclaredField("s")));
		Registry.unregisterField(A.class.getDeclaredField("strings"));
		assertFalse(Registry.getComplexFields(A.class).contains(A.class.getDeclaredField("strings")));
		Registry.unregisterField(A.class.getDeclaredField("o"));
		assertFalse(Registry.getReferenceFields(A.class).contains(A.class.getDeclaredField("o")));

		log.info("\tRegister by domain classes in specific package...");

		assertDoesNotThrow(() -> Registry.registerDomainClasses(SqlDomainObject.class, A.class.getPackage().getName()));

		log.info("\tClass related checks...");

		assertListsEqualButOrder(registeredDomainClasses, Registry.getRegisteredDomainClasses(), "register domain classes by package name");
		assertListsEqualButOrder(relevantDomainClasses, Registry.getRelevantDomainClasses(), "relevant (also removed) domain classes");
		assertListsEqualButOrder(registeredObjectDomainClasses, Registry.getRegisteredObjectDomainClasses(), "registered object domain classes");

		assertTrue(Registry.isRegisteredDomainClass(Z.class));
		assertFalse(Registry.isRegisteredDomainClass(RegistryTest.class));
		assertTrue(Registry.isObjectDomainClass(AA.class));
		assertFalse(Registry.isObjectDomainClass(A.class));
		assertFalse(Registry.isBaseDomainClass(AA.class));
		assertTrue(Registry.isBaseDomainClass(A.class));

		log.info("\tField related checks...");

		assertEquals(A.class.getDeclaredField("i"), Registry.getFieldByName(A.class, "i"), "get field by name");
		assertEquals(AA.class.getDeclaredConstructor(), Registry.getConstructor(AA.class), "get constructor");

		assertListsEqualButOrder(dataFieldsOfA, Registry.getDataFields(A.class), "data fields");
		assertListsEqualButOrder(complexFieldsOfA, Registry.getComplexFields(A.class), "collection and map fields");
		assertListsEqualButOrder(referenceFieldsOfX, Registry.getReferenceFields(X.class), "reference fields");
		assertListsEqualButOrder(accumulationFieldsOfA, Registry.getAccumulationFields(A.class), "accumulation fields");
		assertListsEqualButOrder(dataAndReferenceFieldsOfA, Registry.getDataAndReferenceFields(A.class), "data and reference fields");
		assertListsEqualButOrder(registeredFieldsOfA, Registry.getRegisteredFields(A.class), "registered fields");
		assertListsEqualButOrder(allFieldsReferencingA, Registry.getAllReferencingFields(A.class), "referencing fields");

		assertTrue(Registry.isDataField(A.class.getDeclaredField("l")));
		assertTrue(Registry.isReferenceField(X.class.getDeclaredField("a")));
		assertTrue(Registry.isComplexField(A.class.getDeclaredField("mapOfMaps")));
		assertEquals(A.class.getDeclaredField("xs"), Registry.getAccumulationFieldForReferenceField(X.class.getDeclaredField("a")));

		assertEquals(A.class, Registry.getDeclaringDomainClass(Registry.getFieldByName(A.class, "i")));
		assertListsEqualButOrder(relevantFieldsOfA, Registry.getRelevantFields(A.class), "relevant fields");

		log.info("\tCircular references...");

		Set<List<String>> circularReferences = Registry.determineCircularReferences();
		assertEquals(3, circularReferences.size(), "circular references");
		assertEquals(2, circularReferences.stream().filter(cr -> cr.size() == 1).count());
		assertEquals(1, circularReferences.stream().filter(cr -> cr.size() == 3).count());

		log.info("\t" + circularReferences);
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(2)
	void domainClassDatabaseTableAssociation() throws Exception {

		log.info("\tTEST 2: domainClassDatabaseTableAssociation()");

		log.info("\tRegister domain classes and associate with database tables...");

		assertDoesNotThrow(() -> SqlDomainController.registerDomainClasses(A.class.getPackage().getName()));

		Properties dbProps = Prop.readEnvironmentSpecificProperties(Prop.findPropertiesFile("db.properties"), "local/mysql/junit", CList.newList("dbConnectionString", "dbUser"));

		assertEquals("jdbc:mysql://localhost/junit?useSSL=false", Prop.getStringProperty(dbProps, "dbConnectionString", ""));

		Properties domainProps = Prop.readProperties(Prop.findPropertiesFile("domain.properties"));

		assertNotNull(Prop.getStringProperty(domainProps, "dataHorizonPeriod", null));

		assertDoesNotThrow(() -> SqlDomainController.associateDomainClassesAndDatabaseTables(dbProps, domainProps));

		log.info("\tAssertions on class/table association...");

		SqlDbTable tableA = SqlDomainController.sqlDb.findRegisteredTable("DOM_A");
		SqlDbTable tableAA = SqlDomainController.sqlDb.findRegisteredTable("DOM_AA");
		SqlDbTable tableX = SqlDomainController.sqlDb.findRegisteredTable("DOM_X");
		SqlDbTable tableZ = SqlDomainController.sqlDb.findRegisteredTable("DOM_Z");

		assertEquals("DOM_A", SqlRegistry.getTableFor(A.class).name, "Associated table for class A");
		assertEquals("DOM_AA", SqlRegistry.getTableFor(AA.class).name, "Associated table for class AA (name specified in SqlTable annotation");
		assertEquals("BYTES", SqlRegistry.getColumnFor(A.class.getDeclaredField("bytes")).name, "Associated column for data field");
		assertEquals("BOOLEAN", SqlRegistry.getColumnFor(A.class.getDeclaredField("bool")).name, "Associated column for data field (name specified in SqlColumn annotation");
		assertEquals("O_ID", SqlRegistry.getColumnFor(A.class.getDeclaredField("o")).name, "Associated column for reference field");
		assertEquals("DOM_A_STRINGS", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("strings")).name, "Associated entry table for list");
		assertEquals("DOM_A_DOUBLE_SET", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("doubleSet")).name, "Associated entry table for set");
		assertEquals("DOM_A_BIG_DECIMAL_MAP", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("bigDecimalMap")).name, "Associated entry table map");
		assertEquals("DOM_A_LIST_OF_LISTS", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("listOfLists")).name, "Associated entry table for list of lists");
		assertEquals("DOM_A_LIST_OF_MAPS", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("listOfMaps")).name, "Associated entry table for list of maps");
		assertEquals("DOM_A_MAP_OF_LISTS", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("mapOfLists")).name, "Associated entry table for map of lists");
		assertEquals("DOM_A_MAP_OF_MAPS", SqlRegistry.getEntryTableFor(A.class.getDeclaredField("mapOfMaps")).name, "Associated entry table for map of maps");

		assertEquals("doubleValue", SqlRegistry.getFieldFor(SqlRegistry.getColumnFor(A.class.getDeclaredField("doubleValue"))).getName(), "Associated field for column");

		Set<UniqueConstraint> ucs = tableA.findUniqueConstraintsByColumnName("S");
		assertEquals(1, ucs.size(), "find unique constraints by columm name");
		UniqueConstraint uc = ucs.iterator().next();
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

	@SuppressWarnings("static-method")
	@Test
	@Order(3)
	void java2sql() throws Exception {

		log.info("\tTEST 3: java2sql()");

		log.info("\tJava2Sql...");

		Java2Sql.main(new String[] { "com.icx.dom.junit.domain" });

		log.info("\tError cases...");

		assertNull(SqlRegistry.getTableFor(DomainObject.class));
		assertNull(SqlRegistry.getColumnFor(A.class.getDeclaredField("strings")));
		assertNull(SqlRegistry.getEntryTableFor(A.class.getDeclaredField("l")));
		assertNull(SqlRegistry.getMainTableRefIdColumnFor(A.class.getDeclaredField("i")));
		assertNull(SqlRegistry.getRequiredJdbcTypeFor(null));
		Column domainClassColumn = SqlRegistry.getTableFor(A.class).columns.stream().filter(c -> SqlDomainObject.DOMAIN_CLASS_COL.equals(c.name)).findAny().orElse(null);
		assertNull(SqlRegistry.getFieldFor(domainClassColumn));
	}
}
