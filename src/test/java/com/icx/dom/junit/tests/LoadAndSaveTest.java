package com.icx.dom.junit.tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.common.CFile;
import com.icx.dom.common.CList;
import com.icx.dom.common.CMap;
import com.icx.dom.common.CResource;
import com.icx.dom.common.CSet;
import com.icx.dom.common.Common;
import com.icx.dom.common.Prop;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.sql.FieldError;
import com.icx.dom.domain.sql.SqlDomainController;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDb.DbType;
import com.icx.dom.junit.TestHelpers;
import com.icx.dom.junit.domain.A;
import com.icx.dom.junit.domain.A.Type;
import com.icx.dom.junit.domain.AA;
import com.icx.dom.junit.domain.B;
import com.icx.dom.junit.domain.C;
import com.icx.dom.junit.domain.O;
import com.icx.dom.junit.domain.sub.X;
import com.icx.dom.junit.domain.sub.Y;
import com.icx.dom.junit.domain.sub.Z;

@TestMethodOrder(OrderAnnotation.class)
class LoadAndSaveTest extends TestHelpers {

	static final Logger log = LoggerFactory.getLogger(LoadAndSaveTest.class);

	static DbType dbType = null;

	private static String getLocal(DbType dbType) {

		if (dbType == DbType.MYSQL) {
			return "local/mysql/junit";
		}
		else if (dbType == DbType.MS_SQL) {
			return "local/ms_sql/junit";
		}
		else if (dbType == DbType.ORACLE) {
			return "local/oracle/junit";
		}
		else {
			return null;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(1)
	void initialize() throws Throwable {

		log.info("\tTEST 1: initialize()");

		log.info("\tRegister only domain class AA...");

		try {
			SqlDomainController.registerDomainClasses(AA.class); // For coverage

			assertEquals(A.class, DomainController.getDomainClassByName("A"));

			log.info("\tRegister all domain classes in package...");

			SqlDomainController.registerDomainClasses(A.class.getPackage().getName());

			log.info("\tEstablish logical database connection...");

			dbType = DbType.MYSQL;

			Properties dbProps = Prop.readEnvironmentSpecificProperties(Prop.findPropertiesFile("db.properties"), getLocal(dbType), CList.newList("dbConnectionString", "dbUser"));
			Properties domainProps = Prop.readProperties(Prop.findPropertiesFile("domain.properties"));

			log.info("\tInitialize Java <-> SQL association...");

			SqlDomainController.associateDomainClassesAndDatabaseTables(dbProps, domainProps);
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	static void cleanup() throws Exception {

		log.info("\tcleanup()");

		SqlDomainController.synchronize();

		for (Z z : DomainController.all(Z.class)) {
			z.delete();
		}
		for (Y y : DomainController.all(Y.class)) {
			y.delete();
		}
		for (X x : DomainController.all(X.class)) {
			x.delete();
		}
		for (X.InProgress xInProgress : DomainController.all(X.InProgress.class)) {
			xInProgress.delete();
		}
		for (AA aa : DomainController.all(AA.class)) {
			aa.delete();
		}
		for (B b : DomainController.all(B.class)) {
			b.delete();
		}
		for (O o : DomainController.all(O.class)) {
			o.delete();
		}
		for (C c : DomainController.all(C.class)) {
			c.delete();
		}

		try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, true)) { // Data horizon controlled objects which were not be loaded
			SqlDb.deleteFrom(sqlcn.cn, "DOM_Z", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_Y", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_X", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_B", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_AA", null);
		}
	}

	//
	// Next 3 test belong together - no cleanup meanwhile
	//

	@SuppressWarnings("static-method")
	@Test
	@Order(2)
	void saveAndReload() throws Throwable {

		LocalDateTime now = LocalDateTime.now();

		try {
			cleanup();

			log.info("\tTEST 2: saveAndReload()");

			log.info("\tCreate and save objects...");

			O o1 = SqlDomainController.createAndSave(O.class, null);

			AA aa1 = SqlDomainController.createAndSave(AA.class, aa -> {

				aa.bool = true;
				aa.booleanValue = false;
				aa.i = Integer.MIN_VALUE;
				aa.integerValue = Integer.MAX_VALUE;
				aa.l = Long.MIN_VALUE;
				aa.longValue = Long.MAX_VALUE;
				aa.d = 123456789.123456789; // Double.MIN_VALUE: underflow with Oracle
				aa.doubleValue = -0.0000123456789; // Double.MAX_VALUE: underflow with Oracle
				aa.bigIntegerValue = BigInteger.valueOf(Long.MAX_VALUE);
				aa.bigDecimalValue = BigDecimal.valueOf(123456789.123456789); // Double.MAX_VALUE: underflow with Oracle
				aa.datetime = now;

				aa.s = "S";
				aa.bytes = Common.getBytesUTF8("ÄÖÜäöüß");
				assertDoesNotThrow(() -> aa.picture = CFile.readBinary(new File("src/test/resources/bike.jpg")));

				aa.strings = CList.newList("A", "B", "C", "D", (dbType == DbType.ORACLE ? null : ""), null); // Oracle does not allow empty string values (stored as NULL instead)
				aa.doubleSet = CSet.newSet(0.0, 0.1, 0.2, null);
				aa.bigDecimalMap = CMap.newMap("a", BigDecimal.valueOf(1L), "b", BigDecimal.valueOf(2L), "c", BigDecimal.valueOf(3.1), "d", null, null, BigDecimal.valueOf(0L));

				aa.listOfLists = CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, null));
				aa.listOfMaps = CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", null));
				aa.mapOfLists = CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", null));
				aa.mapOfMaps = CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false, Type.C, null));
			});
			aa1.file = CResource.findFirstJavaResourceFile("x.txt");
			aa1.o = o1;
			aa1.save();

			log.info("\tAssertions on saved objects...");

			assertTrue(DomainController.hasAny(A.class));
			assertTrue(DomainController.hasAny(A.class, a -> a.type == Type.A));
			assertFalse(DomainController.hasAny(A.class, a -> a.type == Type.B));
			assertNotNull(DomainController.findAny(A.class, a -> true));
			assertEquals(1, DomainController.count(A.class, a -> a.type == Type.A));

			log.info("\tReload object to check if no changes will be detected...");

			assertFalse(aa1.reload());

			log.info("\tUnregister objects to force reload...");

			aa1.unregisterForTest();
			o1.unregisterForTest();

			log.info("\tAssertions on unregistered objects...");

			assertFalse(SqlDomainController.hasAny(AA.class));
			assertFalse(SqlDomainController.hasAny(O.class));

			log.info("\tLoad objects from database...");

			SqlDomainController.synchronize();

			log.info("\tAssertions on loaded objects...");

			assertNotSame(aa1, DomainController.findAny(AA.class, aa -> true));
			assertNotSame(o1, DomainController.findAny(O.class, o -> true));

			aa1 = DomainController.findAny(AA.class, aa -> true);
			o1 = DomainController.findAny(O.class, o -> true);

			// Check if loaded object equals object created before

			assertEquals(true, aa1.bool);
			assertEquals(false, aa1.booleanValue);
			assertEquals(Integer.MIN_VALUE, aa1.i);
			assertEquals(Integer.MIN_VALUE, aa1.i);
			assertEquals(Integer.MAX_VALUE, aa1.integerValue);
			assertEquals(Long.MIN_VALUE, aa1.l);
			assertEquals(Long.MAX_VALUE, aa1.longValue);
			assertEquals(123456789.123456789, aa1.d);
			assertEquals(-0.0000123456789, aa1.doubleValue);
			assertEquals(BigInteger.valueOf(Long.MAX_VALUE), aa1.bigIntegerValue);
			assertEquals(BigDecimal.valueOf(123456789.123456789), aa1.bigDecimalValue);
			assertEquals("S", aa1.s);
			assertTrue(logicallyEqual(now, aa1.datetime)); // Check only seconds because milliseconds will not be stored in database (Oracle)
			assertArrayEquals(Common.getBytesUTF8("ÄÖÜäöüß"), aa1.bytes);
			assertArrayEquals(CFile.readBinary(new File("src/test/resources/bike.jpg")), aa1.picture);
			assertEquals(CResource.findFirstJavaResourceFile("x.txt"), aa1.file);
			assertEquals(Type.A, aa1.type);

			assertEquals(CList.newList("A", "B", "C", "D", (dbType == DbType.ORACLE ? null : ""), null), aa1.strings); // Oracle does not allow empty string values (stored as NULL instead)
			assertEquals(CSet.newSet(0.0, 0.1, 0.2, null), aa1.doubleSet);
			assertEquals(CMap.newMap("a", BigDecimal.valueOf(1L), "b", BigDecimal.valueOf(2L), "c", BigDecimal.valueOf(3.1), "d", null, null, BigDecimal.valueOf(0L)), aa1.bigDecimalMap);

			assertEquals(CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, null)), aa1.listOfLists);
			assertEquals(CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", null)), aa1.listOfMaps);
			assertEquals(CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", null)), aa1.mapOfLists);
			assertEquals(CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false, Type.C, null)), aa1.mapOfMaps);

			assertEquals(o1, aa1.o);
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(3)
	void changeInDatabase() throws Throwable {

		log.info("\tTEST 3: changeInDatabase()");

		log.info("\tAssertions on loaded objects...");

		try {
			AA aa1 = DomainController.findAny(AA.class, aa -> true);

			aa1.strings.remove("A");
			aa1.strings.add("E");

			aa1.doubleSet.remove(0.0);
			aa1.doubleSet.remove(null);
			aa1.doubleSet.add(0.3);

			aa1.bigDecimalMap.remove("a");
			aa1.bigDecimalMap.remove(null);
			aa1.bigDecimalMap.put("e", BigDecimal.valueOf(5L));

			aa1.save();

			log.info("\tCreate and save second object...");

			O o2 = SqlDomainController.createAndSave(O.class, null);

			assertEquals(2, DomainController.count(O.class, o -> true));

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {

				log.info("\tDelete second object...");

				o2.delete(sqlcn.cn);

				log.info("\tManipulate database externally...");

				assertEquals(1, DomainController.count(O.class, o -> true));

				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A", CMap.newMap("I", 2, "BYTES", Common.getBytesUTF8("äöüßÄÖÜ"), "DOM_FILE", "y.txt", "DOM_TYPE", "B", "O_ID", null), "S='S'");

				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT_ORDER", 1), "ELEMENT='B'");
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT_ORDER", 0), "ELEMENT='C'");
				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_STRINGS", "ELEMENT='D'");

				if (dbType == DbType.ORACLE) {
					SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT", "E"), "ELEMENT IS NULL");
				}
				else {
					SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT", "E"), "ELEMENT=''");
				}
				SqlDomainController.sqlDb.insertInto(sqlcn.cn, "DOM_A_STRINGS", CMap.newSortedMap("A_ID", aa1.getId(), "ELEMENT", "G", "ELEMENT_ORDER", 6));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_DOUBLE_SET", "ELEMENT IS NULL");
				SqlDomainController.sqlDb.insertInto(sqlcn.cn, "DOM_A_DOUBLE_SET", CMap.newSortedMap("A_ID", aa1.getId(), "ELEMENT", 0.4));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", "ENTRY_KEY='d'");
				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", "ENTRY_KEY IS NULL");
				SqlDomainController.sqlDb.insertInto(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", CMap.newSortedMap("A_ID", aa1.getId(), "ENTRY_KEY", "f", "ENTRY_VALUE", 6L));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_LIST_OF_LISTS", "ELEMENT IS NULL");
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_LIST_OF_LISTS", CMap.newMap("ELEMENT", "A,B,C"), "ELEMENT_ORDER=3");

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_LIST_OF_MAPS", "ELEMENT IS NULL");
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_LIST_OF_MAPS", CMap.newMap("ELEMENT", "A=2;B=1;C=3"), "ELEMENT_ORDER=3");

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_MAP_OF_LISTS", "ENTRY_KEY=0");
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_MAP_OF_LISTS", CMap.newMap("ENTRY_VALUE", "A,B,C"), "ENTRY_KEY=3");

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_MAP_OF_MAPS", "ENTRY_KEY=0");
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_A_MAP_OF_MAPS", CMap.newMap("ENTRY_VALUE", "A=true;B=false"), "ENTRY_KEY=2");

				sqlcn.cn.commit();
			}

			log.info("\tLoad changed object again from database...");

			aa1.reload();

			log.info("\tCheck if database changes are reflected by objects...");

			assertEquals(2, aa1.i);
			assertEquals(Type.B, aa1.type);
			assertArrayEquals(Common.getBytesUTF8("äöüßÄÖÜ"), aa1.bytes);
			assertEquals(new File("y.txt"), aa1.file);
			assertEquals(null, aa1.o);

			if (dbType == DbType.ORACLE) {
				assertEquals(CList.newList("C", "B", "E", "E", "E", "G"), aa1.strings);
			}
			else {
				assertEquals(CList.newList("C", "B", "E", null, "E", "G"), aa1.strings);
			}
			assertEquals(CSet.newSet(0.1, 0.2, 0.3, 0.4), aa1.doubleSet);
			assertEquals(CMap.newMap("b", BigDecimal.valueOf(2L), "c", BigDecimal.valueOf(3.1), "e", BigDecimal.valueOf(5L), "f", BigDecimal.valueOf(6L)), aa1.bigDecimalMap);

			assertEquals(CList.newList(CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, Type.C)), aa1.listOfLists);
			assertEquals(CList.newList(CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 2, "B", 1, "C", 3)), aa1.listOfMaps);
			assertEquals(CMap.newMap(1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", "C")), aa1.mapOfLists);
			assertEquals(CMap.newMap("1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false)), aa1.mapOfMaps);
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(4)
	void changeLocallyAndSave() throws Throwable {

		log.info("\tTEST 4: changeLocallyAndSave()");

		log.info("\tChange objects locally and save again...");

		try {
			O o1 = DomainController.findAny(O.class, o -> true);
			AA aa1 = DomainController.findAny(AA.class, aa -> true);

			aa1.i = 1;
			aa1.integerValue = -1;
			aa1.l = 1L;
			aa1.longValue = -1L;
			aa1.d = 0.1;
			aa1.doubleValue = -0.1;
			aa1.bigIntegerValue = BigInteger.valueOf(-1L);
			aa1.bigDecimalValue = BigDecimal.valueOf(-0.1);
			aa1.s = "T";
			aa1.bytes = Common.getBytesUTF8("éèê");
			aa1.file = CResource.findFirstJavaResourceFile("z.txt");
			aa1.type = Type.B;

			aa1.strings = CList.newList("C", "B", "A");
			aa1.doubleSet = CSet.newSet(0.0, -0.1, -0.2);
			aa1.bigDecimalMap = CMap.newMap("a", BigDecimal.valueOf(-1L), "b", BigDecimal.valueOf(-2L), "c", BigDecimal.valueOf(-3L));

			aa1.listOfLists = CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, Type.C));
			aa1.listOfMaps = CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", 3));
			aa1.mapOfLists = CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", "C"));
			aa1.mapOfMaps = CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false));

			aa1.o = o1;

			aa1.save();

			log.info("\tUnregister changed objects to force reload...");

			aa1.unregisterForTest();
			o1.unregisterForTest();

			log.info("\tLoad objects from database...");

			SqlDomainController.synchronize();

			aa1 = DomainController.findAny(AA.class, aa -> true);
			o1 = DomainController.findAny(O.class, o -> true);

			log.info("\tCheck if reloaded object equals object changed before...");

			assertEquals(1, aa1.i);
			assertEquals(-1, aa1.integerValue);
			assertEquals(1L, aa1.l);
			assertEquals(-1L, aa1.longValue);
			assertEquals(0.1, aa1.d);
			assertEquals(-0.1, aa1.doubleValue);
			assertEquals(BigInteger.valueOf(-1L), aa1.bigIntegerValue);
			assertEquals(BigDecimal.valueOf(-0.1), aa1.bigDecimalValue);
			assertEquals("T", aa1.s);
			assertArrayEquals(Common.getBytesUTF8("éèê"), aa1.bytes);
			assertEquals(CResource.findFirstJavaResourceFile("z.txt"), aa1.file);
			assertEquals(Type.B, aa1.type);

			assertEquals(CList.newList("C", "B", "A"), aa1.strings);
			assertEquals(CSet.newSet(0.0, -0.1, -0.2), aa1.doubleSet);
			assertEquals(CMap.newMap("a", BigDecimal.valueOf(-1L), "b", BigDecimal.valueOf(-2L), "c", BigDecimal.valueOf(-3L)), aa1.bigDecimalMap);

			assertEquals(CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, Type.C)), aa1.listOfLists);
			assertEquals(CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", 3)), aa1.listOfMaps);
			assertEquals(CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", "C")), aa1.mapOfLists);
			assertEquals(CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false)), aa1.mapOfMaps);

			assertEquals(o1, aa1.o);
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	//
	// Independent tests - cleanup object store and database before
	//

	@SuppressWarnings("static-method")
	@Test
	@Order(5)
	void dataHorizon() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 5: dataHorizon()");

			AA aa1 = null;
			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, true)) {

				log.info("\tCreate and save data horizon controlled objects...");

				aa1 = SqlDomainController.createAndSave(sqlcn.cn, AA.class, null); // Under data horizon control

				Set<X> xs = new HashSet<>(); // Children of AA
				for (int i = 0; i < 10; i++) {
					X x = DomainController.create(X.class, null);
					x.s = String.valueOf(i + 1);
					x.a = aa1;
					x.save(sqlcn.cn);
					xs.add(x);
				}

				log.info("\tAssertions on created objects...");

				assertEquals(xs, aa1.xs); // Accumulation
				assertEquals(10, aa1.xs.size()); // Accumulation after unregistering objects deleted in database

				log.info("\tDelete some of the recently created objects in database externally...");

				assertEquals(5, SqlDb.deleteFrom(sqlcn.cn, "DOM_X", "S='6' OR S='7' OR S='8' OR S='9' OR S='10'"));
			}

			log.info("\tSynchronize with database to recognize and unregister externally deleted objects...");

			SqlDomainController.synchronize(); // Determine and unregister X objects meanwhile deleted in database

			log.info("\tAssertions on unregistered objects...");

			assertEquals(5, DomainController.count(X.class, x -> true));
			assertEquals(5, aa1.xs.size()); // Accumulation after unregistering objects deleted in database

			X x1 = DomainController.findAny(X.class, x -> "1".equals(x.s));
			X x2 = DomainController.findAny(X.class, x -> "2".equals(x.s));
			X x3 = DomainController.findAny(X.class, x -> "3".equals(x.s));

			log.info("\tCreate and save a child of a data horizon controlled object which itself is not under data horizon control...");

			B b1 = SqlDomainController.create(B.class, null); // Child of A which is not under data horizon control
			b1.aa = aa1;
			b1.save();

			log.info("\tWait until data horizon data horizon period is over...");

			Thread.sleep(2000); // 'dataHorizonPeriod' in domain.properties must be 1 second or lower

			log.info("\tSynchronize again with database to unregister objects out of data horizon...");

			SqlDomainController.synchronize(); // Unregister x's and aa due to out-of-data-horizon condition (remove from heap)

			log.info("\tAssertions on unregistration...");

			assertEquals(0, DomainController.count(X.class, x -> true));
			assertEquals(0, aa1.xs.size()); // Accumulation after unregistering objects (due to out-of-data horizon condition)
			assertEquals(1, DomainController.count(A.class, a -> true)); // Referenced by b1

			log.info("\tCreate and save objects which reference existing but out-of-data-horizon objects...");

			SqlDomainController.createAndSave(Z.class, z -> z.x = x1); // Re-register referenced out-of-data-horizon objects which were unregistered during synchronization
			SqlDomainController.createAndSave(Z.class, z -> z.x = x2);
			SqlDomainController.createAndSave(Z.class, z -> z.x = x3);

			log.info("\tSynchronize again with database to force (re)loading referenced but unregistered objects...");

			SqlDomainController.synchronize();

			log.info("\tAssert reloading of referneced objects...");

			assertEquals(3, DomainController.count(X.class, x -> true)); // Re-registered X object which are referenced by Z objects
			assertEquals(3, aa1.xs.size()); // Accumulation after reloading out-of-data-horizon objects

			assertEquals(aa1, DomainController.findAny(AA.class, aa -> true)); // A object which were not unregistered on synchronization because it was referenced by B object
			assertEquals(1, DomainController.count(A.class, a -> true));
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(6)
	void saveReferencedObjects() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 6: saveReferencedObjects()");

			log.info("\tCreate objects with - also circular - references and save one object to force saving all others...");

			AA aa1 = DomainController.create(AA.class, aa -> aa.s = "aa1");
			Z z1 = DomainController.create(Z.class, null);
			Y y1 = DomainController.create(Y.class, y -> y.z = z1);
			y1.y = y1;
			X x1 = DomainController.create(X.class, x -> {
				x.a = aa1;
				x.y = y1;
			});
			z1.x = x1;

			x1.save();

			log.info("\tAssert saving...");

			assertTrue(aa1.isStored());
			assertTrue(y1.isStored());
			assertTrue(z1.isStored());
			assertTrue(x1.isStored());

			AA aa2 = SqlDomainController.createAndSave(AA.class, aa -> {
				aa.s = "aa2";
				aa.integerValue = 2;
			});
			X x2 = SqlDomainController.createAndSave(X.class, null);
			Z z2 = SqlDomainController.createAndSave(Z.class, null);
			Y y2 = SqlDomainController.create(Y.class, null);
			y2.z = z2;
			y2.save();

			log.info("\tChange objects locally without saving...");

			y1.z = z2;
			y1.y = y2;
			x1.a = aa2;
			x1.y = null;
			z1.x = x2;

			log.info("\tChange values in database externally...");

			AA aa3 = SqlDomainController.createAndSave(AA.class, aa -> {
				aa.s = "aa3";
				aa.integerValue = 3;
			});
			X x3 = SqlDomainController.createAndSave(X.class, null);
			Z z3 = SqlDomainController.createAndSave(Z.class, null);
			Y y3 = SqlDomainController.create(Y.class, null);
			y3.z = z3;
			y3.save();

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_Y", CMap.newMap("Z_ID", z3.getId()), "ID=" + y1.getId());
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_Y", CMap.newMap("Y_ID", y3.getId()), "ID=" + y1.getId());
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_X", CMap.newMap("A_ID", aa3.getId()), "ID=" + x1.getId());
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_X", CMap.newMap("Y_ID", y3.getId()), "ID=" + x1.getId());
				SqlDomainController.sqlDb.update(sqlcn.cn, "DOM_Z", CMap.newMap("X_ID", x3.getId()), "ID=" + z1.getId());
			}

			log.info("\tSynchronize again with database to recognize unsaved changes...");

			SqlDomainController.synchronize();

			log.info("\tAssert overriding unsaved local changes by reloading objects from database...");

			assertEquals(aa3, x1.a);
			assertEquals(y3, x1.y);
			assertEquals(y3, y1.y);
			assertEquals(z3, y1.z);
			assertEquals(x3, z1.x);
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(7)
	void loadReferencedObjects() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 7: loadReferencedObjects()");

			log.info("\tCreate and save objects with parent child relation...");

			O o1 = SqlDomainController.createAndSave(O.class, null);
			AA aa1 = SqlDomainController.createAndSave(AA.class, aa -> aa.o = o1);
			X x1a = SqlDomainController.createAndSave(X.class, x -> {
				x.a = aa1;
				x.s = "available";
			});
			X x1b = SqlDomainController.createAndSave(X.class, x -> {
				x.a = aa1;
				x.s = "available";
			});

			log.info("\tUnregister all objects to force reload of these objects...");

			x1a.unregisterForTest();
			x1b.unregisterForTest();
			aa1.unregisterForTest();
			o1.unregisterForTest();

			assertFalse(DomainController.hasAny(X.class));
			assertFalse(DomainController.hasAny(AA.class));
			assertFalse(DomainController.hasAny(O.class));

			log.info("\tLoad objects of one domain class and force loading referenced parent objects in separate load cycles...");

			Set<X> xs = SqlDomainController.loadOnly(X.class, null, -1);

			assertEquals(4, xs.size());
			assertEquals(2, DomainController.count(X.class, x -> true));
			assertTrue(DomainController.hasAny(AA.class));
			assertTrue(DomainController.hasAny(O.class));

		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(8)
	void allocateObjectsExclusively() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 8: allocateObjectsExclusively()");

			log.info("\tCreate and save objects...");

			X x1a = SqlDomainController.createAndSave(X.class, x -> { x.s = "available"; });
			X x1b = SqlDomainController.createAndSave(X.class, x -> { x.s = "available"; });

			log.info("\tLoad objects of one domain class for excusive use");

			Set<X> xs = SqlDomainController.allocateExclusively(X.class, X.InProgress.class, "S='available'", -1, x -> x.s = "in_use");
			assertEquals(2, xs.size());
			assertEquals(2, DomainController.count(X.class, x -> true));
			assertEquals(2, SqlDomainController.count(X.InProgress.class, s -> true));

			Iterator<X> it = xs.iterator();
			X xTmp = it.next();
			if (x1a.equals(xTmp)) {
				assertSame(x1b, it.next());
			}
			else {
				assertSame(x1b, xTmp);
				assertSame(x1a, it.next());
			}

			log.info("\tTry to load another object excusively (but no object exists)...");

			xs = SqlDomainController.allocateExclusively(X.class, X.InProgress.class, "s='available'", -1, null);
			assertTrue(xs.isEmpty());

			xs = SqlDomainController.allocateExclusively(X.class, X.InProgress.class, null, -1, null);
			assertTrue(xs.isEmpty());

			log.info("\tRelease one exclusively used object...");

			assertTrue(x1b.release(X.class, X.InProgress.class, x -> x.s = "available"));
			assertEquals(1, SqlDomainController.count(X.InProgress.class, s -> true));

			log.info("\tTry to allocate another object excusively (now one object exists)...");

			Set<X> xs2 = SqlDomainController.allocateExclusively(X.class, X.InProgress.class, null, 1, x -> x.s = "in_use");
			assertEquals(1, xs2.size());
			assertEquals(2, SqlDomainController.count(X.InProgress.class, s -> true));

			x1a.release(X.class, X.InProgress.class, x -> x.s = "available");
			x1b.release(X.class, X.InProgress.class, x -> x.s = "available");
			assertEquals(0, SqlDomainController.count(X.InProgress.class, s -> true));

			assertTrue(x1a.allocateExclusively(X.class, X.InProgress.class, x -> x.s = "in_use"));
			assertEquals(1, SqlDomainController.count(X.InProgress.class, s -> true));
			x1a.release(X.class, X.InProgress.class, x -> x.s = "available");
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(9)
	void delete() throws Throwable {

		log.info("\tTEST 9: delete()");

		log.info("\tDelete objects with circular references...");

		try {
			C c1 = SqlDomainController.create(C.class, null);
			c1.c = c1;
			c1.save();

			assertDoesNotThrow(() -> c1.delete());

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {
				assertEquals(0, SqlDomainController.sqlDb.selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			C c1a = SqlDomainController.createAndSave(C.class, null);
			C c2 = SqlDomainController.create(C.class, c -> c.c = c1a);
			c2.save();
			c1a.c = c2;
			c1a.save();

			assertDoesNotThrow(() -> c2.delete());

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {
				assertEquals(0, SqlDomainController.sqlDb.selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			C c1b = SqlDomainController.create(C.class, null);
			C c2a = SqlDomainController.create(C.class, c -> c.c = c1b);
			C c3 = SqlDomainController.create(C.class, c -> c.c = c2a);
			c1b.c = c3;
			c3.save();

			assertDoesNotThrow(() -> c1b.delete());

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {
				assertEquals(0, SqlDomainController.sqlDb.selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			log.info("\tError case exception on deletion...");

			C ce1 = SqlDomainController.create(C.class, null);
			C ce2 = SqlDomainController.create(C.class, c -> c.c = ce1);
			ce1.c = ce2;
			ce1.save();

			ce2.unregisterForTest();

			assertThrows(SQLException.class, () -> ce1.delete());

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {
				assertEquals(2, SqlDomainController.sqlDb.selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			ce2.reregisterForTest();

			assertEquals(2, SqlDomainController.count(C.class, c -> true));
			assertDoesNotThrow(() -> ce1.delete());

			try (SqlConnection sqlcn = SqlConnection.open(SqlDomainController.sqlDb.pool, false)) {
				assertEquals(0, SqlDomainController.sqlDb.selectCountFrom(sqlcn.cn, "DOM_C", null));
			}
			assertEquals(0, SqlDomainController.count(C.class, c -> true));
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(10)
	void sortAndGroup() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 10: sortAndGroup()");

			log.info("\tCheck default object order (id containes creation time)...");

			X b1 = new X();
			b1.register();
			Thread.sleep(1);
			X b2 = new X();
			b2.register();
			assertTrue(b2.compareTo(b1) > 0);

			log.info("\tCheck object order on overridden compareTo()...");

			O o1 = DomainController.create(O.class, null);

			AA aaa = new AA(o1, "A");
			aaa.bool = false;
			aaa.register();
			AA aab = new AA(o1, "B");
			aab.bool = true;
			aab.register();
			AA aac = new AA(o1, "C");
			aac.bool = false;
			aac.register();
			AA aad = new AA(o1, "D");
			aad.bool = true;
			aad.register();

			assertEquals(CList.newList(aaa, aab, aac, aad), DomainController.sort(CList.newList(aad, aab, aac, aaa)));

			assertEquals(CMap.newMap("A", CSet.newSet(aaa), "B", CSet.newSet(aab), "C", CSet.newSet(aac), "D", CSet.newSet(aad)), DomainController.groupBy(o1.as, a -> a.s));
			assertEquals(CMap.newMap(false, CSet.newSet(aaa, aac), true, CSet.newSet(aab, aad)), DomainController.groupBy(o1.as, a -> a.bool));
			assertEquals(2, DomainController.countBy(o1.as, a -> a.bool).get(true));

			aaa.delete();
			aab.delete();
			aac.delete();
			aad.delete();
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(11)
	void errorCases() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 11: errorCases()");

			AA aa1 = DomainController.create(AA.class, a -> a.s = "aa1");

			log.info("\tGetting and setting inexistent fields...");

			assertNull(aa1.getFieldValue(X.class.getDeclaredField("s")), "get field value error");
			assertDoesNotThrow(() -> aa1.setFieldValue(X.class.getDeclaredField("s"), "a"), "get known field value error");

			log.info("\tNOT NULL constraint violation, warnings and errors...");

			aa1.type = null;

			assertTrue(FieldError.hasConstraintViolations(aa1));
			assertTrue(aa1.hasErrorsOrWarnings());
			assertEquals(1, aa1.getErrorsAndWarnings().size());
			assertFalse(aa1.getErrorOrWarning(aa1.getInvalidFields().iterator().next()).toString().isEmpty());

			assertThrows(SQLException.class, () -> aa1.save());

			aa1.type = Type.A;

			assertFalse(FieldError.hasConstraintViolations(aa1));

			log.info("\tUNIQUE constraint violation...");

			aa1.i = 0;
			aa1.integerValue = 1;
			AA aa2 = DomainController.create(AA.class, a -> a.s = "aa2");
			aa2.i = 0;
			aa2.integerValue = 1;

			assertTrue(FieldError.hasConstraintViolations(aa1));
			assertTrue(FieldError.hasConstraintViolations(aa2));

			aa1.save();
			assertThrows(SQLException.class, () -> aa2.save());

			aa2.integerValue = 2;
			aa2.s = "s";

			assertFalse(FieldError.hasConstraintViolations(aa1));
			assertFalse(FieldError.hasConstraintViolations(aa2));

			aa2.save();

			assertFalse(aa1.hasErrorsOrWarnings());
			assertEquals(2, SqlDomainController.allValid(AA.class).size());

			log.info("\tcolumn size violation...");

			aa1.s = "abcdefghijklmnopqrstuvwxyz"; // Column size

			assertTrue(FieldError.hasConstraintViolations(aa1));

			aa1.save(); // Save with truncated value

			assertTrue(aa1.hasErrorsOrWarnings());
			assertTrue(aa1.isValid());

			aa1.type = null; // Analytical update

			aa1.save(); // Save all but invalid fields
			assertFalse(aa1.isValid());
			assertEquals(2, aa1.getErrorsAndWarnings().size());

			cleanup();
		}
		catch (AssertionFailedError failed) {
			throw failed;
		}
		catch (Throwable ex) {
			log.error(Common.exceptionStackToString(ex));
			throw ex;
		}
	}
}
