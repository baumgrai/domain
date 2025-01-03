package com.icx.dom.junit.tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
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

import com.icx.common.CFile;
import com.icx.common.CList;
import com.icx.common.CMap;
import com.icx.common.CProp;
import com.icx.common.CResource;
import com.icx.common.CSet;
import com.icx.common.Common;
import com.icx.dom.junit.TestHelpers;
import com.icx.dom.junit.domain.A;
import com.icx.dom.junit.domain.A.Stucture;
import com.icx.dom.junit.domain.A.Type;
import com.icx.dom.junit.domain.AA;
import com.icx.dom.junit.domain.AB;
import com.icx.dom.junit.domain.B;
import com.icx.dom.junit.domain.C;
import com.icx.dom.junit.domain.O;
import com.icx.dom.junit.domain.sub.X;
import com.icx.dom.junit.domain.sub.Y;
import com.icx.dom.junit.domain.sub.Z;
import com.icx.domain.sql.ComplexFieldHelpers;
import com.icx.domain.sql.Helpers;
import com.icx.domain.sql.SqlDomainController;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.jdbc.ConnectionPool;
import com.icx.jdbc.SqlConnection;
import com.icx.jdbc.SqlDb;
import com.icx.jdbc.SqlDb.DbType;

/**
 * Tests were performed on a Lenovo X1/2019 laptop with Windows 11 using local databases: Oracle 11g (11.2), Microsoft SQL Sever 2017, MySQL 8.0.36, MariaDB 11.3 - and JDBC drivers:
 * ojdbc8:23.3.0.23.09, mssql-jdbc:12.6.0.jre8, mysql-connector-j:8.0.32, mariadb-java-client:3.3.3.
 */
@TestMethodOrder(OrderAnnotation.class)
class LoadAndSaveTest extends TestHelpers {

	static final Logger log = LoggerFactory.getLogger(LoadAndSaveTest.class);

	// +++ Please set log level to 'trace' in src/main/resources/logback.xml to achieve maximum coverage +++
	// +++ You may run tests multiple times with 'dbType' set to different database types +++
	// +++ Note: initialization of MS-SQL database sometimes takes very long time - about 1-2 minutes +++
	// +++ ---------------------------------------------------------
	public static DbType dbType = DbType.MYSQL;
	// +++ ---------------------------------------------------------

	static SqlDomainController sdc = new SqlDomainController();

	static void cleanup() throws Exception {

		log.info("\tcleanup()");

		sdc.synchronize();

		for (Z z : sdc.all(Z.class)) {
			sdc.delete(z);
		}
		for (Y y : sdc.all(Y.class)) {
			sdc.delete(y);
		}
		for (X x : sdc.all(X.class)) {
			sdc.delete(x);
		}
		for (X.InProgress xInProgress : sdc.all(X.InProgress.class)) {
			sdc.delete(xInProgress);
		}
		for (AA aa : sdc.all(AA.class)) {
			sdc.delete(aa);
		}
		for (B b : sdc.all(B.class)) {
			sdc.delete(b);
		}
		for (O o : sdc.all(O.class)) {
			sdc.delete(o);
		}
		for (C c : sdc.all(C.class)) {
			sdc.delete(c);
		}

		try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), true)) { // Data horizon controlled objects which were not be loaded
			SqlDb.deleteFrom(sqlcn.cn, "DOM_Z", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_Y", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_X", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_SEC_B", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_STRING_ARRAY", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_STRINGS", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_DOUBLE_SET", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_LIST_OF_LISTS", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_LIST_OF_MAPS", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_MAP_OF_LISTS", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A_MAP_OF_MAPS", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_A", null);
			SqlDb.deleteFrom(sqlcn.cn, "DOM_AA", null);
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(1)
	void initialize() throws Throwable {

		log.info("\tTEST 1: initialize()");

		try {
			log.info("\tRegister only domain class AA...");

			sdc.registerDomainClasses(SqlDomainObject.class, AA.class); // For coverage

			assertEquals(A.class, sdc.getDomainClassByName("A"));

			File dbPropsFile = CProp.findPropertiesFile("db.properties");
			String localConf = "local/" + dbType.toString().toLowerCase() + "/junit";
			Properties dbProps = CProp.readEnvironmentSpecificProperties(dbPropsFile, localConf, CList.newList(ConnectionPool.DB_CONNECTION_STRING_PROP, ConnectionPool.DB_USER_PROP));

			Properties domainProps = CProp.readProperties(CProp.findPropertiesFile(SqlDomainController.DOMAIN_PROPERIES_FILE));

			log.info("\tRegister all domain classes in package, establish logical database connection and associate Java <-> SQL...");

			sdc.initialize(dbProps, domainProps, A.class.getPackage().getName());
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
	// Next 3 test belong together - no cleanup meanwhile
	//

	private final static int CHAR_ARRAY_SIZE = 0x100000; // Maximum value which works for all database standard configurations (also MySQL with 'max_allowed_packet=4M')

	@SuppressWarnings("static-method")
	@Test
	@Order(2)
	void saveAndReload() throws Throwable {

		LocalDateTime now = LocalDateTime.now();

		try {
			cleanup();

			log.info("\tTEST 2: saveAndReload()");

			log.info("\tCreate and save objects...");

			O o1 = sdc.createAndSave(O.class, null);

			char[] longtext = new char[CHAR_ARRAY_SIZE];
			for (int c = 0; c < CHAR_ARRAY_SIZE; c++) {
				longtext[c] = (char) (c % 0x80);
			}

			File oriFile = CResource.findFirstJavaResourceFile("bike.jpg");

			AA aa1 = sdc.createAndSave(AA.class, aa -> {

				aa.bool = true;
				aa.booleanValue = false;

				aa.sh = Short.MIN_VALUE;
				aa.shortValue = Short.MAX_VALUE;
				aa.i = Integer.MIN_VALUE;
				aa.integerValue = Integer.MAX_VALUE;
				aa.l = Long.MIN_VALUE;
				aa.longValue = Long.MAX_VALUE;
				aa.d = 123456789.123456; // Double.MIN_VALUE: underflow with Oracle
				aa.doubleValue = -0.0000123456789; // Double.MAX_VALUE: underflow with Oracle

				aa.c = 'a';
				aa.charValue = 'ß';

				aa.bigIntegerValue = BigInteger.valueOf(Long.MAX_VALUE);
				aa.bigDecimalValue = BigDecimal.valueOf(123456789); // BigDecimal.MAX_VALUE: underflow with Oracle
				aa.datetime = now;
				aa.date = now.toLocalDate();
				aa.time = now.toLocalTime();

				aa.setS("S");

				aa.structure = new Stucture("abc", 100);

				assertDoesNotThrow(() -> aa.picture = CFile.readBinary(new File("src/test/resources/bike.jpg")));
				aa.longtext = new char[CHAR_ARRAY_SIZE];
				System.arraycopy(longtext, 0, aa.longtext, 0, CHAR_ARRAY_SIZE);

				aa.strings = CList.newList("A", "B", "C", "D", (dbType == DbType.ORACLE ? null : ""), null); // Oracle does not allow empty string values (stored as NULL instead)
				aa.doubleSet = CSet.newSet(0.0, 0.1, 0.2, null);
				aa.bigDecimalMap = CMap.newMap("a", BigDecimal.valueOf(1L), "b", BigDecimal.valueOf(2L), "c", BigDecimal.valueOf(3.1), "d", null, null, BigDecimal.valueOf(0L));

				aa.listOfLists = CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, null));
				aa.listOfMaps = CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", null));
				aa.mapOfLists = CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", null));
				aa.mapOfMaps = CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false, Type.C, null));
			});
			aa1.file = oriFile;
			aa1.o = o1;
			sdc.save(aa1);

			log.info("\tAssertions on saved objects...");

			assertTrue(sdc.hasAny(A.class));
			assertTrue(sdc.hasAny(A.class, a -> a.type == Type.A));
			assertFalse(sdc.hasAny(A.class, a -> a.type == Type.B));
			assertNotNull(sdc.findAny(A.class, a -> true));
			assertEquals(1, sdc.count(A.class, a -> a.type == Type.A));

			log.info("\tReload object to check if no changes will be detected...");

			assertFalse(sdc.reload(aa1));

			log.info("\tUnregister objects to force reload...");

			sdc.unregisterOnlyForTest(aa1);
			sdc.unregisterOnlyForTest(o1);

			log.info("\tAssertions on unregistered objects...");

			assertFalse(sdc.hasAny(AA.class));
			assertFalse(sdc.hasAny(O.class));

			log.info("\tLoad objects from database...");

			assertTrue(oriFile.setWritable(true));
			sdc.synchronize();

			log.info("\tAssertions on loaded objects...");

			assertNotSame(aa1, sdc.findAny(AA.class, aa -> true));
			assertNotSame(o1, sdc.findAny(O.class, o -> true));

			aa1 = sdc.findAny(AA.class, aa -> true);
			o1 = sdc.findAny(O.class, o -> true);

			// Check if loaded object equals object created before

			assertEquals(true, aa1.bool);
			assertEquals(false, aa1.booleanValue);
			assertEquals(Short.MIN_VALUE, aa1.sh);
			assertEquals(Short.MAX_VALUE, aa1.shortValue);
			assertEquals(Integer.MIN_VALUE, aa1.i);
			assertEquals(Integer.MAX_VALUE, aa1.integerValue);
			assertEquals(Long.MIN_VALUE, aa1.l);
			assertEquals(Long.MAX_VALUE, aa1.longValue);
			assertEquals(123456789.123456, aa1.d);
			assertEquals(-0.0000123456789, aa1.doubleValue);
			assertEquals(BigInteger.valueOf(Long.MAX_VALUE), aa1.bigIntegerValue);
			assertEquals(BigDecimal.valueOf(123456789), aa1.bigDecimalValue);
			assertEquals('a', aa1.c);
			assertEquals('ß', aa1.charValue);
			assertEquals("S", aa1.getS());
			assertEquals("abc", aa1.structure.s);
			assertEquals(100, aa1.structure.i);
			assertTrue(logicallyEqual(now, aa1.datetime)); // Check only seconds because milliseconds will not be stored in database (Oracle)
			assertEquals(now.toLocalDate(), aa1.date);
			assertTrue(logicallyEqual(now.toLocalTime(), aa1.time));
			assertArrayEquals(CFile.readBinary(new File("src/test/resources/bike.jpg")), aa1.picture);
			assertArrayEquals(longtext, aa1.longtext);

			assertEquals(oriFile, aa1.file);
			assertArrayEquals(CFile.readBinary(oriFile), CFile.readBinary(aa1.file));

			assertEquals(Type.A, aa1.type);
			assertEquals("!!!secret!!!", aa1.secretString);
			assertEquals("!!!password!!!", aa1.pwd);

			assertTrue(objectsEqual(new String[] { "a", "äß", null }, aa1.stringArray));

			assertEquals(CList.newList("A", "B", "C", "D", (dbType == DbType.ORACLE ? null : ""), null), aa1.strings); // Oracle does not allow empty string values (stored as NULL instead)
			assertEquals(CSet.newSet(0.0, 0.1, 0.2, null), aa1.doubleSet);
			assertEquals(CMap.newMap("a", BigDecimal.valueOf(1L), "b", BigDecimal.valueOf(2L), "c", BigDecimal.valueOf(3.1), "d", null, null, BigDecimal.valueOf(0L)), aa1.bigDecimalMap);

			assertEquals(CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, null)), aa1.listOfLists);
			assertEquals(CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", null)), aa1.listOfMaps);
			assertEquals(CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", null)), aa1.mapOfLists);
			assertEquals(CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, true, Type.B, false, Type.C, null)), aa1.mapOfMaps);

			assertEquals(o1, aa1.o);

			log.info("\tCheck if file cannot be written...");

			oriFile.setWritable(false);
			sdc.unregisterOnlyForTest(aa1);

			sdc.synchronize();

			aa1 = sdc.findAny(AA.class, aa -> true);

			assertNotEquals(oriFile, aa1.file);
			assertEquals(oriFile.getName(), aa1.file.getName());
			assertArrayEquals(CFile.readBinary(oriFile), CFile.readBinary(aa1.file));

			log.info("\tCheck registered string converter...");

			SqlDomainController.registerStringConvertersForType(A.Stucture.class, cv -> cv.toString(), s -> Stucture.valueOf(s));

			AA aa2 = sdc.createAndSave(AA.class, aa -> { aa.structure = new Stucture("abc", 100); });

			sdc.unregisterOnlyForTest(aa2);

			sdc.synchronize();

			aa2 = sdc.findAny(AA.class, aa -> aa.i == 0); // find aa2, not aa1

			assertEquals("abc", aa2.structure.s);
			assertEquals(100, aa2.structure.i);

			aa2.delete();
			assertEquals(1, sdc.count(AA.class, aa -> true));
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
			AA aa1 = sdc.findAny(AA.class, aa -> true);

			// SortedMap<String, Object> objectRecord = sdc.recordMap.get(aa1.getClass()).get(aa1.getId());
			aa1.strings.remove("A");
			aa1.strings.add("E");

			aa1.doubleSet.remove(0.0);
			aa1.doubleSet.remove(null);
			aa1.doubleSet.add(0.3);

			aa1.bigDecimalMap.remove("a");
			aa1.bigDecimalMap.remove(null);
			aa1.bigDecimalMap.put("e", BigDecimal.valueOf(5L));

			sdc.save(aa1);

			log.info("\tCreate and save second object...");

			O o2 = sdc.createAndSave(O.class, null);

			assertEquals(2, sdc.count(O.class, o -> true));

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {

				log.info("\tDelete second object...");

				sdc.delete(sqlcn.cn, o2);

				log.info("\tManipulate database externally...");

				assertEquals(1, sdc.count(O.class, o -> true));

				log.warn("Next error/warn message is expected here");
				File file = new File("bike1.jpg");
				file.delete(); // Forces putting an error message in file content in entry
				sdc.getSqlDb().update(sqlcn.cn, "DOM_A",
						CMap.newMap("I", 2, "DOM_LONGTEXT", "äöüßÄÖÜ".toCharArray(), "DOM_FILE", Helpers.buildFileByteEntry(file, "DOM_FILE"), "DOM_TYPE", "B", "O_ID", null), "S='S'");

				sdc.getSqlDb().update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT_ORDER", ComplexFieldHelpers.INITIAL_ORDER_INCREMENT * (1 + 1)), "ELEMENT='B'");
				sdc.getSqlDb().update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT_ORDER", ComplexFieldHelpers.INITIAL_ORDER_INCREMENT * (0 + 1)), "ELEMENT='C'");
				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_STRINGS", "ELEMENT='D'");

				if (dbType == DbType.ORACLE) {
					sdc.getSqlDb().update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT", "E"), "ELEMENT IS NULL");
				}
				else {
					sdc.getSqlDb().update(sqlcn.cn, "DOM_A_STRINGS", CMap.newMap("ELEMENT", "E"), "ELEMENT=''");
				}
				sdc.getSqlDb().insertInto(sqlcn.cn, "DOM_A_STRINGS", CMap.newSortedMap("A_ID", aa1.getId(), "ELEMENT", "G", "ELEMENT_ORDER", ComplexFieldHelpers.INITIAL_ORDER_INCREMENT * (7 + 1)));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_DOUBLE_SET", "ELEMENT IS NULL");
				sdc.getSqlDb().insertInto(sqlcn.cn, "DOM_A_DOUBLE_SET", CMap.newSortedMap("A_ID", aa1.getId(), "ELEMENT", 0.4));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", "ENTRY_KEY='d'");
				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", "ENTRY_KEY IS NULL");
				sdc.getSqlDb().insertInto(sqlcn.cn, "DOM_A_BIG_DECIMAL_MAP", CMap.newSortedMap("A_ID", aa1.getId(), "ENTRY_KEY", "f", "ENTRY_VALUE", 6L));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_LIST_OF_LISTS", "ELEMENT IS NULL");
				sdc.getSqlDb().update(sqlcn.cn, "DOM_A_LIST_OF_LISTS", CMap.newMap("ELEMENT", "A,B,C"), "ELEMENT_ORDER=" + ComplexFieldHelpers.INITIAL_ORDER_INCREMENT * (3 + 1));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_LIST_OF_MAPS", "ELEMENT IS NULL");
				sdc.getSqlDb().update(sqlcn.cn, "DOM_A_LIST_OF_MAPS", CMap.newMap("ELEMENT", "A=2;B=1;C=3"), "ELEMENT_ORDER=" + ComplexFieldHelpers.INITIAL_ORDER_INCREMENT * (3 + 1));

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_MAP_OF_LISTS", "ENTRY_KEY=0");
				sdc.getSqlDb().update(sqlcn.cn, "DOM_A_MAP_OF_LISTS", CMap.newMap("ENTRY_VALUE", "A,B,C"), "ENTRY_KEY=3");

				SqlDb.deleteFrom(sqlcn.cn, "DOM_A_MAP_OF_MAPS", "ENTRY_KEY=0");
				sdc.getSqlDb().update(sqlcn.cn, "DOM_A_MAP_OF_MAPS", CMap.newMap("ENTRY_VALUE", "A=true;B=false"), "ENTRY_KEY=2");

				sqlcn.cn.commit();
			}

			log.info("\tLoad changed object again from database...");

			sdc.reload(aa1);

			log.info("\tCheck if database changes are reflected by objects...");

			assertEquals(2, aa1.i);
			assertEquals(Type.B, aa1.type);
			assertArrayEquals("äöüßÄÖÜ".toCharArray(), aa1.longtext);
			File file = new File("bike1.jpg");
			String pathNameFromDatabase = (aa1.file != null ? aa1.file.getAbsolutePath() : null);
			assertEquals(file.getAbsolutePath(), pathNameFromDatabase); // File (without path) does not exist - therefore content could not be assigned on preceding INSERT
			assertTrue(CFile.readText(file, StandardCharsets.UTF_8.toString()).startsWith("File did not exist or could not be read on storing to database!"));
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
			O o1 = sdc.findAny(O.class, o -> true);
			AA aa1 = sdc.findAny(AA.class, aa -> true);

			aa1.i = 1;
			aa1.integerValue = -1;
			aa1.l = 1L;
			aa1.longValue = -1L;
			aa1.d = 0.1;
			aa1.doubleValue = -0.1;
			aa1.bigIntegerValue = BigInteger.valueOf(-1L);
			aa1.bigDecimalValue = BigDecimal.valueOf(-0.1);
			aa1.setS("T");
			aa1.file = CResource.findFirstJavaResourceFile("z.txt");
			aa1.type = Type.B;

			aa1.strings = CList.newList("C", "B", "A");
			aa1.doubleSet = CSet.newSet(0.0, -0.1, -0.2);
			aa1.bigDecimalMap = CMap.newMap("a", BigDecimal.valueOf(-1L), "b", BigDecimal.valueOf(-2L), "c", BigDecimal.valueOf(-3L));

			aa1.listOfLists = CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, Type.C));
			aa1.listOfMaps = CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", 3));
			aa1.mapOfLists = CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", "C"));
			aa1.mapOfMaps = CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, false, Type.B, true));

			aa1.o = o1;

			sdc.save(aa1);

			log.info("\tUnregister changed objects to force reload...");

			sdc.unregisterOnlyForTest(aa1);
			sdc.unregisterOnlyForTest(o1);

			log.info("\tLoad objects from database...");

			sdc.synchronize();

			aa1 = sdc.findAny(AA.class, aa -> true);
			o1 = sdc.findAny(O.class, o -> true);

			log.info("\tCheck if reloaded object equals object changed before...");

			assertEquals(1, aa1.i);
			assertEquals(-1, aa1.integerValue);
			assertEquals(1L, aa1.l);
			assertEquals(-1L, aa1.longValue);
			assertEquals(0.1, aa1.d);
			assertEquals(-0.1, aa1.doubleValue);
			assertEquals(BigInteger.valueOf(-1L), aa1.bigIntegerValue);
			assertEquals(BigDecimal.valueOf(-0.1), aa1.bigDecimalValue);
			assertEquals("T", aa1.getS());
			assertEquals(CResource.findFirstJavaResourceFile("z.txt"), aa1.file);
			assertEquals(Type.B, aa1.type);

			assertEquals(CList.newList("C", "B", "A"), aa1.strings);
			assertEquals(CSet.newSet(0.0, -0.1, -0.2), aa1.doubleSet);
			assertEquals(CMap.newMap("a", BigDecimal.valueOf(-1L), "b", BigDecimal.valueOf(-2L), "c", BigDecimal.valueOf(-3L)), aa1.bigDecimalMap);

			assertEquals(CList.newList(new ArrayList<>(), CList.newList(Type.A), CList.newList(Type.A, Type.B), CList.newList(Type.A, Type.B, Type.C)), aa1.listOfLists);
			assertEquals(CList.newList(new HashMap<>(), CMap.newMap("A", 1), CMap.newMap("A", 1, "B", 2), CMap.newMap("A", 1, "B", 2, "C", 3)), aa1.listOfMaps);
			assertEquals(CMap.newMap(0L, new ArrayList<>(), 1L, CList.newList("A"), 2L, CList.newList("A", "B"), 3L, CList.newList("A", "B", "C")), aa1.mapOfLists);
			assertEquals(CMap.newMap("0", new HashMap<>(), "1", CMap.newMap(Type.A, true), "2", CMap.newMap(Type.A, false, Type.B, true)), aa1.mapOfMaps);

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
			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), true)) {

				log.info("\tCreate and save data horizon controlled objects...");

				aa1 = sdc.createAndSave(sqlcn.cn, AA.class, null); // Under data horizon control

				Set<X> xs = new HashSet<>(); // Children of AA
				for (int i = 0; i < 10; i++) {
					X x = sdc.create(X.class, null);
					x.s = String.valueOf(i + 1);
					x.a = aa1;
					sdc.save(sqlcn.cn, x);
					xs.add(x);
				}

				log.info("\tAssertions on created objects...");

				assertEquals(xs, aa1.xs); // Accumulation
				assertEquals(10, aa1.xs.size()); // Accumulation after unregistering objects deleted in database

				log.info("\tDelete some of the recently created objects in database externally...");

				assertEquals(5, SqlDb.deleteFrom(sqlcn.cn, "DOM_X", "S='6' OR S='7' OR S='8' OR S='9' OR S='10'"));
			}

			log.info("\tSynchronize with database to recognize and unregister externally deleted objects...");

			sdc.synchronize(); // Determine and unregister X objects meanwhile deleted in database

			log.info("\tAssertions on unregistered objects...");

			assertEquals(5, sdc.count(X.class, x -> true));
			assertEquals(5, aa1.xs.size()); // Accumulation after unregistering objects deleted in database

			X x1 = sdc.findAny(X.class, x -> "1".equals(x.s));
			X x2 = sdc.findAny(X.class, x -> "2".equals(x.s));
			X x3 = sdc.findAny(X.class, x -> "3".equals(x.s));

			log.info("\tCreate and save a child of a data horizon controlled object which itself is not under data horizon control...");

			B b1 = sdc.create(B.class, b -> b.name = "!!!value_in_secret_class!!!"); // Child of A which is not under data horizon control
			b1.aa = aa1;
			sdc.save(b1);

			log.info("\tWait until data horizon data horizon period is over...");

			Thread.sleep(2000); // 'dataHorizonPeriod' in domain.properties must be 1 second or lower

			log.info("\tSynchronize again with database to unregister objects out of data horizon...");

			String dataHorizonPeriod = sdc.getDataHorizonPeriodForTest();
			sdc.setDataHorizonPeriodForTest("1s");
			sdc.synchronize(); // Unregister x's and aa due to out-of-data-horizon condition (remove from heap)

			log.info("\tAssertions on unregistration...");

			assertEquals(0, sdc.count(X.class, x -> true));
			assertEquals(0, aa1.xs.size()); // Accumulation after unregistering objects (due to out-of-data horizon condition)
			assertEquals(1, sdc.count(A.class, a -> true)); // Referenced by b1

			log.info("\tCreate and save objects which reference existing but out-of-data-horizon objects...");

			sdc.createAndSave(Z.class, z -> z.x = x1); // Re-register referenced out-of-data-horizon objects which were unregistered during synchronization
			sdc.createAndSave(Z.class, z -> z.x = x2);
			sdc.createAndSave(Z.class, z -> z.x = x3);

			log.info("\tSynchronize again with database to force (re)loading referenced but unregistered objects...");

			sdc.synchronize();
			sdc.setDataHorizonPeriodForTest(dataHorizonPeriod);

			log.info("\tAssert reloading of referenced objects...");

			assertEquals(3, sdc.count(X.class, x -> true)); // Re-registered X object which are referenced by Z objects
			assertEquals(3, aa1.xs.size()); // Accumulation after reloading out-of-data-horizon objects

			assertEquals(aa1, sdc.findAny(AA.class, aa -> true)); // A object which were not unregistered on synchronization because it was referenced by B object
			assertEquals(1, sdc.count(A.class, a -> true));
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

			AA aa1 = sdc.create(AA.class, aa -> aa.setS("aa1"));
			Z z1 = new Z();
			Y y1 = sdc.create(Y.class, y -> y.z = z1);
			y1.y = y1;
			X x1 = sdc.create(X.class, x -> {
				x.a = aa1;
				x.y = y1;
			});
			z1.x = x1;

			sdc.save(x1);

			log.info("\tAssert saving...");

			assertTrue(aa1.isStored());
			assertTrue(y1.isStored());
			assertTrue(z1.isStored());
			assertTrue(x1.isStored());

			AA aa2 = sdc.createAndSave(AA.class, aa -> {
				aa.setS("aa2");
				aa.integerValue = 2;
			});
			X x2 = sdc.createAndSave(X.class, null);
			Z z2 = sdc.createAndSave(Z.class, null);
			Y y2 = sdc.create(Y.class, null);
			y2.z = z2;
			sdc.save(y2);

			log.info("\tChange objects locally without saving...");

			y1.z = z2;
			y1.y = y2;
			x1.a = aa2;
			x1.y = null;
			z1.x = x2;

			log.info("\tChange values in database externally...");

			AA aa3 = sdc.createAndSave(AA.class, aa -> {
				aa.setS("aa3");
				aa.integerValue = 3;
			});
			X x3 = sdc.createAndSave(X.class, null);
			Z z3 = sdc.createAndSave(Z.class, null);
			Y y3 = sdc.create(Y.class, null);
			y3.z = z3;
			sdc.save(y3);

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {
				sdc.getSqlDb().update(sqlcn.cn, "DOM_Y", CMap.newMap("Z_ID", z3.getId()), "ID=" + y1.getId());
				sdc.getSqlDb().update(sqlcn.cn, "DOM_Y", CMap.newMap("Y_ID", y3.getId()), "ID=" + y1.getId());
				sdc.getSqlDb().update(sqlcn.cn, "DOM_X", CMap.newMap("A_ID", aa3.getId()), "ID=" + x1.getId());
				sdc.getSqlDb().update(sqlcn.cn, "DOM_X", CMap.newMap("Y_ID", y3.getId()), "ID=" + x1.getId());
				sdc.getSqlDb().update(sqlcn.cn, "DOM_Z", CMap.newMap("X_ID", x3.getId()), "ID=" + z1.getId());
			}

			log.info("\tSynchronize again with database to recognize unsaved changes...");

			log.warn("Warn messages regarding unsaved changes are expected here...");
			sdc.synchronize();

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

			O o1 = sdc.createAndSave(O.class, null);
			AA aa1 = sdc.createAndSave(AA.class, aa -> aa.o = o1);
			AB ab1 = sdc.createAndSave(AB.class, ab -> {
				ab.i = 1;
				ab.setS("ab");
			});
			X x1a = sdc.createAndSave(X.class, x -> {
				x.a = aa1;
				x.s = "available";
			});
			X x1b = sdc.createAndSave(X.class, x -> {
				x.a = ab1;
				x.s = "available";
			});

			log.info("\tUnregister all objects to force reload of these objects...");

			sdc.unregisterOnlyForTest(x1a);
			sdc.unregisterOnlyForTest(x1b);
			sdc.unregisterOnlyForTest(aa1);
			sdc.unregisterOnlyForTest(ab1);
			sdc.unregisterOnlyForTest(o1);

			assertFalse(sdc.hasAny(X.class));
			assertFalse(sdc.hasAny(AA.class));
			assertFalse(sdc.hasAny(AB.class));
			assertFalse(sdc.hasAny(O.class));

			log.info("\tLoad objects of one domain class and force loading referenced parent objects in separate load cycles...");

			Set<SqlDomainObject> objects = sdc.loadOnly(X.class, null, -1);

			assertEquals(5, objects.size());
			assertEquals(2, sdc.count(X.class, x -> true));
			assertTrue(sdc.hasAny(AA.class));
			assertTrue(sdc.hasAny(AB.class));
			assertTrue(sdc.hasAny(O.class));

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

			X x1a = sdc.createAndSave(X.class, x -> { x.s = "available"; });
			X x1b = sdc.createAndSave(X.class, x -> { x.s = "available"; });

			log.info("\tLoad objects of one domain class for excusive use");

			Set<X> xs = sdc.allocateObjectsExclusively(X.class, X.InProgress.class, "S='available'", -1, x -> x.s = "in_use");
			assertEquals(2, xs.size());
			assertEquals(2, sdc.count(X.class, x -> true));
			assertEquals(2, sdc.count(X.InProgress.class, s -> true));

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

			xs = sdc.allocateObjectsExclusively(X.class, X.InProgress.class, "s='available'", -1, null);
			assertTrue(xs.isEmpty());

			xs = sdc.allocateObjectsExclusively(X.class, X.InProgress.class, null, -1, null);
			assertTrue(xs.isEmpty());

			log.info("\tRelease one exclusively used object...");

			assertTrue(sdc.releaseObject(x1b, X.InProgress.class, x -> x.s = "available"));
			assertEquals(1, sdc.count(X.InProgress.class, s -> true));

			log.info("\tTry to allocate another object excusively (now one object exists)...");

			Set<X> xs2 = sdc.allocateObjectsExclusively(X.class, X.InProgress.class, null, 1, x -> x.s = "in_use");
			assertEquals(1, xs2.size());
			assertEquals(2, sdc.count(X.InProgress.class, s -> true));

			sdc.releaseObject(x1a, X.InProgress.class, x -> x.s = "available");
			sdc.releaseObject(x1b, X.InProgress.class, x -> x.s = "available");
			assertEquals(0, sdc.count(X.InProgress.class, s -> true));

			assertTrue(sdc.allocateObjectExclusively(x1a, X.InProgress.class, x -> x.s = "in_use"));
			assertEquals(1, sdc.count(X.InProgress.class, s -> true));
			sdc.releaseObject(x1a, X.InProgress.class, x -> x.s = "available");
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
			C c1 = sdc.create(C.class, null);
			c1.c = c1;
			sdc.save(c1);

			assertDoesNotThrow(() -> sdc.delete(c1));

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {
				assertEquals(0, sdc.getSqlDb().selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			C c1a = sdc.createAndSave(C.class, null);
			C c2 = sdc.create(C.class, c -> c.c = c1a);
			sdc.save(c2);
			c1a.c = c2;
			sdc.save(c1a);

			assertDoesNotThrow(() -> sdc.delete(c2));

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {
				assertEquals(0, sdc.getSqlDb().selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			C c1b = sdc.create(C.class, null);
			C c2a = sdc.create(C.class, c -> c.c = c1b);
			C c3 = sdc.create(C.class, c -> c.c = c2a);
			c1b.c = c3;
			sdc.save(c3);

			assertDoesNotThrow(() -> sdc.delete(c1b));

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {
				assertEquals(0, sdc.getSqlDb().selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			log.info("\tError case exception on deletion...");

			C ce1 = sdc.create(C.class, null);
			C ce2 = sdc.create(C.class, c -> c.c = ce1);
			ce1.c = ce2;
			sdc.save(ce1);

			sdc.unregisterOnlyForTest(ce2);

			log.warn("Error/warn messages regarding integrity constraint violation are expected here...");
			assertThrows(SQLException.class, () -> sdc.delete(ce1));

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {
				assertEquals(2, sdc.getSqlDb().selectCountFrom(sqlcn.cn, "DOM_C", null));
			}

			sdc.reregisterOnlyForTest(ce2);

			assertEquals(2, sdc.count(C.class, c -> true));
			assertDoesNotThrow(() -> sdc.delete(ce1));

			try (SqlConnection sqlcn = SqlConnection.open(sdc.getPool(), false)) {
				assertEquals(0, sdc.getSqlDb().selectCountFrom(sqlcn.cn, "DOM_C", null));
			}
			assertEquals(0, sdc.count(C.class, c -> true));
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

			X x1 = new X();
			sdc.register(x1);
			X x2 = new X();
			sdc.register(x2);
			assertTrue(x2.compareTo(x1) > 0);

			log.info("\tCheck object order on overridden compareTo()...");

			O o1 = sdc.create(O.class, null);

			AA aaa = new AA(o1, "A");
			aaa.bool = false;
			sdc.register(aaa);
			AA aab = new AA(o1, "B");
			aab.bool = true;
			sdc.register(aab);
			AA aac = new AA(o1, "C");
			aac.bool = false;
			sdc.register(aac);
			AA aad = new AA(o1, "D");
			aad.bool = true;
			sdc.register(aad);

			assertEquals(CList.newList(aaa, aab, aac, aad), sdc.sort(CList.newList(aad, aab, aac, aaa)));

			assertEquals(CMap.newMap("A", CSet.newSet(aaa), "B", CSet.newSet(aab), "C", CSet.newSet(aac), "D", CSet.newSet(aad)), sdc.groupBy(o1.as, a -> a.getS()));
			assertEquals(CMap.newMap(false, CSet.newSet(aaa, aac), true, CSet.newSet(aab, aad)), sdc.groupBy(o1.as, a -> a.bool));
			assertEquals(2, sdc.countBy(o1.as, a -> a.bool).get(true));

			sdc.delete(aaa);
			sdc.delete(aab);
			sdc.delete(aac);
			sdc.delete(aad);
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
	void persistingComplexObjects() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 11: persistingComplexObjects()");

			AA aa = sdc.createAndSave(AA.class, a -> {
				a.i = 42;
				a.strings = new ArrayList<>();
				a.stringArray = null;
				a.pwd = null;
			});

			for (int i = 40; i >= 0; i--) {
				aa.strings.add(0, "" + i);
				aa.save();
			}

			sdc.unregisterOnlyForTest(aa);
			sdc.loadOnly(AA.class, "DOM_A.I=42", 1);
			for (int i = 0; i <= 40; i++) {
				assertEquals("" + i, aa.strings.get(i));
			}

			Collections.reverse(aa.strings);
			aa.save();
			sdc.unregisterOnlyForTest(aa);
			sdc.loadOnly(AA.class, "DOM_A.I=42", 1);
			for (int i = 0; i <= 40; i++) {
				assertEquals("" + (40 - i), aa.strings.get(i));
			}
			Collections.reverse(aa.strings);
			aa.save();

			log.info("{}", aa.strings);

			aa.strings.add(0, "0");
			aa.strings.add(21, "0");
			aa.strings.add(43, "0");
			aa.save();
			sdc.unregisterOnlyForTest(aa);
			sdc.loadOnly(AA.class, "DOM_A.I=42", 1);
			assertEquals("0", aa.strings.get(0));
			assertEquals("0", aa.strings.get(21));
			assertEquals("0", aa.strings.get(43));
			aa.strings.remove(43);
			aa.strings.remove(21);
			aa.strings.remove(1);
			aa.save();

			for (int i = 40; i >= 0; i--) {
				aa.strings.add(1, "0." + i);
				aa.save();
			}

			sdc.unregisterOnlyForTest(aa);
			sdc.loadOnly(AA.class, "DOM_A.I=42", 1);
			assertEquals("0", aa.strings.get(0));
			for (int i = 0; i <= 40; i++) {
				assertEquals("0." + i, aa.strings.get(i + 1));
			}
			for (int i = 41; i <= 80; i++) {
				assertEquals("" + (i - 40), aa.strings.get(i + 1));
			}

			log.info("{}", aa.strings);

			aa.delete();
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
	@Order(12)
	void errorCases() throws Throwable {

		try {
			cleanup();

			log.info("\tTEST 12: errorCases()");

			AA aa1 = sdc.create(AA.class, a -> a.setS("aa1"));

			log.info("\tGetting and setting inexistent fields...");
			log.warn("Test error cases: error/warn messages are expected here...");

			assertNull(aa1.getFieldValue(X.class.getDeclaredField("s")), "get field value error");
			assertDoesNotThrow(() -> aa1.setFieldValue(X.class.getDeclaredField("s"), "a"), "get known field value error");

			log.info("\tNOT NULL constraint violation, warnings and errors...");

			aa1.type = null;

			assertTrue(sdc.hasConstraintViolations(aa1));
			assertTrue(aa1.hasErrorsOrWarnings());
			assertEquals(1, aa1.getErrorsAndWarnings().size());
			assertFalse(aa1.getErrorOrWarning(aa1.getInvalidFields().iterator().next()).toString().isEmpty());

			assertThrows(SQLException.class, () -> sdc.save(aa1));

			aa1.type = Type.A;

			assertFalse(sdc.hasConstraintViolations(aa1));

			log.info("\tUNIQUE constraint violation...");

			aa1.i = 0;
			aa1.integerValue = 1;
			AA aa2 = sdc.create(AA.class, a -> a.setS("aa2"));
			aa2.i = 0;
			aa2.integerValue = 1;

			assertTrue(sdc.hasConstraintViolations(aa1));
			assertTrue(sdc.hasConstraintViolations(aa2));

			sdc.save(aa1);
			assertThrows(SQLException.class, () -> sdc.save(aa2));

			aa2.integerValue = 2;
			aa2.setS("s");

			assertFalse(sdc.hasConstraintViolations(aa1));
			assertFalse(sdc.hasConstraintViolations(aa2));

			sdc.save(aa2);

			assertFalse(aa1.hasErrorsOrWarnings());
			assertEquals(2, sdc.allValid(AA.class).size());

			log.info("\tColumn size violation...");

			aa1.setS("abcdefghijklmnopqrstuvwxyz"); // Column size

			assertTrue(sdc.hasConstraintViolations(aa1));

			sdc.save(aa1); // Save with truncated value

			assertTrue(aa1.hasErrorsOrWarnings());
			assertTrue(aa1.isValid());

			aa1.type = null; // Analytical update

			sdc.save(aa1); // Save all but invalid fields
			assertFalse(aa1.isValid());
			assertEquals(2, aa1.getErrorsAndWarnings().size());

			cleanup();

			sdc.getSqlDb().close();
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
