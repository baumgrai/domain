package com.icx.dom.junit.tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.SortedMap;
import java.util.stream.Collectors;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.bikestore.BikeStoreApp;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.RegionInUse;
import com.icx.dom.common.CCollection;
import com.icx.dom.common.CFile;
import com.icx.dom.common.CList;
import com.icx.dom.common.CLog;
import com.icx.dom.common.CMap;
import com.icx.dom.common.CRandom;
import com.icx.dom.common.CResource;
import com.icx.dom.common.CSet;
import com.icx.dom.common.Common;
import com.icx.dom.common.Prop;
import com.icx.dom.common.Reflection;
import com.icx.dom.jdbc.ConfigException;
import com.icx.dom.jdbc.JdbcHelpers;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDb;
import com.icx.dom.jdbc.SqlDb.DbType;
import com.icx.dom.jdbc.SqlDbException;
import com.icx.dom.jdbc.SqlDbTable;
import com.icx.dom.junit.TestHelpers;

@TestMethodOrder(OrderAnnotation.class)
class CommonTest extends TestHelpers {

	static final Logger log = LoggerFactory.getLogger(CommonTest.class);

	@SuppressWarnings("static-method")
	@Test
	@Order(1)
	void common() throws Exception {

		log.info("\n\ncbase()\n\n");

		assertTrue(Common.logicallyEqual("", null), "logically equals empty string and null");
		assertTrue(Common.logicallyEqual(null, ""), "logically equals null and empty string");
		assertTrue(Common.logicallyEqual(new ArrayList<>(), null), "logically equals empty list and null");
		assertTrue(Common.logicallyEqual(null, new ArrayList<>()), "logically equals null and empty list");
		assertTrue(Common.logicallyEqual(new HashMap<>(), null), "logically equals empty map and null");
		assertTrue(Common.logicallyEqual(null, new HashMap<>()), "logically equals null and empty map");

		assertEquals("a", Common.untilFirst("a.b.s", "."), "until first");
		assertEquals("", Common.untilFirst("a.b.s", "a"), "until first");
		assertEquals("", Common.untilFirst(null, "a"), "until first");
		assertEquals(".b.s", Common.fromFirst("a.b.s", "."), "from first");
		assertEquals("a", Common.fromFirst("a", "."), "from first");
		assertEquals("b.s", Common.behindFirst("a.b.s", "."), "behind first");
		assertEquals("", Common.behindFirst("a.b.s", "s"), "behind first");
		assertEquals("", Common.behindFirst(null, "s"), "behind first");

		assertEquals("a.b", Common.untilLast("a.b.s", "."), "until last");
		assertEquals("", Common.untilLast("a.b.s", "a"), "until last");
		assertEquals("", Common.untilLast(null, "a"), "until last");
		assertEquals(".s", Common.fromLast("a.b.s", "."), "from last");
		assertEquals("", Common.fromLast(null, "."), "from last");
		assertEquals("a", Common.fromLast("a", "."), "from first");
		assertEquals("s", Common.behindLast("a.b.s", "."), "behind last");
		assertEquals("", Common.behindLast("a.b.s", "s"), "behind last");
		assertEquals("", Common.behindLast(null, "s"), "behind last");
		assertEquals("a", Common.behindLast("a", "s"), "behind last");

		assertEquals(true, Common.parseBoolean("true", false), "parse boolean");
		assertEquals(false, Common.parseBoolean(null, false), "parse boolean");
		if (((DecimalFormat) NumberFormat.getInstance()).getDecimalFormatSymbols().getGroupingSeparator() == ',') {
			assertEquals(42000, Common.parseInt("42,000", 42000), "parse int");
			assertEquals(100000, Common.parseLong("100,000", 1L), "parse long");
		}
		else {
			assertEquals(42000, Common.parseInt("42.000", 42000), "parse int");
			assertEquals(100000, Common.parseLong("100.000", 1L), "parse long");
		}
		assertEquals(43, Common.parseInt("", 43), "parse int");
		assertEquals(1L, Common.parseLong(null, 1L), "parse long");
		assertEquals(123.45, Common.parseDouble("123.45", 123.46), "parse double");
		assertEquals(123.46, Common.parseDouble("", 123.46), "parse double");

		assertEquals("a,null,", Common.listToString(CList.newList("a", null, "")), "list to string");
		assertEquals(CList.newList("a", null, ""), Common.stringToList("[a,null,]"), "string to list");

		assertEquals("a=A,b=null,c=", Common.mapToString(CMap.newMap("a", "A", "b", null, "c", "")), "map to string");
		assertEquals(CMap.newMap("a", "A", "b", null, "c", ""), Common.stringToMap("{a=A,b=null,c=}"), "string to map");

		byte[] bytes = { (byte) 0xc3, (byte) 0xa4 };
		assertEquals((byte) 0xc3, Common.getBytesUTF8("ä")[0], "UTF8 string to byte array");
		assertEquals((byte) 0xa4, Common.getBytesUTF8("ä")[1], "UTF8 string to byte array");
		assertEquals("ä", Common.getStringUTF8(bytes), "UTF8 byte array to string");
		assertEquals("a", Common.getStringANSI(Common.getBytesUTF8("a")), "UTF8 byte array to string");

		assertEquals("c3a4", Common.byteArrayToHexString(bytes), "byte array to hex string");
		assertEquals(null, Common.byteArrayToHexString(null), "byte array to hex string");
		assertEquals((byte) 0xc3, Common.hexStringToByteArray("c3")[0], "hex string to byte array");
		assertArrayEquals(new byte[0], Common.hexStringToByteArray(null), "hex string to byte array");

		assertEquals(2, CRandom.randomSelect(CList.newList('a', 'b', 'c'), 2).size(), "random select");
		assertEquals(5, CRandom.randomString(5).length(), "random string");

		assertEquals('c', CList.getLast(CList.newList('a', 'b', 'c')), "list get last");
		assertTrue(CList.isEmpty(null), "list is empty");
		assertFalse(CList.isEmpty(CList.newList('a', 'b', 'c')), "list is not empty");
		assertEquals(CList.newList(3, 2, 1), CList.sort(CList.newList(1, 2, 3), (a, b) -> a.compareTo(b), false), "list get last");

		assertFalse(CCollection.isEmpty(CSet.newSet('a', 'b', 'c')), "set is not empty");
		assertFalse(CCollection.isEmpty(CSet.newSortedSet('a', 'b', 'c')), "sorted set is not empty");

		assertTrue(CCollection.isEmpty(null), "collection is empty");
		assertTrue(CCollection.isEmpty(new ArrayList<>()), "collection is empty");
		assertTrue(CCollection.containsAll(CList.newList(4, 3, 2, 1), CList.newList(1, 2, 3)), "collection contains all");
		assertFalse(CCollection.containsAll(CList.newList(4, 3, 2, 1), CList.newList(1, 2, 5)), "collection contains all");
		assertTrue(CCollection.containsAny(CList.newList(4, 3, 2, 1), CList.newList(1, 5, 6)), "collection contains any");
		assertFalse(CCollection.containsAny(CList.newList(4, 3, 2, 1), CList.newList(7, 5, 6)), "collection contains any");

		SortedMap<String, Integer> map = CMap.newSortedMap("a", 1, "b", 2);
		assertTrue(map.get("a") == 1 && map.get("b") == 2 && map.firstKey().equals("a") && map.lastKey().equals("b"));
		CMap.upperCaseKeysInMap(map);
		assertTrue(map.get("A") == 1 && map.get("B") == 2 && map.firstKey().equals("A") && map.lastKey().equals("B"));

		assertFalse(Common.exceptionStackToString(new Exception()).isEmpty());

		ResourceBundle bundle = ResourceBundle.getBundle("messages", new Locale("en"));
		assertEquals("", CResource.i18n(bundle, null));
		assertEquals("germany", CResource.i18n(null, "germany"));
		assertEquals("Germany", CResource.i18n(bundle, "germany"));
		assertEquals("unknown_county", CResource.i18n(bundle, "unknown_county"));
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(2)
	void cfile() throws Exception {

		log.info("\n\ncfile()\n\n");

		new File("test/text.txt").delete();
		new File("test").delete();
		File testTxt = new File("test/text.txt");
		assertNotNull(CFile.checkOrCreateFile(testTxt), "Create file");
		assertNotNull(CFile.checkOrCreateFile(testTxt), "Create file");
		assertEquals("text.txt", CFile.getRelativeFilePath(testTxt, new File("test")).getPath(), "relative file path");

		assertNotNull(CFile.getCurrentDir(), "get current dir");

		CFile.writeText(testTxt, "abcÄÖÜß", false, StandardCharsets.UTF_8.name());
		CFile.writeText(testTxt, "abcÄÖÜß", true, StandardCharsets.UTF_8.name());
		assertEquals("abcÄÖÜßabcÄÖÜß", CFile.readText(testTxt, StandardCharsets.UTF_8.name()).trim(), "write/read text");

		CFile.writeBinary(testTxt, Common.getBytesUTF8("abcÄÖÜß"));
		assertEquals("abcÄÖÜß", Common.getStringUTF8(CFile.readBinary(testTxt)), "write/read binary");

		new File("test/text.txt").delete();
		new File("test").delete();
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(3)
	void clog() throws Exception {

		log.info("\n\nclog()\n\n");

		assertEquals("Class@java.lang.String", CLog.forAnalyticLogging(String.class), "analytic log class");
		assertEquals("'a'", CLog.forAnalyticLogging('a'), "analytic log character");
		assertEquals("java.sql.Date@1970-01-01", CLog.forAnalyticLogging(new java.sql.Date(0L)), "analytic log sql date");
		assertEquals("java.sql.Time@01:00:00.000", CLog.forAnalyticLogging(new java.sql.Time(0L)), "analytic log sql time");
		assertEquals("java.sql.Timestamp@1970-01-01 01:00:00.000", CLog.forAnalyticLogging(new java.sql.Timestamp(0L)), "analytic log Java timestamp");
		assertEquals("oracle.sql.TIMESTAMP", Common.untilFirst(CLog.forAnalyticLogging(new oracle.sql.TIMESTAMP()), "@"), "analytic log Oracle timestamp");
		assertEquals("java.util.Date@1970-01-01 01:00:00.000", CLog.forAnalyticLogging(new java.util.Date(0L)), "analytic log util date");
		assertEquals("java.time.LocalDateTime", Common.untilFirst(CLog.forAnalyticLogging(LocalDateTime.now()), "@"), "analytic log local datetime");
		assertEquals("java.time.LocalDate", Common.untilFirst(CLog.forAnalyticLogging(LocalDate.now()), "@"), "analytic log local date");
		assertEquals("java.time.LocalTime", Common.untilFirst(CLog.forAnalyticLogging(LocalTime.now()), "@"), "analytic log local time");
		assertEquals("java.util.GregorianCalendar", Common.untilFirst(CLog.forAnalyticLogging(new GregorianCalendar()), "@"), "analytic log local time");

		assertEquals("[ \"a\", \"b\", \"c\" ]", CLog.forAnalyticLogging(CList.newList("a", "b", "c")), "analytic log list");

		assertEquals("\"***\"", CLog.forSecretLogging("pwd", "abc"), "secret logging");
		assertEquals("\"******\"", CLog.forSecretLogging("passwort", ""), "empty secret logging");
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(4)
	void reflection() throws Exception {

		log.info("\n\nreflection()\n\n");

		assertEquals(Client.class, Reflection.loadClass("com.icx.dom.app.bikestore.domain.client.Client"), "load class");
		assertEquals(Client.class, Reflection.loadClass("Client"), "load class");
		assertEquals(RegionInUse.class, Reflection.loadInnerClass("RegionInUse", Client.class), "load inner class");

		Reflection.retrieveLoadedPackageNames();
		assertTrue(Reflection.getLoadedPackageNames().size() > 100, "loaded packages");

		assertNotNull(Reflection.getClassesDir(BikeStoreApp.class).getPath(), "classes dir");
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(5)
	void properties() throws Exception {

		log.info("\n\nproperties()\n\n");

		File propFile = CResource.findFirstJavaResourceFile("test.properties");
		assertNotNull(propFile);

		propFile = Prop.findPropertiesFile("test.properties");
		Properties props = Prop.readProperties(propFile);

		assertEquals("abc", Prop.getStringProperty(props, "s", ""), "string property");
		assertEquals("", Prop.getStringProperty(props, "s1", ""), "default string property");

		assertEquals(true, Prop.getBooleanProperty(props, "b", false), "boolean property");
		assertEquals(false, Prop.getBooleanProperty(props, "b1", false), "default boolean property");

		assertEquals(-1, Prop.getIntProperty(props, "i", 0), "int property");
		assertEquals(-1, Prop.getIntProperty(props, "i1", -1), "default int property");

		assertEquals(100000000000000L, Prop.getLongProperty(props, "l", -1L), "long property");
		assertEquals(-1L, Prop.getLongProperty(props, "l1", -1L), "default long property");

		assertEquals(2.5, Prop.getDoubleProperty(props, "d", -1.01), "double property");
		assertEquals(-1.01, Prop.getDoubleProperty(props, "d1", -1.01), "default double property");

		assertEquals(CList.newList("a", "", null), Prop.getProperty(props, List.class, "list", null), "list property");
		assertEquals(null, Prop.getProperty(props, List.class, "list1", null), "default list property");

		assertEquals(CMap.newMap("a", "A", "b", "", "c", null), Prop.getProperty(props, Map.class, "map", null), "map property");
		assertEquals(null, Prop.getProperty(props, Map.class, "map1", null), "default map property");

		assertNotNull(Prop.findPropertiesFile("equal.properties"), "equal properties files");
		assertNull(Prop.findPropertiesFile("unequal.properties"), "unequal properties files");
		assertNull(Prop.findPropertiesFile("nonexistent.properties"), "non-existent properties file");
	}

	private static void checkDatabase(String sqlConnectionString, String user, String pwd, DbType dbType) throws ConfigException, SQLException, SqlDbException {

		SqlDb sqlDb = new SqlDb(sqlConnectionString, user, pwd, 1, 5000);
		try (SqlConnection sqlcn1 = SqlConnection.open(sqlDb.pool, true)) {
			try (SqlConnection sqlcn2 = SqlConnection.open(sqlDb.pool, true)) {

				sqlDb.select(sqlcn2.cn, "SELECT COUNT(*) FROM DOM_A WHERE I=? AND S=?", CList.newList(0, "S"), null);
				sqlDb.selectCountFrom(sqlcn2.cn, "DOM_O", null);

				sqlDb.registerTable(sqlcn2.cn, "DOM_A");
				SqlDbTable tableA = sqlDb.findRegisteredTable("DOM_A");
				assertNotNull(tableA);

				assertNotNull(tableA.toString());
				Map<String, Object> columnValueMap = new HashMap<>();
				for (String columnName : tableA.columns.stream().map(c -> c.name).collect(Collectors.toList())) {
					columnValueMap.put(columnName, "a");
				}
				String logged = tableA.logColumnValueMap(columnValueMap);
				assertNotNull(logged);
				log.info(logged);
				assertNotNull(tableA.uniqueConstraints.iterator().next().toString());
				assertNotNull(tableA.uniqueConstraints.iterator().next().toStringWithoutTable());

				// int i = 0;
				// List<Object> countList = new ArrayList<>();
				// sqlDb.callStoredProcedure(sqlcn2.cn, "TEST_PROC", false, CList.newList(ParameterMode.IN, ParameterMode.OUT), CList.newList(i, Integer.class), countList);
				// assertNotNull(countList.get(0));
			}
		}

		assertEquals(dbType, sqlDb.getDbType());
		assertEquals(SqlDb.DB_DATE_FUNCT.get(dbType), sqlDb.getSqlDateFunct());

		sqlDb.close();
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(6)
	void jdbc() throws Exception {

		assertEquals("***", JdbcHelpers.forLoggingSql("pwd", "123"));
		assertFalse(JdbcHelpers.forLoggingSql("oracleTimestamp", new oracle.sql.TIMESTAMP()).isEmpty());
		assertFalse(JdbcHelpers.forLoggingSql("timestamp", new java.sql.Timestamp(0L)).isEmpty());
		assertFalse(JdbcHelpers.forLoggingSql("LocalDateTime", LocalDateTime.now()).isEmpty());
		assertFalse(JdbcHelpers.forLoggingSql("LocalDate", LocalDate.now()).isEmpty());
		assertFalse(JdbcHelpers.forLoggingSql("LocalTime", LocalTime.now()).isEmpty());
		assertFalse(JdbcHelpers.forLoggingSql("Calendar", new GregorianCalendar()).isEmpty());
		assertFalse(JdbcHelpers.forLoggingSql("boolean", true).isEmpty());

		// TODO: Test with Oracle and SQL Server
		checkDatabase("jdbc:mysql://localhost/junit?useSSL=false", "infinit", "infinit", DbType.MYSQL);
		checkDatabase("jdbc:sqlserver://localhost;Database=junit", "infinit", "infinit", DbType.MS_SQL);
		// checkDatabase("jdbc:oracle:thin:@//localhost:1521/xe/junit", "infinit", "infinit");
	}
}
