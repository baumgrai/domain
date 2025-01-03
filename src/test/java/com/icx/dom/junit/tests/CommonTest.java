package com.icx.dom.junit.tests;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.SortedMap;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CArray;
import com.icx.common.CCollection;
import com.icx.common.CFile;
import com.icx.common.CList;
import com.icx.common.CLog;
import com.icx.common.CMap;
import com.icx.common.CMath;
import com.icx.common.CProp;
import com.icx.common.CRandom;
import com.icx.common.CReflection;
import com.icx.common.CResource;
import com.icx.common.CSet;
import com.icx.common.Common;
import com.icx.dom.app.bikestore.BikeStoreApp;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.RegionInProgress;
import com.icx.dom.junit.TestHelpers;
import com.icx.domain.sql.Annotations.Secret;

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

		assertEquals(0, Common.compare(null, null));
		assertTrue(Common.compare(null, 1) < 0);
		assertTrue(Common.compare("a", null) > 0);
		assertEquals(0, Common.compare("a", "a"));
		assertTrue(Common.compare("A", "a") < 0);

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

		assertTrue(Common.isBoolean("true"));

		assertEquals("true", Common.formatBoolean(true, false));
		assertEquals("false", Common.formatBoolean(null, false));
		assertEquals("12", Common.formatInt(12, 13));
		assertEquals("13", Common.formatInt(null, 13));
		assertEquals("1234567890", Common.formatLong(1234567890L, 13L));
		assertEquals("13", Common.formatLong(null, 13L));
		assertEquals("1.2", Common.formatDouble(1.2, 1.3));
		assertEquals("1.3", Common.formatDouble(null, 1.3));

		assertEquals("a,null,", Common.listToString(CList.newList("a", null, "")), "list to string");
		assertEquals(CList.newList("a", null, ""), Common.stringToList("[a,null,]"), "string to list");

		assertEquals("a=A,b=null,c=", Common.mapToString(CMap.newMap("a", "A", "b", null, "c", "")), "map to string");
		assertEquals(CMap.newMap("a", "A", "b", null, "c", ""), Common.stringToMap("{a=A,b=null,c=}"), "string to map");

		assertEquals(CMap.newMap("a", "A,A1", "b", null, "c", ""), Common.stringToMultivalueMap("a=A,A1;b=(null);c=", StringSep.SEMICOLON, KeyValueSep.EQUAL_SIGN));

		assertEquals("A 1 !", Common.insertSpaces("A1!"));
		assertEquals("A", Common.capitalizeFirstLetter("a"));
		assertEquals("Abc", Common.capitalizeFirstLetter("abc"));
		assertEquals("12345", Common.removeNonDigits("1a2 3\t4?5"));

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

		List<Map<String, String>> records = new ArrayList<>();
		records.add(CMap.newMap("a", "A1", "b", "B2", "c", "C3"));
		records.add(CMap.newMap("a", "", "b", null, "c", "null"));
		assertEquals("A;B;C;\nA1;B2;C3;\n;;null;\n", CMap.listOfMapsToCsv(records, CList.newList(new SimpleEntry<>("a", "A"), new SimpleEntry<>("b", "B"), new SimpleEntry<>("c", "C")), ""));

		SortedMap<String, Integer> map = CMap.newSortedMap("a", 1, "b", 2);
		assertTrue(map.get("a") == 1 && map.get("b") == 2 && map.firstKey().equals("a") && map.lastKey().equals("b"));

		assertEquals(6, CMath.sum(CList.newList(1, 2, 3)).intValue());
		assertEquals(50.0, CMath.percentage(125, 250));
		assertEquals(5, CMath.intPercentage(50L, 1000.0));
		assertEquals(3, CMath.max(CList.newList(2, 1, 3)));
		assertEquals(3.0, CMath.maxDouble(CList.newList(1.0, 3.0, 2.0)));
		assertEquals(1, CMath.min(CList.newList(2, 1, 3)));
		assertEquals(1.0, CMath.minDouble(CList.newList(1.0, 3.0, 2.0)));
		assertEquals("123.46", CMath.formatDouble(123.456, 2, ".")); // Round
		Object[] array = { 1, 2, 3, 4, 5 };
		assertArrayEquals(array, CArray.sum(CArray.newObjectArray(4, 5), 1, 2, 3));

		assertFalse(Common.exceptionStackToString(new Exception()).isEmpty());

		CResource.setLocalizedMessageFileName("domain.i18n.messages");
		@SuppressWarnings("deprecation")
		Locale de = new Locale("de");
		ResourceBundle bundle = CResource.getBundleForLocale(de);
		assertNotNull(bundle);
		assertEquals("", CResource.i18n(bundle, null));
		assertEquals("true", CResource.i18n((ResourceBundle) null, "true"));
		assertEquals("Richtig", CResource.i18n(de, "true"));
		assertEquals("unknown_county", CResource.i18n(bundle, "unknown_county"));
		File domainManifest = new File("src/main/webapp/META-INF/MANIFEST.MF");
		assertTrue(CResource.getVersion(domainManifest).startsWith("1."));
		File domainJar = new File("build/libs/domain-1.0.1.jar");
		if (domainJar.exists()) {
			assertTrue(CResource.getVersion(domainJar).startsWith("1.0.1"));
		}
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(2)
	void cfile() throws Exception {

		log.info("\n\ncfile()\n\n");

		new File("test/text.txt").delete();
		new File("test").delete();
		File testTxt = new File("test/text.txt");
		assertEquals(CList.newList("test", "text.txt"), CFile.getPathElements(new File("test/text.txt")));
		assertNotNull(CFile.checkOrCreateFile(testTxt), "Create file");
		assertNotNull(CFile.checkOrCreateFile(testTxt), "Create file");
		assertEquals("text.txt", CFile.getRelativeFilePath(testTxt, new File("test")).getPath(), "relative file path");

		assertNotNull(CFile.getCurrentDir(), "get current dir");

		CFile.writeText(testTxt, "abcÄÖÜß", false, StandardCharsets.UTF_8.name());
		CFile.writeText(testTxt, "abcÄÖÜß", true, StandardCharsets.UTF_8.name());
		assertEquals("abcÄÖÜßabcÄÖÜß", CFile.readText(testTxt, StandardCharsets.UTF_8.name()).trim(), "write/read text");

		CFile.writeBinary(testTxt, Common.getBytesUTF8("abcÄÖÜß"));
		assertEquals("abcÄÖÜß", Common.getStringUTF8(CFile.readBinary(testTxt)), "write/read binary");

		assertNotNull(CFile.findFileInDir(new File("test"), "*.txt"));
		assertTrue(!CList.isEmpty(CFile.findFilesInDir(new File("test"), "*.txt")));
		CFile.checkOrCreateFile(new File("test/subdir/text.txt"));
		assertNotNull(CFile.findSubDir(new File("test"), "*"));
		assertEquals(2, CFile.findFilesInTree(new File("test"), "*.txt").size());

		new File("test/subdir/text.txt").delete();
		new File("test/subdir").delete();
		new File("test/text.txt").delete();
		new File("test").delete();

	}

	@Secret
	String secret = "abcdefg";
	String pwd = "123456";
	String field = "value";

	@Test
	@Order(3)
	void clog() throws Exception {

		log.info("\n\nclog()\n\n");

		assertEquals("Class@java.lang.String", CLog.forAnalyticLogging(String.class), "analytic log class");
		assertEquals("'a'", CLog.forAnalyticLogging('a'), "analytic log character");
		assertEquals("java.sql.Date@1970-01-01", CLog.forAnalyticLogging(new java.sql.Date(0L)), "analytic log sql date");
		assertEquals("java.sql.Time@01:00:00.000", CLog.forAnalyticLogging(new java.sql.Time(0L)), "analytic log sql time");
		assertEquals("java.sql.Timestamp@1970-01-01 01:00:00.000", CLog.forAnalyticLogging(new java.sql.Timestamp(0L)), "analytic log Java timestamp");
		assertEquals("java.util.Date@1970-01-01 01:00:00.000", CLog.forAnalyticLogging(new java.util.Date(0L)), "analytic log util date");
		assertEquals("java.time.LocalDateTime", Common.untilFirst(CLog.forAnalyticLogging(LocalDateTime.now()), "@"), "analytic log local datetime");
		assertEquals("java.time.LocalDate", Common.untilFirst(CLog.forAnalyticLogging(LocalDate.now()), "@"), "analytic log local date");
		assertEquals("java.time.LocalTime", Common.untilFirst(CLog.forAnalyticLogging(LocalTime.now()), "@"), "analytic log local time");
		assertEquals("java.util.GregorianCalendar", Common.untilFirst(CLog.forAnalyticLogging(new GregorianCalendar()), "@"), "analytic log local time");

		assertEquals("\"******\"", CLog.forSecretLogging(CommonTest.class.getDeclaredField("secret"), secret), "secret field logging");
		assertEquals("\"******\"", CLog.forSecretLogging(CommonTest.class.getDeclaredField("pwd"), pwd), "password field logging");
		assertEquals("\"******\"", CLog.forSecretLogging(CommonTest.class.getDeclaredField("pwd"), ""), "empty field logging");
		assertEquals("\"value\"", CLog.forSecretLogging(CommonTest.class.getDeclaredField("field"), field), "non-secret field logging");
		assertEquals("\"******\"", CLog.forSecretLogging("DOM_SEC_TABLE", "ANY_COLUMN", "secret_value"), "secret table logging");
		assertEquals("\"******\"", CLog.forSecretLogging("DOM_ANY_TABLE", "SEC_COLUMN", "secret_value"), "secret column logging");
		assertEquals("\"value\"", CLog.forSecretLogging("DOM_ANY_TABLE", "ANY_COLUMN", "value"), "non-secret column logging");
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(4)
	void reflection() throws Exception {

		log.info("\n\nreflection()\n\n");

		assertEquals(Client.class, CReflection.loadClass("com.icx.dom.app.bikestore.domain.client.Client"), "load class");
		assertEquals(Client.class, CReflection.loadClass("Client"), "load class");
		assertEquals(RegionInProgress.class, CReflection.loadInnerClass("RegionInProgress", Client.class), "load inner class");

		CReflection.retrieveLoadedPackageNames();
		assertTrue(CReflection.getLoadedPackageNames().size() > 100, "loaded packages");

		assertNotNull(CReflection.getClassesDir(BikeStoreApp.class).getPath(), "classes dir");
	}

	@SuppressWarnings("static-method")
	@Test
	@Order(5)
	void properties() throws Exception {

		log.info("\n\nproperties()\n\n");

		File propFile = CResource.findFirstJavaResourceFile("test.properties");
		assertNotNull(propFile);

		propFile = CProp.findPropertiesFile("test.properties");
		Properties props = CProp.readProperties(propFile);

		assertEquals("abc", CProp.getStringProperty(props, "s", ""), "string property");
		assertEquals("", CProp.getStringProperty(props, "s1", ""), "default string property");

		assertEquals(true, CProp.getBooleanProperty(props, "b", false), "boolean property");
		assertEquals(false, CProp.getBooleanProperty(props, "b1", false), "default boolean property");

		assertEquals(-1, CProp.getIntProperty(props, "i", 0), "int property");
		assertEquals(-1, CProp.getIntProperty(props, "i1", -1), "default int property");

		assertEquals(100000000000000L, CProp.getLongProperty(props, "l", -1L), "long property");
		assertEquals(-1L, CProp.getLongProperty(props, "l1", -1L), "default long property");

		assertEquals(2.5, CProp.getDoubleProperty(props, "d", -1.01), "double property");
		assertEquals(-1.01, CProp.getDoubleProperty(props, "d1", -1.01), "default double property");

		assertEquals(CList.newList("a", "", null), CProp.getProperty(props, List.class, "list", null), "list property");
		assertEquals(null, CProp.getProperty(props, List.class, "list1", null), "default list property");

		assertEquals(CMap.newMap("a", "A", "b", "", "c", null), CProp.getProperty(props, Map.class, "map", null), "map property");
		assertEquals(null, CProp.getProperty(props, Map.class, "map1", null), "default map property");

		assertNotNull(CProp.findPropertiesFile("equal.properties"), "equal properties files");
		assertNull(CProp.findPropertiesFile("unequal.properties"), "unequal properties files");
		assertNull(CProp.findPropertiesFile("nonexistent.properties"), "non-existent properties file");
	}

}
