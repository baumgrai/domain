package com.icx.dom.junit.domain;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.icx.dom.junit.domain.sub.X;
import com.icx.domain.DomainAnnotations.Accumulation;
import com.icx.domain.DomainAnnotations.Changed;
import com.icx.domain.DomainAnnotations.Created;
import com.icx.domain.DomainAnnotations.Crypt;
import com.icx.domain.DomainAnnotations.Removed;
import com.icx.domain.DomainAnnotations.Secret;
import com.icx.domain.DomainAnnotations.SqlColumn;
import com.icx.domain.DomainAnnotations.SqlTable;
import com.icx.domain.DomainAnnotations.StoreAsString;
import com.icx.domain.sql.SqlDomainObject;

@Changed(versions = { "1.1:notUnique=i;unique=i&integerValue;indexes=l", "1.2:indexesToDrop=longValue" })
@SqlTable(uniqueConstraints = { "i, integerValue" }, indexes = { "l", "longValue" }) // Multi column constraints
public abstract class A extends SqlDomainObject {

	public static class Inner extends SqlDomainObject {
		A a;
	}

	public static class Stucture {

		public String s = null;
		public int i = 0;

		public Stucture(
				String s,
				int i) {
			this.s = s;
			this.i = i;
		}

		@Override
		public String toString() {
			return s + "/" + i;
		}

		public static Stucture valueOf(String cvString) {
			return new A.Stucture(untilFirst(cvString, "/"), Integer.valueOf(behindFirst(cvString, "/")));
		}
	}

	public enum Type {
		A, B, C
	}

	// Data fields

	@SqlColumn(name = "BOOLEAN")
	public boolean bool;
	public Boolean booleanValue;

	// public byte b = 0; Not supported
	// public Byte byteValue;

	public short sh = 0;
	public Short shortValue;

	public int i = 0;
	public Integer integerValue;

	@Created(version = "1.1")
	public long l;
	@Changed(versions = { "1.1:numericalType=Long" })
	public Long longValue;

	// public float f; Not supported
	// public Float floatValue;

	public double d;
	public Double doubleValue;

	public char c;
	public Character charValue;

	@Crypt // Useless here
	public BigInteger bigIntegerValue;

	public BigDecimal bigDecimalValue;

	public LocalDateTime datetime;
	public LocalDate date;
	public LocalTime time;

	@Created(version = "1.1:isText=true")
	@Changed(versions = { "1.2:isText=false;unique=false", "1.3:charsize=16;unique=true" })
	@SqlColumn(unique = true, charsize = 16)
	public String s;

	@StoreAsString
	public Stucture structure = null;

	public byte[] picture;
	public char[] longtext;

	@SqlColumn(charsize = 1024)
	public File file = null;

	@Changed(versions = { "1.1:notNull=true", "1.2:notNull=false", "1.3:notNull=true" })
	@SqlColumn(notNull = true)
	public Type type = Type.A;

	@Removed(version = "1.1")
	public String removedField;

	@Removed(version = "1.1")
	public C removedRefField;

	@Removed(version = "1.1")
	public List<String> removedCollectionField;

	@Secret
	public String secretString = "!!!secret!!!";

	@Crypt
	public String pwd = "!!!password!!!";

	// Arrays, collections and maps

	public String[] stringArray = new String[] { "a", "äß", null };

	@Created(version = "1.1:collectionType=Set")
	@Changed(versions = { "1.2:collectionType=List" })
	@Secret // Useless here
	public List<String> strings;
	@Changed(versions = { "1.1:collectionType=List", "1.2:collectionType=Set" })
	public Set<Double> doubleSet;
	public Map<String, BigDecimal> bigDecimalMap;

	public List<List<Type>> listOfLists;
	public List<Map<String, Integer>> listOfMaps;
	public Map<Long, List<String>> mapOfLists;
	public Map<String, Map<Type, Boolean>> mapOfMaps;

	// Reference fields

	@Created(version = "1.1")
	@SqlColumn(onDeleteCascade = true)
	public O o;

	// Accumulations

	@Accumulation
	public Set<Inner> inners;

	@Accumulation
	public Set<X> xs;

	// Non registerable fields

	@Deprecated
	public String deprecatedField;

	public Set<X> nonRegisterableField; // Missing @Accumulation annotation

	public transient String transientField;

	public static String staticField;

	@Override
	public String toString() {
		return "A: " + s;
	}
}
