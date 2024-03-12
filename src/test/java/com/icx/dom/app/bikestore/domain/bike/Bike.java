package com.icx.dom.app.bikestore.domain.bike;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

import com.icx.common.CList;
import com.icx.common.Common;
import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.domain.DomainObject;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Accumulation;
import com.icx.domain.sql.Annotations.SqlColumn;
import com.icx.domain.sql.Annotations.SqlTable;

/**
 * Bike model with properties and availability by size.
 * 
 * @author baumgrai
 */
@SqlTable(uniqueConstraints = { "manufacturer, model" }) // Define multi column constraints here
public abstract class Bike extends SqlDomainObject {

	// Helper domain class for exclusive allocation by one domain controller instance (to avoid ordering more bikes than available)
	public static class InProgress extends SqlDomainObject {
	}

	// Types

	public enum Size {
		XS, S, M, L, XL
	}

	public enum Frame {
		STEEL, ALLOY, CARBON, TITANIUM
	}

	public enum Breaks {
		CALIPER, DISK
	}

	// Data members

	// Define text column size different from default 512 - increasing size forces awareness of DB specific limits!
	@SqlColumn(notNull = true, charsize = 64)
	public String model;

	@SqlColumn(isText = true)
	public String description;

	public Frame frame; // Enum field -> text column (VARCHAR, TEXT, etc.)
	public Breaks breaks;

	public int gears; // Primitive type forces NOT NULL constraint

	public Double weight; // null for unknown

	public boolean isForWoman = false; // boolean/Boolean field -> text column with 'true' or 'false', primitive type 'boolean' forces NOT NULL constraint

	// Note: Set, List and Map fields are automatically initialized (with empty collections/maps) on object registration (but only if they are not initialized explicitly here)

	// Lists, Sets and Maps will be stored in separate 'entry' tables - there is no column associated with a List/Set or Map field
	public List<Size> sizes;

	public SortedMap<Size, Integer> availabilityMap;

	@SqlColumn(notNull = true) // Forces NOT NULL constraint for assigned column
	public BigDecimal price;

	public byte[] picture; // byte array forces BLOB or related column type

	// References (to other domain objects)

	@SqlColumn(notNull = true)
	public Manufacturer manufacturer; // Reference to other domain object will be realized in database by FOREIGN KEY to (automatically generated) object/record id

	// Accumulations

	// Accumulations are half-automatically maintained sets of child objects and will not be stored in database. An accumulation field in a parent object class requires a corresponding reference field
	// in the child object class. Any reference change to a parent object is reflected in accumulation of parent object after updateAccumulationsOfParentObjects() on child object is called or - better
	// - child is saved (where this method is called internally).
	// Note: For parents of multiple different children you may use DomainController#groupBy() to group parent child relation by relations to another children.

	@Accumulation
	public Set<Order> orders;

	// Constructors

	// Default constructor must exist. It will be used by domain controller for instantiation of objects loaded from database and within create() and createAndSave() methods.
	// You have to define default constructor explicitly only if there are specific constructor(s) too!
	public Bike() {
	}

	// Application specific constructor with internal registration
	public Bike(
			Manufacturer manufacturer,
			String model,
			Frame frame,
			Breaks breaks,
			int gears,
			double price,
			byte[] picture) {

		this.manufacturer = manufacturer;
		this.model = model;
		this.frame = frame;
		this.breaks = breaks;
		this.gears = gears;
		this.price = BigDecimal.valueOf(price);
		this.picture = picture;
	}

	// Methods

	@Override
	public String toString() {
		return (getClass().getSimpleName() + ": " + manufacturer + " " + model + " (" + price + "$)");
	}

	@Override
	public int compareTo(DomainObject o) {
		return Common.compare(getClass().getSimpleName(), ((Bike) o).getClass().getSimpleName()) + Common.compare(price, ((Bike) o).price);
	}

	public Bike forWoman() {
		this.isForWoman = true;
		return this;
	}

	public Bike setWeight(double weight) {
		this.weight = weight;
		return this;
	}

	public Bike setSizes(Size... sizes) {
		this.sizes = CList.newList(sizes);
		return this;
	}

	public Bike allSizes() {
		this.sizes = CList.newList(Size.XS, Size.S, Size.M, Size.L, Size.XL);
		return this;
	}
}
