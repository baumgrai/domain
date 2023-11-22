package com.icx.dom.app.bikestore.domain.bike;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.bikestore.BikeStoreApp;
import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.dom.common.CList;
import com.icx.dom.common.Common;
import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.SqlTable;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.sql.SqlDomainObject;

/**
 * Bike model with properties and availability by size.
 * 
 * @author RainerBaumgärtel
 */
@SqlTable(uniqueConstraints = { "manufacturer, model" }) // Define multi column constraints here
public abstract class Bike extends SqlDomainObject {

	static final Logger log = LoggerFactory.getLogger(Bike.class);

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

	// Define text column size different from default 256 - extending size forces awareness of DB specific limits!
	@SqlColumn(notNull = true, charsize = 64)
	public String model;

	@SqlColumn(isText = true)
	public String description;

	public Frame frame; // Enum field -> text column (VARCHAR, TEXT, etc.)
	public Breaks breaks;

	public int gears; // Primitive type forces NOT NULL constraint

	public Double weight; // null for unknown

	public boolean isForWoman = false; // boolean/Boolean field -> text column with 'TRUE' or 'FALSE', primitive type 'boolean' forces NOT NULL constraint

	// Note: Set, List and Map fields are automatically initialized (with empty collections/maps) on object registration (but only if they are not initialized explicitly here)

	// Lists, Sets and Maps will be stored in separate 'entry' tables - there is no column associated with a List/Set or Map field
	public List<Size> sizes;

	public Map<Size, Integer> availabilityMap;

	@SqlColumn(notNull = true) // Forces NOT NULL constraint for assigned column
	public BigDecimal price;

	public byte[] picture; // byte array forces BLOB

	// References (to other domain objects)

	@SqlColumn(notNull = true)
	public Manufacturer manufacturer; // Reference to other domain object will be realized in database by FOREIGN KEY to (automatically generated) record id

	// Accumulations

	// Accumulations are half-automatically maintained sets of child objects and will not be stored in database. An accumulation field in a parent object class requires a corresponding reference field
	// in the child object class. Any reference change to a parent object is reflected in accumulation of parent object after updateAccumulationsOfParentObjects() on child object is called or - better
	// - child is saved (where this method is called internally).
	// Note: For parents of multiple different children you may use DomainController#groupBy() to group parent child relation by relations to another children.

	@Accumulation
	public Set<Order> orders;

	// Constructors

	// Default constructor must exist. It will be used by domain controller for instantiation of objects loaded from database.
	// Note: If you have specific constructor(s) you have to define default constructor explicitly!
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

		BikeStoreApp.sdc.register(this);
	}

	// Methods

	@Override
	public String toString() {
		return (manufacturer + " " + model + " (" + price + "$)");
	}

	@Override
	public int compareTo(DomainObject o) {
		return Common.compare(manufacturer + model, ((Bike) o).manufacturer + ((Bike) o).model);
	}

	@Override
	public boolean canBeDeleted() {
		return (availabilityMap.isEmpty() || availabilityMap.values().stream().allMatch(v -> v == 0));
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

	public void incrementAvailableCount(Bike.Size bikeSize) throws Exception {
		BikeStoreApp.sdc.computeExclusivelyOnObject(this, Bike.InProgress.class, b -> ((Bike) b).availabilityMap.put(bikeSize, ((Bike) b).availabilityMap.get(bikeSize) + 1));
	}

}
