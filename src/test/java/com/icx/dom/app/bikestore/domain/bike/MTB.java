package com.icx.dom.app.bikestore.domain.bike;

import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.domain.DomainAnnotations.SqlColumn;
import com.icx.domain.DomainAnnotations.SqlTable;

@SqlTable(name = "DOM_MTB") // Auto generated table names are prefixed with 'DOM_' - so we use same prefix here for table name (automatically DOM_M_T_B would be generated)
public class MTB extends Bike {

	// Types

	public enum Type {
		CROSS_COUNTRY, DOWNHILL
	}

	public enum Suspension {
		FRONT, FULL
	}

	public enum WheelSize {
		W26, W27_5, W29
	}

	// Members

	// 'TYPE' may be forbidden as column name; you may also use this annotation if you want to change field name without changing column name to keep existing table
	@SqlColumn(name = "BIKE_TYPE")
	public Type type = Type.CROSS_COUNTRY;

	public Suspension suspension = Suspension.FULL;

	public WheelSize wheelSize = WheelSize.W27_5;

	// Constructors

	public MTB() { // Default constructor must exist! It will be used by domain controller on loading objects from database
	}

	public MTB(
			Manufacturer manufacturer,
			String model,
			Frame frame,
			Breaks breaks,
			int gears,
			double price,
			byte[] picture) {

		super(manufacturer, model, frame, breaks, gears, price, picture);
	}

	// Methods

	public MTB downhill() {
		this.type = Type.DOWNHILL;
		return this;
	}

	public MTB hardtail() {
		this.suspension = Suspension.FRONT;
		return this;
	}

	public MTB wheels(WheelSize wheelSize) {
		this.wheelSize = wheelSize;
		return this;
	}
}
