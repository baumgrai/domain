package com.icx.dom.app.bikestore.domain.bike;

import java.util.Set;

import com.icx.dom.app.bikestore.domain.Manufacturer;

public class CityBike extends Bike {

	// Types

	public enum Feature {
		CLASSIC, HAS_LIGHT, HAS_MUDGUARDS
	}

	// Members

	public Set<Feature> features;

	// Methods

	public CityBike hasLight() {
		features.add(Feature.HAS_LIGHT);
		return this;
	}

	public CityBike hasMudguards() {
		features.add(Feature.HAS_MUDGUARDS);
		return this;
	}

	public CityBike isClassic() {
		features.add(Feature.CLASSIC);
		return this;
	}

	// Constructors

	public CityBike() { // Default constructor must exist! It will be used by domain controller on loading objects from database
	}

	public CityBike(
			Manufacturer manufacturer,
			String model,
			Frame frame,
			Breaks breaks,
			int gears,
			double price,
			byte[] picture) {

		super(manufacturer, model, frame, breaks, gears, price, picture);
	}
}
