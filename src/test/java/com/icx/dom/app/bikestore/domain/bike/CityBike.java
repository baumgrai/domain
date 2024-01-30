package com.icx.dom.app.bikestore.domain.bike;

import java.util.HashSet;
import java.util.Set;

import com.icx.dom.app.bikestore.domain.Manufacturer;

public class CityBike extends Bike {

	// Types

	public enum Feature {
		CLASSIC, HAS_LIGHT, HAS_MUDGUARDS
	}

	// Members

	// 'complex field' (of type Set here) must be initialized explicitly because in will be accessed before bike object was registered by domain controller
	// Note: If object will be registered directly on creation - using #create() or #createAndSave() - explicit initialization of complex fields (Map, Set, List) is not necessary because it will be
	// done automatically on registration
	public Set<Feature> features = new HashSet<>();

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
