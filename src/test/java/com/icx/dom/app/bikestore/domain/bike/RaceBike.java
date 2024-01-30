package com.icx.dom.app.bikestore.domain.bike;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.icx.dom.app.bikestore.domain.Manufacturer;

public class RaceBike extends Bike {

	// Types

	public enum GroupSet {
		SHIMANO, CAMPAGNOLO, SRAM
	}

	public enum Rim {
		CARBON, ALLOY
	}

	// Members

	public boolean isAero = false;
	public boolean isFrameOnly = false;

	// 'complex field' (of type Set here) must be initialized explicitly because in will be accessed before bike object was registered by domain controller
	// Note: If object will be registered directly on creation - using #create() or #createAndSave() - explicit initialization of complex fields (Map, Set, List) is not necessary because it will be
	// done automatically on registration
	public Set<GroupSet> availableGroupSets = new HashSet<>();

	public boolean isGearShiftElectric = false;

	public Rim rim = Rim.CARBON;

	// Constructors

	public RaceBike() { // Default constructor must exist! It will be used by domain controller on loading objects from database
	}

	public RaceBike(
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

	public RaceBike aero() {
		this.isAero = true;
		return this;
	}

	public RaceBike frameOnly() {
		this.isFrameOnly = true;
		return this;
	}

	public RaceBike groups(GroupSet... groupSets) {
		this.availableGroupSets = new HashSet<>(Arrays.asList(groupSets));
		return this;
	}

	public RaceBike electricGearShift() {
		this.isGearShiftElectric = true;
		return this;
	}

	public RaceBike setRim(Rim rim) {
		this.rim = rim;
		return this;
	}

}
