package com.icx.dom.app.bikestore.domain;

import java.util.Set;

import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.app.bikestore.domain.client.Client.Country;
import com.icx.domain.DomainAnnotations.Accumulation;
import com.icx.domain.DomainAnnotations.SqlColumn;
import com.icx.domain.sql.SqlDomainObject;

public class Manufacturer extends SqlDomainObject {

	// Data members

	@SqlColumn(unique = true, notNull = true) // Forces single column UNIQUE constraint
	public String name;

	public Country country;

	// Accumulations

	@Accumulation
	public Set<Bike> bikes; // See Bike class for explanation of accumulation

	// Constructors

	public Manufacturer() { // Default constructor must exist! It will be used by domain controller for instantiation of objects loaded from database and within create() and createAndSave() methods.
	}

	// Application specific constructor without internal domain object registration - instantiated object must be registered afterwards!
	public Manufacturer(
			String name,
			Country country) {

		this.name = name;
		this.country = country;
	}

	// Methods

	@Override
	public String toString() {
		return name;
	}

}
