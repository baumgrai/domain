package com.icx.dom.app.bikestore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.CFile;
import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.app.bikestore.domain.bike.Bike.Breaks;
import com.icx.dom.app.bikestore.domain.bike.Bike.Frame;
import com.icx.dom.app.bikestore.domain.bike.Bike.Size;
import com.icx.dom.app.bikestore.domain.bike.CityBike;
import com.icx.dom.app.bikestore.domain.bike.MTB;
import com.icx.dom.app.bikestore.domain.bike.MTB.WheelSize;
import com.icx.dom.app.bikestore.domain.bike.RaceBike;
import com.icx.dom.app.bikestore.domain.bike.RaceBike.GroupSet;
import com.icx.dom.app.bikestore.domain.bike.RaceBike.Rim;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.Country;
import com.icx.dom.app.bikestore.domain.client.Client.RegionInProgress;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.domain.sql.FieldError;
import com.icx.jdbc.ConfigException;
import com.icx.jdbc.SqlConnection;
import com.icx.jdbc.SqlDbException;

public class Initialize {

	static final Logger log = LoggerFactory.getLogger(Initialize.class);

	// Delete clients, manufacturers and bikes
	public static void deleteExistingObjects() throws SQLException, SqlDbException {

		log.info("Delete existing objects...");

		// Note: To speed up initialization use one transaction to delete all existing objects. Potential exceptions on DELETE will stop test

		try (SqlConnection sqlcn = SqlConnection.open(BikeStoreApp.sdc.getPool(), false)) {

			// Delete means: remove Java object from object store and DELETE object records in database, and do so for all children too (behaves like ON DELETE CASCADE)!
			for (RegionInProgress regionInProgress : BikeStoreApp.sdc.all(RegionInProgress.class)) {
				BikeStoreApp.sdc.delete(sqlcn.cn, regionInProgress);
			}

			for (Order.InProgress orderInProgress : BikeStoreApp.sdc.all(Order.InProgress.class)) {
				BikeStoreApp.sdc.delete(sqlcn.cn, orderInProgress);
			}

			for (Client client : BikeStoreApp.sdc.all(Client.class)) {
				BikeStoreApp.sdc.delete(sqlcn.cn, client);
			}

			for (Manufacturer manufacturer : BikeStoreApp.sdc.all(Manufacturer.class)) {
				BikeStoreApp.sdc.delete(sqlcn.cn, manufacturer);
			}

			// Note: deleting bikes explicitly is not necessary because Bike references Manufacturer with NOT NULL constraint and so all bikes will be deleted by deleting manufacturers
		}

		log.info("Existing objects deleted.");
	}

	static int n = 0;

	// Create manufacturers and bikes
	public static void createBikeStock() throws ConfigException, SQLException, SqlDbException, IOException {

		log.info("Create objects...");

		// There are multiple ways to assign objects to Domain persistence system:

		// I: Use domain controllers #createAndSave() method with init routine which acts as logical constructor

		// Create and save manufacturers
		Manufacturer bianchi = BikeStoreApp.sdc.createAndSave(Manufacturer.class, m -> {
			m.name = "Bianchi";
			m.country = Country.ITALY;
		});
		Manufacturer colnago = BikeStoreApp.sdc.createAndSave(Manufacturer.class, m -> m.init("Colnago", Country.ITALY));
		Manufacturer cervelo = BikeStoreApp.sdc.createAndSave(Manufacturer.class, m -> m.init("Cervélo", Country.ITALY));
		Manufacturer derosa = BikeStoreApp.sdc.createAndSave(Manufacturer.class, m -> m.init("De Rosa", Country.ITALY));
		Manufacturer peugeot = BikeStoreApp.sdc.createAndSave(Manufacturer.class, m -> m.init("Peugeot", Country.FRANCE));
		Manufacturer lapierre = BikeStoreApp.sdc.createAndSave(Manufacturer.class, m -> m.init("Lapierre", Country.FRANCE));

		// II: Use domain controllers #create() method and save objects afterwards (or rely on automatically saving objects on saving children (bikes) which is not really recommended)

		Manufacturer canyon = BikeStoreApp.sdc.create(Manufacturer.class, m -> m.init("Canyon", Country.GERMANY));
		canyon.save(); // object's #save() can be used here because object is already registered if it was created by domain controllers #create()
		Manufacturer cannondale = BikeStoreApp.sdc.create(Manufacturer.class, m -> m.init("Cannondale", Country.UNITED_STATES));
		cannondale.save();
		Manufacturer trek = BikeStoreApp.sdc.create(Manufacturer.class, m -> m.init("Trek", Country.UNITED_STATES));
		Manufacturer specialized = BikeStoreApp.sdc.create(Manufacturer.class, m -> m.init("Specialized", Country.UNITED_STATES));
		Manufacturer marin = BikeStoreApp.sdc.create(Manufacturer.class, m -> m.init("Marin", Country.UNITED_STATES));

		// III: Create objects by individual constructor and (register and) save them afterwards (or rely on automatically registering and saving objects on saving children)
		// Note: If you define individual constructors you have to define the parameterless default constructor too, which is used by domain controller to instantiate objects loaded from database

		Manufacturer santacruz = new Manufacturer("Santa Cruz", Country.UNITED_STATES);
		BikeStoreApp.sdc.save(santacruz); // Note: domain controller's #save() must be used if object was created by constructor - because it must be registered before saving...
		Manufacturer scott = new Manufacturer("Scott", Country.SWITZERLAND);
		BikeStoreApp.sdc.register(scott); // ...or register object explicitly and then use object's #save()
		scott.save();
		Manufacturer koga = new Manufacturer("Koga", Country.NETHERLANDS);
		Manufacturer rockymountain = new Manufacturer("Rocky Mountain", Country.CANADA);
		Manufacturer bmc = new Manufacturer("BMC", Country.SWITZERLAND);
		// Note: objects, which were only created by constructor but which are not registered, are not known to domain controller and cannot be found using DomainController#findAll(), etc.

		byte[] picture = CFile.readBinary(BikeStoreApp.BIKE_PICTURE);

		List<Bike> bikes = new ArrayList<>();

		bikes.add(new RaceBike(bianchi, "SPECIALISSIMA", Frame.CARBON, Breaks.DISK, 24, 11449.0, picture).groups(GroupSet.SHIMANO, GroupSet.CAMPAGNOLO, GroupSet.SRAM).allSizes());
		bikes.add(new RaceBike(colnago, "C69 Titanium", Frame.CARBON, null, 0, 6430.0, picture).frameOnly().groups(GroupSet.SHIMANO, GroupSet.CAMPAGNOLO, GroupSet.SRAM).allSizes());
		bikes.add(new RaceBike(cervelo, "S5 Rival eTap AXS", Frame.CARBON, Breaks.DISK, 24, 13000.0, picture).aero().groups(GroupSet.SRAM).electricGearShift().allSizes());
		bikes.add(new RaceBike(derosa, "Corum", Frame.STEEL, Breaks.DISK, 24, 6588.0, picture).groups(GroupSet.CAMPAGNOLO).setWeight(8.5).allSizes());
		bikes.add(
				new RaceBike(canyon, "Ultimate CFR Di2", Frame.CARBON, Breaks.DISK, 24, 10499.0, picture).groups(GroupSet.SHIMANO).electricGearShift().setWeight(6.2).setSizes(Size.S, Size.M, Size.L));
		bikes.add(new RaceBike(cannondale, "CAAD Optimo 1", Frame.ALLOY, Breaks.CALIPER, 22, 1499.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).allSizes());
		bikes.add(new RaceBike(cannondale, "CAAD Disk Women's", Frame.ALLOY, Breaks.DISK, 22, 2299.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).forWoman().setSizes(Size.XS, Size.S, Size.M));
		bikes.add(new RaceBike(specialized, "Allez Elite", Frame.ALLOY, Breaks.CALIPER, 22, 1800.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).setSizes(Size.M));
		bikes.add(new RaceBike(trek, "Domane SL 5", Frame.ALLOY, Breaks.CALIPER, 22, 3249.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).forWoman().setWeight(9.2).setSizes(Size.S));
		bikes.add(new RaceBike(bmc, "Roadmachine X One petrol", Frame.ALLOY, Breaks.DISK, 12, 5999.0, picture).groups(GroupSet.SHIMANO, GroupSet.SRAM).setWeight(8.6).setSizes(Size.M, Size.L));

		bikes.add(new CityBike(peugeot, "LC01 N7", Frame.STEEL, Breaks.CALIPER, 7, 699.0, picture).hasMudguards().forWoman().setWeight(16.1).setSizes(Size.S, Size.M));
		bikes.add(new CityBike(peugeot, "LR01 Legend", Frame.STEEL, Breaks.CALIPER, 16, 749.0, picture).hasLight().setWeight(11.3).setSizes(Size.S, Size.M, Size.L));
		bikes.add(new CityBike(specialized, "City Sirrus X 4.0", Frame.ALLOY, Breaks.DISK, 12, 1850.0, picture).isClassic().setSizes(Size.S, Size.L));
		bikes.add(new CityBike(koga, "F3 7.0", Frame.ALLOY, Breaks.DISK, 30, 2199.0, picture).forWoman().setWeight(15.0).allSizes());

		bikes.add(new MTB(lapierre, "Edge 5.7 Women", Frame.ALLOY, Breaks.DISK, 16, 759.0, picture).forWoman().setSizes(Size.XS, Size.S, Size.M));
		bikes.add(new MTB(lapierre, "Trekking 1.0", Frame.ALLOY, Breaks.CALIPER, 21, 499.0, picture).setWeight(14.3).setSizes(Size.S, Size.M, Size.L, Size.XL));
		bikes.add(new MTB(scott, "Contessa Spark RC", Frame.CARBON, Breaks.DISK, 12, 7999.0, picture).wheels(WheelSize.W29).forWoman().setWeight(10.7).setSizes(Size.S, Size.L));
		bikes.add(new MTB(trek, "Top Fuel 7", Frame.ALLOY, Breaks.DISK, 12, 3399.0, picture).wheels(WheelSize.W29).setWeight(14.4).setSizes(Size.S, Size.M, Size.L));
		bikes.add(new MTB(canyon, "Grand Canyon 9", Frame.ALLOY, Breaks.DISK, 12, 1049.0, picture).wheels(WheelSize.W27_5).hardtail().setWeight(13.3).allSizes());
		bikes.add(new MTB(cannondale, "Habit 4", Frame.ALLOY, Breaks.DISK, 12, 2899.0, picture).wheels(WheelSize.W29).setWeight(13.3).setSizes(Size.S, Size.M, Size.L, Size.XL));
		bikes.add(new MTB(specialized, "Epic EVO", Frame.CARBON, Breaks.DISK, 12, 4500.0, picture).wheels(WheelSize.W29).setSizes(Size.S, Size.L));
		bikes.add(new MTB(marin, "Wildcat Trail 3", Frame.ALLOY, Breaks.DISK, 16, 825.0, picture).wheels(WheelSize.W27_5).forWoman().setSizes(Size.XS, Size.S, Size.M, Size.L));
		bikes.add(new MTB(santacruz, "Tallboy", Frame.CARBON, Breaks.DISK, 12, 5399.0, picture).wheels(WheelSize.W29).downhill().setWeight(14.0).allSizes());
		bikes.add(new MTB(rockymountain, "Instinct Alloy 10", Frame.ALLOY, Breaks.DISK, 12, 2607.14, picture).wheels(WheelSize.W27_5).forWoman().setWeight(16.0).setSizes(Size.XS, Size.S));

		// Save new bike objects

		// Note: it's generally recommended to save new and changed objects as soon as possible to avoid unsaved invalid objects in object store
		// Note: if an object is saved which has an parent object which is not yet stored in persistence database this parent object is saved automatically too before saving object itself (to have
		// valid child/parent relation realized by FOREIGN KEY column with parent id in database)

		try (SqlConnection sqlcn = SqlConnection.open(BikeStoreApp.sdc.getPool(), false)) {
			for (Bike bike : bikes) {

				for (Size size : bike.sizes) {
					bike.availabilityMap.put(size, BikeStoreApp.AVAILABLE_BIKES);
				}

				BikeStoreApp.sdc.save(sqlcn.cn, bike);
			}
		}

		// Check object validity (formal here)
		if (BikeStoreApp.sdc.all(Bike.class).stream().anyMatch(b -> !b.isValid())) {
			List<FieldError> fieldErrors = BikeStoreApp.sdc.all(Bike.class).stream().filter(b -> !b.isValid()).flatMap(b -> b.getErrorsAndWarnings().stream()).collect(Collectors.toList());
			throw new ConfigException("Not all bikes could be saved! (" + fieldErrors + ")");
		}

		log.info("Objects created and saved.");
	}
}
