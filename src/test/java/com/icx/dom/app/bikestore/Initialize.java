package com.icx.dom.app.bikestore;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.icx.dom.common.CFile;
import com.icx.dom.domain.sql.FieldError;
import com.icx.dom.jdbc.ConfigException;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDbException;

public class Initialize {

	static final Logger log = LoggerFactory.getLogger(Initialize.class);

	// Delete clients, manufacturers and bikes
	public static void deleteExistingObjects() throws SQLException, SqlDbException {

		log.info("Delete existing objects...");

		// Note: To speed up initialization use one transaction to delete all existing objects. So possible exceptions on DELETE will stop test

		try (SqlConnection sqlcn = SqlConnection.open(BikeStoreApp.sdc.sqlDb.pool, false)) {

			// Delete means: remove Java object from object store and DELETE object records in database, and do so for all children too (behaves like ON DELETE CASCADE)!
			for (Client client : BikeStoreApp.sdc.all(Client.class)) {
				BikeStoreApp.sdc.delete(sqlcn.cn, client);
			}

			for (Manufacturer manufacturer : BikeStoreApp.sdc.all(Manufacturer.class)) {
				BikeStoreApp.sdc.delete(sqlcn.cn, manufacturer);
			}

			// Note: deleting bikes explicitly is not necessary because Bike references Manufacturer with NOT NULL constraint and so all bikes will be deleted by deleting manufacturers

			// Note: transaction will automatically be committed on closing connection
		}

		log.info("Existing objects deleted.");
	}

	static int n = 0;

	// Create manufacturers and bikes
	public static void createObjects() throws ConfigException, SQLException, SqlDbException, IOException {

		log.info("Create objects...");

		// There are three ways to assign objects to Domain persistence system:

		// I: Use any specific constructor for object instantiation here
		// Note: If you define individual constructors you have to define the parameterless default constructor too, which is used by domain controller to instantiate objects loaded from database

		// Create and register manufacturers - explicit registration
		Manufacturer bianchi = BikeStoreApp.sdc.register(new Manufacturer("Bianchi", Country.ITALY));
		Manufacturer colnago = BikeStoreApp.sdc.register(new Manufacturer("Colnago", Country.ITALY));
		Manufacturer cervelo = BikeStoreApp.sdc.register(new Manufacturer("Cervélo", Country.ITALY));
		Manufacturer derosa = BikeStoreApp.sdc.register(new Manufacturer("De Rosa", Country.ITALY));
		Manufacturer peugeot = BikeStoreApp.sdc.register(new Manufacturer("Peugeot", Country.FRANCE));
		Manufacturer lapierre = BikeStoreApp.sdc.register(new Manufacturer("Lapierre", Country.FRANCE));
		Manufacturer canyon = BikeStoreApp.sdc.register(new Manufacturer("Canyon", Country.GERMANY));
		Manufacturer cannondale = BikeStoreApp.sdc.register(new Manufacturer("Cannondale", Country.UNITED_STATES));
		Manufacturer trek = BikeStoreApp.sdc.register(new Manufacturer("Trek", Country.UNITED_STATES));
		Manufacturer specialized = BikeStoreApp.sdc.register(new Manufacturer("Specialized", Country.UNITED_STATES));
		Manufacturer marin = BikeStoreApp.sdc.register(new Manufacturer("Marin", Country.UNITED_STATES));
		Manufacturer santacruz = BikeStoreApp.sdc.register(new Manufacturer("Santa Cruz", Country.UNITED_STATES));
		Manufacturer scott = BikeStoreApp.sdc.register(new Manufacturer("Scott", Country.SWITZERLAND));
		Manufacturer koga = BikeStoreApp.sdc.register(new Manufacturer("Koga", Country.NETHERLANDS));
		Manufacturer rockymountain = BikeStoreApp.sdc.register(new Manufacturer("Rocky Mountain", Country.CANADA));
		Manufacturer bmc = BikeStoreApp.sdc.register(new Manufacturer("BMC", Country.SWITZERLAND));

		// Save manufacturers - INSERTs are performed here

		// Note: To speed up initialization use one transaction to save multiple new objects. Transaction will automatically be committed on closing connection
		try (SqlConnection sqlcn = SqlConnection.open(BikeStoreApp.sdc.sqlDb.pool, false)) {
			for (Manufacturer manufacturer : BikeStoreApp.sdc.all(Manufacturer.class)) {
				BikeStoreApp.sdc.save(sqlcn.cn, manufacturer);
			}
		}

		// II: Use specific constructor for object instantiation which internally registers objects.

		byte[] picture = CFile.readBinary(BikeStoreApp.BIKE_PICTURE);

		// Create and initially save bikes - registration is done by constructor
		new RaceBike(bianchi, "SPECIALISSIMA", Frame.CARBON, Breaks.DISK, 24, 11449.0, picture).groups(GroupSet.SHIMANO, GroupSet.CAMPAGNOLO, GroupSet.SRAM).allSizes();
		new RaceBike(colnago, "C69 Titanium", Frame.CARBON, null, 0, 6430.0, picture).frameOnly().groups(GroupSet.SHIMANO, GroupSet.CAMPAGNOLO, GroupSet.SRAM).allSizes();
		new RaceBike(cervelo, "S5 Rival eTap AXS", Frame.CARBON, Breaks.DISK, 24, 13000.0, picture).aero().groups(GroupSet.SRAM).electricGearShift().allSizes();
		new RaceBike(derosa, "Corum", Frame.STEEL, Breaks.DISK, 24, 6588.0, picture).groups(GroupSet.CAMPAGNOLO).setWeight(8.5).allSizes();
		new RaceBike(canyon, "Ultimate CFR Di2", Frame.CARBON, Breaks.DISK, 24, 10499.0, picture).groups(GroupSet.SHIMANO).electricGearShift().setWeight(6.2).setSizes(Size.S, Size.M, Size.L);
		new RaceBike(cannondale, "CAAD Optimo 1", Frame.ALLOY, Breaks.CALIPER, 22, 1499.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).allSizes();
		new RaceBike(cannondale, "CAAD Disk Women's", Frame.ALLOY, Breaks.DISK, 22, 2299.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).forWoman().setSizes(Size.XS, Size.S, Size.M);
		new RaceBike(specialized, "Allez Elite", Frame.ALLOY, Breaks.CALIPER, 22, 1800.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).setSizes(Size.M);
		new RaceBike(trek, "Domane SL 5", Frame.ALLOY, Breaks.CALIPER, 22, 3249.0, picture).groups(GroupSet.SHIMANO).setRim(Rim.ALLOY).forWoman().setWeight(9.2).setSizes(Size.S);
		new RaceBike(bmc, "Roadmachine X One petrol", Frame.ALLOY, Breaks.DISK, 12, 5999.0, picture).groups(GroupSet.SHIMANO, GroupSet.SRAM).setWeight(8.6).setSizes(Size.M, Size.L);

		new CityBike(peugeot, "LC01 N7", Frame.STEEL, Breaks.CALIPER, 7, 699.0, picture).hasMudguards().forWoman().setWeight(16.1).setSizes(Size.S, Size.M);
		new CityBike(peugeot, "LR01 Legend", Frame.STEEL, Breaks.CALIPER, 16, 749.0, picture).hasLight().setWeight(11.3).setSizes(Size.S, Size.M, Size.L);
		new CityBike(specialized, "City Sirrus X 4.0", Frame.ALLOY, Breaks.DISK, 12, 1850.0, picture).isClassic().setSizes(Size.S, Size.L);
		new CityBike(koga, "F3 7.0", Frame.ALLOY, Breaks.DISK, 30, 2199.0, picture).forWoman().setWeight(15.0).allSizes();

		new MTB(lapierre, "Edge 5.7 Women", Frame.ALLOY, Breaks.DISK, 16, 759.0, picture).forWoman().setSizes(Size.XS, Size.S, Size.M);
		new MTB(lapierre, "Trekking 1.0", Frame.ALLOY, Breaks.CALIPER, 21, 499.0, picture).setWeight(14.3).setSizes(Size.S, Size.M, Size.L, Size.XL);
		new MTB(scott, "Contessa Spark RC", Frame.CARBON, Breaks.DISK, 12, 7999.0, picture).wheels(WheelSize.W29).forWoman().setWeight(10.7).setSizes(Size.S, Size.L);
		new MTB(trek, "Top Fuel 7", Frame.ALLOY, Breaks.DISK, 12, 3399.0, picture).wheels(WheelSize.W29).setWeight(14.4).setSizes(Size.S, Size.M, Size.L);
		new MTB(canyon, "Grand Canyon 9", Frame.ALLOY, Breaks.DISK, 12, 1049.0, picture).wheels(WheelSize.W27_5).hardtail().setWeight(13.3).allSizes();
		new MTB(cannondale, "Habit 4", Frame.ALLOY, Breaks.DISK, 12, 2899.0, picture).wheels(WheelSize.W29).setWeight(13.3).setSizes(Size.S, Size.M, Size.L, Size.XL);
		new MTB(specialized, "Epic EVO", Frame.CARBON, Breaks.DISK, 12, 4500.0, picture).wheels(WheelSize.W29).setSizes(Size.S, Size.L);
		new MTB(marin, "Wildcat Trail 3", Frame.ALLOY, Breaks.DISK, 16, 825.0, picture).wheels(WheelSize.W27_5).forWoman().setSizes(Size.XS, Size.S, Size.M, Size.M);
		new MTB(santacruz, "Tallboy", Frame.CARBON, Breaks.DISK, 12, 5399.0, picture).wheels(WheelSize.W29).downhill().setWeight(14.0).allSizes();
		new MTB(rockymountain, "Instinct Alloy 10", Frame.ALLOY, Breaks.DISK, 12, 2607.14, picture).wheels(WheelSize.W27_5).forWoman().setWeight(16.0).setSizes(Size.XS, Size.S);

		// Assign # of bikes available for sizes
		n = 0;
		for (Bike bike : BikeStoreApp.sdc.all(Bike.class)) {
			for (Size size : bike.sizes) {
				bike.availabilityMap.put(size, 10 + n % 10);
				n++;
			}
		}

		// Save bikes

		// Note: it's generally recommended to save new and changed objects as soon as possible to avoid unsaved invalid objects in object store
		// Note: if an object is saved which has an unsaved parent object this parent object is saved automatically before saving object itself

		try (SqlConnection sqlcn = SqlConnection.open(BikeStoreApp.sdc.sqlDb.pool, false)) {
			for (Bike bike : BikeStoreApp.sdc.all(Bike.class)) {
				BikeStoreApp.sdc.save(sqlcn.cn, bike);
			}
		}

		// Check object validity
		if (BikeStoreApp.sdc.all(Bike.class).stream().anyMatch(b -> !b.isValid())) {
			List<FieldError> fieldErrors = BikeStoreApp.sdc.all(Bike.class).stream().filter(b -> !b.isValid()).flatMap(b -> b.getErrorsAndWarnings().stream()).collect(Collectors.toList());
			throw new ConfigException("Not all bikes could be saved! (" + fieldErrors + ")");
		}

		// Note: On object registration object class fields of type List, Set or Map are initialized by empty ArrayList, HashSet and HashMap objects automatically if they are not already
		// initialized. Accumulation fields will be initialized by empty Concurrent set.

		log.info("Objects created and saved.");
	}
}
