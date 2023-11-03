package com.icx.dom.app.bikestore;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.app.bikestore.domain.bike.Bike.Size;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.Country;
import com.icx.dom.app.bikestore.domain.client.Client.Gender;
import com.icx.dom.app.bikestore.domain.client.Client.Region;
import com.icx.dom.app.bikestore.domain.client.Client.RegionInUse;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.dom.common.Prop;
import com.icx.dom.domain.sql.SqlDomainController;

/**
 * International bike store. Test simulates bike ordering by different clients, order processing and and bike delivery.
 * 
 * Code demonstrates how to register domain classes and associate them with tables of (existing) persistence database, how to load and save domain objects and also most of the additional features
 * provided by Domain persistence mechanism. See comments in {@code Java2Sql.java} for how to create persistence database from domain classes.
 * 
 * One instance of this test program processes orders from clients of world regions ({@link Client#country}, {@link RegionInUse#region}). So one can run six parallel instances to cover whole world.
 * Parallel database operations are synchronized by database synchronization using unique shadow records for records to update exclusively.
 * 
 * @author RainerBaumgärtel
 */
public class BikeStoreApp {

	static final Logger log = LoggerFactory.getLogger(BikeStoreApp.class);

	public static final File BIKE_PICTURE = new File("src/test/resources/bike.jpg");

	// Delay time between client bike order requests. One client tries to order bikes of 3 different types. Acts also as delay time for start of client order threads.
	public static final long ORDER_DELAY_TIME_MS = 100;

	// Domain controller
	public static SqlDomainController sdc = new SqlDomainController();

	// List of bike models with availabilities for different sizes
	protected static List<Bike> bikes = null;

	static int n = 0;
	static RegionInUse regionInUse = null;

	// To check order processing
	public static class Counters {

		public int numberOfOrdersCreated = 0;
		public int numberOfOrdersCanceledByProcessingTimeout = 0;
		public int numberOfOrdersCanceledByInabilityToPay = 0;
	}

	// Main
	public static void main(String[] args) throws Exception {

		Counters counters = new Counters();

		// Read JDBC and Domain properties. Note: you should not have multiple properties files with same name in your class path
		Properties dbProps = Prop.readEnvironmentSpecificProperties(Prop.findPropertiesFile("db.properties"), "local/mssql/bikes", null);
		Properties domainProps = Prop.readProperties(Prop.findPropertiesFile("domain.properties"));

		// Associate domain classes and database tables
		sdc.initialize(dbProps, domainProps, Manufacturer.class.getPackage().getName() /* use any class directly in 'domain' package to find all domain classes */);

		// Initially load existing domain objects from database - exclude historical data from loading (SELECT statements are performed here)
		sdc.synchronize(Order.class);

		// Force deleting bikes, clients, etc. on first instance (first region)
		boolean cleanupDatabaseOnStartup = false;
		if (!sdc.hasAny(RegionInUse.class, r -> true)) {
			cleanupDatabaseOnStartup = true;
		}

		// Select and reserve world region for this instance
		for (Region reg : Region.values()) {
			if (!sdc.hasAny(RegionInUse.class, r -> r.region == reg)) {
				regionInUse = sdc.createAndSave(RegionInUse.class, r -> r.region = reg);
				break;
			}
		}
		if (regionInUse == null) {
			log.warn("All regions already in use!");
			return;
		}
		else {
			log.info("Process orders for clients in region {}", regionInUse.region);
		}

		// Cleanup database on start of first instance
		if (cleanupDatabaseOnStartup) {
			Initialize.deleteExistingObjects();
			Initialize.createObjects();
		}

		// Sort bikes by manufacturer and model using overridden compareTo() method
		// Note: No difference between all() and allValid() here - but generally, if domain objects exist which are not savable (by database constraint violation), they are marked as invalid
		bikes = sdc.sort(sdc.allValid(Bike.class));

		// Log bike store before ordering/buying bikes...
		// Note: use groupBy() to group accumulated children by classifier or countBy() to get # of accumulated children grouped by classifier
		bikes.forEach(b -> log.info("'{}': price: {}€, sizes: {}, availability: {}, orders: {}'", b, b.price, b.sizes, b.availabilityMap, sdc.countBy(b.orders, o -> o.client.bikeSize)));

		// Start order processing and bike delivery thread
		Thread orderProcessingThread = new Thread(new Order.SendInvoices());
		orderProcessingThread.setName("--ORDER--");
		orderProcessingThread.start();

		Thread bikeDeliveryThread = new Thread(new Order.DeliverBikes());
		bikeDeliveryThread.setName("-DELIVER-");
		bikeDeliveryThread.start();

		// Create clients for region and start client threads to order bikes
		List<Thread> clientThreads = new ArrayList<>();
		for (Country country : RegionInUse.getRegionCountryMap().get(regionInUse.region)) {
			for (String clientName : Client.getCountryNamesMap().get(country)) {

				// Use create() method of domain controller to instantiate, initialize and register new object or createAndSave() to additionally save object immediately after registration.
				// Logical initialization by init routine will be performed before object registration.
				// Note: Alternatively you may use specific constructors for object creation and register and save objects there or after creation explicitly - see examples in Initialize.java

				// Create client
				Client client = sdc.create(Client.class, c -> c.init(clientName, (n < 5 ? Gender.MALE : Gender.FEMALE), country, Size.values()[n % 5], 1000.0 * (1 + n % 20)));
				n++;

				// Save client
				sdc.save(client);

				// Create client thread to order bikes
				Thread clientThread = new Thread(client.new OrderBikes(client, counters));
				clientThread.setName(client.firstName);
				clientThread.start();
				clientThreads.add(clientThread);

				Thread.sleep(ORDER_DELAY_TIME_MS);
			}
		}

		// Wait until ordering bikes is completed...
		for (Thread thread : clientThreads) {
			thread.join();
			if (log.isDebugEnabled()) {
				log.debug("Joined: '{}'", thread.getName());
			}
		}

		// Force ending order threads
		orderProcessingThread.interrupt();
		orderProcessingThread.join();

		bikeDeliveryThread.interrupt();
		bikeDeliveryThread.join();

		// Log results
		int numberOfPendingOrders = (int) sdc.count(Order.class, o -> RegionInUse.getRegion(o.client.country) == regionInUse.region && o.payDate != null && o.deliveryDate == null);
		int numberOfInvoicesSent = (int) sdc.count(Order.class, o -> RegionInUse.getRegion(o.client.country) == regionInUse.region && o.invoiceDate != null);
		int numberOfDeliveryNotesSent = (int) sdc.count(Order.class, o -> RegionInUse.getRegion(o.client.country) == regionInUse.region && o.deliveryDate != null);
		log.info("Order processing and bike delivery completed for {} clients of region {}. Total # of orders generated: {}", sdc.count(Client.class, c -> true), regionInUse.region,
				counters.numberOfOrdersCreated);
		log.info("# of invoices sent: {}", numberOfInvoicesSent);
		log.info("# of bikes delivered: {}", numberOfDeliveryNotesSent);
		log.info("# of pending orders: {}", numberOfPendingOrders);
		log.info("# of orders canceled by inability to pay: {}", counters.numberOfOrdersCanceledByInabilityToPay);
		log.info("# of orders canceled by order processing timeout: {}", counters.numberOfOrdersCanceledByProcessingTimeout);
		log.info("Order processing time statistic (# of order processing operations in less than 10, 20, 30, 40, 50, 60, 70 ms): {}", Order.orderProcessingDurationMap);
		log.info("Bike delivery time statistic (# of bike delivery operations in less than 10, 20, 30, 40, 50, 60, 70 ms): {}", Order.bikeDeliveryDurationMap);

		// Check results
		int sumOfCounters = numberOfDeliveryNotesSent + counters.numberOfOrdersCanceledByInabilityToPay + counters.numberOfOrdersCanceledByProcessingTimeout + numberOfPendingOrders;
		if (sumOfCounters != counters.numberOfOrdersCreated) {
			log.error("");
			log.error("Sum of # of delivered bikes, canceled and pending orders {} differs from total # of orders created {}!", sumOfCounters, counters.numberOfOrdersCreated);
			log.error("");
		}

		// // Free region in use (to allow re-run test with multiple parallel instances)
		sdc.delete(regionInUse);

		// Close open database connections
		sdc.sqlDb.close();
	}
}
