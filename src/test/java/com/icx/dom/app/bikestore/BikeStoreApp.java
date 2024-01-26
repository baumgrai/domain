package com.icx.dom.app.bikestore;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Prop;
import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.app.bikestore.domain.bike.Bike.Size;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.Country;
import com.icx.dom.app.bikestore.domain.client.Client.Gender;
import com.icx.dom.app.bikestore.domain.client.Client.Region;
import com.icx.dom.app.bikestore.domain.client.Client.RegionInProgress;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.dom.domain.sql.SqlDomainController;

/**
 * International bike store. Test app simulates bike ordering by different clients, order processing and and bike delivery.
 * 
 * Code demonstrates how to register domain classes and associate them with tables of (existing) persistence database, how to load and save domain objects and also most of the additional features
 * provided by Domain persistence mechanism. See comments in {@code Java2Sql.java} for how to create persistence database from domain classes.
 * 
 * One instance of this test program processes orders from clients of one of six world regions ({@link Client#country}, {@link RegionInProgress#region}). So one can run six parallel instances to cover
 * whole world. Parallel database operations are synchronized by database synchronization using unique shadow 'in-progress' records for records of objects to update exclusively.
 * 
 * @author baumgrai
 */
public class BikeStoreApp {

	static final Logger log = LoggerFactory.getLogger(BikeStoreApp.class);

	public static final File BIKE_PICTURE = new File("src/test/resources/bike.jpg");

	// Delay time between client bike order requests. One client tries to order bikes of 3 different types. Acts also as delay time for start of client's ordering threads.
	public static final long ORDER_DELAY_TIME_MS = 200;

	// Domain controller object
	public static SqlDomainController sdc = new SqlDomainController();

	// List of bike models with availabilities for different sizes
	protected static List<Bike> bikes = null;

	static int n = 0;
	static RegionInProgress regionInProgress = null; // Client of this region will be processed by this application instance

	// Main
	public static void main(String[] args) throws Exception {

		// Read JDBC and Domain properties. Note: you should not have multiple properties files with same name in your class path
		Properties dbProps = Prop.readEnvironmentSpecificProperties(Prop.findPropertiesFile("db.properties"), "local/mysql/bikes", null);
		Properties domainProps = Prop.readProperties(Prop.findPropertiesFile("domain.properties"));

		// Associate domain classes and database tables
		sdc.initialize(dbProps, domainProps, Manufacturer.class.getPackage().getName() /* use any class directly in 'domain' package to find all domain classes */);

		// Initially load existing domain objects from database (SELECT statements are performed here)
		// Note - (historical) orders are generally excluded from loading here - without excluding Order class from loading only orders newer than start time minus data horizon (see Order.java) would
		// be loaded on startup
		sdc.synchronize(Order.class);

		// Select and reserve world region for this instance
		for (Region reg : Region.values()) {
			if (!sdc.hasAny(RegionInProgress.class, r -> r.region == reg)) {
				try {
					regionInProgress = sdc.createAndSave(RegionInProgress.class, r -> r.region = reg);
					break;
				}
				catch (SQLException sqlex) {
					log.warn("Region {} already in use!", reg);
				}
			}
		}
		if (regionInProgress == null) {
			log.warn("All regions already in use!");
			return;
		}

		log.info("Process orders for clients in region {}", regionInProgress.region);

		// Cleanup persistence database on start of first instance
		if (regionInProgress.region == Region.values()[0]) {
			Initialize.deleteExistingObjects();
			Initialize.createObjects();
		}

		// Sort bikes by manufacturer and model using overridden compareTo() method
		// Note: No difference between all() and allValid() here - but generally, if domain objects exist which are not savable (by database constraint violation), they are marked as invalid
		bikes = sdc.sort(sdc.allValid(Bike.class));

		// Log bike store before ordering/buying bikes...
		// Note: use groupBy() to group accumulated children by classifier or countBy() to get # of accumulated children grouped by classifier
		bikes.forEach(b -> log.info("'{}': price: {}€, sizes: {}, availability: {}", b, b.price, b.sizes, b.availabilityMap));

		// Start order processing and bike delivery thread
		Thread orderProcessingThread = new Thread(new Order.SendInvoices());
		orderProcessingThread.setName("--ORDER--");
		orderProcessingThread.start();

		Thread bikeDeliveryThread = new Thread(new Order.DeliverBikes());
		bikeDeliveryThread.setName("-DELIVER-");
		bikeDeliveryThread.start();

		// Create clients for region and start client threads to order bikes
		List<Thread> clientThreads = new ArrayList<>();
		try {
			for (Country country : RegionInProgress.getRegionCountryMap().get(regionInProgress.region)) {
				for (String clientName : Client.getCountryNamesMap().get(country)) {

					// Use create() method of domain controller to instantiate, initialize and register new object or createAndSave() to additionally save object immediately after registration.
					// Logical initialization by init routine will be performed before object registration.
					// Note: Alternatively you may use specific constructors for object creation and register and save objects there or afterwards explicitly - see examples in Initialize.java

					// Create client
					Client client = sdc.create(Client.class, c -> c.init(clientName, ((n % 10) < 5 ? Gender.MALE : Gender.FEMALE), country, Size.values()[n % 5], 12000.0 + (n % 10) * 1000.0));
					n++;

					// Save client
					sdc.save(client);

					// Create client thread to order bikes
					Thread clientThread = new Thread(client.new OrderBikes(client));
					clientThread.setName(client.firstName);
					clientThread.start();
					clientThreads.add(clientThread);

					Thread.sleep(ORDER_DELAY_TIME_MS);
				}
			}
		}
		finally {
			log.info("{} client threads started.", clientThreads.size());

			// Wait until ordering bikes is completed...
			for (Thread thread : clientThreads) {
				thread.join(3000);
				if (thread.isAlive()) {
					log.warn("Timeout waiting for end of client thread ('{}')", thread.getName());
				}
				else {
					log.info("Joined: '{}'", thread.getName());
				}
			}

			log.info("{} client threads ended.", clientThreads.size());

			// Force ending order threads
			orderProcessingThread.interrupt();
			orderProcessingThread.join(10000);

			bikeDeliveryThread.interrupt();
			bikeDeliveryThread.join(10000);
		}

		// Log bike store after ordering/buying bikes...
		// Note: use groupBy() to group accumulated children by classifier or countBy() to get # of accumulated children grouped by classifier
		bikes.forEach(b -> log.info("'{}': price: {}€, sizes: {}, availability: {}, orders: {}'", b, b.price, b.sizes, b.availabilityMap,
				sdc.countBy(b.orders.stream().filter(o -> !o.wasCanceled).collect(Collectors.toSet()), o -> o.client.bikeSize)));

		// Check sum of ordered bikes and still available bikes
		List<Order> orders = new ArrayList<>();
		List<Bike> sortedBikes = new ArrayList<>(BikeStoreApp.sdc.all(Bike.class));
		Collections.sort(sortedBikes);
		for (Bike bike : sortedBikes) {
			for (Size size : bike.sizes) {
				long availableBikesInSizeCount = bike.availabilityMap.get(size);
				List<Order> successfulOrdersForBikeInSize = bike.orders.stream().filter(o -> o.bike == bike && o.client.bikeSize == size && !o.wasCanceled).collect(Collectors.toList());
				orders.addAll(successfulOrdersForBikeInSize);
				if (availableBikesInSizeCount + successfulOrdersForBikeInSize.size() != 15) {
					log.error("Sum of ordered and still avaliable {}s in size {} does differs from initial availability: {}+{} != {}!", bike, size, availableBikesInSizeCount,
							successfulOrdersForBikeInSize.size(), 15);
				}
			}
		}

		// Log results
		Predicate<Order> regionPredicate = o -> RegionInProgress.getRegion(o.client.country) == regionInProgress.region;
		int totalNumberOfOrders = (int) sdc.count(Order.class, null);
		int numberOfPendingOrders = (int) sdc.count(Order.class, o -> regionPredicate.test(o) && o.payDate != null && o.deliveryDate == null);
		int numberOfInvoicesSent = (int) sdc.count(Order.class, o -> regionPredicate.test(o) && o.invoiceDate != null);
		int numberOfDeliveryNotesSent = (int) sdc.count(Order.class, o -> regionPredicate.test(o) && o.deliveryDate != null);
		int numberOfCanceledOrders = (int) sdc.count(Order.class, o -> regionPredicate.test(o) && o.wasCanceled);

		log.info("# of total orders created : {}", totalNumberOfOrders);
		log.info("# of invoices sent: {}", numberOfInvoicesSent);
		log.info("# of bikes delivered: {}", numberOfDeliveryNotesSent);
		log.info("# of pending orders: {}", numberOfPendingOrders);
		log.info("# of orders canceled by inability to pay: {}", numberOfCanceledOrders);
		log.info("# of orders where order processing exceeded timeout: {}", Order.orderProcessingExceededCount);
		log.info("# of orders where bike delivery exceeded timeout: {}", Order.bikeDeliveryExceededCount);
		log.info("Order processing time statistic (# of order processing operations in less than ? ms): {}", Order.orderProcessingDurationMap);
		log.info("Bike delivery time statistic (# of bike delivery operations in less than ? ms): {}", Order.bikeDeliveryDurationMap);

		// // Free region in use (to allow re-run test with multiple parallel instances)
		sdc.delete(regionInProgress);

		// Close open database connections
		sdc.sqlDb.close();
	}
}
