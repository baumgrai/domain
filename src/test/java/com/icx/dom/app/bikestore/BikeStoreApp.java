package com.icx.dom.app.bikestore;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.Prop;
import com.icx.common.base.CList;
import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.app.bikestore.domain.bike.Bike.Size;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.Region;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.domain.sql.LoadHelpers;
import com.icx.domain.sql.SqlDomainController;
import com.icx.jdbc.SqlDb;
import com.icx.jdbc.SqlDb.DbType;

/**
 * International bike store. Test app simulates bike ordering by different clients, order processing, bike delivery and decrementing bike stock.
 * 
 * Code demonstrates how to register domain classes and associate them with tables of (existing) persistence database, how to load and save domain objects and also most of the additional features
 * provided by Domain persistence mechanism. See comments in {@code Java2Sql.java} for how to create persistence database from domain classes.
 * 
 * Test program processes orders from clients of six world regions, where a region is represented by one domain controller instance and clients are represented by threads. Concurrent access will be
 * made to decrement bike stock if one bike could successfully ordered and also to bike orders for processing and delivering bikes. Concurrent database operations will be synchronized by database
 * internal synchronization using unique 'in-progress' shadow records ({@code InProgress} objects). SELECT FOR UPDATE statements will not be used anywhere!
 * 
 * @author baumgrai
 */
public class BikeStoreApp {

	static final Logger log = LoggerFactory.getLogger(BikeStoreApp.class);

	// Finals

	public static final File BIKE_PICTURE = new File("src/test/resources/bike.jpg");

	// Delay time between client bike order requests. One client tries to order bikes of 3 different types. Acts also as delay time for start of client's ordering threads.
	// Note: Values lower 100ms lead to problems with Oracle connections - at least in local test environment with Oracle 11g Express edition (11.2)
	public static final long ORDER_DELAY_TIME_MS = 100;

	// Initially available bikes for any provided size
	public static final int AVAILABLE_BIKES = 15;

	// Domain controller object
	public static SqlDomainController sdc = new SqlDomainController();

	// List of bike models with availabilities for different sizes
	protected static List<Bike> bikes = null;

	// To force ending order processing and bike delivery threads
	public static boolean stopOrderProcessing = false;

	// Methods

	private static void checkOrdersAndStock() {

		List<Order> orders = new ArrayList<>();
		List<Bike> sortedBikes = new ArrayList<>(BikeStoreApp.sdc.all(Bike.class));
		Collections.sort(sortedBikes);

		for (Bike bike : sortedBikes) {
			for (Size size : bike.sizes) {

				long availableBikesInSizeCount = bike.availabilityMap.get(size);
				// Set<Order> successfulOrdersForBikeInSize = sdc.findAll(Order.class, o -> o.bike == bike && o.client.bikeSize == size && !o.wasCanceled);
				Set<Order> successfulOrdersForBikeInSize = bike.orders.stream().filter(o -> o.client.bikeSize == size && !o.wasCanceled).collect(Collectors.toSet());
				orders.addAll(successfulOrdersForBikeInSize);

				if (availableBikesInSizeCount + successfulOrdersForBikeInSize.size() != AVAILABLE_BIKES) {
					log.error("Sum of ordered and still avaliable {}s in size {} differs from initial availability: {}+{} != {}!", bike, size, availableBikesInSizeCount,
							successfulOrdersForBikeInSize.size(), AVAILABLE_BIKES);
				}
			}
		}
	}

	// Main
	public static void main(String[] args) throws Exception {

		// -------------------------------------------------
		SqlDb.DbType dbType = DbType.MYSQL;
		// -------------------------------------------------

		// Read JDBC and Domain properties. Note: you should not have multiple properties files with same name in your class path
		File dbPropsFile = Prop.findPropertiesFile("db.properties");
		String localConf = "local/" + dbType.toString().toLowerCase() + "/bikes";
		Properties dbProps = Prop.readEnvironmentSpecificProperties(dbPropsFile, localConf, CList.newList("dbConnectionString", "dbUser"));

		Properties domainProps = Prop.readProperties(Prop.findPropertiesFile("domain.properties"));

		// Associate domain classes and database tables
		sdc.initialize(dbProps, domainProps, Manufacturer.class.getPackage().getName() /* use any class directly in 'domain' package to find all domain classes */);

		// Synchronize with database on startup to load potentially existing objects (manufacturers. bikes, orders, etc.) which will be deleted during initial cleanup
		sdc.synchronize();

		// Cleanup persistence database and create bike stock on startup
		Initialize.deleteExistingObjects();
		Initialize.createBikeStock();

		// Sort bikes by manufacturer and model using overridden compareTo() method
		bikes = sdc.sort(sdc.all(Bike.class));

		// Log bike store before ordering and delivering bikes...
		bikes.forEach(b -> log.info("'{}': price: {}€, sizes: {}, availability: {}", b, b.price, b.sizes, b.availabilityMap));

		try {
			// Create and start bike store instances for world regions
			Map<Region, Thread> instanceThreadMap = new HashMap<>();
			Map<Region, Thread> orderThreadMap = new HashMap<>();
			Map<Region, Thread> bikeDeliveryThreadMap = new HashMap<>();

			for (Region region : Region.values()) {

				// Start order processing and bike delivery thread for region
				// Note: there are separate order and bike delivery threads for every region but they use the same domain controller and have access to all orders independently in which region they
				// were generated - this is to force concurrent access collisions
				Thread orderProcessingThread = new Thread(new Order.ProcessOrder(sdc));
				orderProcessingThread.setName("ORDER " + region.shortname());
				orderProcessingThread.start();
				orderThreadMap.put(region, orderProcessingThread);

				Thread bikeDeliveryThread = new Thread(new Order.DeliverBikes(sdc));
				bikeDeliveryThread.setName("DELIVER " + region.shortname());
				bikeDeliveryThread.start();
				bikeDeliveryThreadMap.put(region, bikeDeliveryThread);

				// Start bike store client thread for region
				Thread instanceThread = new Thread(new ClientInstance(dbProps, domainProps, region));
				instanceThread.setName("INSTANCE " + region.shortname());
				instanceThread.start();
				instanceThreadMap.put(region, instanceThread);
			}

			log.info("Threads started.");

			// Wait until all client instances have finished
			for (Entry<Region, Thread> entry : instanceThreadMap.entrySet()) {
				Thread instanceThread = entry.getValue();
				instanceThread.join(120000);
			}

			log.info("Client threads ended.");
			stopOrderProcessing = true;

			// Force ending order and bike delivery threads
			for (Entry<Region, Thread> entry : orderThreadMap.entrySet()) {
				Thread orderProcessingThread = entry.getValue();
				orderProcessingThread.join(10000);
			}

			log.info("Order threads ended.");

			for (Entry<Region, Thread> entry : bikeDeliveryThreadMap.entrySet()) {
				Thread bikeDeliveryThread = entry.getValue();
				bikeDeliveryThread.join(10000);
			}

			log.info("Delivery threads ended.");

			// Synchronize with database to load orders generated from bike store instances
			sdc.synchronize();

			// Check sum of ordered bikes and still available bikes for any bike and size
			checkOrdersAndStock();

			// Log bike store after ordering/buying bikes...
			// Note: use groupBy() to group accumulated children by classifier or countBy() to get # of accumulated children grouped by classifier
			bikes.forEach(b -> log.info("'{}': price: {}€, sizes: {}, availability: {}, orders: {}'", b, b.price, b.sizes, b.availabilityMap,
					sdc.countBy(b.orders.stream().filter(o -> !o.wasCanceled).collect(Collectors.toSet()), o -> o.client.bikeSize)));

			// Log results
			log.info("");
			log.info("# of total orders created: {}", sdc.count(Order.class, null));
			log.info("# of invoices sent: {}", sdc.count(Order.class, o -> o.invoiceDate != null));
			log.info("# of bikes delivered: {}", sdc.count(Order.class, o -> o.deliveryDate != null));
			log.info("# of pending orders: {}", sdc.count(Order.class, o -> o.payDate != null && o.deliveryDate == null));
			log.info("# of orders canceled by inability to pay: {}", sdc.count(Order.class, o -> o.wasCanceled));
			log.info("");
			log.info("# of orders where order processing exceeded timeout: {}", Order.orderProcessingExceededCount);
			log.info("# of orders where bike delivery exceeded timeout: {}", Order.bikeDeliveryExceededCount);
			log.info("");
			log.info("Order processing time statistic (# of order processing operations in less than ? ms): {}", Order.orderProcessingDurationMap);
			log.info("Bike delivery time statistic (# of bike delivery operations in less than ? ms): {}", Order.bikeDeliveryDurationMap);
			log.info("");
			log.info("Concurrent access denied because of use by another thread of same controller instance: {}", LoadHelpers.inUseBySameInstanceAccessCount);
			log.info("Concurrent access denied because of use by another controller instance: {}", LoadHelpers.inUseByDifferentInstanceAccessCount);
			log.info("Unsuccessful tries allocating bikes to order: {}", Client.unsuccessfulTriesAllocatingBikesToOrderCount);
			log.info("Successful tries allocating bikes to order: {}", Client.successfulTriesAllocatingBikesToOrderCount);
		}
		finally {
			// Close potential open database connections
			sdc.sqlDb.close();
		}
	}
}
