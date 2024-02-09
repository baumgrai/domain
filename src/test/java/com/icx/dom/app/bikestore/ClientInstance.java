package com.icx.dom.app.bikestore;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.common.base.Common;
import com.icx.dom.app.bikestore.domain.Manufacturer;
import com.icx.dom.app.bikestore.domain.bike.Bike.Size;
import com.icx.dom.app.bikestore.domain.client.Client;
import com.icx.dom.app.bikestore.domain.client.Client.Country;
import com.icx.dom.app.bikestore.domain.client.Client.Gender;
import com.icx.dom.app.bikestore.domain.client.Client.Region;
import com.icx.dom.app.bikestore.domain.client.Client.RegionInProgress;
import com.icx.dom.app.bikestore.domain.client.Order;
import com.icx.domain.sql.SqlDomainController;

public class ClientInstance extends Common implements Runnable {

	static final Logger log = LoggerFactory.getLogger(ClientInstance.class);

	// Members

	// Read JDBC and Domain properties. Note: you should not have multiple properties files with same name in your class path
	private Properties dbProps = null;
	private Properties domainProps = null;

	// World region
	private Region region = null;

	// Domain controller object
	private SqlDomainController sdc = new SqlDomainController();

	private int n; // may not be a local variable which is not accepted by Java Functional interface (not 'static' enough'

	// Constructor

	public ClientInstance(
			Properties dbProps,
			Properties domainProps,
			Region region) {

		this.dbProps = dbProps;
		this.domainProps = domainProps;
		this.region = region;
	}

	// Methods

	@Override
	public void run() {

		try {
			// Associate domain classes and database tables
			sdc.initialize(dbProps, domainProps, Manufacturer.class.getPackage().getName() /* use any class directly in 'domain' package to find all domain classes */);

			// Initially load existing domain objects from database (SELECT statements are performed here)
			// Note - orders are excluded from loading here because they won't be needed for bike store instance
			sdc.synchronize(Order.class);

			// World region for this instance (here only for demonstration of using inner class as domain class - has no logical impact)
			RegionInProgress regionInProgress = sdc.createAndSave(RegionInProgress.class, r -> r.region = region);

			log.info("Process orders for clients in region {}", regionInProgress.region);

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

						Thread.sleep(BikeStoreApp.ORDER_DELAY_TIME_MS);
					}
				}
			}
			finally {
				log.info("{} client threads started.", clientThreads.size());

				// Wait until ordering bikes is completed...
				for (Thread thread : clientThreads) {
					thread.join(30000);
					if (thread.isAlive()) {
						log.warn("Timeout waiting for end of client thread ('{}')", thread.getName());
					}
					else {
						log.info("Joined: '{}'", thread.getName());
					}
				}

				log.info("{} client threads ended.", clientThreads.size());

				// Free region in use
				sdc.delete(regionInProgress);

				// Close open database connections
				sdc.sqlDb.close();
			}
		}
		catch (Exception ex) {
			log.error("Exception occurred", ex);
		}
	}

}
