package com.icx.dom.app.bikestore.domain.client;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.bikestore.BikeStoreApp;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.common.CDateTime;
import com.icx.dom.common.CMap;
import com.icx.dom.common.Common;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
// This means that older orders which were processed before data horizon (last modification date is before database synchronization time minus 'data horizon period') will not be loaded from database
// into object store anymore using SqlDomainController#synchronize() and will be removed from object store if they are still registered. 'Data horizon period' is configured in 'domain.properties'.
// 'Data horizon' mechanism protects from having a potential infinite amount of data in heap (object store) if objects will be created continuously but never be deleted. You have to periodically call
// SqlDomainController#synchronize() to force removing old objects from object store.
// Note: If @UseDataHorizon is present for a class ON DELETE CASCADE will automatically assigned to FOREIGN KEY constraint of all reference fields of this class to allow deletion of parent objects
// even if not all children are registered in object store (due to 'data horizon' control)
public class Order extends SqlDomainObject {

	static final Logger log = LoggerFactory.getLogger(Order.class);

	// Helper domain class to select orders exclusively
	public static class InProgress extends SqlDomainObject {
	}

	// Members

	@SqlColumn(notNull = true) // ON DELETE CASCADE is automatically added to FOREIGN KEY constraints on @UseDataHorizon annotated class (see above)
	public Client client;

	@SqlColumn(notNull = true)
	public Bike bike;

	boolean wasCanceled = false;

	public LocalDateTime orderDate; // Java Date/LocalDateTime -> database specific date/time column
	public LocalDateTime invoiceDate;
	public LocalDateTime payDate;
	public LocalDateTime deliveryDate;

	// Constructor

	// Note: Default constructor must be defined if any specific constructor is defined
	public Order() {
		this.orderDate = LocalDateTime.now();
	}

	public Order(
			Bike bike,
			Client client) {

		this.bike = bike;
		this.client = client;
		this.orderDate = LocalDateTime.now();
	}

	// Methods

	@Override
	public String toString() {
		return (client + " ordered " + bike.getClass().getSimpleName() + " '" + bike + "' (" + client.bikeSize + ")");
	}

	// Override compareTo() to sort objects using DomanController#sort(). If not overridden compareTo() works on internal object id which follows date of object creation
	@Override
	public int compareTo(DomainObject o) {
		return Common.compare(client.firstName + bike.manufacturer, ((Order) o).client.firstName + ((Order) o).bike.manufacturer);
	}

	// Only for runtime statistics
	public static final Map<Integer, Integer> orderProcessingDurationMap = CMap.newMap(0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0);
	public static final Map<Integer, Integer> bikeDeliveryDurationMap = CMap.newMap(0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0);

	public enum Operation {
		ORDER_PROCESSING, BIKE_DELIVERY
	}

	// For synchronization of order processing
	void waitFor(Operation operation) throws InterruptedException {

		LocalDateTime start = LocalDateTime.now();
		long maxWaitTimeMs = 5000;

		synchronized (this) {
			wait(maxWaitTimeMs);
		}

		LocalDateTime end = LocalDateTime.now();
		if (end.isAfter(CDateTime.add(start, "" + (maxWaitTimeMs) + "ms"))) {
			log.warn("Waiting for {} timed out! ({}ms)", operation, maxWaitTimeMs);
		}

		Map<Integer, Integer> durationMap = (operation == Operation.ORDER_PROCESSING ? orderProcessingDurationMap : bikeDeliveryDurationMap);

		int duration = (int) ChronoUnit.MILLIS.between(start, LocalDateTime.now()) / 10;
		duration = (duration >= 7 ? 7 : duration);
		synchronized (durationMap) {
			durationMap.put(duration, durationMap.get(duration) + 1);
		}
	}

	// A try to delete() an object will recursively check if this object and all of it's direct and indirect children can be deleted by using this canBeDeleted() method
	// (This means in our case an order can only be deleted if it was canceled if we set 'allowDeletingSuccessfulOrders' to 'false' here)

	@Override
	public boolean canBeDeleted() {
		return wasCanceled;
	}

	// Send invoice
	void sendInvoice() {
		invoiceDate = LocalDateTime.now();
	}

	// Deliver bike
	void deliverBike() {
		deliveryDate = LocalDateTime.now();
	}

	// Thread

	// Check for incoming orders and send invoices
	public static class SendInvoices implements Runnable {

		@Override
		public void run() {

			log.info("Order processing thread started");
			LocalDateTime start = LocalDateTime.now();

			while (true) {
				try {
					Set<Order> orders = BikeStoreApp.sdc.computeExclusively(Order.class, Order.InProgress.class, "INVOICE_DATE IS NULL", o -> o.sendInvoice());
					for (Order order : orders) {

						log.info("Invoice for order {} was sent", order);

						synchronized (order) {
							order.notifyAll();
						}
					}
				}
				catch (Exception e) {
					log.error(" {} exception occured sending invoices!", e.getClass().getSimpleName());
				}

				try {
					Thread.sleep(0, 1);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.info("Invoice thread ended {}s after start", ChronoUnit.SECONDS.between(start, LocalDateTime.now()));
					return;
				}
			}
		}
	}

	// Check for payed orders and deliver bikes
	public static class DeliverBikes implements Runnable {

		@Override
		public void run() {

			log.info("Bike delivery thread started");
			LocalDateTime start = LocalDateTime.now();

			while (true) {

				try {
					Set<Order> orders = BikeStoreApp.sdc.computeExclusively(Order.class, Order.InProgress.class, "PAY_DATE IS NOT NULL AND DELIVERY_DATE IS NULL", o -> o.deliverBike());
					for (Order order : orders) {

						log.info("Bike for order {} was delivered", order);

						synchronized (order) {
							order.notifyAll();
						}
					}
				}
				catch (Exception e) {
					log.error(" {} exception occured sending delivery notes!", e.getClass().getSimpleName());
				}

				try {
					Thread.sleep(0, 1);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.info("Bike delivery thread ended {}s after start", ChronoUnit.SECONDS.between(start, LocalDateTime.now()));
					return;
				}
			}
		}
	}

}
