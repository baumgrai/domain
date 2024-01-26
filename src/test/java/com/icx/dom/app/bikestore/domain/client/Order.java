package com.icx.dom.app.bikestore.domain.client;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.SortedMap;
import java.util.TreeMap;

import com.icx.common.base.Common;
import com.icx.dom.app.bikestore.BikeStoreApp;
import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
// This means that older orders, which were processed before data horizon (last modification date is before database synchronization time minus 'data horizon period') will not be loaded from database
// into object store anymore and will be removed from object store if they are still registered. 'Data horizon period' is configured in 'domain.properties'.
// 'Data horizon' mechanism protects from having a potential infinite amount of data in heap (object store) if objects will be created continuously but will never be deleted. You have to periodically
// call SqlDomainController#synchronize() to force removing old objects from object store.
// Note: If @UseDataHorizon is present for a class ON DELETE CASCADE will automatically assigned to FOREIGN KEY constraint of all reference fields of this class to allow deletion of parent objects
// even if not all children are registered in object store (due to 'data horizon' control)
public class Order extends SqlDomainObject {

	// Helper domain class for exclusive order selection - one in-progress object will be created with the same id as the order object on selecting it exclusively and because id field is unique this
	// can be done only one time for one order - even if multiple domain controller instances operate on the same persistence database
	public static class InProgress extends SqlDomainObject {
	}

	// Members

	@SqlColumn(notNull = true)
	public Client client;

	@SqlColumn(notNull = true)
	public Bike bike;

	public boolean wasCanceled = false;

	public LocalDateTime orderDate; // Java Date/LocalDateTime -> database specific date/time column
	public LocalDateTime invoiceDate;
	public LocalDateTime payDate;
	public LocalDateTime deliveryDate;
	// ON DELETE CASCADE is automatically added to FOREIGN KEY constraints on @UseDataHorizon annotated class (see above)

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
		return (client + " ordered '" + bike + "' (" + client.bikeSize + ")");
	}

	// Override compareTo() to sort objects using DomanController#sort(). If not overridden compareTo() works on internal object id which follows date of object creation
	@Override
	public int compareTo(DomainObject o) {
		return Common.compare(client.firstName + bike.manufacturer, ((Order) o).client.firstName + ((Order) o).bike.manufacturer);
	}

	// Only for runtime statistics
	public static final SortedMap<Integer, Integer> orderProcessingDurationMap = new TreeMap<>();
	public static int orderProcessingExceededCount = 0;
	public static final SortedMap<Integer, Integer> bikeDeliveryDurationMap = new TreeMap<>();
	public static int bikeDeliveryExceededCount = 0;

	public enum Operation {
		ORDER_PROCESSING, BIKE_DELIVERY
	}

	// A try to delete() an object will recursively check if this object and all of it's direct and indirect children can be deleted by using this canBeDeleted() method
	// This means in our case that an order can only be deleted if it was canceled before
	// Note: this is not part of the application logic here - canBeDeleted() is overridden here only for demonstration and explanation purposes

	@Override
	public boolean canBeDeleted() {
		return wasCanceled;
	}

	// Send invoice
	void sendInvoice() {
		invoiceDate = LocalDateTime.now();
		log.info("Invoice for order '{}' was sent", this);
	}

	// Deliver bike
	void deliverBike() {
		deliveryDate = LocalDateTime.now();
		log.info("Bike was delivered for order '{}'", this);
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
					BikeStoreApp.sdc.computeExclusivelyOnObjects(Order.class, Order.InProgress.class, "INVOICE_DATE IS NULL", o -> o.sendInvoice());
				}
				catch (Exception e) {
					log.error(" {} exception occured sending invoices!", e.getClass().getSimpleName());
				}

				try {
					Thread.sleep(0, 1);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.info("Order processing thread ended {}s after start", ChronoUnit.SECONDS.between(start, LocalDateTime.now()));
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
					BikeStoreApp.sdc.computeExclusivelyOnObjects(Order.class, Order.InProgress.class, "PAY_DATE IS NOT NULL AND DELIVERY_DATE IS NULL", o -> o.deliverBike());
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
