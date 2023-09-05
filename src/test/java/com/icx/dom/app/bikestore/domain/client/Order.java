package com.icx.dom.app.bikestore.domain.client;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.bikestore.domain.bike.Bike;
import com.icx.dom.common.CBase;
import com.icx.dom.common.CDateTime;
import com.icx.dom.common.CMap;
import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.sql.SqlDomainController;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
// This means that older orders which were processed before data horizon (last modification date is before database synchronization time minus 'data horizon period') will not be loaded from database
// into object store anymore using SqlDomainController#synchronize() and will be removed from object store if they are still registered. 'Data horizon period' is configured in 'domain.properties'.
// 'Data horizon' mechanism protects from having a potential infinite amount of data in heap (object store) if objects will be created continuously but never deleted. But you have to periodically call
// SqlDomainController#synchronize() to force removing old objects from object store.
// Note: If @UseDataHorizon is present for a class ON DELETE CASCADE will automatically assigned to FOREIGN KEY constraint of all reference fields of this class to allow deletion of parent objects
// even if not all children are registered in object store (due to 'data horizon' control)
public class Order extends SqlDomainObject {

	static final Logger log = LoggerFactory.getLogger(Order.class);

	// Members

	@SqlColumn(notNull = true) // ON DELETE CASCADE is automatically added to FOREIGN KEY constraints on @UseDataHorizon annotated class (see above)
	public Client client;

	@SqlColumn(notNull = true)
	public Bike bike;

	public LocalDateTime orderDate; // Java Date/LocalDateTime -> database specific date/time column

	// Accumulations
	@Accumulation(refField = "order") // 'refField' specification not necessary here - it would be needed if 'invoices' would have multiple references to 'orders'
	public Set<Invoice> invoices; // Only one invoice per order can exist (typically one would use a simple field like 'billDate' here) - this is for demonstration of frequently deleting objects

	@Accumulation(refField = "DeliveryNote.order") // Would be necessary if inherited domain classes of 'invoices' would exist and more than one of them would reference 'orders'
	public Set<DeliveryNote> deliveryNotes; // Typically one would use a simple field like 'deliveredDate' here

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
		return CBase.compare(client.firstName + bike.manufacturer, ((Order) o).client.firstName + ((Order) o).bike.manufacturer);
	}

	public static final Map<Integer, Integer> orderProcessingDurationMap = CMap.newMap(0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0);
	public static final Map<Integer, Integer> bikeDeliveryDurationMap = CMap.newMap(0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0);

	public enum Operation {
		ORDER_PROCESSING, BIKE_DELIVERY
	}

	void wait(Operation operation) throws InterruptedException {

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

		int duration = (int) ChronoUnit.MILLIS.between(start, LocalDateTime.now());
		duration = (duration >= 7 ? 7 : duration);
		synchronized (durationMap) {
			durationMap.put(duration, durationMap.get(duration) + 1);
		}
	}

	public synchronized void payAndWaitForDelivery(Invoice invoice) throws Exception {

		invoice.payedDate = LocalDateTime.now();
		invoice.save();

		wait(Operation.BIKE_DELIVERY);
	}

	// A try to delete() an object will recursively check if this object and all of it's direct and indirect children can be deleted - using this canBeDeleted() method
	// (This means in our case an order can only be deleted if it was canceled if we set 'allowDeletingSuccessfulOrders' to 'false' here)

	private static boolean allowDeletingSuccessfulOrders = true;

	@Override
	public boolean canBeDeleted() {

		if (!deliveryNotes.isEmpty()) {
			return allowDeletingSuccessfulOrders;
		}
		else {
			return super.canBeDeleted();
		}
	}

	// Thread

	// Check for incoming orders and send invoices
	public static class ProcessOrders implements Runnable {

		@Override
		public void run() {

			log.info("Order processing thread started");
			LocalDateTime start = LocalDateTime.now();

			while (true) {

				Set<Order> ordersToProcess = DomainController.findAll(Order.class, o -> o.invoices.isEmpty());

				if (!ordersToProcess.isEmpty() && log.isDebugEnabled()) {
					log.debug("ORD: {} orders to process in current loop", ordersToProcess.size());
				}

				for (Order order : ordersToProcess) {

					synchronized (order) {

						if (!order.isRegistered()) {
							log.warn("Order '{}' was deleted before beeing processed completely! No invoice will be sent", order);
						}
						else {
							log.info("Send invoice for order '{}'", order);

							// Note: Using init function on object creation for logical initialization ensures that object cannot be found in object store before its logical initialization is
							// completed
							try {
								SqlDomainController.createAndSave(Invoice.class, i -> i.order = order);
							}
							catch (Exception e) {
								log.error(" {} exception occured on save invoice for order '{}'! No invoice will be sent", e.getClass().getSimpleName(), order);
							}
						}

						order.notifyAll();
					}
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

			while (true) {

				Set<Order> ordersToDeliverBikeFor = DomainController.findAll(Order.class, o -> o.deliveryNotes.isEmpty() && !o.invoices.isEmpty() && o.invoices.iterator().next().payedDate != null);

				if (!ordersToDeliverBikeFor.isEmpty() && log.isDebugEnabled()) {
					log.debug("ORD: {} orders to deliver bikes for", ordersToDeliverBikeFor.size());
				}

				for (Order order : ordersToDeliverBikeFor) {

					synchronized (order) {
						log.info("Deliver bike for order '{}'...", order);

						// Note: Using init function on object creation for logical initialization ensures that object cannot be found in object store before its logical initialization is completed
						try {
							SqlDomainController.createAndSave(DeliveryNote.class, dn -> {
								dn.order = order;
								dn.deliveryDate = LocalDateTime.now();
							});
						}
						catch (Exception e) {
							log.error(" {} exception occured on save delivery note for order '{}'! No invoice will be sent", e.getClass().getSimpleName(), order);
						}

						order.notifyAll();
					}
				}

				try {
					Thread.sleep(0, 1);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();

					log.info("Bike delivery thread ended");

					return;
				}
			}
		}
	}

}
