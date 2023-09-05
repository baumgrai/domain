package com.icx.dom.app.bikestore.domain.client;

import java.time.LocalDateTime;

import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon // see Order.java
public class Invoice extends SqlDomainObject {

	@SqlColumn(notNull = true) // ON DELETE CASCADE is automatically added to FOREIGN KEY constraint on @UseDataHorizon
	public Order order;

	public LocalDateTime payedDate; // Java Date/LocalDateTime -> database specific date/time column

	public boolean isOpen() {
		return (payedDate == null);
	}
}
