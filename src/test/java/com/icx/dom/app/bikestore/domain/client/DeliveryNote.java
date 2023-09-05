package com.icx.dom.app.bikestore.domain.client;

import java.time.LocalDateTime;

import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon // see Order.java
public class DeliveryNote extends SqlDomainObject {

	@SqlColumn(notNull = true)
	public Order order;

	public LocalDateTime deliveryDate;
}
