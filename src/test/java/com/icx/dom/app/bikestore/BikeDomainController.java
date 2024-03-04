package com.icx.dom.app.bikestore;

import com.icx.domain.sql.SqlDomainController;

public class BikeDomainController extends SqlDomainController {

	// Method may be overridden by a more sophisticated one
	@Override
	protected long generateUniqueId() {
		return super.generateUniqueId();
	}

}
