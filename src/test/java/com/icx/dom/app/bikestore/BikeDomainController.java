package com.icx.dom.app.bikestore;

import com.icx.domain.sql.SqlDomainController;
import com.icx.domain.sql.SqlDomainObject;

public class BikeDomainController extends SqlDomainController {

	// Method may be overridden by a more sophisticated one
	@Override
	protected synchronized <S extends SqlDomainObject> long generateUniqueId(Class<S> domainObjectClass) {
		return super.generateUniqueId(domainObjectClass);
	}

}
