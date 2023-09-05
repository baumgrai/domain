package com.icx.dom.junit.domain.sub;

import com.icx.dom.domain.DomainAnnotations.SqlColumn;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
public class Y extends SqlDomainObject {

	public Y y;

	@SqlColumn(notNull = true)
	public Z z;
}
