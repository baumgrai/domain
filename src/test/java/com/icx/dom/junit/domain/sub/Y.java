package com.icx.dom.junit.domain.sub;

import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.SqlColumn;
import com.icx.domain.sql.Annotations.UseDataHorizon;

@UseDataHorizon
public class Y extends SqlDomainObject {

	public Y y;

	@SqlColumn(notNull = true)
	public Z z;
}
