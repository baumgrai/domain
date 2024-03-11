package com.icx.dom.junit.domain.sub;

import java.util.Set;

import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Accumulation;
import com.icx.domain.sql.Annotations.Created;
import com.icx.domain.sql.Annotations.UseDataHorizon;

@UseDataHorizon
@Created(version = "1.1")
public class Z extends SqlDomainObject {

	public X x;

	@Accumulation(refField = "Y.z") // refField definition not necessary here - just for unit test
	public Set<Y> ys;
}
