package com.icx.dom.junit.domain.sub;

import java.util.Set;

import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.Created;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
@Created(version = "1.1")
public class Z extends SqlDomainObject {

	public X x;

	@Accumulation(refField = "Y.z") // refField definition not necessary here - just for unit test
	public Set<Y> ys;
}
