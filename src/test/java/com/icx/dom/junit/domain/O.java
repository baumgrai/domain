package com.icx.dom.junit.domain;

import java.util.Set;

import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Accumulation;

public class O extends SqlDomainObject {

	@Accumulation
	public Set<A> as;

}
