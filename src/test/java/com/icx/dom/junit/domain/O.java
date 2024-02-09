package com.icx.dom.junit.domain;

import java.util.Set;

import com.icx.domain.DomainAnnotations.Accumulation;
import com.icx.domain.sql.SqlDomainObject;

public class O extends SqlDomainObject {

	@Accumulation
	public Set<A> as;

}
