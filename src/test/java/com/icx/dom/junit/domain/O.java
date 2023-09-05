package com.icx.dom.junit.domain;

import java.util.Set;

import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.sql.SqlDomainObject;

public class O extends SqlDomainObject {

	@Accumulation
	public Set<A> as;

}
