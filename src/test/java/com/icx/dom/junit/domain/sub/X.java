package com.icx.dom.junit.domain.sub;

import java.util.List;
import java.util.Set;

import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;
import com.icx.dom.junit.domain.A;

@UseDataHorizon
public class X extends SqlDomainObject {

	public String s;

	public List<Integer> is;

	public A a;

	public Y y;

	@Accumulation(refField = "x") // refField definition not necessary here - just for coverage
	public Set<Z> zs;
}
