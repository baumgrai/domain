package com.icx.dom.junit.domain.sub;

import java.util.List;
import java.util.Set;

import com.icx.dom.junit.domain.A;
import com.icx.domain.DomainAnnotations.Accumulation;
import com.icx.domain.DomainAnnotations.UseDataHorizon;
import com.icx.domain.sql.SqlDomainObject;

@UseDataHorizon
public class X extends SqlDomainObject {

	public static class InProgress extends SqlDomainObject {
	}

	public String s;

	public List<Integer> is;

	public A a;

	public Y y;

	@Accumulation(refField = "x") // refField definition not necessary here - just for coverage
	public Set<Z> zs;
}
