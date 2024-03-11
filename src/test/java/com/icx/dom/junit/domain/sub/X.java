package com.icx.dom.junit.domain.sub;

import java.util.List;
import java.util.Set;

import com.icx.dom.junit.domain.A;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Accumulation;
import com.icx.domain.sql.Annotations.UseDataHorizon;

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
