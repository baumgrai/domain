package com.icx.dom.junit.domain;

import com.icx.domain.DomainAnnotations.SqlTable;
import com.icx.domain.DomainAnnotations.UseDataHorizon;
import com.icx.domain.DomainObject;

@UseDataHorizon
@SqlTable(name = "DOM_AB")
public class AB extends A {

	public AB() {
	}

	public AB(
			O o,
			String s) {

		this.o = o;
		this.s = s;
	}

	@Override
	public int compareTo(DomainObject o) {
		return s.compareTo(((AB) o).s);
	}
}
