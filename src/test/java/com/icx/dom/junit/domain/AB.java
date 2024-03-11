package com.icx.dom.junit.domain;

import com.icx.domain.DomainObject;
import com.icx.domain.sql.Annotations.SqlTable;
import com.icx.domain.sql.Annotations.UseDataHorizon;

@UseDataHorizon
@SqlTable(name = "DOM_AB")
public class AB extends A {

	public AB() {
	}

	public AB(
			O o,
			String s) {

		this.o = o;
		setS(s);
	}

	@Override
	public int compareTo(DomainObject o) {
		return getS().compareTo(((AB) o).getS());
	}
}
