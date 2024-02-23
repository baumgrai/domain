package com.icx.dom.junit.domain;

import com.icx.domain.DomainAnnotations.SqlTable;
import com.icx.domain.DomainAnnotations.UseDataHorizon;
import com.icx.domain.DomainObject;

@UseDataHorizon
@SqlTable(name = "DOM_AA")
public class AA extends A {

	public AA() {
	}

	public AA(
			O o,
			String s) {

		this.o = o;
		setS(s);
	}

	@Override
	public int compareTo(DomainObject o) {
		return getS().compareTo(((AA) o).getS());
	}
}
