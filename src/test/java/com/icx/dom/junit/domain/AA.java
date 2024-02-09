package com.icx.dom.junit.domain;

import com.icx.dom.domain.DomainAnnotations.SqlTable;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.DomainObject;

@UseDataHorizon
@SqlTable(name = "DOM_AA")
public class AA extends A {

	public AA() {
	}

	public AA(
			O o,
			String s) {

		this.o = o;
		this.s = s;
	}

	@Override
	public int compareTo(DomainObject o) {
		return s.compareTo(((AA) o).s);
	}
}
