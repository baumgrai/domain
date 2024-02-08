package com.icx.dom.junit.domain;

import com.icx.dom.domain.DomainAnnotations.Secret;
import com.icx.dom.domain.sql.SqlDomainObject;

@Secret
public class B extends SqlDomainObject {

	public String name;

	public AA aa;

}
