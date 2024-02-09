package com.icx.dom.junit.domain;

import com.icx.domain.DomainAnnotations.Secret;
import com.icx.domain.sql.SqlDomainObject;

@Secret
public class B extends SqlDomainObject {

	public String name;

	public AA aa;

}
