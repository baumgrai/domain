package com.icx.dom.junit.domain;

import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Secret;

@Secret
public class B extends SqlDomainObject {

	public String name;

	public AA aa;

}
