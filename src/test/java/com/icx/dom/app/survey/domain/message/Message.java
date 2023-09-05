package com.icx.dom.app.survey.domain.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.survey.domain.call.Call;
import com.icx.dom.domain.sql.SqlDomainObject;

public abstract class Message extends SqlDomainObject {

	protected static final Logger log = LoggerFactory.getLogger(Message.class);

	// Members

	public Call call;

}
