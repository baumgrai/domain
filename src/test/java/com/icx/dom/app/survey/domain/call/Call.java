package com.icx.dom.app.survey.domain.call;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.survey.domain.config.Survey;
import com.icx.dom.app.survey.domain.message.Message;
import com.icx.dom.domain.DomainAnnotations.Accumulation;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
public abstract class Call extends SqlDomainObject {

	protected static final Logger log = LoggerFactory.getLogger(Call.class);

	// Members

	public Survey survey;

	// Accumulations

	@Accumulation
	Set<Answer> answers;

	@Accumulation
	Set<Message> messages;

}
