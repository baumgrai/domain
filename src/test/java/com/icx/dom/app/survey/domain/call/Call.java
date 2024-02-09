package com.icx.dom.app.survey.domain.call;

import java.util.Set;

import com.icx.dom.app.survey.domain.config.Survey;
import com.icx.dom.app.survey.domain.message.Message;
import com.icx.domain.DomainAnnotations.Accumulation;
import com.icx.domain.DomainAnnotations.UseDataHorizon;
import com.icx.domain.sql.SqlDomainObject;

@UseDataHorizon
public abstract class Call extends SqlDomainObject {

	// Members

	public Survey survey;

	// Accumulations

	@Accumulation
	Set<Answer> answers;

	@Accumulation
	Set<Message> messages;

}
