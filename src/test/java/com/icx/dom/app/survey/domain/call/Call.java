package com.icx.dom.app.survey.domain.call;

import java.util.Set;

import com.icx.dom.app.survey.domain.config.Survey;
import com.icx.dom.app.survey.domain.message.Message;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Accumulation;
import com.icx.domain.sql.Annotations.UseDataHorizon;

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
