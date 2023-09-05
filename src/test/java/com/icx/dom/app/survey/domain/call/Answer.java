package com.icx.dom.app.survey.domain.call;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.sql.SqlDomainObject;

@UseDataHorizon
public class Answer extends SqlDomainObject {

	protected static final Logger log = LoggerFactory.getLogger(Answer.class);

	// Members

	public String answerText;

	public String questionText;

	public Call call;

}
