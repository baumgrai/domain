package com.icx.dom.app.survey.domain.call;

import com.icx.domain.DomainAnnotations.UseDataHorizon;
import com.icx.domain.sql.SqlDomainObject;

@UseDataHorizon
public class Answer extends SqlDomainObject {

	// Members

	public String answerText;

	public String questionText;

	public Call call;

}
