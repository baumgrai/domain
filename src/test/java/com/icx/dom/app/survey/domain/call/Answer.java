package com.icx.dom.app.survey.domain.call;

import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.UseDataHorizon;

@UseDataHorizon
public class Answer extends SqlDomainObject {

	// Members

	public String answerText;

	public String questionText;

	public Call call;

}
