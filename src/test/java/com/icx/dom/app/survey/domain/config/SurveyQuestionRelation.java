package com.icx.dom.app.survey.domain.config;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.survey.SurveyApp;
import com.icx.domain.DomainAnnotations.SqlColumn;
import com.icx.domain.DomainObject;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.jdbc.SqlConnection;
import com.icx.jdbc.SqlDbException;

public class SurveyQuestionRelation extends SqlDomainObject {

	static final Logger log = LoggerFactory.getLogger(SurveyQuestionRelation.class);

	// Members

	@SqlColumn(notNull = true)
	public Survey survey;

	@SqlColumn(notNull = true)
	public Question question;

	public int position;

	// Overrides

	@Override
	public String toString() {
		return survey + "/" + question + ": " + position;
	}

	@Override
	public int compareTo(DomainObject o) {
		return position - ((SurveyQuestionRelation) o).position;
	}

	public void save(SqlConnection sqlcn) {
		try {
			SurveyApp.sdc.save(sqlcn.cn, this);
		}
		catch (SQLException | SqlDbException e) {
			log.error("Exception on save occurred: ", e);
		}
	}

}
