package com.icx.dom.app.survey.domain.config;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.survey.SurveyApp;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.jdbc.SqlConnection;

public class Question extends SqlDomainObject {

	protected static final Logger log = LoggerFactory.getLogger(Question.class);

	// Statics

	public enum QuestionType {
		HELP, SOLVED, SERVICE, EXPECTATION, WAITING_TIME, RECOMMEND;

		public String text() {
			return (this == HELP ? "Could we help you?"
					: this == SOLVED ? "Could your problem finally be solved?"
							: this == SERVICE ? "Please rate our service on a 1 to 6 scale where 1 is 'excellent' and 6 is 'very poor'?"
									: this == EXPECTATION ? "Did our answers to your questions met your expectation? Please rate on a 1 to 6 scale where 1 is 'perfectly' and 6 is 'absolutely not'."
											: this == WAITING_TIME
													? "How long did you have to wait until you reached one of our agents? Please rate 1 for 'no waiting time', 2 for 'less than one minute' or 3 for 'more than one minute'."
													: this == RECOMMEND ? "Would you recommend our service to a friend? Please rate from 0 for 'absolutely not' to 10 for 'definitely'." : "");
		}

		public Scale scale() {
			return (this == HELP || this == SOLVED ? Scale.yesNoScale : this == SERVICE || this == EXPECTATION ? Scale.gradesScale : this == WAITING_TIME ? Scale.oneTwoThreeScale : Scale.npsScale);
		}
	}

	public static Map<QuestionType, Question> questionMap = new HashMap<>();

	// Members

	public QuestionType type;

	public String text;

	public Scale answerScale;

	// Methods

	@Override
	public String toString() {
		return "\"" + text + "\"";
	}

	public static Question createQuestion(SqlConnection sqlcn, QuestionType questionType) throws Exception {

		return SurveyApp.sdc.createAndSave(sqlcn.cn, Question.class, q -> {
			q.type = questionType;
			q.text = questionType.text();
			q.answerScale = questionType.scale();
		});

	}

}
