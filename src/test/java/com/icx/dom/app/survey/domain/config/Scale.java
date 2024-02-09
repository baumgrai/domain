package com.icx.dom.app.survey.domain.config;

import java.util.List;
import java.util.Map;

import com.icx.dom.app.survey.SurveyApp;
import com.icx.domain.DomainAnnotations.SqlColumn;
import com.icx.domain.sql.SqlDomainObject;

public class Scale extends SqlDomainObject {

	// Statics

	public enum Metric {
		BAD, NEUTRAL, GOOD
	}

	// Static domain objects (to simplify code)
	public static Scale yesNoScale;
	public static Scale gradesScale;
	public static Scale oneTwoThreeScale;
	public static Scale npsScale;

	// Members

	@SqlColumn(unique = true)
	public String name;

	public List<String> answers;

	public Map<String, Metric> answerMetricMap;

	// Constructors

	public Scale() { // Default constructor must be explicitly declared if any other constructor is declared (because it is used by domain controller for instantiation of loaded objects)
	}

	public Scale(
			String name,
			Object... answersAndMetrics) {

		// Register before accessing 'answers' and 'answerMetricMap' to have these fields initialized (alternatively initialize fields on declaration and register scale object later)
		SurveyApp.sdc.register(this);

		this.name = name;

		int i = 0;
		String answer = null;
		Metric metric = null;

		for (Object answerOrMetric : answersAndMetrics) {

			if (i++ % 2 == 0) {
				answer = (String) answerOrMetric;
				answers.add(answer);
			}
			else {
				metric = (Metric) answerOrMetric;
				answerMetricMap.put(answer, metric);
			}
		}
	}

	// Methods

	public int size() {
		return answers.size();
	}
}
