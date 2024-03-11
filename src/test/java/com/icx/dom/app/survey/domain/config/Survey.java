package com.icx.dom.app.survey.domain.config;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.icx.common.CRandom;
import com.icx.dom.app.survey.SurveyApp;
import com.icx.dom.app.survey.domain.call.Call;
import com.icx.dom.app.survey.domain.config.Question.QuestionType;
import com.icx.domain.sql.SqlDomainObject;
import com.icx.domain.sql.Annotations.Accumulation;
import com.icx.domain.sql.Annotations.SqlColumn;
import com.icx.jdbc.SqlConnection;

public class Survey extends SqlDomainObject {

	// Helper domain class avoid ordering more bikes than available
	public static class InProgress extends SqlDomainObject {
	}

	public enum Semaphore {
		GREEN, RED
	}

	// Members

	@SqlColumn(unique = true, charsize = 256)
	public String name;

	public int number;

	public String greeting;

	public String leaveMessagePrompt;

	public Semaphore semaphore = Semaphore.GREEN;

	// Accumulations

	@Accumulation
	public Set<Call> calls;

	// Overrides

	@Override
	public String toString() {
		return name;
	}

	// Methods

	public synchronized List<SurveyQuestionRelation> getOrderedQuestionRelations() {
		return SurveyApp.sdc.sort(SurveyApp.sdc.findAll(SurveyQuestionRelation.class, sqr -> sqr.survey == this));
	}

	public synchronized List<Question> getQuestions() {
		return getOrderedQuestionRelations().stream().map(sqr -> sqr.question).collect(Collectors.toList());
	}

	public synchronized int getQuestionCount() {
		return (int) SurveyApp.sdc.count(SurveyQuestionRelation.class, sqr -> sqr.survey == this);
	}

	private Question getAndCheckQuestionOnPos(int pos, String op) {

		int questionCount = getQuestionCount();
		if (pos < 0 || pos > questionCount) {
			log.error("{} position {} not in allowed range [0, {}]", op, pos, questionCount);
			return null;
		}

		if (pos == questionCount) { // Question to append
			return null;
		}

		Collection<SurveyQuestionRelation> sqrs = SurveyApp.sdc.findAll(SurveyQuestionRelation.class, sqr -> sqr.survey == this && sqr.position == pos);
		int count = (int) SurveyApp.sdc.count(SurveyQuestionRelation.class, sqr -> sqr.survey == this);
		if (sqrs.isEmpty()) {
			log.error("No question at position {} in survey '{}' (question count: {})", pos, this, count);
			return null;
		}
		else if (sqrs.size() > 1) {
			log.error("Multiple questions with same position {} in survey '{}' (question count: {})", pos, this, count);
			return null;
		}

		return sqrs.iterator().next().question;
	}

	public synchronized void insertQuestion(SqlConnection sqlcn, Question question, int pos) throws Exception {

		if (question == null) {
			log.error("Insert question: question is null!");
			return;
		}

		getAndCheckQuestionOnPos(pos, "Insert");

		SurveyApp.sdc.findAll(SurveyQuestionRelation.class, sqr -> sqr.survey == this && sqr.position >= pos).forEach(sqr -> {
			sqr.position++;
			sqr.save(sqlcn);
		});

		SurveyApp.sdc.createAndSave(sqlcn.cn, SurveyQuestionRelation.class, sqr -> {
			sqr.survey = this;
			sqr.question = question;
			sqr.position = pos;
		});

		log.debug("Inserted question on position {} (question count: {})", pos, SurveyApp.sdc.count(SurveyQuestionRelation.class, sqr -> sqr.survey == this));
	}

	public synchronized void appendQuestion(SqlConnection sqlcn, Question question) throws Exception {
		insertQuestion(sqlcn, question, getQuestionCount());
	}

	public synchronized Question removeQuestion(SqlConnection sqlcn, int pos) throws Exception {

		Question question = getAndCheckQuestionOnPos(pos, "Remove");
		if (question == null) {
			return null;
		}

		SurveyApp.sdc.delete(SurveyApp.sdc.findAny(SurveyQuestionRelation.class, sqr -> sqr.survey == this && sqr.position == pos));

		SurveyApp.sdc.findAll(SurveyQuestionRelation.class, sqr -> sqr.survey == this && sqr.position > pos).forEach(sqr -> {
			sqr.position--;
			sqr.save(sqlcn);
		});

		if (log.isDebugEnabled()) {
			log.debug("Removed question on position {}", pos);
		}

		return question;
	}

	public synchronized void moveQuestion(SqlConnection sqlcn, int oldPos, int newPos) throws Exception {

		Question question = removeQuestion(sqlcn, oldPos);
		if (question != null) {
			insertQuestion(sqlcn, question, newPos);
		}
	}

	// Test admin thread

	private boolean hasQuestionOfType(QuestionType type) {
		return (SurveyApp.sdc.count(SurveyQuestionRelation.class, sqr -> sqr.survey == this && sqr.question.type == type) > 0);
	}

	public void addQuestionRandomly(SqlConnection sqlcn, int questionCount) throws Exception {

		QuestionType questionType = null;
		for (QuestionType qt : CRandom.randomSelect(Arrays.asList(QuestionType.values()), QuestionType.values().length)) {
			if (!hasQuestionOfType(qt)) {
				questionType = qt;
				break;
			}
		}

		if (questionType != null) {

			int pos = CRandom.randomInt(questionCount + 1);

			log.info("Insert question of type {} on position {}", questionType, pos);
			insertQuestion(sqlcn, Question.questionMap.get(questionType), pos);
			log.debug("Question on position {} inserted", pos);
		}
	}

	public void moveQuestionRandomly(SqlConnection sqlcn, int questionCount) throws Exception {

		int oldPos = CRandom.randomInt(questionCount);
		int newPos = oldPos;
		while (newPos == oldPos) {
			newPos = CRandom.randomInt(questionCount);
		}

		log.info("Move question from position {} to {}", oldPos, newPos);
		moveQuestion(sqlcn, oldPos, newPos);
		log.debug("Question moved from position {} to {}", oldPos, newPos);
	}

	public void removeQuestionRandomly(SqlConnection sqlcn, int questionCount) throws Exception {

		int pos = (questionCount == 1 ? 0 : CRandom.randomInt(questionCount));

		log.info("Remove question on position {}", pos);
		removeQuestion(sqlcn, pos);
		log.debug("Question on position {} removed", pos);
	}

}
