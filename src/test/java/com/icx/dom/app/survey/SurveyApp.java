package com.icx.dom.app.survey;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.icx.dom.app.survey.domain.call.PhoneCall.PhoneCallSurvey;
import com.icx.dom.app.survey.domain.config.Question;
import com.icx.dom.app.survey.domain.config.Question.QuestionType;
import com.icx.dom.app.survey.domain.config.Scale;
import com.icx.dom.app.survey.domain.config.Scale.Metric;
import com.icx.dom.app.survey.domain.config.Survey;
import com.icx.dom.app.survey.domain.config.Survey.Semaphore;
import com.icx.dom.app.survey.domain.message.VoiceMessage;
import com.icx.dom.common.CFile;
import com.icx.dom.common.CRandom;
import com.icx.dom.common.Prop;
import com.icx.dom.domain.DomainObject;
import com.icx.dom.domain.sql.SqlDomainController;
import com.icx.dom.jdbc.SqlConnection;
import com.icx.dom.jdbc.SqlDbException;

public class SurveyApp extends SqlDomainController {

	static final Logger log = LoggerFactory.getLogger(SurveyApp.class);

	public static final int SURVEY_COUNT = 3;
	public static final int ADMIN_THREAD_COUNT = 6;

	public static final File AUDIOFILE = new File("src/test/resources/1234567890.wav");
	public static final File AUDIOFILE_FROM_DATABASE = new File("src/test/resources/1234567890_from_db.wav");

	// Domain controller
	public static SqlDomainController sdc = new SqlDomainController();

	// Main
	public static void main(String[] args) throws Exception {

		// Read JDBC and Domain properties. Note: you should not have multiple properties files with same name in your class path
		Properties dbProps = Prop.readEnvironmentSpecificProperties(Prop.findPropertiesFile("db.properties"), "local/mysql/survey_test", null);
		Properties domainProps = Prop.readProperties(Prop.findPropertiesFile("domain.properties"));

		// Register domain classes and domain classes and database tables
		sdc.initialize(dbProps, domainProps, com.icx.dom.app.survey.SurveyApp.class.getPackage().getName() + ".domain");

		// Load objects from database
		sdc.synchronize();

		// Cleanup on start to have defined state (if wished)
		boolean cleanupDatabaseOnStartup = false;
		if (cleanupDatabaseOnStartup) {
			for (Question question : sdc.all(Question.class)) {
				sdc.delete(question);
			}
			for (Scale scale : sdc.all(Scale.class)) {
				sdc.delete(scale);
			}
			for (Survey survey : sdc.all(Survey.class)) {
				sdc.delete(survey);
			}
		}

		// Create or load survey
		if (!sdc.hasAny(Survey.class)) { // If no objects were loaded

			// Create scales
			Scale.yesNoScale = new Scale("yes_no", "yes", Metric.GOOD, "no", Metric.BAD);
			Scale.gradesScale = new Scale("grades", "1", Metric.GOOD, "2", Metric.GOOD, "3", Metric.NEUTRAL, "4", Metric.NEUTRAL, "5", Metric.BAD, "6", Metric.BAD);
			Scale.oneTwoThreeScale = new Scale("one_two_three", "1", Metric.GOOD, "2", Metric.NEUTRAL, "3", Metric.BAD);
			Scale.npsScale = new Scale("nps", "0", Metric.BAD, "1", Metric.BAD, "2", Metric.BAD, "3", Metric.BAD, "4", Metric.BAD, "5", Metric.BAD, "6", Metric.BAD, "7", Metric.NEUTRAL, "8",
					Metric.GOOD, "9", Metric.GOOD, "10", Metric.GOOD);
			sdc.all(Scale.class).forEach(s -> {
				try {
					sdc.save(s);
				}
				catch (SQLException | SqlDbException e) {
					log.error(" {} exception occured on save scale '{}'", e.getClass().getSimpleName(), s);
				}
			});

			// Create surveys
			for (int i = 0; i < SURVEY_COUNT; i++) {

				Survey survey = sdc.create(Survey.class, s -> {
					s.greeting = "Welcome to our customer satisfaction survey!";
					s.leaveMessagePrompt = "If you want you can leave a message after the beep...";
				});

				survey.name = "Customer Satisfaction Survey " + (i + 1);
				survey.number = i;
				sdc.save(survey);
			}

			// Create questions and assign them to survey
			try (SqlConnection sqlcn = SqlConnection.open(sdc.sqlDb.pool, false)) {

				for (QuestionType qt : QuestionType.values()) {
					Question.questionMap.put(qt, Question.createQuestion(sqlcn, qt));
				}

				for (Survey survey : sdc.all(Survey.class)) {
					for (QuestionType qt : Question.questionMap.keySet()) {
						survey.appendQuestion(sqlcn, Question.questionMap.get(qt));
					}
				}
			}
		}
		else {
			// Retrieve base objects from database
			Scale.yesNoScale = sdc.findAny(Scale.class, s -> s.size() == 2);
			Scale.oneTwoThreeScale = sdc.findAny(Scale.class, s -> s.size() == 3);
			Scale.gradesScale = sdc.findAny(Scale.class, s -> s.size() == 6);
			Scale.npsScale = sdc.findAny(Scale.class, s -> s.size() == 11);
		}

		// Check base objects
		List<DomainObject> objects = new ArrayList<>();
		objects.add(Scale.yesNoScale);
		objects.add(Scale.oneTwoThreeScale);
		objects.add(Scale.yesNoScale);
		objects.add(Scale.npsScale);
		for (Question question : sdc.all(Question.class)) {
			objects.add(question);
		}
		for (Survey survey : sdc.all(Survey.class)) {
			objects.add(survey);
		}
		if (objects.contains(null)) {
			log.error("Not all domain objects were initialized!");
			return;
		}

		// Check audio file
		if (!AUDIOFILE.exists()) {
			log.error("'{}' does not exist!", AUDIOFILE);
			return;
		}

		for (Survey survey : sdc.all(Survey.class)) {
			log.info("{}: {}", survey, survey.getQuestions());
		}

		log.info("Start admin threads");

		// Start admin threads
		List<Thread> adminThreads = new ArrayList<>();
		for (int a = 0; a < ADMIN_THREAD_COUNT; a++) {

			Thread adminThread = new Thread(new AdminSurvey());
			adminThread.setName("Admin-" + a);
			adminThreads.add(adminThread);
			adminThread.start();

			Thread.sleep(100);
		}

		log.info("Start call threads");

		// Perform calls
		List<Thread> callThreads = new ArrayList<>();
		for (int i = 0; i < 100; i++) {

			// Threads perform phone calls
			Thread callThread = new Thread(new PhoneCallSurvey());
			callThread.setName("Call-" + i);
			callThreads.add(callThread);
			callThread.start();

			Thread.sleep(300);
		}

		// Wait until calls ended
		for (Thread thread : callThreads) {
			thread.join();
		}

		log.info("Call threads ended");

		// Wait until admin threads ended
		for (Thread thread : adminThreads) {
			thread.interrupt();
			thread.join();
		}

		log.info("Admin threads ended");

		// Load audio data from database and write new audio file
		VoiceMessage voiceMessage = sdc.findAny(VoiceMessage.class, vm -> vm.audioData != null && vm.audioData.length > 0);
		if (voiceMessage != null) {
			sdc.reload(voiceMessage);
			CFile.writeBinary(AUDIOFILE_FROM_DATABASE, voiceMessage.audioData);
		}
	}

	public static class AdminSurvey implements Runnable {

		@Override
		public void run() {

			log.info("Admin started");

			for (int i = 0; i < 1000; i++) {

				// Manipulate survey randomly
				try (SqlConnection sqlcn = SqlConnection.open(sdc.sqlDb.pool, false)) {

					try {
						Set<Survey> surveys = sdc.allocateExclusively(Survey.class, Survey.InProgress.class, "SEMAPHORE=GREEN", 1, s -> s.semaphore = Semaphore.RED);
						if (!surveys.isEmpty()) {

							Survey survey = surveys.iterator().next();

							int questionCount = survey.getQuestionCount();

							log.debug("{}", survey.getQuestions());

							if (questionCount <= 1) {
								survey.addQuestionRandomly(sqlcn, questionCount);
							}
							else if (questionCount < QuestionType.values().length) {

								int op = CRandom.randomInt(3);
								if (op == 0) {
									survey.removeQuestionRandomly(sqlcn, questionCount);
								}
								else if (op == 1) {
									survey.moveQuestionRandomly(sqlcn, questionCount);
								}
								else {
									survey.addQuestionRandomly(sqlcn, questionCount);
								}
							}
							else {
								survey.removeQuestionRandomly(sqlcn, questionCount);
							}

							survey.semaphore = Semaphore.GREEN;
							sdc.save(survey);
						}
					}
					catch (Exception e) {
						log.error("Exception occurred: ", e);
						sqlcn.cn.rollback();
					}
				}
				catch (SQLException e) {
					log.error("Exception occurred: ", e);
				}

				try {
					Thread.sleep(1000);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					break;
				}
			}

			log.info("Admin ended");
		}
	}
}
