package com.icx.dom.app.survey.domain.call;

import java.io.IOException;

import com.icx.dom.app.survey.SurveyApp;
import com.icx.dom.app.survey.domain.config.Question;
import com.icx.dom.app.survey.domain.config.Scale;
import com.icx.dom.app.survey.domain.config.Survey;
import com.icx.dom.app.survey.domain.message.VoiceMessage;
import com.icx.dom.common.CBase;
import com.icx.dom.common.CFile;
import com.icx.dom.common.CRandom;
import com.icx.dom.domain.DomainAnnotations.UseDataHorizon;
import com.icx.dom.domain.DomainController;
import com.icx.dom.domain.sql.SqlDomainController;

@UseDataHorizon
public class PhoneCall extends Call {

	// Members

	public String fromPhone;

	// Methods

	public static class PhoneCallSurvey implements Runnable {

		String answerString = null;

		@Override
		public void run() {

			try {
				// Create call object - call randomly selected survey
				PhoneCall phoneCall = SqlDomainController.createAndSave(PhoneCall.class, c -> {
					int i = CRandom.randomInt((int) SurveyApp.count(Survey.class, s -> true));
					c.survey = DomainController.findAny(Survey.class, s -> s.number == i);
					c.fromPhone = "+" + String.format("%08d", CRandom.randomInt(100000000));
				});

				if (phoneCall == null) {
					log.warn("Phone call could not be saved - end call");
					return;
				}

				log.info("Call for survey '{}' started", phoneCall.survey.name);

				for (Question question : phoneCall.survey.getQuestions()) {

					if (question.answerScale == Scale.yesNoScale) {
						answerString = CRandom.randomInt(2) == 1 ? "yes" : "no";
					}
					else if (question.answerScale == Scale.oneTwoThreeScale) {
						answerString = "" + (CRandom.randomInt(3) + 1);
					}
					else if (question.answerScale == Scale.gradesScale) {
						answerString = "" + (CRandom.randomInt(6) + 1);
					}
					else {
						answerString = "" + (CRandom.randomInt(11));
					}

					SqlDomainController.createAndSave(Answer.class, a -> {
						a.answerText = answerString;
						a.questionText = question.text;
						a.call = phoneCall;
					});

					log.info("{}: {}", question, answerString);

					try {
						Thread.sleep(100);
					}
					catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return;
					}
				}

				if (CRandom.randomInt(10) == 0) {

					SqlDomainController.createAndSave(VoiceMessage.class, v -> {
						v.call = phoneCall;
						v.audioFile = SurveyApp.AUDIOFILE;
						try {
							v.audioData = CFile.readBinary(SurveyApp.AUDIOFILE);
						}
						catch (IOException e) {
							log.error("Audio data could not be read: ", e);
						}
					});

					log.info("Audio message created");
				}

				phoneCall.save();
			}
			catch (Exception ex) {
				log.error(CBase.exceptionStackToString(ex));
			}

			log.info("Call ended");
		}
	}

}
