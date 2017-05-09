package wdsr.exercise4.sender;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JmsMyTopicSenderTest {
    private final String JMS_TOPIC_NAME = "VEGO1MAR.TOPIC";

    @Test
    public void shouldSentTrancheOfTextMessagesToQueue() {
        // given
        JmsMyTopicSender service = new JmsMyTopicSender(JMS_TOPIC_NAME);

        // when
        boolean result = service.sendTrancheOfMessages();

        // then
        assertTrue(result);
    }

}