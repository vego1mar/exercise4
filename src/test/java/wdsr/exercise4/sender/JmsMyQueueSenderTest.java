package wdsr.exercise4.sender;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class JmsMyQueueSenderTest {
    private final String JMS_QUEUE_NAME = "VEGO1MAR.QUEUE";

    @Test
    public void shouldSentTrancheOfTextMessagesToQueue() {
        // given
        JmsMyQueueSender service = new JmsMyQueueSender(JMS_QUEUE_NAME);

        // when
        boolean result = service.sendTrancheOfMessages();

        // then
        assertTrue(result);
    }

}
