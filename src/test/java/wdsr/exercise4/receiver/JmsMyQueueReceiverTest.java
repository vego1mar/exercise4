package wdsr.exercise4.receiver;

import org.junit.Test;

public class JmsMyQueueReceiverTest {
    private final String JMS_QUEUE_NAME = "VEGO1MAR.QUEUE";

    @Test
    public void shouldReceivedMessagesFromQueue() throws InterruptedException {
        // given
        JmsMyQueueReceiver receiver = new JmsMyQueueReceiver(JMS_QUEUE_NAME);
        receiver.registerCallback();

        // when
        Thread.currentThread().wait(1_000_000);

        // then
        long numberOfReceivedMessages = receiver.shutdown();

        if (numberOfReceivedMessages <= 0) {
            String argument = Long.toString(numberOfReceivedMessages);
            throw new IllegalArgumentException(argument);
        }

    }

}
