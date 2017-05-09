package wdsr.exercise4.receiver;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.logging.log4j.core.net.mom.jms.JmsQueueReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsMyQueueReceiver {

    private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
    private final String jmsBrokerURL = "tcp://localhost:62616";
    private final boolean isJMSSessionTransacted = false;
    private final int jmsAcknowledgementMode = Session.AUTO_ACKNOWLEDGE;
    private MessageConsumer consumer;
    private Session session;
    private Connection connection;
    private Destination destination;
    private long numberOfReceivedMessages;

    public JmsMyQueueReceiver(final String queueName) {
        numberOfReceivedMessages = 0;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(jmsBrokerURL);

        try {
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(isJMSSessionTransacted, jmsAcknowledgementMode);
            destination = session.createQueue(queueName);
        } catch (JMSException x) {
            log.error(x.getMessage());
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }

    public void registerCallback() {
        try {
            consumer = session.createConsumer(destination);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        TextMessage textMessage = (TextMessage) message;
                        numberOfReceivedMessages++;
                        log.info(textMessage.getText());
                    } catch (JMSException x) {
                        log.error(x.getMessage());
                    } catch (Exception x) {
                        log.error(x.getMessage());
                    }
                }
            });
        } catch (JMSException x) {
            log.error(x.getMessage());
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }

    public long shutdown() {
        try {
            log.info("Number of received messages: " + numberOfReceivedMessages);
            consumer.setMessageListener(null);
            consumer.close();
            session.close();
            connection.close();
        } catch (JMSException x) {
            log.error(x.getMessage());
            return -2;
        } catch (Exception x) {
            log.error(x.getMessage());
            return -1;
        }

        return numberOfReceivedMessages;
    }

}
