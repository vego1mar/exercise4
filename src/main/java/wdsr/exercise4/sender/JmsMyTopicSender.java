package wdsr.exercise4.sender;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JmsMyTopicSender {
    private static final Logger log = LoggerFactory.getLogger(JmsMyTopicSender.class);
    private final long TRANCHE_OF_MESSAGES_SIZE = 10_000;
    private final String jmsTopicName;
    private final String jmsBrokerURL = "tcp://localhost:61616";
    private final boolean isJMSSessionTransacted = false;
    private final int jmsAcknowledgmentMode = Session.AUTO_ACKNOWLEDGE;

    public JmsMyTopicSender(final String jmsTopicName) {
        this.jmsTopicName = jmsTopicName;
    }

    public boolean sendTrancheOfMessages() {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jmsBrokerURL);
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(isJMSSessionTransacted, jmsAcknowledgmentMode);
            Destination destination = session.createTopic(jmsTopicName);
            MessageProducer producer = session.createProducer(destination);
            produceTextMessages(producer, DeliveryMode.PERSISTENT, session);
            produceTextMessages(producer, DeliveryMode.NON_PERSISTENT, session);
            session.close();
            connection.close();
        } catch (JMSException x) {
            log.error(x.getMessage());
            return false;
        } catch (Exception x) {
            log.error(x.getMessage());
            return false;
        }

        return true;
    }

    private void produceTextMessages(MessageProducer producer, int deliveryMode, Session session) {
        try {
            producer.setDeliveryMode(deliveryMode);
            long startTime = System.currentTimeMillis();

            for (long i = 1; i <= TRANCHE_OF_MESSAGES_SIZE; i++) {
                String text = "test_" + i;
                TextMessage message = session.createTextMessage(text);
                producer.send(message);
            }

            long endTime = System.currentTimeMillis() - startTime;
            String deliveryModeString = null;

            switch (deliveryMode) {
                case 1:
                    deliveryModeString = "NON-PERSISTENT";
                    break;
                case 2:
                    deliveryModeString = "PERSISTENT";
                    break;
            }

            log.info(TRANCHE_OF_MESSAGES_SIZE + " " + deliveryModeString + " TextMessages sent in " + endTime + " miliseconds.");
        } catch (JMSException x) {
            log.error(x.getMessage());
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }

}
