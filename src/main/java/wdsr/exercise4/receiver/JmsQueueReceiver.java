package wdsr.exercise4.receiver;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;

/**
 * TODO Complete this class so that it consumes messages from the given queue and invokes the
 * registered callback when an alert is received.
 * 
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
    private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
    private final String jmsBrokerURL = "tcp://localhost:62616";
    private final String queueName;
    private final boolean isJMSSessionTransacted = false;
    private final int jmsAcknowledgementMode = Session.AUTO_ACKNOWLEDGE;
    private MessageConsumer consumer;
    private Session session;
    private Connection connection;

    /**
     * Creates this object
     * 
     * @param queueName Name of the queue to consume messages from.
     */
    public JmsQueueReceiver(final String queueName) {
        this.queueName = queueName;
    }

    /**
     * Registers the provided callback. The callback will be invoked when a price or volume alert is
     * consumed from the queue.
     * 
     * @param alertService Callback to be registered.
     */
    public void registerCallback(AlertService alertService) {
        try {
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(jmsBrokerURL);
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(isJMSSessionTransacted, jmsAcknowledgementMode);
            Destination destination = session.createQueue(queueName);
            consumer = session.createConsumer(destination);

            consumer.setMessageListener(new MessageListener() {
                @Override
                public void onMessage(Message message) {
                    try {
                        switch (message.getJMSType()) {
                            case "PriceAlert":
                                alertPrice(alertService, message);
                                break;
                            case "VolumeAlert":
                                break;
                        }
                    } catch (JMSException x) {
                        log.error(x.getMessage() + " [registerCallback::onMessage]");
                    }
                }
            });

        } catch (Exception x) {
            log.error(x.getMessage() + " [registerCallback]");
        }
    }

    /**
     * Deregisters all consumers and closes the connection to JMS broker.
     */
    public void shutdown() {
        try {
            consumer.setMessageListener(null);
            consumer.close();
            session.close();
            connection.close();
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }


    private void alertPrice(AlertService alertService, Message message) {
        if (message instanceof ObjectMessage) {
            try {
                ObjectMessage objectMessage = (ObjectMessage) message;
                PriceAlert priceAlert = (PriceAlert) objectMessage.getObject();
                alertService.processPriceAlert(priceAlert);
                log.info("[registerListenerForPriceAlert::onMessage::ObjectMessage]");
            } catch (Exception x) {
                log.error(x.getMessage() + " [registerListenerForPriceAlert::onMessage::ObjectMessage]");
            }

        } else if (message instanceof TextMessage) {
            try {
                TextMessage textMessage = (TextMessage) message;
                PriceAlert priceAlert = (PriceAlert) textMessage.getBody(PriceAlert.class);
                alertService.processPriceAlert(priceAlert);
                log.info("[registerListenerForPriceAlert::onMessage::TextMessage]");
            } catch (Exception x) {
                log.error(x.getMessage() + " [registerListenerForPriceAlert::onMessage::TextMessage]");
            }
        }
    }

    // TODO
    // This object should start consuming messages when registerCallback method is invoked.

    // This object should consume two types of messages:
    // 1. Price alert - identified by header JMSType=PriceAlert - should invoke
    // AlertService::processPriceAlert
    // 2. Volume alert - identified by header JMSType=VolumeAlert - should invoke
    // AlertService::processVolumeAlert
    // Use different message listeners for and a JMS selector

    // Each alert can come as either an ObjectMessage (with payload being an instance of PriceAlert
    // or VolumeAlert class)
    // or as a TextMessage.
    // Text for PriceAlert looks as follows:
    // Timestamp=<long value>
    // Stock=<String value>
    // Price=<long value>
    // Text for VolumeAlert looks as follows:
    // Timestamp=<long value>
    // Stock=<String value>
    // Volume=<long value>

    // When shutdown() method is invoked on this object it should remove the listeners and close
    // open connection to the broker.
}
