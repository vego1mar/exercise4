package wdsr.exercise4.receiver;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageFormatException;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.PriceAlert;
import wdsr.exercise4.VolumeAlert;

/**
 * Complete this class so that it consumes messages from the given queue and invokes the
 * registered callback when an alert is received.
 * Assume the ActiveMQ broker is running on tcp://localhost:62616
 */
public class JmsQueueReceiver {
    private static final Logger log = LoggerFactory.getLogger(JmsQueueReceiver.class);
    private final String jmsBrokerURL = "tcp://localhost:62616";
    private final boolean isJMSSessionTransacted = false;
    private final int jmsAcknowledgementMode = Session.AUTO_ACKNOWLEDGE;
    private MessageConsumer consumer;
    private Session session;
    private Connection connection;
    private Destination destination;

    /**
     * Creates this object
     * 
     * @param queueName Name of the queue to consume messages from.
     */
    public JmsQueueReceiver(final String queueName) {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(jmsBrokerURL);
        factory.setTrustedPackages(getListOfTrustedPackages());

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

    private List<String> getListOfTrustedPackages() {
        List<String> trustedPackages = new ArrayList<String>();
        trustedPackages.add("wdsr.exercise4");
        trustedPackages.add("java.math");
        return trustedPackages;
    }

    /**
     * Registers the provided callback. The callback will be invoked when a price or volume alert is
     * consumed from the queue.
     * 
     * @param alertService Callback to be registered.
     */
    public void registerCallback(AlertService alertService) {
        try {
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
                                alertVolume(alertService, message);
                                break;
                        }
                    } catch (JMSException x) {
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
            } catch (MessageFormatException x) {
                log.error(x.getMessage());
            } catch (JMSException x) {
                log.error(x.getMessage());
            } catch (Exception x) {
                log.error(x.getMessage());
            }

        } else if (message instanceof TextMessage) {
            try {
                TextMessage textMessage = (TextMessage) message;
                PriceAlert priceAlert = getPriceAlertFromStringMessage(textMessage.getText());
                alertService.processPriceAlert(priceAlert);
            } catch (JMSException x) {
                log.error(x.getMessage());
            } catch (Exception x) {
                log.error(x.getMessage());
            }
        }
    }

    private void alertVolume(AlertService alertService, Message message) {
        if (message instanceof ObjectMessage) {
            try {
                ObjectMessage objectMessage = (ObjectMessage) message;
                VolumeAlert volumeAlert = (VolumeAlert) objectMessage.getObject();
                alertService.processVolumeAlert(volumeAlert);
            } catch (MessageFormatException x) {
                log.error(x.getMessage());
            } catch (JMSException x) {
                log.error(x.getMessage());
            } catch (Exception x) {
                log.error(x.getMessage());
            }

        } else if (message instanceof TextMessage) {
            try {
                TextMessage textMessage = (TextMessage) message;
                VolumeAlert volumeAlert = getVolumeAlertFromStringMessage(textMessage.getText());
                alertService.processVolumeAlert(volumeAlert);
            } catch (JMSException x) {
                log.error(x.getMessage());
            } catch (Exception x) {
                log.error(x.getMessage());
            }
        }
    }

    private PriceAlert getPriceAlertFromStringMessage(String message) {
        PriceAlert priceAlert = null;

        try {
            int indexOfTimestampStart = message.indexOf('=') + 1;
            int indexOfTimestampEnd = message.indexOf('\n');
            String timestampString = message.substring(indexOfTimestampStart, indexOfTimestampEnd);
            String cuttedOffMessage = message.substring(indexOfTimestampEnd + 1, message.length());
            int indexOfStockStart = cuttedOffMessage.indexOf('=') + 1;
            int indexOfStockEnd = cuttedOffMessage.indexOf('\n');
            String stockString = cuttedOffMessage.substring(indexOfStockStart, indexOfStockEnd);
            cuttedOffMessage = cuttedOffMessage.substring(indexOfStockEnd + 1, cuttedOffMessage.length());
            int indexOfPriceStart = cuttedOffMessage.indexOf('=') + 1;
            String priceString = cuttedOffMessage.substring(indexOfPriceStart + 1, cuttedOffMessage.length());
            long timestamp = Long.parseLong(timestampString);
            long price = Long.parseLong(priceString);
            BigDecimal bigPrice = new BigDecimal(price);
            priceAlert = new PriceAlert(timestamp, stockString, bigPrice);
        } catch (NumberFormatException x) {
            log.error(x.getMessage());
        } catch (IndexOutOfBoundsException x) {
            log.error(x.getMessage());
        } catch (Exception x) {
            log.error(x.getMessage());
        }

        return priceAlert;
    }

    private VolumeAlert getVolumeAlertFromStringMessage(String message) {
        VolumeAlert volumeAlert = null;

        try {
            int indexOfTimestampStart = message.indexOf('=') + 1;
            int indexOfTimestampEnd = message.indexOf('\n');
            String timestampString = message.substring(indexOfTimestampStart, indexOfTimestampEnd);
            String cuttedOffMessage = message.substring(indexOfTimestampEnd + 1, message.length());
            int indexOfStockStart = cuttedOffMessage.indexOf('=') + 1;
            int indexOfStockEnd = cuttedOffMessage.indexOf('\n');
            String stockString = cuttedOffMessage.substring(indexOfStockStart, indexOfStockEnd);
            cuttedOffMessage = cuttedOffMessage.substring(indexOfStockEnd + 1, cuttedOffMessage.length());
            int indexOfVolumeStart = cuttedOffMessage.indexOf('=') + 1;
            String volumeString = cuttedOffMessage.substring(indexOfVolumeStart, cuttedOffMessage.length());
            long timestamp = Long.parseLong(timestampString);
            long volume = Long.parseLong(volumeString);
            volumeAlert = new VolumeAlert(timestamp, stockString, volume);
        } catch (NumberFormatException x) {
            log.error(x.getMessage());
        } catch (IndexOutOfBoundsException x) {
            log.error(x.getMessage());
        } catch (Exception x) {
            log.error(x.getMessage());
        }

        return volumeAlert;
    }

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
