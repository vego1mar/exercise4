package wdsr.exercise4.sender;

import java.math.BigDecimal;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wdsr.exercise4.Order;

public class JmsSender {
    private static final Logger log = LoggerFactory.getLogger(JmsSender.class);

    private final String queueName;
    private final String topicName;
    private final String jmsBrokerURL = "tcp://localhost:62616";
    private final int jmsDeliveryMode = DeliveryMode.NON_PERSISTENT;
    private final boolean isJMSSessionTransacted = false;
    private final int jmsAcknowledgmentMode = Session.AUTO_ACKNOWLEDGE;

    public JmsSender(final String queueName, final String topicName) {
        this.queueName = queueName;
        this.topicName = topicName;
    }

    /**
     * This method creates an Order message with the given parameters and sends it as an
     * ObjectMessage to the queue.
     * 
     * @param orderId ID of the product
     * @param product Name of the product
     * @param price Price of the product
     */
    public void sendOrderToQueue(final int orderId, final String product, final BigDecimal price) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jmsBrokerURL);
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(isJMSSessionTransacted, jmsAcknowledgmentMode);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(jmsDeliveryMode);
            Order order = new Order(orderId, product, price);
            ObjectMessage message = session.createObjectMessage(order);
            producer.send(message);
            session.close();
            connection.close();
            log.info("sendOrderToQueue( " + orderId + ", " + product + ", " + price + " )");
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }

    /**
     * This method sends the given String to the queue as a TextMessage.
     * 
     * @param text String to be sent
     */
    public void sendTextToQueue(String text) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jmsBrokerURL);
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(isJMSSessionTransacted, jmsAcknowledgmentMode);
            Destination destination = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(jmsDeliveryMode);
            TextMessage message = session.createTextMessage(text);
            producer.send(message);
            session.close();
            connection.close();
            log.info("sendTextToQueue( " + text + " )");
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }

    /**
     * Sends key-value pairs from the given map to the topic as a MapMessage.
     * 
     * @param map Map of key-value pairs to be sent.
     */
    public void sendMapToTopic(Map<String, String> map) {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(jmsBrokerURL);
            Connection connection = connectionFactory.createConnection();
            connection.start();
            Session session = connection.createSession(isJMSSessionTransacted, jmsAcknowledgmentMode);
            Destination destination = session.createTopic(topicName);
            MessageProducer producer = session.createProducer(destination);
            producer.setDeliveryMode(jmsDeliveryMode);
            MapMessage message = session.createMapMessage();

            for (Map.Entry<String, String> entry : map.entrySet()) {
                message.setObject(entry.getKey(), entry.getValue());
            }

            producer.send(message);
            session.close();
            connection.close();
            log.info("sendMapToQueue( " + map + " )");
        } catch (Exception x) {
            log.error(x.getMessage());
        }
    }

}
