package com.chaohappy.activemq.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 先启动订阅再启动生产
 * 持久的发布主题生产者
 * 持久的订阅主题消费者
 */
public class JmsProduce_Topic_Persistence {
    public static final String ACTIVEMQ_URL = "tcp://localhost:61616";
    private static final String TOPIC_NAME = "topic01";

    public static void main(String[] args) throws JMSException {
        //1、创建连接工厂，按照给定的url地址，采用默认用户名和密码
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2、通过连接工厂获取connection
        Connection connection = activeMQConnectionFactory.createConnection();
        //3、创建会话session
        //两个参数  transacted 事物, acknowledgeMode 签收
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、创建目的地（具体是队列还是主题topic）
        Topic topic = session.createTopic(TOPIC_NAME);

        //5、创建消息的生产者
        MessageProducer messageProducer = session.createProducer(topic);
        //创建持久化的生产者
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        connection.start();
        //6、 通过使用messageProducer 生产3条消息发送到MQ的队列里面
        for (int i = 0; i < 3; i++) {
            //7、创建消息
            TextMessage textMessage = session.createTextMessage("msg----" + i);
            // 通过messageProducer发送给MQ
            messageProducer.send(textMessage);
        }
        //关闭资源
        messageProducer.close();
        session.close();
        connection.close();
        System.out.println("*****消息发布到MQ完成");
    }
}
