package com.chaohappy.activemq.topic;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.io.IOException;

/**
 * 先启动订阅再启动生产
 * 持久的发布主题生产者
 * 持久的订阅主题消费者
 */
public class JmsConsumer_Topic_Persistence {
    public static final String ACTIVEMQ_URL = "tcp://localhost:61616";
    private static final String TOPIC_NAME = "topic01";

    public static void main(String[] args) throws JMSException, IOException {

        System.out.println("*****z3");

        //1、创建连接工厂，按照给定的url地址，采用默认用户名和密码
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2、通过连接工厂获取connection
        Connection connection = activeMQConnectionFactory.createConnection();
        //表明张三的用户订阅
        connection.setClientID("z3");
        //3、创建会话session
        //两个参数  transacted 事物, acknowledgeMode 签收
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、创建目的地（具体是队列还是主题topic）
        Topic topic = session.createTopic(TOPIC_NAME);

        //创建持久化的订阅者
        TopicSubscriber topicSubscriber = session.createDurableSubscriber(topic,"remark...");

        connection.start();
        Message message = topicSubscriber.receive();
        while (message!=null){
            TextMessage textMessage = (TextMessage)message;
            System.out.println("*****收到持久化的topic："+textMessage.getText());
            message = topicSubscriber.receive(5000l);
        }
        session.close();
        connection.close();
        System.out.println("*****消息发布到MQ完成");
    }
}
