package com.chaohappy.activemq.queue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JmsProduce {

    public static final String ACTIVEMQ_URL = "tcp://localhost:61616";
    private static final String QUEUE_NAME = "queue01";

    public static void main(String[] args) throws JMSException {
        //1、创建连接工厂，按照给定的url地址，采用默认用户名和密码
        ActiveMQConnectionFactory  activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2、通过连接工厂获取connection
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        //3、创建会话session
        //两个参数  transacted 事物, acknowledgeMode 签收
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //4、创建目的地（具体是队列还是主题topic）
        Queue queue = session.createQueue(QUEUE_NAME);

        //5、创建消息的生产者
        MessageProducer messageProducer = session.createProducer(queue);
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
