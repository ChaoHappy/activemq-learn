package com.chaohappy.activemq.queue;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class JmsConsumer_TX {
    public static final String ACTIVEMQ_URL = "tcp://localhost:61616";
    private static final String QUEUE_NAME = "queue01";

    public static void main(String[] args) throws JMSException {
        //1、创建连接工厂，按照给定的url地址，采用默认用户名和密码
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2、通过连接工厂获取connection
        Connection connection = activeMQConnectionFactory.createConnection();
        connection.start();
        //3、创建会话session
        //两个参数  transacted 事物, acknowledgeMode 签收
        Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        //4、创建目的地（具体是队列还是主题topic）
        Queue queue = session.createQueue(QUEUE_NAME);

        //5、创建消息的生产者
        MessageConsumer messageConsumer = session.createConsumer(queue);

        while (true){
            TextMessage textMessage = (TextMessage) messageConsumer.receive(4000L);
            if(textMessage!=null){
                System.out.println("******消费者接收到消息："+textMessage.getText());
                textMessage.acknowledge();
            }else{
                break;
            }
        }
        //关闭资源
        messageConsumer.close();
        session.commit();
        session.close();
        connection.close();
        System.out.println("*****消息接收完成");
    }
}
