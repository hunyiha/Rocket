package com.example.mq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.UUID;

/**
 * @program: rocketmq->Producer
 * @description:
 * @author: hunyiha
 * @create: 2020-05-19 21:57
 **/
public class OrderProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {

        // 1.实例化消息生产者DefaultMQProducer  Producer  参数是生产组名
        DefaultMQProducer producer = new DefaultMQProducer("demo_producer_group1");

        // 设置NameServer的地址
        producer.setNamesrvAddr("122.51.83.231:9876");

        // 启动Producer实例
        producer.start();
        for (int i = 0; i < 5000; i++) {
            // 创建消息，并指定Topic，Tag和消息体
            Message msg = new Message("TopicTest" /* Topic */,
                    "TagA" /* Tag */,
                    UUID.randomUUID().toString(),
                    ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 发送消息到一个Broker
            SendResult sendResult = producer.send(msg,   //发送的消息信息
                    new MessageQueueSelector() {   //选定指定的消息队列对象(会将所有的消息队列传递进来，如果是集群环境，消息队列个数等于集群数*每个的队列数)
                        @Override//list是消息队列，message是发送的消息，o是arg
                        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {

                            //获取队列的下标
                            Integer index = (Integer) o;

                            //获取对应下标的队列
                            return list.get(index);
                        }
                    },
                    0); //指定对应的队列下标
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
        }

        // 如果不再发送消息，关闭Producer实例。
        producer.shutdown();
    }
}
