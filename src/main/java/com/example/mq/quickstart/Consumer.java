package com.example.mq.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @program: rocketmq->Consumer
 * @description:
 * @author: hunyiha
 * @create: 2020-05-19 22:22
 **/
public class Consumer {
    public static void main(String[] args) throws MQClientException {

        //1.创建消费者DefaultMQPushConsumer Consumer，指定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("demo_consumer_group");

        //2.指定Nameserver地址
        consumer.setNamesrvAddr("122.51.83.231:9876");

        //3.设置subscribe，这里就是要读取的主题信息，订阅主题Topic和Tag 第一个参数是那一个主题，第二个参数是过滤规则，所有消息就写*
        consumer.subscribe("TopicTest", "TagA");

        //设定消费模式：负载均衡|广播模式
        //consumer.setMessageModel(MessageModel.BROADCASTING);

        //设置消息最大拉取数
        consumer.setConsumeMessageBatchMaxSize(64);

        //4.设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            //接受消息内容
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.println("consumeThread=" + Thread.currentThread().getName() + "," + new String(msg.getBody(),RemotingHelper.DEFAULT_CHARSET));
                        //可以从msg中获取主题、标签、信息
                        String topic = msg.getTopic();
                        String tags = msg.getTags();


                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();

                        //消息重试
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;

                    }

                }
                //消息消费完成
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }

        });
        //5.启动消费者consumer
        consumer.start();
    }
}
