package broadcast;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * <pre>
 *  广播模式和集群模式消费者
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/12 17:54:25
 */
public class BroadcastConsumer {
    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("BroadCastConsumer");
        consumer.setNamesrvAddr("localhost:9876");
        consumer.subscribe("broadcast", "*");
        // 广播模式
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 集群模式
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                list.forEach(n -> {
                    System.out.println("QueueId:" + n.getQueueId() + "收到消息内容 " + new String(n.getBody()));
                });
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Broadcast Consumer Started.%n");
    }
}
