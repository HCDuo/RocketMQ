package simple;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashSet;
import java.util.Set;

/**
 * <pre>
 *  简单消费者拉模式DefaultMQPullConsumerImpl过期
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/11 15:45:33
 */
public class PullConsumer {
    public static void main(String[] args) throws MQClientException {
        // 创建拉取型消费者实例，设置 Consumer Group 名称
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("SimpleConsumer");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 创建一个包含 "TestTopic" 的主题集合
        Set<String> topics = new HashSet<>();
        topics.add("TestTopic");
        // 设置订阅主题集合
        consumer.setRegisterTopics(topics);
        // 启动拉取型消费者
        consumer.start();
        // 消费者主循环
        while (true) {
            consumer.getRegisterTopics().forEach(topic -> {
                try {
                    // 获取指定主题的消息队列
                    Set<MessageQueue> queues = consumer.fetchSubscribeMessageQueues(topic);
                    // 遍历消息队列
                    queues.forEach(queue -> {
                        try {
                            // 从内存中读取消费偏移量
                            long offset = consumer.getOffsetStore().readOffset(queue, ReadOffsetType.READ_FROM_MEMORY);
                            // 如果内存中没有消费偏移量，则从存储中读取
                            if (offset < 0) {
                                offset = consumer.getOffsetStore().readOffset(queue, ReadOffsetType.READ_FROM_STORE);
                            }
                            // 如果存储中也没有消费偏移量，则获取最大偏移量
                            if (offset < 0) {
                                offset = consumer.maxOffset(queue);
                            }
                            // 如果还是小于0，则设置为0
                            if (offset < 0) {
                                offset = 0;
                            }
                            // 拉取消息，每次最多拉取32条
                            PullResult pullResult = consumer.pull(queue, "*", offset, 32);
                            System.out.println("消息循环拉取成功：" + pullResult);
                            // 根据拉取结果进行处理
                            switch (pullResult.getPullStatus()) {
                                case FOUND:
                                    // 遍历找到的消息列表
                                    pullResult.getMsgFoundList().forEach(messageExt -> {
                                        System.out.println("消息发送成功：" + messageExt);
                                    });
                                    // 更新消费偏移量
                                    consumer.updateConsumeOffset(queue, pullResult.getNextBeginOffset());
                                    break;
                                // 其他处理逻辑可以继续添加
                            }
                        } catch (MQBrokerException | RemotingException | InterruptedException | MQClientException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (MQClientException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }
}
