package simple;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <pre>
 *  简单消费者指定获取一个queue消息
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/11 16:13:50
 */
public class LitePullConsumerAssign {
    public static void main(String[] args) throws MQClientException {
        // 创建轻量级拉取型消费者实例，设置 Consumer Group 名称
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("LitePullConsumer");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 获取主题 TestTopic 的所有消息队列
        Collection<MessageQueue> messageQueues = consumer.fetchMessageQueues("TestTopic");
        // 将消息队列放入列表中
        ArrayList<MessageQueue> messages = new ArrayList<>(messageQueues);
        // 分配消息队列给消费者
        consumer.assign(messages);
        // 从指定队列的指定偏移量处开始拉取消息
        consumer.seek(messages.get(0), 10);
        System.out.println("Consumer started!");
        // 消费者主循环
        while (true) {
            // 调用 poll 方法拉取消息
            List<MessageExt> messageExtList = consumer.poll();
            System.out.println("消息拉取成功");
            // 遍历拉取到的消息列表
            messageExtList.forEach(messageExt -> {
                System.out.println("消息消费成功：" + messageExt);
            });
        }
    }
}
