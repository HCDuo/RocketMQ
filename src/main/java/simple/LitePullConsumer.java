package simple;


import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * <pre>
 *  简单消费者拉模式随机获取
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/11 16:08:11
 */
public class LitePullConsumer {
    public static void main(String[] args) throws MQClientException {
        // 创建轻量级拉取型消费者实例，设置 Consumer Group 名称
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer("LitePullConsumer");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅主题和标签（这里的 "*" 表示订阅所有标签）
        consumer.subscribe("TestTopic", "*");
        // 启动轻量级拉取型消费者
        consumer.start();
        // 消费者主循环
        while (true) {
            // 调用 poll 方法拉取消息
            List<MessageExt> messageExtsList = consumer.poll();
            System.out.println("消息拉取成功");
            // 遍历拉取到的消息列表
            messageExtsList.forEach(messageExt -> {
                System.out.println("消息消费成功：" + messageExt);
            });
        }
    }
}

