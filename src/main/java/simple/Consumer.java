package simple;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * <pre>
 *  简单消费者推模式
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/11 14:29:02
 */
// 消费者代码
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 创建消费者实例，设置 Consumer Group 名称
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("SimpleConsumer");
        // 指定 NameServer 地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅 Topic，使用通配符 "*" 表示订阅该 TestTopic 下的所有 Tags
        consumer.subscribe("TestTopic", "*");
        // 设置消息监听器
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 遍历消费消息列表
                for (int i = 0; i < list.size(); i++) {
                    // 打印消息消费成功的信息
                    System.out.println(i + "_消息消费成功.%n"+ new String(list.get(i).getBody()));
                }
                // 返回消费成功状态
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        // 打印消费者已启动信息
        System.out.println("consumer started.%n");
    }
}
