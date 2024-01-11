package simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * <pre>
 *  单向发送
 *  1日志收集
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/11 15:32:48
 */
public class OnewayProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，设置 Producer Group 名称
        DefaultMQProducer producer = new DefaultMQProducer("SyncProducer");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();

        // 循环发送两条消息
        for (int i = 0; i < 2; i++) {
            // 创建消息对象，指定 Topic、Tags 和消息体
            Message message = new Message("TestTopic", "Tags", (i + "_SyncProducer").getBytes(StandardCharsets.UTF_8));
            // 发送消息并获取发送结果
            producer.sendOneway(message);
            // 打印消息发送成功的信息
            System.out.printf(i+"_消息发送成功%n");
        }
        // 关闭生产者
        producer.shutdown();
    }
}
