package schedule;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.time.LocalTime;

/**
 * <pre>
 *  定时发送生产者
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/14 17:00:47
 */
public class TimeProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("TimeProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 2; i++) {
            Message msg = new Message("Schedule", //主题
                    "TagA", //设置消息Tag，用于消费端根据指定Tag过滤消息。
                    "TimeProducer".getBytes(StandardCharsets.UTF_8) //消息体。
            );
            // 相对时间：延时消息。此消息将在 10 秒后传递给消费者。
            msg.setDelayTimeMs(10000L);
            // 绝对时间：定时消息。设置一个具体的时间，然后在这个时间之后多久在进行发送消息
            // msg.setDeliverTimeMs(System.currentTimeMillis() + 10000L);
            producer.send(msg);
            System.out.printf(i + ".发送消息成功：%s%n", LocalTime.now());
        }
        producer.shutdown();
    }
}
