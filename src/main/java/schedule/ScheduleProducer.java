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
 *  预定日程生产者
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/14 16:53:09
 */
public class ScheduleProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("ScheduleProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 2; i++) {
            Message msg = new Message("Schedule",                     //主题
                    "TagA",                                                 //设置消息Tag，用于消费端根据指定Tag过滤消息。
                    "ScheduleProducer".getBytes(StandardCharsets.UTF_8)     //消息体。
            );
            //1到18分别对应messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
            msg.setDelayTimeLevel(3);
            producer.send(msg);
            System.out.printf(i + ".发送消息成功：%s%n", LocalTime.now());
        }
        producer.shutdown();
    }
}
