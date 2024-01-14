package batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * <pre>
 *  批量发送消息
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/14 17:06:24
 */
public class BatchProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("BatchProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        ArrayList<Message> messages = new ArrayList<>();
        messages.add(new Message("simple", "TagA", "BatchProducer0".getBytes(StandardCharsets.UTF_8)));
        messages.add(new Message("simple", "TagA", "BatchProducer1".getBytes(StandardCharsets.UTF_8)));
        messages.add(new Message("simple", "TagA", "BatchProducer2".getBytes(StandardCharsets.UTF_8)));
        SendResult send = producer.send(messages);
        System.out.printf(".发送消息成功：%s%n", send);
        producer.shutdown();
    }
}
