package order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

/**
 * <pre>
 *  顺序消息生产者
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/12 17:32:00
 */
public class OrderProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("OrderProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 10; j++) {
                Message message = new Message("OrderTopic","TagA", ("order_" + j + "_step_" + i).getBytes());
                SendResult send = producer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        Integer id = (Integer) o;
                        int index = id % list.size();
                        return list.get(index);
                    }
                },j);
                System.out.printf("%s%n",send);
            }
        }
        producer.shutdown();
    }
}
