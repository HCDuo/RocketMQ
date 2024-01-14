package batch;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 *  分批批量发送消息
 *  注意修改SIZE_LIMIT为 = 10 * 1000，不然发送消息时会提示消息体积过大
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/14 17:07:51
 */
public class SplitBatchProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("SplitBatchProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        ArrayList<Message> messages = new ArrayList<>();
        for(int i = 0;i< 10000; i++){
            messages.add(new Message("simple", "TagA", ("SplitBatchProducer" + i).getBytes(StandardCharsets.UTF_8)));
        }

        ListSplitter splitter = new ListSplitter(messages); while(splitter.hasNext())

        {
            List<Message> listItem = splitter.next();
            SendResult sendResult = producer.send(listItem);
            System.out.printf(".发送消息成功：%s%n", sendResult);
        } producer.shutdown();
    }
}

class ListSplitter implements Iterator<List<Message>> {
    private static final int SIZE_LIMIT = 10 * 1000; // 每个消息批次的最大大小
    private final List<Message> messages; // 待发送的消息列表
    private int currIndex; // 当前拆分到的位置

    public ListSplitter(ArrayList<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currIndex < messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < messages.size(); nextIndex++) {
            Message message = messages.get(nextIndex);
            int tmpSize = message.getTopic().length() + message.getBody().length;
            Map<String, String> properties = message.getProperties();
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                tmpSize += entry.getKey().length() + entry.getValue().length();
            }
            tmpSize = tmpSize + 20; // 如果超过了单个批次所允许的大小，就将此消息之前的消息作为下一个子列表返回
            if (tmpSize > SIZE_LIMIT) { // 如果是第一条消息就超出大小限制，就跳过这条消息再继续扫描
                if (nextIndex - currIndex == 0) {
                    nextIndex++;
                }
                break;
            } // 如果当前子列表大小已经超出所允许的单个批次大小，那么就暂停添加消息
            if (tmpSize + totalSize > SIZE_LIMIT) {
                break;
            } else {
                totalSize += tmpSize;
            }
        } // 返回从currIndex到nextIndex之间的所有消息
        List<Message> subList = messages.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }
}
