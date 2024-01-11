package simple;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *  异步发送
 * </pre>
 *
 * @author <a href="https://github.com/HCDUO">HCDUO</a>
 * @project RocketMQ
 * @date 2024/1/11 15:22:16
 */
public class AsyncProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 创建生产者实例，设置 Producer Group 名称
        DefaultMQProducer producer = new DefaultMQProducer("SyncProducer");
        // 指定 NameServer 地址
        producer.setNamesrvAddr("localhost:9876");
        // 启动生产者
        producer.start();
        // 创建一个倒计数器，用于等待所有消息发送完成
        CountDownLatch latch = new CountDownLatch(100);
        // 循环发送100条消息
        for (int i = 0; i < 100; i++) {
            final int index = i;
            // 创建消息对象，指定 Topic、Tags 和消息体
            Message message = new Message("TestTopic", "Tags", (i + "_SyncProducer").getBytes(StandardCharsets.UTF_8));
            // 发送消息并获取发送结果
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    // 每次成功发送消息，减少倒计数器
                    latch.countDown();
                    System.out.println(index + "_消息发送成功_" + sendResult);
                }
                @Override
                public void onException(Throwable throwable) {
                    // 每次发送消息出现异常，减少倒计数器
                    latch.countDown();
                    System.out.println(index + "_消息发送失败_" + throwable.getStackTrace());
                }
            });
        }
        // 等待倒计数器归零，最多等待5秒
        latch.await(5, TimeUnit.SECONDS);
        // 关闭生产者
        producer.shutdown();
    }
}
