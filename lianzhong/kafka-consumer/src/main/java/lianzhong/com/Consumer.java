package lianzhong.com;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 消费者
 */
public class Consumer extends Thread{
    private String topic;
    KafkaConsumer<String, String> consumer;
    public Consumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaProperties.broker);
        properties.setProperty("group.id",KafkaProperties.group_id);
        properties.put("key.deserializer", KafkaProperties.key_deserializer);
        properties.put("value.deserializer", KafkaProperties.value_deserializer);
        //创建Kafka消费者
        consumer = new KafkaConsumer<>(properties);
        //订阅topic:创建一个只包含单个元素的列表，Topic的名字叫作canal
        consumer.subscribe(Arrays.asList(KafkaProperties.topic));
        //支持正则表达式，订阅所有与test相关的Topic
        //consumer.subscribe("test.*");
    }

    @Override
    public void run() {
        //3.轮询:
        //消息轮询是消费者的核心API，通过一个简单的轮询向服务器请求数据，一旦消费者订阅了Topic，轮询就会处理所欲的细节，包括群组协调、partition再均衡、发送心跳
        //以及获取数据，开发者只要处理从partition返回的数据即可。
        //消费者是一个长期运行的程序，通过持续轮询向Kafka请求数据。在其他线程中调用consumer.wakeup()可以退出循环
        while (true){
            //在100ms内等待Kafka的broker返回数据.超时参数指定poll在多久之后可以返回，不管有没有可用的数据都要返回
            ConsumerRecords<String, String> records = consumer.poll(100);
             for(ConsumerRecord<String, String> record : records) {
                   System.out.println("topic:" + record.topic() + "\n" + "partition:" + record.partition() + "\n"
                    + "offset:" + record.offset() + "\n"
                           + "key:" + record.key() + "\n"
                              + "value" + record.value()
                   );
             }
            consumer.commitSync();
        }
    }

    public static void main(String[] args) {

    }

}
