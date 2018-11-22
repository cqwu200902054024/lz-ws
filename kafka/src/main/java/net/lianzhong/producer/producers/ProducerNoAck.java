package net.lianzhong.producer.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;
/**
 * Kafka生产者: 同步发送
 * 消息发送后进行确认
 */
public class ProducerNoAck {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.42.89:9092,192.168.42.89:9093,192.168.42.89:9094");
        props.put("acks", "1"); props.put("retries", 3);
        props.put("batch.size", 16384); // 16K
        props.put("linger.ms", 1); props.put("buffer.memory", 33554432); // 32M
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String filePath = ProducerNoAck.class.getClassLoader().getResource("wechat_data.txt").getPath();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        while((line = br.readLine()) != null) {
        // 创建 ProducerRecord 可以指定 topic、partition、key、value，其中 partition 和 key 是可选的
        // ProducerRecord<String, String> record = new ProducerRecord<>("dev3-yangyunhe-topic001", 0, "key", line);
        // ProducerRecord<String, String> record = new ProducerRecord<>("dev3-yangyunhe-topic001", "key", line);
            ProducerRecord<String, String> record = new ProducerRecord<>("dev3-yangyunhe-topic001", line);
           // 只管发送消息，不管是否发送成功
            producer.send(record);
            Thread.sleep(100);
        }
        producer.close();
    }
}