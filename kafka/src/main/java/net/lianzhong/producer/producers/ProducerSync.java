package net.lianzhong.producer.producers;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * 生产者：异步发送
 */
public class ProducerSync {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "datamaster1:9092,dataslave1:9092,dataslave2:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String filePath = ProducerSync.class.getClassLoader().getResource("rs.json").getPath();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        while((line = br.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>("HULU-XS1", line);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // 如果发送消息成功，返回了 RecordMetadata
                if(metadata != null) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("message has been sent successfully! ")
                                .append("send to partition ")
                                .append(metadata.partition())
                                .append(", offset = ")
                                .append(metadata.offset());
                        System.out.println(sb.toString());
                    }
                    // 如果消息发送失败，抛出异常
                    if(e != null) {
                        e.printStackTrace();
                    }
                }
            });
            Thread.sleep(100);
        }
        producer.close();
    }
}