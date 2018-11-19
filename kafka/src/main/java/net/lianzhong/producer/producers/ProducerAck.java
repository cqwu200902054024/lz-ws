package net.lianzhong.producer.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * 消息发送后不进行确认是否发送成功
 */
public class ProducerAck {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "datamaster1:9092,dataslave1:9092,dataslave2:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String filePath = ProducerAck.class.getClassLoader().getResource("rs.json").getPath();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        while((line = br.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>("HULU-XS1", line);
             // 程序阻塞，直到该条消息发送成功返回元数据信息或者报错
            RecordMetadata metadata = producer.send(record).get();
            StringBuilder sb = new StringBuilder();
            sb.append("record [").append(line).append("] has been sent successfully!")
                    .append("\n")
                    .append("send to partition ")
                    .append(metadata.partition())
                    .append(", offset = ")
                    .append(metadata.offset());
            System.out.println(sb.toString());
            Thread.sleep(100);
        }
        producer.close();
    }
}