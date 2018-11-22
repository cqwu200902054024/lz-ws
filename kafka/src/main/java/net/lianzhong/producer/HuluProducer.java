package net.lianzhong.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;
import java.util.Properties;

public class HuluProducer {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "datamaster1:9092");

/*        ZooKeeper zk = new ZooKeeper("192.168.2.243:2181", 10000, null);
        List<String> ids = zk.getChildren("/brokers/ids", false);
        for (String id : ids) {
            String brokerInfo = new String(zk.getData("/brokers/ids/" + id, false, null));
            System.out.println(id + ": " + brokerInfo);
        }*/

       /* props.put("acks", "1");
        props.put("retries", 3);
        props.put("batch.size", 16384); // 16K
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432); // 32M*/
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String filePath = HuluProducer.class.getClassLoader().getResource("rs.json").getPath();
        BufferedReader br = new BufferedReader(new FileReader(filePath));
        String line;
        while((line = br.readLine()) != null) {
            // 创建 ProducerRecord 可以指定 topic、partition、key、value，其中 partition 和 key 是可选的
            // ProducerRecord<String, String> record = new ProducerRecord<>("dev3-yangyunhe-topic001", 0, "key", line);
            // ProducerRecord<String, String> record = new ProducerRecord<>("dev3-yangyunhe-topic001", "key", line);
            System.out.println(line);
            System.out.println("========");
            ProducerRecord<String, String> record = new ProducerRecord<>("HULU-XS1","key1", "test11");
            ProducerRecord<String, String> record2 = new ProducerRecord<>("HULU-XS1","key2", "test22");
            ProducerRecord<String, String> record3 = new ProducerRecord<>("HULU-XS1","key3", "test33");
            ProducerRecord<String, String> record4 = new ProducerRecord<>("HULU-XS1","key4", "test44");
            ProducerRecord<String, String> record5 = new ProducerRecord<>("HULU-XS1","key5", "test55");
            ProducerRecord<String, String> record6 = new ProducerRecord<>("HULU-XS1","key6", "test66");
            producer.send(record);
            producer.send(record2);
            producer.send(record3);
            producer.send(record4);
            producer.send(record5);
            producer.send(record6);
            //只管发送消息，不管是否发送成功
    /*        try {
                producer.send(record);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
            }*/
        }
        producer.close();
    }
}