package net.lianzhong.producer.producers;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerMultithread implements Runnable{
    public static void main(String[] args) {
       // new Thread(new ProducerMultithread()).start();
    }

    private KafkaProducer<String, String> producer;
    private ProducerRecord<String, String> record;
    public ProducerMultithread(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        this.producer = producer;
        this.record = record;
    }

    @Override
    public void run() {
        producer.send(record,new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception != null) {
                            System.out.println("exception occurs when sending message: " + exception);
                        } if(metadata != null) {
                            StringBuilder result = new StringBuilder();
                            result.append("message[" + record.value() + "] has been sent successfully! ")
                                    .append("send to partition ")
                                    .append(metadata.partition())
                                    .append(", offset = ")
                                    .append(metadata.offset());
                            System.out.println(result.toString());
                        }
                    }
                });
    }
}