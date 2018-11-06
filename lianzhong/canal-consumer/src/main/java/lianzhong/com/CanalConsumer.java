package lianzhong.com;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalConsumer  extends Thread{
    private ConsumerConnector consumer;
    private ConsumerIterator<byte[],byte[]> it;

    @Override
    public void run() {
        byte[] kafkaMessage;
        byte[] kafkaKey;
        // get next message
        System.out.println(it + "==");
        while (it.hasNext()) {
            System.out.println("++++++++++++++++++++");
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
            kafkaMessage = messageAndMetadata.message();
            kafkaKey = messageAndMetadata.key();
            Message message = KafkaSourceUtil.deserialize(kafkaMessage);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (!entries.isEmpty()) {
                for (CanalEntry.Entry entry : entries) {
                    if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                        continue;
                    }
                    CanalEntry.RowChange rowChage = null;
                    String schemaName = entry.getHeader().getSchemaName();
                    String tableName = entry.getHeader().getTableName();
                    try {
                        rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    } catch (Exception e) {
                        throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                                e);
                    }
                    CanalEntry.EventType eventType = rowChage.getEventType();
                    for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                        if (eventType == CanalEntry.EventType.DELETE) {
                            continue;
                        } else if (eventType == CanalEntry.EventType.INSERT) {
                            String jsondata = KafkaSourceUtil.getMetaDataAndData(rowData.getBeforeColumnsList(), schemaName, tableName);
                            System.out.println(jsondata);
                        } else {
                            String jsondata = KafkaSourceUtil.getMetaDataAndData(rowData.getAfterColumnsList(), schemaName, tableName);
                            System.out.println(jsondata);
                        }
                    }
                }
            }
        }
    }
    @Override
    public synchronized void start() {
        System.out.println("=====");
        consumer = KafkaSourceUtil.getConsumer(KafkaSourceUtil.generateDefaultKafkaProps());
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(KafkaSourceConstants.TOPIC, 1);
        try {
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                    consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> topicList = consumerMap.get(KafkaSourceConstants.TOPIC);
            KafkaStream<byte[], byte[]> stream = topicList.get(0);
            it = stream.iterator();
            System.out.println(it + "=====");
        } catch (Exception e) {
        }
        super.start();
    }
    public static void main(String[] args) {
              new CanalConsumer().start();
    }
}
