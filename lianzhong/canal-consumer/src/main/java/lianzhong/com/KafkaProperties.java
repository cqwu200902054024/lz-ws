package lianzhong.com;

/**
 * kafka参数配置
 */
public class KafkaProperties {
    //blocker连接
    public static final String broker = "slave1:2181,slave2:2181,slave3:2181";
    //topic
    public static final String topic = "canal";
    //group id
    public static final String group_id = "test";
    //反序列化key
    public static final String key_deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    //反序列化value
    public static final String value_deserializer = "org.apache.kafka.common.serialization.StringDeserializer";
}
