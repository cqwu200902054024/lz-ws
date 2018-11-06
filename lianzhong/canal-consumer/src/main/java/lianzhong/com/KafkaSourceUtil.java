/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package lianzhong.com;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalPacket;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.ByteString;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class KafkaSourceUtil {
  private static final Logger log =
          LoggerFactory.getLogger(KafkaSourceUtil.class);

  public static ConsumerConnector getConsumer(Properties kafkaProps) {
    ConsumerConfig consumerConfig =
            new ConsumerConfig(kafkaProps);
    ConsumerConnector consumer =
            Consumer.createJavaConsumerConnector(consumerConfig);
    return consumer;
  }



  /**
   * Generate consumer properties object with some defaults
   * @return
   */
  public static Properties generateDefaultKafkaProps() {
    Properties props = new Properties();
    props.put(KafkaSourceConstants.AUTO_COMMIT_ENABLED,
            KafkaSourceConstants.DEFAULT_AUTO_COMMIT);
    props.put(KafkaSourceConstants.CONSUMER_TIMEOUT,
            KafkaSourceConstants.DEFAULT_CONSUMER_TIMEOUT);
      props.put("zookeeper.connect", KafkaProperties.broker);
      props.put("group.id", KafkaSourceConstants.TOPIC);
      props.put("zookeeper.session.timeout.ms", "40000");
      props.put("zookeeper.sync.time.ms", "200");
      props.put("auto.commit.interval.ms", "1000");
    return props;
  }

    /**
     * 将字节数据组转为Message格式
     * @param data
     * @return
     */
    public static Message deserialize(byte[] data) {
        try {
            if (data == null) {
                return null;
            } else {
                CanalPacket.Packet p = CanalPacket.Packet.parseFrom(data);
                switch (p.getType()) {
                    case MESSAGES: {
                        if (!p.getCompression().equals(CanalPacket.Compression.NONE)) {
                            throw new CanalClientException("compression is not supported in this connector");
                        }

                        CanalPacket.Messages messages = CanalPacket.Messages.parseFrom(p.getBody());
                        Message result = new Message(messages.getBatchId());
                        for (ByteString byteString : messages.getMessagesList()) {
                            result.addEntry(CanalEntry.Entry.parseFrom(byteString));
                        }
                        return result;
                    }
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

public static String getMetaDataAndData(List<CanalEntry.Column> columns,String schemaName,String tableName) {
    StringBuffer  jsonData =  new StringBuffer();
    jsonData.append("{\"database\":" + "\"" + schemaName + "\"," + "\"table\":" + "\""  + tableName + "\",");
    for(CanalEntry.Column column : columns) {
        //{“database”:”test”,”table”:”e”,”type”:”update”,”ts”:1488857869,”xid”:8924,”commit”:true,”data”:{“id”:1,”m”:5.556666,”torvalds”:null},”old”:{“m”:5.55}}
        jsonData.append("\"" + column.getName() + "\":" + "\"" + column.getValue() + "\"," );
    }
    jsonData.delete(jsonData.length() - 1,jsonData.length());
    jsonData.append("}");
    return jsonData.toString();
    }

}