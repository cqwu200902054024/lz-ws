package com.lianzhong.sparkstreaming

import com.lianzhong.sparkstreaming.SparkStreamingKafka.setUpSparkStreamingContext
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreamingToMySQL {
     //
    def main(args: Array[String]): Unit = {
      val kafkaParams = Map[String,String]("metadata.broker.list" -> "datamaster1:9092,dataslave1:9092,dataslave2:9092")
      val topics = Set("HULU-XS1")
      val ssc =  setupSsc(topics, kafkaParams)
      ssc.start()
      ssc.awaitTermination();
    }

  /**
    * 创建streamingContext
    * @param topicsSet
    * @param kafkaParams
    * @return
    */
  private def setupSsc(topicsSet: Set[String],kafkaParams: Map[String,String])(): StreamingContext = {
      val sparkConf = new SparkConf().setMaster("local[2]").setAppName(this.getClass.getSimpleName)
      val ssc = new StreamingContext(sparkConf,Seconds(10))
      val message = createCustomDirectKafkaStream(ssc,kafkaParams,"datamaster1","/kafka", topicsSet)
      message.repartition(1).foreachRDD{rdd =>
        rdd.foreachPartition{partiton =>
          partiton.foreach{data =>
            println(data._2)
          }
        }
      }
      ssc
  }
  private def createCustomDirectKafkaStream(ssc: StreamingContext,kafkaParams: Map[String,String],zkHosts: String,zkPath: String,topics: Set[String]): InputDStream[(String,String)] = {
    val topic = topics.last
    val zkClient = new ZkClient(zkHosts,30000,30000)
    val storedOffsets = readOffsets(zkClient,zkHosts,zkPath,topic)
    val kafkaStream = storedOffsets match {
      case None =>
        KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
      case Some(fromOffsets) =>
        val messageHandler = (mmd: MessageAndMetadata[String,String]) => (mmd.key(),mmd.message())
        KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,
          kafkaParams,
          fromOffsets,
          messageHandler)
    }
    kafkaStream.foreachRDD(rdd => saveOffsets(zkClient,zkHosts, zkPath, rdd))
    kafkaStream
  }

  private def readOffsets(zkClient: ZkClient,zkHosts: String,zkPath: String,topic: String): Option[Map[TopicAndPartition,Long]] = {
    println("Reading offsets from Zookeeper")
    val (offsetsRangesStrOpt,_) = ZkUtils.readDataMaybeNull(zkClient,zkPath)
    offsetsRangesStrOpt match {
      case Some(offsetsRangesStr) =>
        val offsets = offsetsRangesStr.split(",")
             .map(s => s.split(":"))
             .map {
               case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong)
             }.toMap
        Some(offsets)
      case None =>
        None
    }
  }

  private def saveOffsets(zkClient: ZkClient,zkHosts:String, zkPath: String, rdd: RDD[_]): Unit = {
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    ZkUtils.updatePersistentPath(zkClient, zkPath, offsetsRangesStr)
  }
}
