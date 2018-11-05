package com.lianzhong.sparkstreaming

import com.lianzhong.utils.C3p0Utils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}


object SparkStreamingKafka {

  def main(args: Array[String]) = {
    val checkpointDir = "/user/data/"
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "datamaster1:9092,dataslave1:9092,dataslave2:9092")
    val topics = Set("lztest")
    val ssc = StreamingContext.getOrCreate(checkpointDir,setUpSparkStreamingContext(topics,kafkaParams,checkpointDir) _)
    ssc.start()
    ssc.awaitTermination()
  }

  def setUpSparkStreamingContext(topics: Set[String],kafkaParams: Map[String,String],checkpointDir: String)(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    // val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    val line = message.map(_._2).map((_,1)).reduceByKey(_ + _)
    //word: key
    //value: 当前值
    //state: 历史值
   val updateFunction = (word: String,value: Option[Int],state: State[Int]) => {
          val sum = state.getOption().getOrElse(0) + value.getOrElse(0)
          val output = (word,sum)
          state.update(sum)
          output
    }

    val res = line.mapWithState(StateSpec.function(updateFunction))
    res.foreachRDD{rdd =>
      rdd.foreachPartition{data =>
        val connection = C3p0Utils.getConnection
        connection.setAutoCommit(false)
        val sql = "insert into test (name,age) values(?,?)"
        val preparedStatement = connection.prepareStatement(sql)
        data.foreach{d =>
          println(d._1 + "---->" + d._2)
          preparedStatement.setString(1,d._1)
          preparedStatement.setInt(2,d._2)
          preparedStatement.addBatch
        }
        preparedStatement.executeBatch
        connection.commit
        C3p0Utils.close(connection,preparedStatement,null)
      }
    }
    ssc.checkpoint(checkpointDir)
    ssc
  }
}