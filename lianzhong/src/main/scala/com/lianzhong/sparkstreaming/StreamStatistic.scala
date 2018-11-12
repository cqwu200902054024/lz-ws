package com.lianzhong.sparkstreaming

import com.lianzhong.sparkstreaming.SparkStreamingKafka.setUpSparkStreamingContext
import com.lianzhong.utils.JsonUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object StreamStatistic {
    def main(args: Array[String]): Unit = {
      val checkpointDir = "/user/data/"
      val kafkaParams = Map[String,String]("metadata.broker.list" -> "wg-bigdata-002:9092,wg-bigdata-003:9092,wg-bigdata-004:9092")
      val topics = Set("dbdata")
      val ssc = StreamingContext.getOrCreate(checkpointDir,setUpSparkStreamingContext(topics,kafkaParams,checkpointDir) _)
      ssc.start()
      ssc.awaitTermination()
    }

  def setUpSparkStreamingContext(topics: Set[String],kafkaParams: Map[String,String],checkpointDir: String)(): StreamingContext = {
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[2]")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    // val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val message = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
    //val jsonData = me
    message.foreachRDD{rdd =>
      rdd.foreachPartition{p=>
        p.foreach{data=>
          val smap = JsonUtils.json2Map(data._2)
          val eventType = smap.get("type")
        }
      }
    }
    // val line = message.map(_._2).map((_,1)).reduceByKey(_ + _)
    //word: key
    //value: 当前值
    //state: 历史值
    val updateFunction = (word: String,value: Option[Int],state: State[Int]) => {
              val sum = state.getOption().getOrElse(0) + value.getOrElse(0)
              val output = (word,sum)
              state.update(sum)
              output
        }

    // val res = line.mapWithState(StateSpec.function(updateFunction))
    /*   res.foreachRDD{rdd =>
         rdd.foreachPartition{data =>
          // val connection = C3p0Utils.getConnection
          // connection.setAutoCommit(false)
           //val sql = "insert into test (name,age) values(?,?)"
           //val preparedStatement = connection.prepareStatement(sql)
           data.foreach{d =>
             println(d._1 + "---->" + d._2)
            // preparedStatement.setString(1,d._1)
            // preparedStatement.setInt(2,d._2)
            // preparedStatement.addBatch
           }
          // preparedStatement.executeBatch
          // connection.commit
         //  C3p0Utils.close(connection,preparedStatement,null)
         }
       }*/
    ssc.checkpoint(checkpointDir)
    ssc
  }
}


