package com.lianzhong.utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.JavaConversions._
import scala.io.Source
import java.util.Map

object JsonUtils {

  def main(args: Array[String]) = {
    val map: Map[String,String] = JSON.parseObject(fromFile2Text("C:\\Users\\Administrator\\Desktop\\工作\\工作文件\\rs.json"),classOf[Map[String,String]])
    map.foreach{m => {
     val array: Seq[Map[String,String]] = JSON.parseArray(m._2,classOf[Map[String,String]])
      array.foreach(d =>
        d.clear()
      )
    }
    }
  }

  def fromFile2Text(path: String): String = {
    Source.fromFile(path).mkString
  }

  /**
    * 将文本转为JSON对象
    * @param text
    * @return JSONObject
    */
    def parseObject(text: String): JSONObject = {
      JSON.parseObject(text)
    }

  def json2Map(text: String): Map[String,JSONObject] = {
    JSON.parseObject(text, classOf[Map[String,JSONObject]])
  }
}