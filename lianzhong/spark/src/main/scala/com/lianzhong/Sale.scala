package com.lianzhong

import org.apache.spark.sql.{DataFrame, SparkSession}

object Sale {
  def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName(this.getClass.getSimpleName)
          .master("local[2]")
          .getOrCreate()
         val jdbc_conf: Map[String,String] = Map(
           "url" -> "jdbc:mysql://rr-uf6g9344ho0695b53ko.mysql.rds.aliyuncs.com:3306/lz_sale",
           "driver" -> "com.mysql.jdbc.Driver",
           /*"dbtable" -> "sa_sale_info",*/
           "user" -> "lz_bi",
           "password" -> "OgJ7B5NG0NY0F8TW1URGX8bHw6jT9q"
         )

/*    val jdbc_conf: Map[String,String] = Map(
      "url" -> "jdbc:mysql://rr-uf6g9344ho0695b53ko.mysql.rds.aliyuncs.com:3306/lz_sale",
      "driver" -> "com.mysql.jdbc.Driver",
      /*"dbtable" -> "sa_sale_info",*/
      "user" -> "lz_bi",
      "password" -> "OgJ7B5NG0NY0F8TW1URGX8bHw6jT9q"
    )*/

        val sa_sale_info = spark.read.format("jdbc")
          .options(jdbc_conf)
          .option("dbtable","sa_sale_info")
          .load()
        val sy_position_dept = spark.read.format("jdbc")
            .options(jdbc_conf)
            .option("dbtable","sy_position_dept")
            .load()
      sa_sale_info.createOrReplaceTempView("sa_sale_info")
      sy_position_dept.createOrReplaceTempView("sy_position_dept")
      val sd = spark.sql("SELECT s1.id as id,d1.name,s2.id as top_id,d2.name as top_position_name FROM sa_sale_info s1 LEFT JOIN sa_sale_info s2 ON s1.top_id = s2.id LEFT JOIN sy_position_dept d1 on s1.position = d1.id LEFT JOIN sy_position_dept d2 on s2.position = d2.id")
      sd.foreach{row =>
        val id = row.getLong(0)
        val position = row.getString(1)
        val top_id = row.getLong(2)
        val top_position_name = row.getString(3)
       // val top_id_id = getTopIdById(top_id,sa_sale_info)
       // println("top_id_id" + top_id_id)
        println(id + "------")
        sa_sale_info.show(10)
        println(sa_sale_info("id"))
       val i = sa_sale_info.filter(sa_sale_info("id") === id)
        println(i.count() + "=====")
     /*   var sale_manager_id = ""
        if(position.equals("服务代表")) {
             val sale_manager_id =
          }*/
        println(position)
      }

         spark.stop()
  }

  /**
    * 根据ID获取上一级ID和职位名称
    * @param id
    * @return
    */
  private def getTopById(id: Long): (Long,String) = {

    (1,"")
  }

  /**
    *根据ID获取下一级ID和职位名称
    * @param id
    * @return
    */
  private def getLowById(id: Long): (Long,String) = {
    (1,"")
  }

  /**
    * 根据ID获取上一级ID
    * @param id
    * @return
    */
  private def getTopIdById(id: Long,df: DataFrame): Long = {
    df.filter(df("id")===id).first().getAs[Long]("top_id")
  }
}
