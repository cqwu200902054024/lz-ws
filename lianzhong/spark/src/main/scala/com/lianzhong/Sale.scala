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
          .load().cache()

        val sy_position_dept = spark.read.format("jdbc")
            .options(jdbc_conf)
            .option("dbtable","sy_position_dept")
            .load()

    //val alldata = sa_sale_info.collectAsList().

    sa_sale_info.foreach{row =>
      val id = row.getAs[Long]("id")
      val top_id = row.getAs[Int]("top_id")
      val position = row.getAs[String]("position")
      val df = sa_sale_info
      println(df + "======")
     // val top_id_id = sa_sale_info.where("id =" + id)
     // println(top_id_id)
    }


    //sa_sale_info.join(sy_position_dept,"")

      /*sa_sale_info.createGlobalTempView("sa_sale_info")
      sy_position_dept.createGlobalTempView("sy_position_dept")

      val sd = spark.sql("SELECT s1.id as id,d1.name,s2.id as top_id,d2.name as top_position_name FROM global_temp.sa_sale_info s1 LEFT JOIN global_temp.sa_sale_info s2 ON s1.top_id = s2.id LEFT JOIN global_temp.sy_position_dept d1 on s1.position = d1.id LEFT JOIN global_temp.sy_position_dept d2 on s2.position = d2.id")
      val topIdRow = spark.newSession().sql("SELECT s.top_id FROM global_temp.sa_sale_info s WHERE s.id = " + 2100001455)
    println("topIdRow=============" + topIdRow)
    sd.foreach{row =>
        val id = row.getLong(0)
        val position_name = row.getString(1)
        //val top_id = row.getLong(2)
        //val top_position_name = row.getString(3)
         //println(id + "--" + position_name + "--" + top_id + "--" + top_position_name)
        if(position_name != null && position_name.contains("服务代表")) {
          println("===================>")
          val topIdRow = spark.newSession().sql("SELECT s.top_id FROM global_temp.sa_sale_info s WHERE s.id = " + id)
          val sale_manager = topIdRow.first().getLong(0)
          println(sale_manager + "====")
          //销售主管
          val sale_manager_id = getTopIdById(id,spark)
          //城市经理
          val city_manager_id = getTopIdById(sale_manager_id,spark)
          //区域经理
          val area_manager_id =  getTopIdById(city_manager_id,spark)
          //大区经理
          val region_manager_id = getTopIdById(area_manager_id,spark)
          //销售总监
          val cso_id = getTopIdById(region_manager_id,spark)
          println(sale_manager_id + "--" + city_manager_id + "--" + area_manager_id + "--" + region_manager_id + "--" + cso_id)
          //销售主管
          //城市经理
          //区域经理
          //大区经理
          //销售总监
            // val sale_manager_id =
          }
       // println(position)
      }
*/
         spark.stop()
  }

  /**
    * 获取职位名称
    * @param id
    * @return
    */
/*  private def getPositionNameById(id: Long): String = {

  }*/

 // private def getTopById(id: Long): ()

/*  /**
    * 获取上一级ID
    * @param id
    * @param spark
    * @return
    */
  private def getTopIdById(id: Long,spark: SparkSession): Long = {
       println(id + "spark" + spark)
       val topIdRow = spark.sql("SELECT s.top_id FROM global_temp.sa_sale_info s WHERE s.id = " + 2100001455)
       println("topIdRow==========================" + topIdRow)
       //topIdRow.first().getLong(0)
    1
  }

  /**
    * 根据ID获取职位名称
    * @param id
    * @return
    */
  private def getTopById(id: Long,spark: SparkSession): String = {
   val positionRow = spark.sql("SELECT s.top_id FROM sa_sale_info s WHERE s.id = " + id)
       positionRow.first().getString(0)
  }

  /**
    *根据ID获取下一级ID和职位名称
    * @param id
    * @return
    */
  private def getLowById(id: Long): (Long,String) = {
    (1,"")
  }*/

  /**
    * 根据ID获取上一级ID
    * @param id
    * @return
    */
/*  private def getTopIdById(id: Long,spark: SparkSession): Long = {
      spark.sql()
  }*/
}
