package cn.njupt.bigdata.spark

import org.apache.spark.sql.SparkSession


/**
  * 第一步清洗数据：抽取出我们想要的指定的列的数据
  */
object AccesslogPrimaryCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AccesslogPrimaryCleanJob").
                master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file///opt/spark/data/10000_access.log")

    //access.take(10).foreach(println)
    accessRDD.map(lines => {
      val fields = lines.split(" ")
      val ip = fields(0)

      //原始日志的第三个和第四个字段拼接起来就是完整的访问时间：
      //[10/Nov/2016:00:01:02 +0800] ==> yyyy-MM-dd HH:mm:ss
      val time = fields(3) + " " + fields(4)
      val traffic = fields(9)
      val url = fields(11).replace("\"","")
      //(ip,time,traffic,url)
      (DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip)
    }).saveAsTextFile("file///opt/spark/data/primarycleaned")

    spark.stop()
  }
}
