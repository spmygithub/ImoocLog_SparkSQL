package cn.njupt.bigdata.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 在第一步清洗过的数据上进行进一步的数据清洗解析
  */
object AccessLogFinalCleanJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AccessLogFinalCleanJob")
                .master("local[2]").getOrCreate()

    val accessRDD = spark.sparkContext.textFile("file///opt/spark/data/primarycleaned")

    //RDD ==> DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtils.parse(x)),AccessConvertUtils.struct)

    accessDF.printSchema()
    accessDF.show(false)

    //coalesce(1):控制输出文件数为1，实际生产环境中为调优点之一
    accessDF.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite)
        .partitionBy("day").save("file///opt/spark/data/finalcleaned")

    spark.stop()
  }
}
