package cn.njupt.bigdata.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  *   TopN统计：运行在YARN之上
  */
object TopNCourseJob {
  def main(args: Array[String]): Unit = {
    if(args.length !=2) {
      println("Usage: TopNCourseJob <inputPath> <day>")
      System.exit(1)
    }
    val Array(inputPath, day) = args

    val spark = SparkSession.builder()
      .config("spark.sql.sources.partitionColumnTypeInference.enabled","false").getOrCreate()

    val accessDF = spark.read.format("parqut").load("file///opt/spark/data/finalcleaned")

//    accessDF.printSchema()
//    accessDF.show(false)

    StatDAO.deleteData(day)

    //最受欢迎的TopN课程
    videoAccessDayTopN(spark,accessDF,day)

    //按照地市进行统计TopN课程
    videoAccessCityTopN(spark,accessDF,day)

    //按照流量进行统计TopN课程
    videoAccessTrafficTopN(spark,accessDF,day)

    spark.stop()
  }


  /**
    *   每天统计最受欢迎的TopN课程
    */
  def videoAccessDayTopN(spark:SparkSession, accessDF:DataFrame, day:String)={

    /**
      *   使用DataFrame的方式进行统计
      */
//    import spark.implicits._
//    val videoAccessDayTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
//      .groupBy("day", "cmsId").agg(count("cmsId").as("times")).orderBy($"times".desc)
//
//    videoAccessDayTopNDF.show(false)



    /**
      *   使用SQL的方式进行统计
      */
    accessDF.createOrReplaceTempView("access_logs")
    val videoAccessDayTopNDF = spark.sql("select day,cmsId,count(1) as times from access_logs" +
                                                    "where day = '20170511' and cmsId = 'video'" +
                                                    "group by day, cmsId order by times desc")

    videoAccessDayTopNDF.show(false)

    /**
      *   将统计结果写入到MySQL中
      */
    try{
      videoAccessDayTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[VideoAccessDailyStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          /**
            * 不建议在此处进行数据库的数据插入
            */
          list.append(VideoAccessDailyStat(day,cmsId,times))
        })

        StatDAO.insertVideoAccessDailyStatTopN(list)

      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }


  /**
    *   按省市统计最受欢迎的TopN课程
    */
  def videoAccessCityTopN(spark:SparkSession, accessDF:DataFrame, day:String)={

    /**
      *   使用DataFrame的方式进行统计
      */
        import spark.implicits._
        val videoAccessCityTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
          .groupBy("day","city", "cmsId").agg(count("cmsId").as("times"))

    videoAccessCityTopNDF.show(false)

    //window函数在spark sql中的使用

    val top3DF = videoAccessCityTopNDF.select(
      videoAccessCityTopNDF("day"),
      videoAccessCityTopNDF("city"),
      videoAccessCityTopNDF("cmsId"),
      videoAccessCityTopNDF("times"),
      row_number().over(Window.partitionBy(videoAccessCityTopNDF("city"))
        .orderBy(videoAccessCityTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <=3") //.show(false)  //Top3


//    /**
//      *   使用SQL的方式进行统计
//      */
//    accessDF.createOrReplaceTempView("access_logs")
//    val videoAccessCityTopNDF = spark.sql("select day,cmsId,count(1) as times from access_logs" +
//      "where day = '20170511' and cmsId = 'video'" +
//      "group by day,city, cmsId order by times desc")
//
//    videoAccessDayTopNDF.show(false)

    /**
      *   将统计结果写入到MySQL中
      */
    try{
      videoAccessCityTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[VideoAccessCityStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val times_rank = info.getAs[Int]("times_rank")

          /**
            * 不建议在此处进行数据库的数据插入
            */
          list.append(VideoAccessCityStat(day,cmsId,city,times,times_rank))
        })

        StatDAO.insertVideoAccessCityStatTopN(list)

      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  /**
    *   按流量统计最受欢迎的TopN课程
    */
  def videoAccessTrafficTopN(spark:SparkSession, accessDF:DataFrame, day:String)={

    /**
      *   使用DataFrame的方式进行统计
      */
    import spark.implicits._
    val videoAccessTrafficTopNDF = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy("day", "cmsId").agg(sum("traffic").as("traffics")).orderBy($"traffics".desc)

    //videoAccessTrafficTopNDF.show(false)

    /**
      *   将统计结果写入到MySQL中
      */
    try{
      videoAccessTrafficTopNDF.foreachPartition(partitionOfRecords => {
        val list = new ListBuffer[VideoAccessTrafficsStat]

        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          /**
            * 不建议在此处进行数据库的数据插入
            */
          list.append(VideoAccessTrafficsStat(day,cmsId,traffics))
        })

        StatDAO.insertVideoAccessTrafficStatTopN(list)

      })
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

}
