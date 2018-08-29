package cn.njupt.bigdata.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * 访问日志转换（输入 ==> 输出）工具类
  */
object AccessConvertUtils {

  //定义的输出字段
  val struct = StructType(
    Array(
      StructField("url",StringType),
      StructField("cmsType",StringType),
      StructField("cmsId",LongType),
      StructField("traffic",LongType),
      StructField("ip",StringType),
      StructField("city",StringType),
      StructField("time",StringType),
      StructField("day",StringType)
    )
  )

  /**
    *   根据输入的每一行信息转换成输出的样式
    *   @param log  输入的每一行记录信息
    */
  def parse(log : String) = {
    try{
      val fields = log.split("\t")
      val url = fields(1)
      val traffic = fields(2).toLong
      val ip = fields(3)

      val domain = "http://www.imooc.com/"
      val cms = url.substring(url.indexOf(domain) + domain.length)
      val cmsTypeId = cms.split("/")
      var cmsType = ""
      var cmsId = 0L
      if(cmsTypeId.length>1){
        cmsType = cmsTypeId(0)
        cmsId = cmsTypeId(1).toLong
      }

      val city = IpUtils.getCity(ip)
      val time = fields(0)
      val day = time.substring(0,10).replaceAll("-","")

      ////这个row里面的字段要和struct中的字段对应上
      Row(url,cmsType,cmsId,traffic,ip,city,time,day)
    } catch {
      case e:Exception => Row(0)
    }
  }
}