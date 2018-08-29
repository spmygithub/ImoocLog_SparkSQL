package cn.njupt.bigdata.spark

import com.ggstar.util.ip.IpHelper
/**
  *   IP解析工具类
  */
object IpUtils {
  def getCity(ip :String) ={
    IpHelper.findRegionByIp(ip)
  }
  def main(args: Array[String]) {
    println(getCity("218.75.35.226"))
  }
}
