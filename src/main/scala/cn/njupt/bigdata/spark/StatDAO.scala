package cn.njupt.bigdata.spark

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  *   各个维度统计的DAO操作
  */
object StatDAO {

  /**
    * 批量保存VideoAccessDailyStat到数据库
    */
  def insertVideoAccessDailyStatTopN(list: ListBuffer[VideoAccessDailyStat])={
    var conn:Connection = null
    var psmt:PreparedStatement = null

    try{
      conn = MySQLUtils.getConnection()
      //设置手动提交
      conn.setAutoCommit(false)
      val sql = "insert into video_access_daily_topN(day.cms_id,times) values(?,?,?)"
      psmt = conn.prepareStatement(sql)

      for (ele <- list){
        psmt.setString(1,ele.day)
        psmt.setLong(2,ele.cmsId)
        psmt.setLong(3,ele.times)

        psmt.addBatch()
      }
      //执行批量处理
      psmt.executeBatch()
      //手工提交
      conn.commit()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,psmt)
    }
  }


  /**
    * 批量保存VideoAccessCityStat到数据库
    */
  def insertVideoAccessCityStatTopN(list: ListBuffer[VideoAccessCityStat])={
    var conn:Connection = null
    var psmt:PreparedStatement = null

    try{
      conn = MySQLUtils.getConnection()
      //设置手动提交
      conn.setAutoCommit(false)
      val sql = "insert into video_access_city_topN(day.cms_id,city,times,times_rank) values(?,?,?,?,?)"
      psmt = conn.prepareStatement(sql)

      for (ele <- list){
        psmt.setString(1,ele.day)
        psmt.setLong(2,ele.cmsId)
        psmt.setString(3,ele.city)
        psmt.setLong(4,ele.times)
        psmt.setInt(5,ele.timesRank)

        psmt.addBatch()
      }
      //执行批量处理
      psmt.executeBatch()
      //手工提交
      conn.commit()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,psmt)
    }
  }

  /**
    * 批量保存VideoAccessTrafficStat到数据库
    */
  def insertVideoAccessTrafficStatTopN(list: ListBuffer[VideoAccessTrafficsStat])={
    var conn:Connection = null
    var psmt:PreparedStatement = null

    try{
      conn = MySQLUtils.getConnection()
      //设置手动提交
      conn.setAutoCommit(false)
      val sql = "insert into video_access_traffic_topN(day.cms_id,traffics) values(?,?,?)"
      psmt = conn.prepareStatement(sql)

      for (ele <- list){
        psmt.setString(1,ele.day)
        psmt.setLong(2,ele.cmsId)
        psmt.setLong(3,ele.traffics)

        psmt.addBatch()
      }
      //执行批量处理
      psmt.executeBatch()
      //手工提交
      conn.commit()

    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn,psmt)
    }
  }


  /**
    *   删除指定日期的数据
    */
  def deleteData(day:String)={
    val tables = Array("video_access_daily_topN","video_access_city_topN","video_access_traffic_topN")
    var conn:Connection = null
    var psmt:PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection()
      for (table <- tables){
        val deletaSQL = s" delete from $table where day = ?"
        psmt = conn.prepareStatement(deletaSQL)
        psmt.setString(1,day)
        psmt.executeUpdate()
      }
    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtils.release(conn,psmt)
    }

  }

}
