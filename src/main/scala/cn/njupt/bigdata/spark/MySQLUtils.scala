package cn.njupt.bigdata.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  *  MySQL操作工具类
  */
object MySQLUtils {

  /**
    * 获取数据库连接
    * @return
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://local:3306/imooclog?user=root&password=123456")
  }

  /**
    * 释放数据库连接等资源
    */
  def release(conn:Connection, pstmt:PreparedStatement) = {
    try{
      if(pstmt != null){
        pstmt.close()
      }
    } catch {
      case e:Exception => e.printStackTrace()
    } finally {
      if(conn != null){
        conn.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }

}
