package com.lianzhong.utils

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import com.mchange.v2.c3p0.ComboPooledDataSource

object C3p0Utils {
  private val dataSource = new ComboPooledDataSource("mysql")

  /**
    * 从当前连接池中获取链接
    * @return
    */
  def getConnection(): Connection = {
    try {
      dataSource.getConnection
    } catch {
      case e: SQLException   => {
        e.printStackTrace()
        null
      }
    }
  }
  def close(conn: Connection,preparedStatement: PreparedStatement,rs: ResultSet): Unit = {
    if(rs != null) {
      try {
        rs.close
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    if(preparedStatement != null) {
      try {
        preparedStatement.close
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
    if(conn != null) {
      try {
        conn.close()
      } catch {
        case e: SQLException => e.printStackTrace()
      }
    }
  }
}
