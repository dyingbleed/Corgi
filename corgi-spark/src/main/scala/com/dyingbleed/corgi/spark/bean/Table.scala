package com.dyingbleed.corgi.spark.bean

import java.sql.Connection

import com.dyingbleed.corgi.spark.util.JDBCUtils

import scala.util.{Failure, Success, Try}

/**
  * Created by 李震 on 2018/11/29.
  */
case class Table (
                   db: String,
                   table: String,
                   url: String,
                   username: String,
                   password: String,
                   ts: Option[String] // 时间戳字段名
                 ) {
  /*
   * 参数非空断言
   * */
  assert(url != null, "数据库 URL 不能为空")
  assert(username != null, "数据库用户名不能为空")
  assert(password != null, "数据库密码不能为空")
  assert(db != null, "数据库名不能为空")
  assert(table != null, "表名不能为空")

  /**
    * 数据库厂商
    * */
  lazy val vendor: String = {
    if (url.startsWith("jdbc:mysql")) {
      "mysql"
    } else if (url.startsWith("jdbc:oracle:thin")) {
      "oracle"
    } else {
      throw new RuntimeException("不支持的数据源")
    }
  }

  private[this] def withConnection[R](url: String, username: String, password: String, query: Connection => R): R = {
    Try(JDBCUtils.getConnection(url, username, password)) match {
      case Success(conn) => {
        val r = query(conn)
        conn.close()
        r
      }
      case Failure(e) => throw e
    }
  }

  /**
    * 主键
    * */
  lazy val pk: Option[Seq[Column]] = withConnection(url, username, password, conn => {
    Option(JDBCUtils.getPrimaryKey(conn, db, table))
  })

  /* *********
   * 统计方法 *
   * *********/
  /**
    * 统计指标
    * */
  def stat[MAX, MIN](columnName: String, maxClz: Class[MAX], minClz: Class[MIN]): ColumnStat[MAX, MIN] = withConnection(url, username, password, conn => {
    JDBCUtils.getColumnStat(conn, db, table, columnName, maxClz, minClz)
  })

  /**
    * 最大值
    * */
  def max[MAX](columnName: String, clz: Class[MAX]): MAX = withConnection(url, username, password, conn => {
    JDBCUtils.getColumnMax(conn, db, table, columnName, clz)
  })

  /**
    * 最小值
    * */
  def min[MIN](columnName: String, clz: Class[MIN]): MIN = withConnection(url, username, password, conn => {
    JDBCUtils.getColumnMin(conn, db, table, columnName, clz)
  })

  /**
    * 基数
    * */
  def cardinality(): Long = withConnection(url, username, password, conn => {
    JDBCUtils.getCardinality(conn, db, table)
  })

}