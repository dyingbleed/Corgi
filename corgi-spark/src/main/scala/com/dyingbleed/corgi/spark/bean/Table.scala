package com.dyingbleed.corgi.spark.bean

import java.sql.{Connection, Date, Timestamp}

import com.dyingbleed.corgi.spark.util.JDBCUtils
import oracle.sql.{DATE, Datum, TIMESTAMP}
import org.joda.time.LocalDateTime

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
                   tsColumnName: Option[String] // 时间戳字段名
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
  lazy val pk: Seq[Column] = withConnection(url, username, password, conn => {
    JDBCUtils.getPrimaryKey(conn, db, table)
  })

  /**
    * 时间戳
    * */
  lazy val ts: Option[Column] = {
    tsColumnName match {
      case Some(cn) => Option(columns.filter(c => c.name.equalsIgnoreCase(cn)).last)
      case None => None
    }
  }

  /**
    * 时间戳默认值
    *
    * 最小值前一天的零点零分零秒
    *
    * */
  lazy val tsDefaultVal: LocalDateTime = {
    val minVal = min(tsColumnName.get)
    val minDateTime = minVal match {
      case t: Timestamp => new LocalDateTime(t)
      case d: Date => new LocalDateTime(d)
      case ot: TIMESTAMP => new LocalDateTime(ot.timestampValue())
      case od: DATE => new LocalDateTime(od.dateValue())
      case null => LocalDateTime.now()
      case _ => throw new RuntimeException("不支持的日期时间类型")
    }
    minDateTime.minusDays(1).withTime(0, 0, 0, 0)
  }

  /**
   * 列
   * */
  lazy val columns: Seq[Column] = withConnection(url, username, password, conn => {
    JDBCUtils.getColumns(conn, db, table)
  })

  /* *********
   * 统计方法 *
   * *********/
  /**
    * 统计指标
    * */
  def stat(columnName: String): ColumnStat = withConnection(url, username, password, conn => {
    JDBCUtils.getColumnStat(conn, db, table, columnName)
  })

  /**
    * 最大值
    * */
  def max(columnName: String): Any = withConnection(url, username, password, conn => {
    JDBCUtils.getColumnMax(conn, db, table, columnName)
  })

  /**
    * 最小值
    * */
  def min(columnName: String): Any = withConnection(url, username, password, conn => {
    JDBCUtils.getColumnMin(conn, db, table, columnName)
  })

  /**
    * 基数
    * */
  def cardinality(): Long = withConnection(url, username, password, conn => {
    JDBCUtils.getCardinality(conn, db, table)
  })

}