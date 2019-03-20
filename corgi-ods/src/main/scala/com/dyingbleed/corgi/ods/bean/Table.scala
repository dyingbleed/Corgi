package com.dyingbleed.corgi.ods.bean

import java.sql.{Connection, Date, Timestamp}

import com.dyingbleed.corgi.core.bean.Column
import com.dyingbleed.corgi.core.constant.DBMSVendor
import com.dyingbleed.corgi.core.constant.DBMSVendor._
import com.dyingbleed.corgi.ods.util.JDBCUtils
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
  lazy val vendor: DBMSVendor = DBMSVendor.fromURL(url)

  /**
    * 驱动类路径
    * */
  lazy val driver: String = vendor.getDriverClassName

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
      case Some(cn) => Option(columns.filter(c => c.getName.equalsIgnoreCase(cn)).last)
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

    val minDateTime = vendor match {
      case MYSQL => {
        minVal match {
          case t: Timestamp => new LocalDateTime(t)
          case d: Date => new LocalDateTime(d)
          case _ => LocalDateTime.now()
        }
      }
      case ORACLE => {
        minVal match {
          case ot: oracle.sql.TIMESTAMP => new LocalDateTime(ot.timestampValue())
          case od: oracle.sql.DATE => new LocalDateTime(od.dateValue())
          case _ => LocalDateTime.now()
        }
      }
    }

    minDateTime.minusDays(1).withTime(0, 0, 0, 0)
  }

  /**
   * 列
   * */
  lazy val columns: Seq[Column] = withConnection(url, username, password, conn => {
    JDBCUtils.getColumns(conn, db, table)
  })

  /**
    * 基数
    * */
  lazy val cardinality: Long = withConnection(url, username, password, conn => {
    JDBCUtils.getCardinality(conn, db, table)
  })

  /* *********
   * 公共方法 *
   * *********/
  /**
    * 统计指标
    * */
  def stats(columnName: String): ColumnStat = withConnection(url, username, password, conn => {
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
    * 字段值集合
    * */
  def distinct(columnName: String): Set[Any] = withConnection(url, username, password, conn => {
    JDBCUtils.getDistinct(conn, db, table, columnName).toSet
  })

  /**
    * 字段值集合
    * */
  def distinct(columnName: String, beginTime: LocalDateTime, endTime: LocalDateTime): Set[Any] = withConnection(url, username, password, conn => {
    assert(tsColumnName.nonEmpty)
    JDBCUtils.getDistinct(conn, db, table, columnName, tsColumnName.get, beginTime, endTime).toSet
  })

  /**
    * 将字段转换为 SQL SELECT 表达式
    * @param columns 字段
    * @return SQL SELECT 表达式
    * */
  def toSelectExpr(columns: Seq[Column]): String = {
    vendor match {
      case MYSQL => columns.map(c => c.getName).mkString(",")
      case ORACLE => {
        columns.map(c => {
          // fix bug #9
          if (c.isDate) s"CAST(${c.getName} AS TIMESTAMP) AS ${c.getName}"
          else c.getName
        }).mkString(",")
      }
    }
  }

}