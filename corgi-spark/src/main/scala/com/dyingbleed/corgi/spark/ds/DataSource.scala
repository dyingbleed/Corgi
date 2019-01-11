package com.dyingbleed.corgi.spark.ds

import java.sql.{Date, Timestamp}

import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.core.DBMSVendor._
import com.dyingbleed.corgi.spark.core.{Conf, Constants}
import com.google.inject.Inject
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/6/26.
  */
trait DataSource {

  @Inject
  var conf: Conf = _

  @Inject
  var executeDateTime: LocalDateTime = _

  @Inject
  var tableMeta: Table = _

  @Inject
  var spark: SparkSession = _

  /* *********
   * 公共方法 *
   * *********/

  def loadSourceDF: DataFrame

  /* *********
   * 工具方法 *
   * *********/

  protected final def jdbcDF(sql: String): DataFrame = {
    val options = tableMeta.vendor match {
      case MYSQL => {
        Map(
          "url" -> tableMeta.url,
          "dbtable" -> sql,
          "user" -> tableMeta.username,
          "password" -> tableMeta.password,
          "driver" -> tableMeta.driver
        )
      }
      case ORACLE => {
        Map(
          "url" -> tableMeta.url,
          "dbtable" -> sql,
          "user" -> tableMeta.username,
          "password" -> tableMeta.password,
          "driver" -> tableMeta.driver,
          "fetchsize" -> "1000"
        )
      }
    }

    spark.read
      .format("jdbc")
      .options(options)
      .load()
  }

  protected final def jdbcDF(sql: String, partitionColumn: String, upper: Long, lower: Long, numPartitions: Long): DataFrame = {
    val options = tableMeta.vendor match {
      case MYSQL => {
        Map(
          "url" -> tableMeta.url,
          "dbtable" -> sql,
          "user" -> tableMeta.username,
          "password" -> tableMeta.password,
          "driver" -> tableMeta.driver,
          "partitionColumn" -> partitionColumn,
          "upperBound" -> upper.toString,
          "lowerBound" -> lower.toString,
          "numPartitions" -> numPartitions.toString
        )
      }
      case ORACLE => {
        Map(
          "url" -> tableMeta.url,
          "dbtable" -> sql,
          "user" -> tableMeta.username,
          "password" -> tableMeta.password,
          "driver" -> tableMeta.driver,
          "partitionColumn" -> partitionColumn,
          "upperBound" -> upper.toString,
          "lowerBound" -> lower.toString,
          "numPartitions" -> numPartitions.toString,
          "fetchsize" -> "1000"
        )
      }
    }

    spark.read
      .format("jdbc")
      .options(options)
      .load()
  }

  lazy val lastExecuteDateTime: LocalDateTime = {
    if (conf.executeTime.isDefined) {
      executeDateTime.minusDays(1)
    } else {
      if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
        executeDateTime.minusDays(1).withTime(0, 0, 0, 0) // 昨天零点零分零秒
      } else {
        val sql =
          s"""
             |SELECT
             |  MAX(t.${conf.sourceTimeColumn}) AS last_execute_time
             |FROM ${conf.sinkDb}.${conf.sinkTable} t
             |WHERE ${Constants.DATE_PARTITION} = '${executeDateTime.minusDays(1).toString(Constants.DATE_FORMAT)}'
          """.stripMargin

        val lastExecuteTime = spark.sql(sql).collect()(0).get(0)
        lastExecuteTime match {
          case d: Date =>
            LocalDateTime.fromDateFields(d)
          case t: Timestamp =>
            LocalDateTime.fromDateFields(t)
          case _ =>
            executeDateTime.minusDays(1).withTime(0, 0, 0, 0) // 昨天零点零分零秒
        }
      }
    }
  }

  protected final def toSQLExpr(v: Any): String = {
    tableMeta.vendor match {
      case MYSQL => {
        v match {
          case s: Short => s.toString
          case i: Int => i.toString
          case l: Long => l.toString
          case d: Double => d.toString
          case bd: BigDecimal => bd.bigDecimal.doubleValue().toString
          case str: String => s"'$str'"
          case ts: Timestamp =>  s"TIMESTAMP(${new LocalDateTime(ts).toString(Constants.DATETIME_FORMAT)})"
          case d: Date => s"TIMESTAMP(${new LocalDateTime(d).toString(Constants.DATETIME_FORMAT)})"
          case _ => throw new RuntimeException(s"不支持的数据类型")
        }
      }
      case ORACLE => {
        v match {
          case f: oracle.sql.BINARY_FLOAT => f.stringValue()
          case d: oracle.sql.BINARY_DOUBLE => d.stringValue()
          case n: oracle.sql.NUMBER => n.stringValue()
          case str: String => s"'$str'"
          case c: oracle.sql.CHAR => s"'${c.toString}'"
          case ts: oracle.sql.TIMESTAMP =>  s"TIMESTAMP(${new LocalDateTime(ts.timestampValue()).toString(Constants.DATETIME_FORMAT)})"
          case d: oracle.sql.DATE => s"TIMESTAMP(${new LocalDateTime(d.dateValue()).toString(Constants.DATETIME_FORMAT)})"
          case _ => throw new RuntimeException(s"不支持的数据类型")
        }
      }
    }
  }

}
