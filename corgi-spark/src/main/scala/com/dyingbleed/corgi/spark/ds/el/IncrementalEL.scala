package com.dyingbleed.corgi.spark.ds.el

import java.sql.{Date, Timestamp}

import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.ds.DataSourceEL
import com.dyingbleed.corgi.spark.ds.el.split.SplitManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat

/**
  * Created by 李震 on 2018/6/26.
  */
private[spark] abstract class IncrementalEL extends DataSourceEL with Logging {

  /**
    * 加载数据源
    *
    * @return
    **/
  override def loadSourceDF: DataFrame = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable) && !conf.ignoreHistory) {
      // 全量
      logInfo(s"加载全量数据 ${conf.sinkDb}.${conf.sinkTable}")
      val splitManager = SplitManager(spark, tableMeta, executeTime)
      if (splitManager.canSplit) {
        logDebug("数据可以分片")
        splitManager.loadDF
      } else {
        logDebug("数据无法分片")
        jdbcDF(historySQL(tableMeta))
      }
    } else {
      // 增量
      logInfo(s"加载增量数据 ${conf.sinkDb}.${conf.sinkTable}")
      jdbcDF(incrementalSQL(tableMeta))
    }
  }

  /**
    * 历史全量数据 SQL
    * */
  protected def historySQL(tableMeta: Table): String

  /**
    * 增量数据 SQL
    * */
  protected def incrementalSQL(tableMeta: Table): String

  protected def getLastExecuteTime: LocalDateTime = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
      executeTime.minusDays(1).withTime(0, 0, 0, 0) // 昨天零点零分零秒
    } else {
      val sql =
        s"""
           |SELECT
           |  MAX(t.${conf.sourceTimeColumn}) AS last_execute_time
           |FROM ${conf.sinkDb}.${conf.sinkTable} t
           |WHERE ods_date = '${executeTime.minusDays(1).toString("yyyy-MM-dd")}'
      """.stripMargin
      logDebug(s"执行 SQL: $sql")

      val lastExecuteTime = spark.sql(sql).collect()(0).get(0)
      lastExecuteTime match {
        case exeuteDate: Date =>
          LocalDateTime.fromDateFields(exeuteDate)
        case executeTimestamp: Timestamp =>
          LocalDateTime.fromDateFields(executeTimestamp)
        case executeTimeStr: String =>
          LocalDateTime.parse(executeTimeStr, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"))
        case _ =>
          logError(s"获取最近一次执行时间失败，不支持的时间类型 $lastExecuteTime")
          executeTime.minusDays(1).withTime(0, 0, 0, 0) // 昨天零点零分零秒
      }
    }
  }

}
