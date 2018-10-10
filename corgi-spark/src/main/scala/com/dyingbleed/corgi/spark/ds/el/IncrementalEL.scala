package com.dyingbleed.corgi.spark.ds.el

import com.dyingbleed.corgi.spark.core.{Conf, Rpc}
import com.dyingbleed.corgi.spark.ds.el.split.SplitManager
import com.dyingbleed.corgi.spark.ds.DataSourceEL
import com.dyingbleed.corgi.spark.util.DataSourceUtils
import com.google.inject.Inject
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/6/26.
  */
private[spark] abstract class IncrementalEL extends DataSourceEL with Logging {

  @Inject
  var spark: SparkSession = _

  @Inject
  var conf: Conf = _

  @Inject
  var rpc: Rpc = _

  protected val executeTime = LocalDateTime.now()

  /**
    * 加载数据源
    *
    * @return
    **/
  override def loadSourceDF: DataFrame = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
      // 全量
      logInfo(s"加载全量数据 ${conf.sinkDb}.${conf.sinkTable}")
      val splitManager = SplitManager.create(
        spark,
        conf.sourceDbUrl,
        conf.sourceDbUser,
        conf.sourceDbPassword,
        conf.sourceDb,
        conf.sourceTable,
        conf.sourceTimeColumn,
        executeTime
      )
      if (splitManager.canSplit) {
        logDebug("数据可以分片")
        splitManager.loadDF
      } else {
        logDebug("数据无法分片")
        loadCompleteSourceDF
      }
    } else {
      // 增量
      logInfo(s"加载增量数据 ${conf.sinkDb}.${conf.sinkTable}")
      loadIncrementalSourceDF
    }
  }

  /**
    * 加载全量源数据
    * */
  protected def loadCompleteSourceDF: DataFrame

  /**
    * 加载增量源数据
    * */
  protected def loadIncrementalSourceDF: DataFrame

  /**
    * 持久化数据源
    *
    * @return
    **/
  override def persistSinkDF(df: DataFrame): Unit = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
      df.createOrReplaceTempView("sink")

      // 创建表并插入数据
      spark.sql(
        s"""
           |create table if not exists ${conf.sinkDb}.${conf.sinkTable}
           |using PARQUET
           |partitioned by (ods_date)
           |as select * from sink
            """.stripMargin
      )
    } else {
      // 增加分区
      spark.sql(
        s"""
           |alter table ${conf.sinkDb}.${conf.sinkTable}
           |add if not exists partition (ods_date='${executeTime.toString("yyyy-MM-dd")}')
          """.stripMargin)

      DataSourceUtils.forceInsertOverwriteTablePartition(df, conf.sinkDb, conf.sinkTable, executeTime.toLocalDate)
    }

    rpc.saveExecuteTime(executeTime) // 保存执行时间戳
  }
}
