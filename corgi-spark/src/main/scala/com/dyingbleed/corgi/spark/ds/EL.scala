package com.dyingbleed.corgi.spark.ds

import com.dyingbleed.corgi.spark.annotation.{Complete, Incremental}
import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.core.ODSMode._
import com.dyingbleed.corgi.spark.core.{Conf, Constants}
import com.dyingbleed.corgi.spark.util.DataSourceUtils
import com.google.inject.Inject
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, date_format, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/3/2.
  */
private[spark] class EL extends Logging {

  @Inject
  var conf: Conf = _

  @Inject
  var executeDateTime: LocalDateTime = _

  @Inject
  var tableMeta: Table = _

  @Inject
  var spark: SparkSession = _

  @Inject
  @Complete
  var completeDataSource: DataSource = _

  @Inject
  @Incremental
  var incrementalDataSource: DataSource = _

  def loadSourceDF: DataFrame = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) { // 表不存在
      conf.mode match {
        case COMPLETE => {
          completeDataSource.loadSourceDF
        }
        case UPDATE | APPEND => {
          if (conf.ignoreHistory) {
            incrementalDataSource.loadSourceDF
          } else {
            completeDataSource.loadSourceDF
          }
        }
      }
    } else { // 表已存在
      conf.mode match {
        case COMPLETE => {
          completeDataSource.loadSourceDF
        }
        case UPDATE | APPEND => {
          incrementalDataSource.loadSourceDF
        }
      }
    }
  }

  def persistSinkDF(df: DataFrame): Unit = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) { // 表不存在
      val dfWithDatePartition = conf.mode match {
        case COMPLETE => {
          df.withColumn(Constants.DATE_PARTITION, lit(executeDateTime.toString(Constants.DATE_FORMAT)))
        }
        case UPDATE | APPEND => {
          if (conf.ignoreHistory) {
            df.withColumn(Constants.DATE_PARTITION, lit(executeDateTime.toString(Constants.DATE_FORMAT)))
          } else {
            df.withColumn(Constants.DATE_PARTITION, date_format(col(tableMeta.tsColumnName.get), Constants.DATE_FORMAT))
          }
        }
      }

      DataSourceUtils.createAndInsertHiveTable(dfWithDatePartition, conf.sinkDb, conf.sinkTable, conf.partitionColumns)
    } else { // 表已存在
      val columns = spark.catalog.listColumns(conf.sinkDb, conf.sinkTable)
        .collect()
        .map(c => c.name)
        .filter(cn => !Constants.DATE_PARTITION.equalsIgnoreCase(cn)) // 过滤分区字段
        .map(cn => col(cn))
      val dfFiltered = df.select(columns:_*)

      DataSourceUtils.insertHiveTable(dfFiltered, conf.sinkDb, conf.sinkTable, executeDateTime.toLocalDate, conf.partitionColumns)
    }
  }

}
