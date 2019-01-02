package com.dyingbleed.corgi.spark.ds

import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.core.{Conf, Constants, ODSMode}
import com.dyingbleed.corgi.spark.util.DataSourceUtils
import com.google.inject.Inject
import com.google.inject.name.Named
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.joda.time.LocalDateTime

/**
  * Created by 李震 on 2018/6/26.
  */
trait DataSourceEL {

  @Inject
  var spark: SparkSession = _

  private[this] var _conf: Conf = _

  def conf: Conf = _conf

  @Inject
  def conf_=(c: Conf): Unit = {
    _conf = c
    tableMeta = Table(conf.sourceDb, conf.sourceTable, conf.sourceDbUrl, conf.sourceDbUser, conf.sourceDbPassword, Option(conf.sourceTimeColumn))
  }

  @Inject
  @Named("executeTime")
  var executeTime: LocalDateTime = _

  protected var tableMeta: Table = _

  /**
    * 加载数据源
    *
    * @return
    * */
  def loadSourceDF: DataFrame

  protected def jdbcDF(sql: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", tableMeta.url)
      .option("dbtable", sql)
      .option("user", tableMeta.username)
      .option("password", tableMeta.password)
      .option("driver", tableMeta.driver)
      .load()
  }

  /**
    * 持久化数据源
    *
    * @return
    * */
  def persistSinkDF(df: DataFrame): Unit = {
    if (!spark.catalog.tableExists(conf.sinkDb, conf.sinkTable)) {
      val dfWithDatePartition = conf.mode match {
        case ODSMode.COMPLETE => {
          df.withColumn(Constants.DATE_PARTITION, lit(executeTime.toString(Constants.DATE_FORMAT)))
        }
        case ODSMode.UPDATE | ODSMode.APPEND => {
          if (conf.ignoreHistory) {
            df.withColumn(Constants.DATE_PARTITION, lit(executeTime.toString(Constants.DATE_FORMAT)))
          } else {
            df.withColumn(Constants.DATE_PARTITION, date_format(col(tableMeta.tsColumnName.get), Constants.DATE_FORMAT))
          }
        }
      }

      DataSourceUtils.createAndInsertHiveTable(dfWithDatePartition, conf.sinkDb, conf.sinkTable, conf.partitionColumns)
    } else {
      val columns = spark.catalog.listColumns(conf.sinkDb, conf.sinkTable)
        .collect()
        .map(c => c.name)
        .filter(cn => !Constants.DATE_PARTITION.equalsIgnoreCase(cn))
        .map(cn => col(cn))
      val dfWithDatePartition = df.select(columns:_*).withColumn(Constants.DATE_PARTITION, lit(executeTime.toString(Constants.DATE_FORMAT)))

      DataSourceUtils.insertHiveTable(dfWithDatePartition, conf.sinkDb, conf.sinkTable, conf.partitionColumns)
    }
  }

}
