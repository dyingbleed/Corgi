package com.dyingbleed.corgi.spark.ds.el.split

import com.dyingbleed.corgi.spark.bean.Table
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
trait SplitManager {

  /**
    * 是否可以分区
    *
    * 建议在加载 DataFrame 之前调用此方法
    *
    * @return 是否可以分区
    * */
  def canSplit: Boolean

  /**
    * 加载 DataFrame
    *
    * @return DataFrame
    * */
  def loadDF: DataFrame

}

object SplitManager extends {

  def apply(spark: SparkSession, table: Table, executeTime: LocalDateTime): SplitManager = {
    table.vendor match {
      case "mysql" => new MySQLSplitManager(spark, table, executeTime)
      case "oracle" => new OracleSplitManager(spark, table, executeTime)
    }
  }

}
