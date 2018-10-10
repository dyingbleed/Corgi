package com.dyingbleed.corgi.spark.ds

import com.dyingbleed.corgi.spark.annotation.{MySQLIncrementalSource, OracleIncrementalSource}
import com.dyingbleed.corgi.spark.core.{Conf, Rpc}
import com.dyingbleed.corgi.spark.core.ODSMode._
import com.google.inject.Inject
import com.google.inject.name.Named
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by 李震 on 2018/3/2.
  */
private[spark] class DataSource {
  
  @Inject
  var spark: SparkSession = _
  
  @Inject
  var conf: Conf = _

  @Inject
  var rpc: Rpc = _

  @Inject
  @Named("COMPLETE")
  var completeEL: DataSourceEL = _

  @Inject
  @MySQLIncrementalSource
  var mysqlIncrementalEL: DataSourceEL = _

  @Inject
  @OracleIncrementalSource
  var oracleIncrementalEL: DataSourceEL = _

  def loadSourceDF: DataFrame = {
    (conf.mode match {
      case COMPLETE => completeEL
      case UPDATE | APPEND => {
        if (conf.sourceDbUrl.startsWith("jdbc:mysql")) {
          mysqlIncrementalEL
        } else if(conf.sourceDbUrl.startsWith("jdbc:oracle:thin")) {
          oracleIncrementalEL
        } else {
          throw new RuntimeException("不支持的数据源")
        }
      }
    }).loadSourceDF
  }

  def persistSinkDF(df: DataFrame): Unit = {
    (conf.mode match {
      case COMPLETE => completeEL
      case UPDATE | APPEND => {
        if (conf.sourceDbUrl.startsWith("jdbc:mysql")) {
          mysqlIncrementalEL
        } else if(conf.sourceDbUrl.startsWith("jdbc:oracle:thin")) {
          oracleIncrementalEL
        } else {
          throw new RuntimeException("不支持的数据源")
        }
      }
    }).persistSinkDF(df)
  }

}
