package com.dyingbleed.corgi.spark.ds

import com.dyingbleed.corgi.spark.core.Conf
import com.google.inject.Inject
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
  var el: DataSourceEL = _

  def loadSourceDF: DataFrame = el.loadSourceDF

  def persistSinkDF(df: DataFrame): Unit = el.persistSinkDF(df)

}
