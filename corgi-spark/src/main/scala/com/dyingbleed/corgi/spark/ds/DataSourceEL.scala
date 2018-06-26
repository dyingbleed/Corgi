package com.dyingbleed.corgi.spark.ds

import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2018/6/26.
  */
trait DataSourceEL {

  /**
    * 加载数据源
    *
    * @return
    * */
  def loadSourceDF: DataFrame

  /**
    * 持久化数据源
    *
    * @return
    * */
  def persistSinkDF(df: DataFrame): Unit

}
