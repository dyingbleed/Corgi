package com.dyingbleed.corgi.dm.sink

import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2019/3/12.
  */
trait Sink {

  def sink(df: DataFrame): Unit

}
