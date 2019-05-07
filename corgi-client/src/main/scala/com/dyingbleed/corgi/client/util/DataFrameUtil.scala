package com.dyingbleed.corgi.client.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{column, max}

/**
  * Created by 李震 on 2019-05-05.
  */
object DataFrameUtil {

  def getNewestDF(df: DataFrame, primaryKey: Seq[String], timestamp: String): DataFrame = {
    df.join(df.groupBy(primaryKey.map(column): _*).agg(max(timestamp) as timestamp), primaryKey :+ timestamp)
  }

}
