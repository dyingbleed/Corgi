package com.dyingbleed.corgi.spark.ds.el

import com.dyingbleed.corgi.spark.core.Constants

/**
  * Created by 李震 on 2018/10/10.
  */
private[spark] class MySQLIncrementalDataSource extends IncrementalDataSource {

  override protected def incrementalSQL: String = {
    s"""
       |(SELECT
       |  *
       |FROM ${tableMeta.db}.${tableMeta.table}
       |WHERE ${tableMeta.tsColumnName.get} > TIMESTAMP('${lastExecuteDateTime.toString(Constants.DATETIME_FORMAT)}')
       |AND ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}')
       |) t
    """.stripMargin
  }
}
