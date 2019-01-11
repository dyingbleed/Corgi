package com.dyingbleed.corgi.spark.ds.el

import com.dyingbleed.corgi.spark.core.Constants

/**
  * Created by 李震 on 2018/10/10.
  */
private[spark] class OracleIncrementalDataSource extends IncrementalDataSource {

  override protected def incrementalSQL: String = {
    s"""
       |(SELECT
       |  *
       |FROM ${conf.sourceDb}.${conf.sourceTable}
       |WHERE ${conf.sourceTimeColumn} > TO_DATE('${lastExecuteDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
       |AND ${conf.sourceTimeColumn} < TO_DATE('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
       |) t
    """.stripMargin
  }
}
