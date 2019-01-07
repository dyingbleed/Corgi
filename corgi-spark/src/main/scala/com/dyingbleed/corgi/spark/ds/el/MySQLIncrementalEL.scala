package com.dyingbleed.corgi.spark.ds.el
import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.core.Constants

/**
  * Created by 李震 on 2018/10/10.
  */
private[spark] class MySQLIncrementalEL extends IncrementalEL {

  override protected def historySQL(tableMeta: Table): String = {
    val selectExr = tableMeta.columns
      .filter(c => c.name.equals(tableMeta.tsColumnName.get))
      .map(c => c.name).mkString(",")

    s"""
       |(SELECT
       |  $selectExr,
       |  IFNULL(${tableMeta.tsColumnName.get}, TIMESTAMP('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}')) AS ${tableMeta.tsColumnName.get}
       |FROM ${tableMeta.db}.${tableMeta.table}
       |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}')
       |) t
       """.stripMargin
  }

  override protected def incrementalSQL(tableMeta: Table): String = {
    s"""
       |(SELECT
       |  *
       |FROM ${tableMeta.db}.${tableMeta.table}
       |WHERE ${tableMeta.tsColumnName.get} > TIMESTAMP('${getLastExecuteTime.toString(Constants.DATETIME_FORMAT)}')
       |AND ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeDateTime.toString(Constants.DATETIME_FORMAT)}')
       |) t
       """.stripMargin
  }
}
