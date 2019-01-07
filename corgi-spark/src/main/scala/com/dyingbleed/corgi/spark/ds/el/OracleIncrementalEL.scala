package com.dyingbleed.corgi.spark.ds.el
import com.dyingbleed.corgi.spark.bean.Table
import com.dyingbleed.corgi.spark.core.Constants

/**
  * Created by 李震 on 2018/10/10.
  */
private[spark] class OracleIncrementalEL extends IncrementalEL {

  override protected def historySQL(tableMeta: Table): String = {
    val selectExp = tableMeta.columns
      .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
      .map(c => c.name).mkString(",")

    s"""
       |(SELECT
       |  $selectExp,
       |  NVL(${tableMeta.tsColumnName.get}, TO_DATE('${tableMeta.tsDefaultVal.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
       |FROM ${tableMeta.db}.${tableMeta.table}
       |WHERE ${tableMeta.tsColumnName.get} < TO_DATE('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
       |) t
       """.stripMargin
  }

  override protected def incrementalSQL(tableMeta: Table): String = {
    s"""
       |(SELECT
       |  *
       |FROM ${conf.sourceDb}.${conf.sourceTable}
       |WHERE ${conf.sourceTimeColumn} > TO_DATE('${getLastExecuteTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
       |AND ${conf.sourceTimeColumn} < TO_DATE('${executeDateTime.toString(Constants.DATETIME_FORMAT)}', 'yyyy-mm-dd hh24:mi:ss')
       |) t
       """.stripMargin
  }
}
