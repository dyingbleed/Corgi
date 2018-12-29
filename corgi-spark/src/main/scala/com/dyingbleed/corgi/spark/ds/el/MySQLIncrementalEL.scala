package com.dyingbleed.corgi.spark.ds.el
import com.dyingbleed.corgi.spark.bean.Table

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
       |  IFNULL(${tableMeta.tsColumnName.get}, TIMESTAMP('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}')) AS ${tableMeta.tsColumnName.get}
       |FROM ${tableMeta.db}.${tableMeta.table}
       |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}')
       |) t
       """.stripMargin
  }

  override protected def incrementalSQL(tableMeta: Table): String = {
    s"""
       |(SELECT
       |  *
       |FROM ${tableMeta.db}.${tableMeta.table}
       |WHERE ${tableMeta.tsColumnName.get} > TIMESTAMP('${getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}')
       |AND ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}')
       |) t
       """.stripMargin
  }
}
