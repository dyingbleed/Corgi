package com.dyingbleed.corgi.spark.ds.el
import com.dyingbleed.corgi.spark.bean.Table
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2018/10/10.
  */
class OracleIncrementalEL extends IncrementalEL {
  /**
    * 加载全量源数据
    **/
  override protected def loadAllSourceDF(tableMeta: Table): DataFrame = {
    val selectExp = tableMeta.columns
      .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
      .map(c => c.name).mkString(",")

    val table =
      s"""
         |(SELECT
         |  s.*, TO_CHAR(s.${tableMeta.tsColumnName.get}, 'yyyy-mm-dd') as ods_date
         |FROM (
         |  SELECT
         |    $selectExp,
         |    NVL(${tableMeta.tsColumnName.get}, TO_DATE('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
         |  FROM ${tableMeta.db}.${tableMeta.table}
         |) s
         |WHERE ${conf.sourceTimeColumn} < TO_DATE('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')
         |) t
         """.stripMargin
    logDebug(s"执行 SQL：$table")

    spark.read
      .format("jdbc")
      .option("url", conf.sourceDbUrl)
      .option("dbtable", table)
      .option("user", conf.sourceDbUser)
      .option("password", conf.sourceDbPassword)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load()
  }

  /**
    * 加载增量源数据
    **/
  override protected def loadIncrementalSourceDF(tableMeta: Table): DataFrame = {
    val table =
      s"""
         |(SELECT
         |  *
         |FROM ${conf.sourceDb}.${conf.sourceTable}
         |WHERE ${conf.sourceTimeColumn} > TO_DATE('${getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')
         |AND ${conf.sourceTimeColumn} < TO_DATE('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')
         |) t
         """.stripMargin
    logDebug(s"执行 SQL：$table")

    spark.read
      .format("jdbc")
      .option("url", conf.sourceDbUrl)
      .option("dbtable", table)
      .option("user", conf.sourceDbUser)
      .option("password", conf.sourceDbPassword)
      .option("driver", "oracle.jdbc.OracleDriver")
      .load()
  }
}
