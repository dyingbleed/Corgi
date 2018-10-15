package com.dyingbleed.corgi.spark.ds.el
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2018/10/10.
  */
class OracleIncrementalEL extends IncrementalEL {
  /**
    * 加载全量源数据
    **/
  override protected def loadAllSourceDF: DataFrame = {
    val table =
      s"""
         |(SELECT
         |  *
         |FROM ${conf.sourceDb}.${conf.sourceTable}
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

  /**
    * 加载增量源数据
    **/
  override protected def loadIncrementalSourceDF: DataFrame = {
    val table =
      s"""
         |(SELECT
         |  *
         |FROM ${conf.sourceDb}.${conf.sourceTable}
         |WHERE ${conf.sourceTimeColumn} >= TO_DATE('${rpc.getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')
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
