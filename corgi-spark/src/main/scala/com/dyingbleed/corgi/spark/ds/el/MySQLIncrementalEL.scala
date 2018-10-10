package com.dyingbleed.corgi.spark.ds.el
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2018/10/10.
  */
class MySQLIncrementalEL extends IncrementalEL {
  /**
    * 加载全量源数据
    **/
  override protected def loadCompleteSourceDF: DataFrame = {
    val sql =
      s"""
         |(select
         |  *
         |from ${conf.sourceDb}.${conf.sourceTable}
         |and ${conf.sourceTimeColumn} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |) t
         """.stripMargin
    logDebug(s"执行 SQL：$sql")

    spark.read
      .format("jdbc")
      .option("url", conf.sourceDbUrl)
      .option("dbtable", sql)
      .option("user", conf.sourceDbUser)
      .option("password", conf.sourceDbPassword)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
  }

  /**
    * 加载增量源数据
    **/
  override protected def loadIncrementalSourceDF: DataFrame = {
    val sql =
      s"""
         |(select
         |  *
         |from ${conf.sourceDb}.${conf.sourceTable}
         |where ${conf.sourceTimeColumn} >= '${rpc.getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |and ${conf.sourceTimeColumn} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |) t
         """.stripMargin
    logDebug(s"执行 SQL：$sql")

    spark.read
      .format("jdbc")
      .option("url", conf.sourceDbUrl)
      .option("dbtable", sql)
      .option("user", conf.sourceDbUser)
      .option("password", conf.sourceDbPassword)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
  }
}
