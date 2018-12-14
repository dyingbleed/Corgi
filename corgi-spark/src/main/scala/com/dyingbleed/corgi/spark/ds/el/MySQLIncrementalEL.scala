package com.dyingbleed.corgi.spark.ds.el
import com.dyingbleed.corgi.spark.bean.Table
import org.apache.spark.sql.DataFrame

/**
  * Created by 李震 on 2018/10/10.
  */
class MySQLIncrementalEL extends IncrementalEL {
  /**
    * 加载全量源数据
    **/
  override protected def loadAllSourceDF(tableMeta: Table): DataFrame = {
    val selectExr = tableMeta.columns
      .filter(c => c.name.equals(tableMeta.tsColumnName.get))
      .map(c => c.name).mkString(",")

    val sql =
      s"""
         |(select
         |  *
         |from (
         |  select
         |    $selectExr,
         |    ifnull(${tableMeta.tsColumnName.get}, '${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}') as ${tableMeta.tsColumnName.get}
         |  from ${tableMeta.db}.${tableMeta.table}
         |) s
         |where ${tableMeta.tsColumnName.get} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |) t
         """.stripMargin
    logDebug(s"执行 SQL：$sql")

    spark.read
      .format("jdbc")
      .option("url", tableMeta.url)
      .option("dbtable", sql)
      .option("user", tableMeta.username)
      .option("password", tableMeta.password)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
  }

  /**
    * 加载增量源数据
    **/
  override protected def loadIncrementalSourceDF(tableMeta: Table): DataFrame = {
    val sql =
      s"""
         |(select
         |  *
         |from ${tableMeta.db}.${tableMeta.table}
         |where ${tableMeta.tsColumnName.get} > '${getLastExecuteTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |and ${tableMeta.tsColumnName.get} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
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
