package com.dyingbleed.corgi.spark.ds.el.split
import com.dyingbleed.corgi.spark.bean.{Column, Table}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * MySQLIncrementalSource 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
private[split] class MySQLSplitManager(spark: SparkSession, tableMeta: Table, executeTime: LocalDateTime) extends AbstractSplitManager(spark, tableMeta)  with Logging {

  /**
    * 获取 DataFrame
    *
    * @param splitBy 分区字段
    * @param upper   值上界
    * @param lower   值下界
    * @param m       并发度
    * @return DataFrame
    **/
  override def getDF(splitBy: Column, upper: Long, lower: Long, m: Long): DataFrame = {
    val sql = if (tableMeta.tsColumnName.isEmpty) {
      s"${tableMeta.db}.${tableMeta.table}"
    } else {
      val selectExp = tableMeta.columns
        .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
        .map(c => c.name)
        .mkString(",")

      s"""
         |(select
         |  *, date(${tableMeta.tsColumnName.get}) as ods_date
         |from (
         |  select
         |    $selectExp,
         |    ifnull(${tableMeta.tsColumnName.get}, timestamp('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}')) as ${tableMeta.tsColumnName.get}
         |  from ${tableMeta.db}.${tableMeta.table}
         |) s
         |where ${tableMeta.tsColumnName.get} < timestamp('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}')
         |) t
          """.stripMargin
    }
    logDebug(s"执行 SQL: $sql")

    spark.read
      .format("jdbc")
      .option("url", tableMeta.url)
      .option("dbtable", sql)
      .option("user", tableMeta.username)
      .option("password", tableMeta.password)
      .option("driver", "com.mysql.jdbc.Driver")
      .option("partitionColumn", splitBy.name)
      .option("upperBound", upper)
      .option("lowerBound", lower)
      .option("numPartitions", m)
      .load()
  }

  /**
    * 获取 DataFrame
    *
    * @param splitBy 分区字段
    * @param m       并发度
    * @return DataFrame
    **/
  override def getDF(splitBy: Seq[Column], m: Long): DataFrame = {
    var unionDF: DataFrame = null

    for (mod <- 0l until m) {
      val hashExpr = if (splitBy.size > 1) {
        splitBy.foldLeft("''")((x, y) => s"CONCAT($x, ${y.name})")
      } else {
        splitBy.last.name
      }

      val sql = if (tableMeta.tsColumnName.isEmpty) {
        s"""
           |(select
           |  *
           |from ${tableMeta.db}.${tableMeta.table}
           |where mod(conv(md5($hashExpr), 16, 10), $m)
           |) t
          """.stripMargin
      } else {
        val selectExp = tableMeta.columns
          .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
          .map(c => c.name)
          .mkString(",")

        s"""
           |(select
           |  *, date(${tableMeta.tsColumnName.get}) as ods_date
           |from (
           |  select
           |    $selectExp,
           |    ifnull(${tableMeta.tsColumnName}, timestamp('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}')) as ${tableMeta.tsColumnName}
           |  from ${tableMeta.db}.${tableMeta.table}
           |) s
           |where ${tableMeta.tsColumnName.get} < timestamp('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}')
           |and mod(conv(md5($hashExpr), 16, 10), $m)
           |) t
          """.stripMargin
      }
      logDebug(s"执行 SQL: $sql")

      val df = spark.read
        .format("jdbc")
        .option("url", tableMeta.url)
        .option("dbtable", sql)
        .option("user", tableMeta.username)
        .option("password", tableMeta.password)
        .option("driver", "com.mysql.jdbc.Driver")
        .load()
      if (unionDF == null) {
        unionDF = df
      } else {
        unionDF = unionDF.union(df)
      }
    }

    unionDF
  }
}
