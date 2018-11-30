package com.dyingbleed.corgi.spark.ds.el.split
import com.dyingbleed.corgi.spark.bean.{Column, Table}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * OracleIncrementalSource 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
private[split] class OracleSplitManager(spark: SparkSession, tableMeta: Table, executeTime: LocalDateTime) extends AbstractSplitManager(spark, tableMeta) with Logging {

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
      s"""
         |(SELECT
         |	t.*
         |FROM ${tableMeta.db}.${tableMeta.table} t
         |) t
          """.stripMargin
    } else {
      val selectExp = tableMeta.columns
        .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
        .map(c => c.name)
        .mkString(",")

      s"""
         |(SELECT
         |	s.*,
         |	TO_CHAR(s.${tableMeta.tsColumnName.get}, 'yyyy-mm-dd') as ods_date
         |FROM (
         |  select
         |    $selectExp,
         |    NVL(${tableMeta.tsColumnName.get}, TO_DATE('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
         |  from ${tableMeta.db}.${tableMeta.table}
         |) s
         |WHERE s.${tableMeta.tsColumnName.get} < TO_DATE('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')
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
      .option("driver", "oracle.jdbc.OracleDriver")
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
           |(SELECT
           |	t.*
           |FROM ${tableMeta.db}.${tableMeta.table} t
           |WHERE MOD(ORA_HASH($hashExpr), $m) = $mod
           |) t
        """.stripMargin
      } else {
        val selectExp = tableMeta.columns
          .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
          .map(c => c.name)
          .mkString(",")

        s"""
           |(SELECT
           |	s.*,
           |	TO_CHAR(s.${tableMeta.tsColumnName.get}, 'yyyy-mm-dd') as ods_date
           |FROM (
           |  select
           |    $selectExp,
           |    NVL(${tableMeta.tsColumnName.get}, TO_DATE('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')) AS ${tableMeta.tsColumnName.get}
           |  from ${tableMeta.db}.${tableMeta.table}
           |) s
           |WHERE s.${tableMeta.tsColumnName.get} < TO_DATE('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}', 'yyyy-mm-dd hh24:mi:ss')
           |AND MOD(ORA_HASH($hashExpr), $m) = $mod
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
        .option("driver", "oracle.jdbc.OracleDriver")
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
