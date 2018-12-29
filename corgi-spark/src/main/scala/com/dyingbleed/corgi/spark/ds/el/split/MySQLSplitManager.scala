package com.dyingbleed.corgi.spark.ds.el.split
import com.dyingbleed.corgi.spark.bean.{Column, Table}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.{Days, LocalDate, LocalDateTime, LocalTime}

/**
  * MySQLSource 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
private[split] class MySQLSplitManager(spark: SparkSession, tableMeta: Table, executeTime: LocalDateTime) extends AbstractSplitManager(spark, tableMeta)  with Logging {

  override def getDF(splitBy: Column, upper: Long, lower: Long, m: Long): DataFrame = {
    val sql = if (tableMeta.tsColumnName.isEmpty) s"${tableMeta.db}.${tableMeta.table}" else {
      val selectExp = tableMeta.columns
        .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
        .map(c => c.name)
        .mkString(",")

      s"""
         |(SELECT
         |  *
         |FROM (
         |  SELECT
         |    $selectExp,
         |    IFNULL(${tableMeta.tsColumnName.get}, TIMESTAMP('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}')) AS ${tableMeta.tsColumnName.get}
         |  FROM ${tableMeta.db}.${tableMeta.table}
         |) s
         |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}')
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
      .option("driver", tableMeta.driver)
      .option("partitionColumn", splitBy.name)
      .option("upperBound", upper)
      .option("lowerBound", lower)
      .option("numPartitions", m)
      .load()
  }

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
           |  *
           |FROM ${tableMeta.db}.${tableMeta.table}
           |WHERE MOD(CONV(MD5($hashExpr), 16, 10), $m)
           |) t
          """.stripMargin
      } else {
        val selectExp = tableMeta.columns
          .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
          .map(c => c.name)
          .mkString(",")

        s"""
           |(SELECT
           |  *
           |FROM (
           |  SELECT
           |    $selectExp,
           |    IFNULL(${tableMeta.tsColumnName}, TIMESTAMP('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}')) AS ${tableMeta.tsColumnName}
           |  FROM ${tableMeta.db}.${tableMeta.table}
           |) s
           |WHERE ${tableMeta.tsColumnName.get} < TIMESTAMP('${executeTime.toString("yyyy-MM-dd HH:mm:ss")}')
           |AND MOD(CONV(MD5($hashExpr), 16, 10), $m)
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
        .option("driver", tableMeta.driver)
        .load()
      if (unionDF == null) {
        unionDF = df
      } else {
        unionDF = unionDF.union(df)
      }
    }

    unionDF
  }

  /**
    * 获取 DataFrame
    * 使用日期分区
    *
    * @param splitBy 分区字段
    * @return DataFrame
    **/
  override protected def getDF(splitBy: Column, beginDate: LocalDate): DataFrame = {
    var unionDF: DataFrame = null

    val m = Days.daysBetween(beginDate, LocalDate.now()).getDays
    for (d <- 0 to m) {
      val selectExp = tableMeta.columns
        .filter(c => !c.name.equals(tableMeta.tsColumnName.get))
        .map(c => c.name)
        .mkString(",")

      val sql = s"""
         |(SELECT
         |  *
         |FROM (
         |  SELECT
         |    $selectExp,
         |    IFNULL(${tableMeta.tsColumnName}, TIMESTAMP('${tableMeta.tsDefaultVal.toString("yyyy-MM-dd HH:mm:ss")}')) AS ${tableMeta.tsColumnName}
         |  FROM ${tableMeta.db}.${tableMeta.table}
         |) s
         |WHERE ${splitBy.name} >= TIMESTAMP('${beginDate.plusDays(d).toDateTime(LocalTime.MIDNIGHT).toString("yyyy-MM-dd HH:mm:ss")}')
         |AND ${splitBy.name} < TIMESTAMP('${beginDate.plusDays(d + 1).toDateTime(LocalTime.MIDNIGHT).toString("yyyy-MM-dd HH:mm:ss")}')
         |) t
          """.stripMargin
      logDebug(s"执行 SQL: $sql")

      val df = spark.read
        .format("jdbc")
        .option("url", tableMeta.url)
        .option("dbtable", sql)
        .option("user", tableMeta.username)
        .option("password", tableMeta.password)
        .option("driver", tableMeta.driver)
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
