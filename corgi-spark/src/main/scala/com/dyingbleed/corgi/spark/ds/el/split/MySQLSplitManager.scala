package com.dyingbleed.corgi.spark.ds.el.split
import com.dyingbleed.corgi.spark.bean.{Column, Table}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * MySQLIncrementalSource 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
private[split] class MySQLSplitManager(spark: SparkSession, table: Table, executeTime: LocalDateTime) extends AbstractSplitManager(spark, table) {

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
    val sql = if (table.ts.isEmpty) {
      s"${table.db}.${table.table}"
    } else {
      s"""
         |(select
         |  *, date(${table.ts.get}) as ods_date
         |from ${table.db}.${table.table}
         |where ${table.ts.get} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |) t
          """.stripMargin
    }

    spark.read
      .format("jdbc")
      .option("url", table.url)
      .option("dbtable", sql)
      .option("user", table.username)
      .option("password", table.password)
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

      val sql = if (table.ts.isEmpty) {
        s"""
           |(select
           |  *
           |from ${table.db}.${table.table}
           |where mod(conv(md5($hashExpr), 16, 10), $m)
           |) t
          """.stripMargin
      } else {
        s"""
           |(select
           |  *, date(${table.ts.get}) as ods_date
           |from ${table.db}.${table.table}
           |where ${table.ts.get} < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
           |and mod(conv(md5($hashExpr), 16, 10), $m)
           |) t
          """.stripMargin
      }

      val df = spark.read
        .format("jdbc")
        .option("url", table.url)
        .option("dbtable", sql)
        .option("user", table.username)
        .option("password", table.password)
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
