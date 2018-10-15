package com.dyingbleed.corgi.spark.ds.el.split
import com.dyingbleed.corgi.spark.bean.Column
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.LocalDateTime

/**
  * MySQLIncrementalSource 分区管理器
  *
  * Created by 李震 on 2018/9/27.
  */
private[split] class MySQLSplitManager(
                                        spark: SparkSession,
                                        url: String,
                                        username: String,
                                        password: String,
                                        db: String,
                                        table: String,
                                        timeColumn: String,
                                        executeTime: LocalDateTime
                                      ) extends AbstractSplitManager(spark, url, username, password, db, table) {

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
    val sql = if (timeColumn == null || timeColumn.isEmpty) {
      s"$db.$table"
    } else {
      s"""
         |(select
         |  *, date($timeColumn) as ods_date
         |from $db.$table
         |where $timeColumn < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
         |) t
          """.stripMargin
    }

    spark.read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", sql)
      .option("user", username)
      .option("password", password)
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
      val hashExpr = splitBy.map(_.name).mkString(", ")

      for (mod <- 0l until m) {
        val hashExpr = if (splitBy.size > 1) {
          splitBy.foldLeft("''")((x, y) => s"CONCAT($x, ${y.name})")
        } else {
          splitBy.last.name
        }

        val sql = if (timeColumn == null || timeColumn.isEmpty) {
          s"""
             |(select
             |  *
             |from $db.$table
             |where mod(conv(md5($hashExpr), 16, 10), $m)
             |) t
          """.stripMargin
        } else {
          s"""
             |(select
             |  *, date($timeColumn) as ods_date
             |from $db.$table
             |where $timeColumn < '${executeTime.toString("yyyy-MM-dd HH:mm:ss")}'
             |and mod(conv(md5($hashExpr), 16, 10), $m)
             |) t
          """.stripMargin
        }

        val df = spark.read
          .format("jdbc")
          .option("url", url)
          .option("dbtable", sql)
          .option("user", username)
          .option("password", password)
          .option("driver", "oracle.jdbc.OracleDriver")
          .load()
        if (unionDF == null) {
          unionDF = df
        } else {
          unionDF = unionDF.union(df)
        }
      }

    }

    unionDF
  }
}
